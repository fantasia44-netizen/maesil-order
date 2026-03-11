"""
bank_excel_service.py -- 은행 거래내역 엑셀 업로드 파싱 서비스.
KB국민은행 기본 지원, 범용 컬럼 자동감지로 다른 은행도 대응.
"""
import hashlib
import logging
import re
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)

# ── 은행별 컬럼 매핑 ──
# 각 은행이 엑셀에서 사용하는 컬럼명 → 표준 필드명 매핑
COLUMN_ALIASES = {
    'transaction_date': [
        '거래일', '거래일자', '거래일시', '일자', '날짜', '처리일자', '처리일',
    ],
    'transaction_time': [
        '거래시간', '시간', '처리시간',
    ],
    'deposit': [
        '입금액', '입금', '수입금액', '입금(원)', '받은금액', '입금금액',
    ],
    'withdrawal': [
        '출금액', '출금', '인출액', '지급금액', '출금(원)', '보낸금액', '출금금액',
    ],
    'balance': [
        '잔액', '거래후잔액', '계좌잔액', '잔고', '거래 후 잔액',
    ],
    'counterpart': [
        '상대방', '입금자', '보내는분', '받는분', '거래상대', '이름',
        '의뢰인', '수취인', '기재내용',
    ],
    'description': [
        '적요', '거래내용', '메모', '비고', '내용', '거래구분', '통장메모',
    ],
    'branch': [
        '거래점', '거래지점', '지점', '취급점',
    ],
}

# 은행 코드 (수동 등록용)
MANUAL_BANK_LIST = {
    '004': 'KB국민은행',
    '088': '신한은행',
    '020': '우리은행',
    '011': 'NH농협은행',
    '003': 'IBK기업은행',
    '081': 'KEB하나은행',
    '071': '우체국',
    '023': 'SC제일은행',
    '027': '한국씨티은행',
    '031': 'DGB대구은행',
    '032': '부산은행',
    '034': '광주은행',
    '035': '제주은행',
    '037': '전북은행',
    '039': '경남은행',
    '045': '새마을금고',
    '048': '신협',
    '090': '카카오뱅크',
    '092': '토스뱅크',
    '089': '케이뱅크',
}


def _find_header_row(df_raw):
    """엑셀에서 실제 데이터 헤더 행을 자동 탐지.
    은행 엑셀은 상단에 은행명, 계좌번호, 기간 등 메타 행이 있을 수 있음.
    '거래일' 또는 '일자' 같은 키워드가 포함된 행을 헤더로 인식.
    """
    date_keywords = {'거래일', '거래일자', '거래일시', '일자', '날짜', '처리일', '처리일자'}
    for idx, row in df_raw.iterrows():
        row_vals = [str(v).strip() for v in row.values if pd.notna(v)]
        for val in row_vals:
            if val in date_keywords:
                return idx
    return 0  # 찾지 못하면 첫 행을 헤더로


def _map_columns(columns):
    """실제 컬럼명을 표준 필드명으로 매핑.
    Returns: dict {표준필드명: 실제컬럼명}
    """
    mapping = {}
    col_list = [str(c).strip() for c in columns]

    for field, aliases in COLUMN_ALIASES.items():
        for alias in aliases:
            for col in col_list:
                if alias == col or alias in col:
                    if field not in mapping:
                        mapping[field] = col
                    break
            if field in mapping:
                break

    return mapping


def _parse_date(val):
    """다양한 날짜 형식을 YYYY-MM-DD로 정규화."""
    if pd.isna(val) or not val:
        return None

    val = str(val).strip()

    # datetime 객체
    if isinstance(val, datetime):
        return val.strftime('%Y-%m-%d')

    # YYYYMMDD
    if re.match(r'^\d{8}$', val):
        return f'{val[:4]}-{val[4:6]}-{val[6:8]}'

    # YYYY-MM-DD (이미 정규화됨)
    if re.match(r'^\d{4}-\d{2}-\d{2}', val):
        return val[:10]

    # YYYY.MM.DD or YYYY/MM/DD
    m = re.match(r'^(\d{4})[./](\d{1,2})[./](\d{1,2})', val)
    if m:
        return f'{m.group(1)}-{m.group(2).zfill(2)}-{m.group(3).zfill(2)}'

    # pandas Timestamp
    try:
        dt = pd.to_datetime(val)
        return dt.strftime('%Y-%m-%d')
    except Exception:
        return None


def _parse_time(val):
    """시간값을 HH:MM:SS로 정규화."""
    if pd.isna(val) or not val:
        return '00:00:00'

    val = str(val).strip()

    # HHMMSS
    if re.match(r'^\d{6}$', val):
        return f'{val[:2]}:{val[2:4]}:{val[4:6]}'

    # HH:MM:SS
    if re.match(r'^\d{2}:\d{2}:\d{2}$', val):
        return val

    # HH:MM
    if re.match(r'^\d{2}:\d{2}$', val):
        return f'{val}:00'

    return '00:00:00'


def _parse_amount(val):
    """금액 문자열을 정수로 변환. 콤마, 공백, 원 제거."""
    if pd.isna(val) or val == '' or val is None:
        return 0
    val = str(val).strip()
    val = val.replace(',', '').replace(' ', '').replace('원', '').replace('₩', '')
    val = val.replace('−', '-').replace('–', '-')  # 유니코드 대시
    if not val or val == '-':
        return 0
    try:
        return abs(int(float(val)))
    except (ValueError, TypeError):
        return 0


def _generate_tx_hash(date, time, amount, balance, counterpart):
    """거래내역 고유 해시 생성 (중복 방지용).
    codef_transaction_id 컬럼에 저장하여 UNIQUE 제약조건 활용.
    """
    raw = f'{date}|{time}|{amount}|{balance}|{counterpart}'
    return f'excel_{hashlib.sha256(raw.encode()).hexdigest()[:16]}'


def parse_bank_excel(file_obj, bank_code='004', filename=''):
    """은행 거래내역 엑셀 파싱.

    Args:
        file_obj: 파일 객체 (Flask request.files)
        bank_code: 은행 코드 (기본 KB국민은행)
        filename: 원본 파일명

    Returns:
        dict: {
            transactions: [parsed rows],
            summary: {total, deposits, withdrawals, skipped},
            errors: [error messages],
            columns_found: {mapped columns},
        }
    """
    errors = []

    # 파일 확장자 확인
    ext = filename.rsplit('.', 1)[-1].lower() if filename else ''

    try:
        if ext == 'csv':
            df_raw = pd.read_csv(file_obj, header=None, dtype=str, encoding='utf-8')
        elif ext in ('xls',):
            df_raw = pd.read_excel(file_obj, header=None, dtype=str, engine='xlrd')
        else:
            df_raw = pd.read_excel(file_obj, header=None, dtype=str, engine='openpyxl')
    except Exception as e:
        logger.error(f'[엑셀파싱] 파일 읽기 실패: {e}')
        return {'transactions': [], 'summary': {}, 'errors': [f'파일 읽기 실패: {e}'],
                'columns_found': {}}

    if df_raw.empty:
        return {'transactions': [], 'summary': {}, 'errors': ['빈 파일입니다.'],
                'columns_found': {}}

    # 헤더 행 탐지
    header_idx = _find_header_row(df_raw)
    logger.info(f'[엑셀파싱] 헤더 행: {header_idx}')

    # 헤더 설정 후 데이터 프레임 재구성
    df = df_raw.iloc[header_idx + 1:].copy()
    df.columns = [str(c).strip() for c in df_raw.iloc[header_idx].values]
    df = df.reset_index(drop=True)

    # 컬럼 매핑
    col_map = _map_columns(df.columns)
    logger.info(f'[엑셀파싱] 컬럼 매핑: {col_map}')

    if 'transaction_date' not in col_map:
        return {'transactions': [], 'summary': {},
                'errors': ['거래일자 컬럼을 찾을 수 없습니다. 엑셀 양식을 확인하세요.'],
                'columns_found': col_map}

    if 'deposit' not in col_map and 'withdrawal' not in col_map:
        return {'transactions': [], 'summary': {},
                'errors': ['입금액/출금액 컬럼을 찾을 수 없습니다.'],
                'columns_found': col_map}

    transactions = []
    deposit_count = 0
    withdrawal_count = 0
    skipped = 0

    for _, row in df.iterrows():
        # 날짜 파싱
        date_val = row.get(col_map.get('transaction_date', ''), '')
        tx_date = _parse_date(date_val)
        if not tx_date:
            skipped += 1
            continue

        # 시간
        time_val = row.get(col_map.get('transaction_time', ''), '')
        tx_time = _parse_time(time_val)

        # 입출금액
        dep_amt = _parse_amount(row.get(col_map.get('deposit', ''), 0))
        wdr_amt = _parse_amount(row.get(col_map.get('withdrawal', ''), 0))

        if dep_amt == 0 and wdr_amt == 0:
            skipped += 1
            continue

        tx_type = '입금' if dep_amt > 0 else '출금'
        amount = dep_amt if dep_amt > 0 else wdr_amt

        # 잔액
        balance = _parse_amount(row.get(col_map.get('balance', ''), 0))

        # 상대방
        counterpart = str(row.get(col_map.get('counterpart', ''), '') or '').strip()
        if pd.isna(counterpart) or counterpart == 'nan':
            counterpart = ''

        # 적요/메모
        description = str(row.get(col_map.get('description', ''), '') or '').strip()
        if pd.isna(description) or description == 'nan':
            description = ''

        # 거래점
        branch = str(row.get(col_map.get('branch', ''), '') or '').strip()
        if pd.isna(branch) or branch == 'nan':
            branch = ''
        if branch and description:
            description = f'{description} [{branch}]'
        elif branch:
            description = branch

        # 상대방이 없으면 적요에서 추출 시도
        if not counterpart and description:
            counterpart = description.split('[')[0].strip()

        # 중복 방지 해시
        tx_hash = _generate_tx_hash(tx_date, tx_time, amount, balance, counterpart)

        tx_record = {
            'transaction_date': tx_date,
            'transaction_time': tx_time,
            'transaction_type': tx_type,
            'amount': amount,
            'balance': balance,
            'counterpart_name': counterpart,
            'description': description,
            'codef_transaction_id': tx_hash,
        }

        transactions.append(tx_record)

        if tx_type == '입금':
            deposit_count += 1
        else:
            withdrawal_count += 1

    summary = {
        'total': len(transactions),
        'deposits': deposit_count,
        'withdrawals': withdrawal_count,
        'skipped': skipped,
        'deposit_total': sum(t['amount'] for t in transactions if t['transaction_type'] == '입금'),
        'withdrawal_total': sum(t['amount'] for t in transactions if t['transaction_type'] == '출금'),
    }

    logger.info(f'[엑셀파싱] 완료: {summary}')
    return {
        'transactions': transactions,
        'summary': summary,
        'errors': errors,
        'columns_found': col_map,
    }


def save_transactions(db, bank_account_id, transactions):
    """파싱된 거래내역을 DB에 저장.

    Args:
        db: DB 인스턴스
        bank_account_id: 은행 계좌 ID
        transactions: parse_bank_excel() 결과의 transactions 리스트

    Returns:
        dict: {new_count, skipped_count}
    """
    new_count = 0
    skipped = 0

    for tx in transactions:
        payload = {
            'bank_account_id': bank_account_id,
            **tx,
        }
        try:
            db.insert_bank_transaction(payload)
            new_count += 1
        except Exception:
            skipped += 1  # UNIQUE 중복

    # 동기화 시각 업데이트
    from services.tz_utils import now_kst
    latest_date = max((t['transaction_date'] for t in transactions), default='')
    update_data = {'last_synced_at': now_kst().isoformat()}
    if latest_date:
        update_data['last_synced_date'] = latest_date
    db.update_bank_account(bank_account_id, update_data)

    logger.info(f'[엑셀저장] 계좌 {bank_account_id}: 신규 {new_count}건, 스킵 {skipped}건')
    return {'new_count': new_count, 'skipped_count': skipped}
