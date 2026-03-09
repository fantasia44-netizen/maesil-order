"""
tax_invoice_service.py -- 세금계산서 비즈니스 로직.
팝빌에서 세금계산서 조회/동기화 + 발행 기능.
"""
import uuid
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════
# Popbill 세금계산서 동기화 (매출/매입 가져오기)
# ══════════════════════════════════════════

def _fmt_date(yyyymmdd):
    """YYYYMMDD → YYYY-MM-DD."""
    if not yyyymmdd or len(str(yyyymmdd)) != 8:
        return str(yyyymmdd) if yyyymmdd else ''
    s = str(yyyymmdd)
    return f'{s[:4]}-{s[4:6]}-{s[6:8]}'


def _map_tax_type(popbill_tax_type):
    """팝빌 과세유형 → DB 값."""
    return {'T': '과세', 'N': '면세', 'Z': '영세'}.get(popbill_tax_type, '과세')


def _map_status(state_code):
    """팝빌 상태코드 → DB status.
    3xx: 발행 / 4xx: 국세청 전송 완료 / 2xx: 임시저장
    """
    if not state_code:
        return 'issued'
    code = int(state_code) if str(state_code).isdigit() else 0
    if code >= 300:
        return 'issued'
    elif code >= 200:
        return 'draft'
    return 'issued'


def _get_val(obj, key, default=''):
    """팝빌 반환 객체에서 값을 안전하게 가져오기 (dict 또는 object)."""
    if isinstance(obj, dict):
        return obj.get(key, default) or default
    return getattr(obj, key, default) or default


def sync_tax_invoices(db, popbill_svc, start_date, end_date, direction='SELL'):
    """팝빌에서 세금계산서 목록을 조회하여 DB에 동기화.

    Args:
        db: SupabaseDB
        popbill_svc: PopbillService 인스턴스
        start_date: 시작일 (YYYYMMDD)
        end_date: 종료일 (YYYYMMDD)
        direction: 'SELL'(매출) / 'BUY'(매입)

    Returns:
        dict: {new_count, skipped_count, updated_count, total_fetched}
    """
    if not popbill_svc.is_ready:
        raise RuntimeError('Popbill SDK가 초기화되지 않았습니다.')

    new_count = 0
    skipped = 0
    updated = 0
    total_fetched = 0
    db_direction = 'sales' if direction == 'SELL' else 'purchase'

    # 페이지네이션으로 전체 조회
    page = 1
    per_page = 100

    while True:
        try:
            result = popbill_svc.search_invoices(
                direction=direction,
                start_date=start_date,
                end_date=end_date,
                page=page,
                per_page=per_page,
            )
        except Exception as e:
            logger.error(f"팝빌 세금계산서 조회 오류 (page={page}): {e}")
            raise

        items = getattr(result, 'list', None) or []
        total_fetched += len(items)

        for item in items:
            # 팝빌 SDK는 ntsconfirmNum (소문자 c) 또는 ntsConfirmNum 둘 다 가능
            nts_num = _get_val(item, 'ntsconfirmNum') or _get_val(item, 'ntsConfirmNum')
            mgt_key = _get_val(item, 'invoicerMgtKey') if direction == 'SELL' \
                else _get_val(item, 'invoiceeMgtKey')
            write_date_raw = _get_val(item, 'writeDate')
            issue_date_raw = _get_val(item, 'issueDate')
            tax_type_code = _get_val(item, 'taxType', 'T')
            state_code = _get_val(item, 'stateCode')

            supply_cost = int(_get_val(item, 'supplyCostTotal', '0') or '0')
            tax_total = int(_get_val(item, 'taxTotal', '0') or '0')
            total_amount = int(_get_val(item, 'totalAmount', '0') or '0')

            # 공급자/공급받는자 정보
            invoicer_corp_num = _get_val(item, 'invoicerCorpNum')
            invoicer_corp_name = _get_val(item, 'invoicerCorpName')
            invoicer_ceo = _get_val(item, 'invoicerCEOName')
            invoicee_corp_num = _get_val(item, 'invoiceeCorpNum')
            invoicee_corp_name = _get_val(item, 'invoiceeCorpName')
            invoicee_ceo = _get_val(item, 'invoiceeCEOName')

            # 중복 체크
            existing_id = db.check_tax_invoice_exists(
                invoice_number=nts_num if nts_num else None,
                mgt_key=mgt_key if mgt_key else None,
            )

            if existing_id:
                # 기존 건: 상태 업데이트만 (팝빌 상태가 바뀔 수 있음)
                new_status = _map_status(state_code)
                try:
                    db.update_tax_invoice(existing_id, {
                        'status': new_status,
                        'invoice_number': nts_num or None,
                    })
                    updated += 1
                except Exception:
                    pass
                skipped += 1
                continue

            # 신규 건 DB 저장
            payload = {
                'direction': db_direction,
                'invoice_number': nts_num,
                'mgt_key': mgt_key,
                'write_date': _fmt_date(write_date_raw),
                'issue_date': _fmt_date(issue_date_raw),
                'tax_type': _map_tax_type(tax_type_code),
                'supplier_corp_num': invoicer_corp_num,
                'supplier_corp_name': invoicer_corp_name,
                'supplier_ceo_name': invoicer_ceo,
                'buyer_corp_num': invoicee_corp_num,
                'buyer_corp_name': invoicee_corp_name,
                'buyer_ceo_name': invoicee_ceo,
                'supply_cost_total': supply_cost,
                'tax_total': tax_total,
                'total_amount': total_amount,
                'status': _map_status(state_code),
                'registered_by': 'popbill_sync',
            }

            try:
                invoice_id = db.insert_tax_invoice(payload)
                if invoice_id:
                    new_count += 1
                    logger.info(f"세금계산서 동기화 신규: ID={invoice_id}, "
                                f"방향={db_direction}, 금액={total_amount:,}")
                else:
                    skipped += 1
            except Exception as e:
                logger.error(f"세금계산서 동기화 저장 실패: {nts_num or mgt_key} — {e}")
                skipped += 1

        # 다음 페이지 여부
        total_count = getattr(result, 'total', 0) or 0
        if total_fetched >= total_count or len(items) < per_page:
            break
        page += 1

    logger.info(f"세금계산서 동기화 완료 ({direction}): "
                f"전체 {total_fetched}건, 신규 {new_count}건, "
                f"업데이트 {updated}건, 스킵 {skipped}건")

    return {
        'new_count': new_count,
        'skipped_count': skipped,
        'updated_count': updated,
        'total_fetched': total_fetched,
    }


def sync_all_tax_invoices(db, popbill_svc, start_date=None, end_date=None):
    """매출 + 매입 세금계산서 일괄 동기화.

    Args:
        db: SupabaseDB
        popbill_svc: PopbillService
        start_date: YYYYMMDD (기본: 3개월 전)
        end_date: YYYYMMDD (기본: 오늘)

    Returns:
        dict: {sell: {...}, buy: {...}}
    """
    from services.tz_utils import today_kst, days_ago_kst

    if not end_date:
        end_date = today_kst().replace('-', '')
    if not start_date:
        start_date = days_ago_kst(90).replace('-', '')

    results = {}
    for direction in ['SELL', 'BUY']:
        try:
            r = sync_tax_invoices(db, popbill_svc, start_date, end_date, direction)
            results[direction.lower()] = r
        except Exception as e:
            logger.error(f"세금계산서 동기화 오류 ({direction}): {e}")
            results[direction.lower()] = {'error': str(e)}

    return results


def generate_mgt_key():
    """팝빌 관리번호 생성 (유니크, 24자 이내).
    형식: AT + YYYYMMDDHHMMSS + 8자리 UUID = 24자
    """
    now = datetime.now().strftime('%Y%m%d%H%M%S')
    short_uid = uuid.uuid4().hex[:8].upper()
    return f'AT{now}{short_uid}'


def build_invoice_from_trade(db, partner_id, trade_date, items, tax_type='과세'):
    """거래 데이터 → 세금계산서 발행 데이터 빌드.

    Args:
        db: SupabaseDB
        partner_id: 거래처 ID (business_partners)
        trade_date: 작성일자 (YYYY-MM-DD)
        items: list of {product_name, qty, unit_price}
        tax_type: '과세' | '면세' | '영세'

    Returns:
        dict: issue_sales_invoice()에 전달할 형태
    """
    # 거래처 조회 (business_partners 테이블)
    partner = db.query_partner_by_id(partner_id)
    if not partner:
        raise ValueError(f'거래처 ID {partner_id}를 찾을 수 없습니다')

    # 우리 사업장 정보
    my_biz = db.query_default_business()

    # 면세/영세는 부가세 0%
    vat_rate = 0.1 if tax_type == '과세' else 0

    detail_items = []
    supply_total = 0
    for item in items:
        qty = int(item.get('qty', 0))
        unit_cost = int(item.get('unit_price', 0))
        supply = qty * unit_cost
        tax = int(supply * vat_rate)
        supply_total += supply
        detail_items.append({
            'name': item.get('product_name', ''),
            'qty': qty,
            'unit_cost': unit_cost,
            'supply_cost': supply,
            'tax': tax,
        })

    tax_total = int(supply_total * vat_rate)
    write_date = trade_date.replace('-', '')  # YYYYMMDD

    return {
        'write_date': write_date,
        'tax_type': tax_type,
        'mgt_key': generate_mgt_key(),
        'buyer_corp_num': partner.get('business_number', '').replace('-', ''),
        'buyer_corp_name': partner.get('partner_name', ''),
        'buyer_ceo_name': partner.get('representative', ''),
        'buyer_addr': partner.get('address', ''),
        'buyer_biz_type': partner.get('type', ''),
        'buyer_biz_class': partner.get('business_item', ''),
        'buyer_email': partner.get('email', ''),
        'supplier_corp_num': my_biz.get('business_number', '').replace('-', '') if my_biz else '',
        'supplier_corp_name': my_biz.get('business_name', '') if my_biz else '',
        'supplier_ceo_name': my_biz.get('representative', '') if my_biz else '',
        'supplier_addr': my_biz.get('address', '') if my_biz else '',
        'supplier_biz_type': my_biz.get('biz_type', '') if my_biz else '',
        'supplier_biz_class': my_biz.get('biz_class', '') if my_biz else '',
        'supplier_email': my_biz.get('email', '') if my_biz else '',
        'items': detail_items,
        'supply_cost_total': supply_total,
        'tax_total': tax_total,
        'total_amount': supply_total + tax_total,
    }


def save_invoice_to_db(db, invoice_data, direction, popbill_result=None, registered_by=''):
    """발행 결과를 DB에 저장.

    Returns:
        int: 생성된 tax_invoices.id
    """
    write_date = invoice_data['write_date']
    if len(write_date) == 8:
        write_date = f'{write_date[:4]}-{write_date[4:6]}-{write_date[6:8]}'

    payload = {
        'direction': direction,
        'invoice_number': popbill_result.get('nts_confirm_num', '') if popbill_result else '',
        'mgt_key': invoice_data.get('mgt_key', ''),
        'write_date': write_date,
        'issue_date': write_date,
        'tax_type': invoice_data.get('tax_type', '과세'),
        'supplier_corp_num': invoice_data.get('supplier_corp_num', ''),
        'supplier_corp_name': invoice_data.get('supplier_corp_name', ''),
        'supplier_ceo_name': invoice_data.get('supplier_ceo_name', ''),
        'buyer_corp_num': invoice_data.get('buyer_corp_num', ''),
        'buyer_corp_name': invoice_data.get('buyer_corp_name', ''),
        'buyer_ceo_name': invoice_data.get('buyer_ceo_name', ''),
        'supply_cost_total': invoice_data.get('supply_cost_total', 0),
        'tax_total': invoice_data.get('tax_total', 0),
        'total_amount': invoice_data.get('total_amount', 0),
        'items': invoice_data.get('items', []),
        'status': 'issued' if popbill_result else 'draft',
        'registered_by': registered_by,
    }
    invoice_id = db.insert_tax_invoice(payload)
    logger.info(f"세금계산서 DB 저장: ID={invoice_id}, 방향={direction}")

    # ── 자동 전표 생성 ──
    if invoice_id:
        try:
            from services.journal_service import (
                create_sales_invoice_journal, create_purchase_invoice_journal,
            )
            if direction == 'sales':
                create_sales_invoice_journal(db, invoice_id, created_by=registered_by or 'system')
            elif direction == 'purchase':
                create_purchase_invoice_journal(db, invoice_id, created_by=registered_by or 'system')
        except Exception as e:
            logger.error(f"세금계산서 자동 전표 생성 실패 (ID={invoice_id}): {e}")

    return invoice_id


# ══════════════════════════════════════════
# 홈택스 엑셀 업로드 파싱
# ══════════════════════════════════════════

def _safe_int(val):
    """안전한 정수 변환 (빈값, 콤마, 문자열 처리)."""
    if val is None or str(val).strip() in ('', 'nan', 'NaN'):
        return 0
    try:
        return int(float(str(val).replace(',', '').replace(' ', '')))
    except (ValueError, TypeError):
        return 0


def _normalize_date(val):
    """다양한 날짜 형식 → YYYY-MM-DD."""
    if not val or str(val).strip() in ('', 'nan', 'NaN'):
        return ''
    s = str(val).strip().replace('.', '-').replace('/', '-')
    if len(s) == 8 and s.isdigit():
        return f'{s[:4]}-{s[4:6]}-{s[6:8]}'
    if len(s) >= 10 and s[4] == '-':
        return s[:10]
    return s


def _normalize_corp_num(val):
    """사업자번호 하이픈 제거."""
    if not val or str(val).strip() in ('', 'nan', 'NaN'):
        return ''
    return str(val).replace('-', '').replace(' ', '').strip()


def _safe_str(val):
    """안전한 문자열 변환."""
    if val is None or str(val).strip() in ('nan', 'NaN'):
        return ''
    return str(val).strip()


def _find_header_row(df):
    """홈택스 엑셀에서 실제 헤더 행 번호를 자동 탐지.
    '작성일자'와 '승인번호'가 포함된 행을 찾는다.
    """
    for i in range(min(10, len(df))):
        row_vals = [str(v).strip() for v in df.iloc[i].values]
        if '작성일자' in row_vals and '승인번호' in row_vals:
            return i
    return None


def _detect_tax_type_from_filename(direction_hint):
    """파일명/방향으로 과세/면세 추정 (계산서=면세, 세금계산서=과세)."""
    # 이 함수는 호출측에서 is_tax_exempt 플래그로 대체
    pass


def parse_hometax_excel(df_or_filepath, direction='sales', is_tax_exempt=False):
    """홈택스에서 다운받은 세금계산서/계산서 엑셀 → DB insert용 dict 목록.

    홈택스 엑셀 구조:
    - Row 0: 사업자 등록번호 / 상호 / 대표자 (메타 정보)
    - Row 1: 빈행
    - Row 2: 합계 금액 요약
    - Row 3: 빈행
    - Row 4: 분류 제목행 (ex: "매출 전자(세금)계산서 발급목록조회")
    - Row 5: 실제 컬럼 헤더 ← 여기가 핵심
    - Row 6~: 데이터

    세금계산서(과세): 35컬럼 — 세액 포함
    계산서(면세):     33컬럼 — 세액 없음

    컬럼명이 중복됨 (상호, 대표자명, 종사업장번호 등) → 위치 기반 매핑

    Args:
        df_or_filepath: pandas DataFrame (header=None으로 읽은 것)
        direction: 'sales' (매출) / 'purchase' (매입)
        is_tax_exempt: True면 면세 계산서 (세액 없음)

    Returns:
        list[dict]: insert_tax_invoice()에 전달할 데이터 목록
    """
    import pandas as pd

    if isinstance(df_or_filepath, str):
        df = pd.read_excel(df_or_filepath, header=None, dtype=str).fillna('')
    else:
        df = df_or_filepath

    # ── 헤더 행 탐지 ──
    header_idx = _find_header_row(df)
    if header_idx is None:
        raise ValueError(
            '홈택스 엑셀 형식을 인식할 수 없습니다. '
            '"작성일자", "승인번호" 컬럼이 포함된 홈택스 표준 양식인지 확인하세요.'
        )

    header_vals = [str(v).strip() for v in df.iloc[header_idx].values]
    data_df = df.iloc[header_idx + 1:].reset_index(drop=True)
    num_cols = len(header_vals)

    logger.info(f"홈택스 엑셀: 헤더행={header_idx}, 컬럼수={num_cols}, "
                f"데이터행={len(data_df)}, 면세={is_tax_exempt}")

    # ── 위치 기반 컬럼 매핑 ──
    # 세금계산서 (과세, 35컬럼):
    #   0:작성일자  1:승인번호  2:발급일자  3:전송일자
    #   4:공급자사업자등록번호  5:종사업장번호  6:상호(공급자)  7:대표자명(공급자)  8:주소(공급자)
    #   9:공급받는자사업자등록번호 10:종사업장번호 11:상호(공급받는자) 12:대표자명(공급받는자) 13:주소(공급받는자)
    #   14:합계금액  15:공급가액  16:세액
    #   17:전자세금계산서분류  18:전자세금계산서종류  19:발급유형
    #   20:비고  21:영수/청구구분  22:공급자이메일
    #   23:공급받는자이메일1  24:공급받는자이메일2
    #   25:수탁사업자등록번호  26:상호(수탁)
    #   27~34: 품목 정보
    #
    # 계산서 (면세, 33컬럼): 16(세액) 없음 → 이후 인덱스 1씩 앞당겨짐

    # '세액' 컬럼 존재 여부로 면세/과세 자동 판별
    has_tax_col = '세액' in header_vals
    if not has_tax_col:
        is_tax_exempt = True

    # 위치 인덱스 (과세 기준)
    IDX = {
        'write_date': 0,
        'invoice_number': 1,
        'issue_date': 2,
        'send_date': 3,
        'supplier_corp_num': 4,
        'supplier_corp_name': 6,
        'supplier_ceo_name': 7,
        'buyer_corp_num': 9,
        'buyer_corp_name': 11,
        'buyer_ceo_name': 12,
        'total_amount': 14,
        'supply_cost_total': 15,
    }

    if has_tax_col:
        # 세금계산서 (과세): 세액이 16번
        IDX['tax_total'] = 16
        IDX['classification'] = 17
        IDX['invoice_kind'] = 18
    else:
        # 계산서 (면세): 세액 없음, 분류가 16번부터
        IDX['tax_total'] = None
        IDX['classification'] = 16
        IDX['invoice_kind'] = 17

    results = []
    seen_numbers = set()

    for row_idx in range(len(data_df)):
        row = data_df.iloc[row_idx].values

        # 작성일자가 없으면 빈 행 / 합계 행 → 건너뜀
        write_date_raw = _safe_str(row[IDX['write_date']] if IDX['write_date'] < len(row) else '')
        if not write_date_raw or write_date_raw in ('nan', ''):
            continue

        write_date = _normalize_date(write_date_raw)
        issue_date = _normalize_date(
            _safe_str(row[IDX['issue_date']] if IDX['issue_date'] < len(row) else ''))

        # 승인번호
        invoice_number = _safe_str(row[IDX['invoice_number']] if IDX['invoice_number'] < len(row) else '')

        # 같은 엑셀 내 중복 건 건너뛰기 (승인번호 기준)
        if invoice_number and invoice_number in seen_numbers:
            continue
        if invoice_number:
            seen_numbers.add(invoice_number)

        # 금액
        supply = _safe_int(row[IDX['supply_cost_total']] if IDX['supply_cost_total'] < len(row) else 0)
        total = _safe_int(row[IDX['total_amount']] if IDX['total_amount'] < len(row) else 0)

        if IDX['tax_total'] is not None and IDX['tax_total'] < len(row):
            tax = _safe_int(row[IDX['tax_total']])
        else:
            tax = 0  # 면세

        # 공급가액과 합계 모두 0이면 건너뜀
        if supply == 0 and total == 0:
            continue

        # 합계금액 보정
        if total == 0 and supply != 0:
            total = supply + tax

        # 과세유형 결정
        if is_tax_exempt:
            tax_type = '면세'
        else:
            # 전자세금계산서 분류에서 추가 판별
            classification = _safe_str(
                row[IDX['classification']] if IDX['classification'] < len(row) else '')
            if '영세' in classification:
                tax_type = '영세'
            elif '면세' in classification:
                tax_type = '면세'
            else:
                tax_type = '과세'

        # 공급자/공급받는자 정보
        supplier_num = _normalize_corp_num(
            row[IDX['supplier_corp_num']] if IDX['supplier_corp_num'] < len(row) else '')
        supplier_name = _safe_str(
            row[IDX['supplier_corp_name']] if IDX['supplier_corp_name'] < len(row) else '')
        supplier_ceo = _safe_str(
            row[IDX['supplier_ceo_name']] if IDX['supplier_ceo_name'] < len(row) else '')
        buyer_num = _normalize_corp_num(
            row[IDX['buyer_corp_num']] if IDX['buyer_corp_num'] < len(row) else '')
        buyer_name = _safe_str(
            row[IDX['buyer_corp_name']] if IDX['buyer_corp_name'] < len(row) else '')
        buyer_ceo = _safe_str(
            row[IDX['buyer_ceo_name']] if IDX['buyer_ceo_name'] < len(row) else '')

        payload = {
            'direction': direction,
            'invoice_number': invoice_number,
            'mgt_key': '',
            'write_date': write_date or issue_date,
            'issue_date': issue_date or write_date,
            'tax_type': tax_type,
            'supplier_corp_num': supplier_num,
            'supplier_corp_name': supplier_name,
            'supplier_ceo_name': supplier_ceo,
            'buyer_corp_num': buyer_num,
            'buyer_corp_name': buyer_name,
            'buyer_ceo_name': buyer_ceo,
            'supply_cost_total': supply,
            'tax_total': tax,
            'total_amount': total,
            'status': 'issued',
            'registered_by': 'hometax_upload',
        }
        results.append(payload)

    logger.info(f"홈택스 엑셀 파싱 완료: {len(results)}건 ({direction}, "
                f"{'면세' if is_tax_exempt else '과세'})")
    return results
