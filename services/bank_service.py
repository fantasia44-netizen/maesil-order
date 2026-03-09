"""
bank_service.py -- 은행 거래내역 비즈니스 로직.
db 인스턴스를 파라미터로 받는 순수 함수 패턴 (기존 revenue_service.py 패턴).
"""
import logging

logger = logging.getLogger(__name__)


def sync_bank_transactions(db, codef_svc, bank_account_id):
    """특정 계좌의 최신 거래내역을 CODEF에서 조회하여 DB 동기화.

    Returns:
        dict: {new_count, skipped_count, last_date}
    """
    account = db.query_bank_account_by_id(bank_account_id)
    if not account:
        logger.error(f"[은행동기화] 계좌 ID {bank_account_id} 조회 실패")
        raise ValueError(f'계좌 ID {bank_account_id} 없음')

    logger.info(f"[은행동기화] 시작: {account.get('bank_name', '')} "
                f"{account.get('account_number', '')[-4:]}****")

    # 마지막 동기화 날짜 이후부터 (없으면 30일 전)
    last_date = account.get('last_synced_date', '')
    if last_date:
        start = last_date.replace('-', '')
    else:
        from services.tz_utils import days_ago_kst
        start = days_ago_kst(30).replace('-', '')

    from services.tz_utils import today_kst
    end = today_kst().replace('-', '')

    logger.info(f"[은행동기화] CODEF 조회 기간: {start} ~ {end}")

    # CODEF 거래내역 조회 (개인/기업 구분)
    acct_client_type = account.get('client_type', 'P')
    try:
        raw_list = codef_svc.get_transactions(
            connected_id=account['connected_id'],
            bank_code=account['bank_code'],
            account=account['account_number'],
            start_date=start,
            end_date=end,
            client_type=acct_client_type,
        )
        logger.info(f"[은행동기화] CODEF 응답: {len(raw_list)}건 조회됨")
    except Exception as e:
        logger.error(f"[은행동기화] CODEF API 오류: {e}")
        raise

    new_count = 0
    skipped = 0
    latest_date = ''

    for tx in raw_list:
        tx_date_raw = tx.get('resAccountTrDate', '')
        tx_time = tx.get('resAccountTrTime', '')
        in_amt = int(tx.get('resAccountIn', '0') or '0')
        out_amt = int(tx.get('resAccountOut', '0') or '0')
        balance = int(tx.get('resAfterTranBalance', '0') or '0')
        desc1 = tx.get('resAccountDesc1', '') or ''  # 의뢰인/수취인
        desc2 = tx.get('resAccountDesc2', '') or ''  # 거래구분
        desc3 = tx.get('resAccountDesc3', '') or ''  # 적요
        desc4 = tx.get('resAccountDesc4', '') or ''  # 거래점

        # YYYYMMDD → YYYY-MM-DD
        if len(tx_date_raw) == 8:
            tx_date = f'{tx_date_raw[:4]}-{tx_date_raw[4:6]}-{tx_date_raw[6:8]}'
        else:
            tx_date = tx_date_raw

        # HHMMSS → HH:MM:SS
        if len(tx_time) == 6:
            tx_time_fmt = f'{tx_time[:2]}:{tx_time[2:4]}:{tx_time[4:6]}'
        else:
            tx_time_fmt = tx_time

        tx_type = '입금' if in_amt > 0 else '출금'
        amount = in_amt if in_amt > 0 else out_amt

        # 상대방: desc1(의뢰인/수취인) > desc3(적요) > desc2(거래구분)
        counterpart = desc1 or desc3 or desc2
        # 설명: 적요 + 거래점
        description = desc3
        if desc4:
            description = f'{desc3} [{desc4}]' if desc3 else desc4

        payload = {
            'bank_account_id': bank_account_id,
            'transaction_date': tx_date,
            'transaction_time': tx_time_fmt,
            'transaction_type': tx_type,
            'amount': amount,
            'balance': balance,
            'counterpart_name': counterpart,
            'description': description,
            'codef_transaction_id': tx.get('resTransactionId', ''),
        }

        try:
            db.insert_bank_transaction(payload)
            new_count += 1
            logger.debug(f"[은행동기화] 신규: {tx_date} {tx_type} {amount:,}원 {counterpart}")
        except Exception:
            skipped += 1  # UNIQUE 중복

        if tx_date > latest_date:
            latest_date = tx_date

    # 동기화 시각 항상 업데이트 (KST) — 신규 0건이어도 동기화 시각 갱신
    from services.tz_utils import now_kst
    update_data = {'last_synced_at': now_kst().isoformat()}
    if latest_date:
        update_data['last_synced_date'] = latest_date
    db.update_bank_account(bank_account_id, update_data)

    logger.info(f"은행 동기화 완료: 계좌 {bank_account_id}, 신규 {new_count}건, 스킵 {skipped}건")
    return {'new_count': new_count, 'skipped_count': skipped, 'last_date': latest_date}


def sync_all_accounts(db, codef_svc):
    """전체 활성 은행 계좌 일괄 동기화 (카드/대출/외화 제외)."""
    # 동기화 불가 유형: 카드, 대출, 외화 (CODEF 입출금 거래내역 API 미지원)
    SKIP_TYPES = {'카드', '대출', '외화'}
    accounts = db.query_bank_accounts()
    active = [a for a in accounts
              if a.get('account_type') not in SKIP_TYPES and a.get('is_active', True)]
    logger.info(f"[은행동기화] 전체 계좌 일괄 동기화 시작: {len(active)}개 계좌")

    results = []
    for acc in active:
        try:
            r = sync_bank_transactions(db, codef_svc, acc['id'])
            results.append({'account_id': acc['id'], 'bank_name': acc['bank_name'], **r})
        except Exception as e:
            logger.error(f"[은행동기화] {acc['bank_name']} 오류: {e}")
            results.append({'account_id': acc['id'], 'bank_name': acc['bank_name'], 'error': str(e)})

    total_new = sum(r.get('new_count', 0) for r in results if 'error' not in r)
    logger.info(f"[은행동기화] 전체 완료: {len(results)}개 계좌, 신규 {total_new}건")
    return results


def get_transaction_summary(db, date_from=None, date_to=None, bank_account_id=None):
    """거래내역 요약 (입금 합계, 출금 합계, 건수, 카테고리별).

    Returns:
        dict: {total_in, total_out, net, count, by_category, by_date}
    """
    txns = db.query_bank_transactions(
        date_from=date_from, date_to=date_to,
        bank_account_id=bank_account_id,
    )

    total_in = 0
    total_out = 0
    by_cat = {}
    by_date = {}

    for tx in txns:
        amt = tx.get('amount', 0)
        tx_type = tx.get('transaction_type', '')
        tx_date = str(tx.get('transaction_date', ''))

        if tx_type == '입금':
            total_in += amt
        else:
            total_out += amt

        # 카테고리별 집계
        cat = tx.get('category', '미분류') or '미분류'
        by_cat[cat] = by_cat.get(cat, 0) + amt

        # 날짜별 집계 (차트용)
        if tx_date not in by_date:
            by_date[tx_date] = {'in': 0, 'out': 0}
        if tx_type == '입금':
            by_date[tx_date]['in'] += amt
        else:
            by_date[tx_date]['out'] += amt

    return {
        'total_in': total_in,
        'total_out': total_out,
        'net': total_in - total_out,
        'count': len(txns),
        'by_category': by_cat,
        'by_date': dict(sorted(by_date.items())),
    }


# ── 거래 분류 카테고리 ──
TRANSACTION_CATEGORIES = [
    '매출입금', '정산금', '원재료', '포장재', '배송비',
    '급여', '임차료', '광고비', '수수료', '세금',
    '보험료', '통신비', '수도광열비', '소모품',
    '대출상환', '이자', '기타수입', '기타지출', '미분류',
]
