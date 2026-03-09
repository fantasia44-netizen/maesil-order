"""
card_service.py -- 카드 이용내역 비즈니스 로직.
CODEF 카드내역 조회 → DB 동기화.
"""
import logging

logger = logging.getLogger(__name__)


def sync_card_transactions(db, codef_svc, bank_account_id):
    """특정 카드의 최신 이용내역을 CODEF에서 조회하여 DB 동기화.

    Returns:
        dict: {new_count, skipped_count, last_date}
    """
    account = db.query_bank_account_by_id(bank_account_id)
    if not account:
        raise ValueError(f'카드 ID {bank_account_id} 없음')
    if account.get('account_type') != '카드':
        raise ValueError(f'계좌 {bank_account_id}은 카드가 아닙니다')

    # 마지막 동기화 날짜 이후부터
    last_date = account.get('last_synced_date', '')
    if last_date:
        start = last_date.replace('-', '')
    else:
        from services.tz_utils import days_ago_kst
        start = days_ago_kst(90).replace('-', '')  # 카드는 최대 90일

    from services.tz_utils import today_kst
    end = today_kst().replace('-', '')

    acct_client_type = account.get('client_type', 'P')
    card_no = account.get('account_number', '')

    raw_list = codef_svc.get_card_transactions(
        connected_id=account['connected_id'],
        card_code=account['bank_code'],
        card_no=card_no,
        start_date=start,
        end_date=end,
        client_type=acct_client_type,
    )

    new_count = 0
    skipped = 0
    latest_date = ''

    for tx in raw_list:
        # CODEF 카드 응답 필드 매핑
        date_raw = tx.get('resApprovalDate', tx.get('resUsedDate', ''))
        time_raw = tx.get('resApprovalTime', '')
        approval_no = tx.get('resApprovalNo', '')
        merchant = tx.get('resMemberStoreName', tx.get('resStoreName', ''))
        amount = int(tx.get('resApprovalAmount', tx.get('resUsedAmount', '0')) or '0')
        card_type = tx.get('resCardType', '')  # 신용/체크
        installment = tx.get('resInstallmentCount', '일시불') or '일시불'
        is_cancelled = tx.get('resCardApprovalType', '') in ('취소', '승인취소')
        desc = tx.get('resAccountDesc', '') or ''

        # YYYYMMDD → YYYY-MM-DD
        if len(date_raw) == 8:
            tx_date = f'{date_raw[:4]}-{date_raw[4:6]}-{date_raw[6:8]}'
        else:
            tx_date = date_raw

        # HHMMSS → HH:MM:SS
        if len(time_raw) == 6:
            tx_time = f'{time_raw[:2]}:{time_raw[2:4]}:{time_raw[4:6]}'
        else:
            tx_time = time_raw

        # 중복 체크: 동일 기업카드의 복수 카드번호에서 같은 거래 중복 방지
        if approval_no and db.check_card_transaction_exists(tx_date, approval_no, amount):
            skipped += 1
            continue

        payload = {
            'bank_account_id': bank_account_id,
            'approval_date': tx_date,
            'approval_time': tx_time,
            'approval_no': approval_no,
            'merchant_name': merchant,
            'amount': amount,
            'card_type': card_type,
            'installment': installment,
            'is_cancelled': is_cancelled,
            'description': desc,
        }

        try:
            db.insert_card_transaction(payload)
            new_count += 1
        except Exception as e:
            logger.error(f"카드거래 저장 실패: {approval_no} ({merchant}) {amount}원 — {e}")
            skipped += 1  # UNIQUE 중복 또는 기타 오류

        if tx_date > latest_date:
            latest_date = tx_date

    # 동기화 시각 항상 업데이트 (KST) — 신규 0건이어도 동기화 시각 갱신
    from services.tz_utils import now_kst
    update_data = {'last_synced_at': now_kst().isoformat()}
    if latest_date:
        update_data['last_synced_date'] = latest_date
    db.update_bank_account(bank_account_id, update_data)

    logger.info(f"카드 동기화 완료: 카드 {bank_account_id}, 신규 {new_count}건, 스킵 {skipped}건")
    return {'new_count': new_count, 'skipped_count': skipped, 'last_date': latest_date}


def sync_all_card_accounts(db, codef_svc):
    """전체 카드 계좌 일괄 동기화."""
    accounts = db.query_bank_accounts()
    results = []
    for acc in accounts:
        if acc.get('account_type') != '카드':
            continue
        if not acc.get('is_active', True):
            continue
        try:
            r = sync_card_transactions(db, codef_svc, acc['id'])
            results.append({'account_id': acc['id'], 'bank_name': acc['bank_name'],
                            'card_no': acc['account_number'], **r})
        except Exception as e:
            logger.error(f"카드 동기화 실패: {acc['id']} — {e}")
            results.append({'account_id': acc['id'], 'bank_name': acc['bank_name'],
                            'error': str(e)})
    return results


def get_card_summary(db, date_from=None, date_to=None, bank_account_id=None):
    """카드 이용내역 요약.

    Returns:
        dict: {total_amount, count, by_category}
    """
    txns = db.query_card_transactions(
        date_from=date_from, date_to=date_to,
        bank_account_id=bank_account_id,
    )

    total = 0
    cancelled = 0
    by_cat = {}

    for tx in txns:
        amt = tx.get('amount', 0)
        if tx.get('is_cancelled'):
            cancelled += amt
            continue
        total += amt
        cat = tx.get('category', '미분류') or '미분류'
        by_cat[cat] = by_cat.get(cat, 0) + amt

    return {
        'total_amount': total,
        'cancelled_amount': cancelled,
        'net_amount': total - cancelled,
        'count': len(txns),
        'by_category': by_cat,
    }


# ── 카드 비용 분류 카테고리 ──
CARD_CATEGORIES = [
    '원재료', '포장재', '배송비', '급여', '임차료',
    '광고비', '수수료', '세금', '보험료', '통신비',
    '수도광열비', '소모품', '차량유지', '접대비',
    '교통비', '식대', '기타경비', '미분류',
]
