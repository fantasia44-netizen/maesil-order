"""
marketplace_validation_service.py — API vs 엑셀 교차검증 서비스.

api_orders (API) ↔ order_transactions (엑셀) 교차검증.
api_settlements (API) ↔ platform_settlements (계산) 비교.
"""
import logging

logger = logging.getLogger(__name__)


def _query_all_order_transactions(db, channel, date_from, date_to):
    """order_transactions 전체 조회 (1000행 제한 우회)."""
    all_rows = []
    page_size = 1000
    offset = 0
    while True:
        rows = db.query_order_transactions(
            channel=channel, date_from=date_from, date_to=date_to,
            limit=page_size, offset=offset)
        all_rows.extend(rows)
        if len(rows) < page_size:
            break
        offset += page_size
    return all_rows


    # 교차검증에서 제외할 주문 상태 (취소/결제대기 등)
EXCLUDE_STATUSES = {
    # 네이버 스마트스토어
    'CANCELED', 'CANCEL_DONE', 'CANCEL_REQUEST',
    'RETURN_DONE', 'RETURN_REQUEST',
    'EXCHANGE_DONE', 'EXCHANGE_REQUEST',
    'PAYMENT_WAITING',
    # 쿠팡
    'CANCEL',
    # Cafe24
    'canceled', 'refunded',
}


def validate_orders(db, channel, date_from, date_to):
    """API 주문 vs 엑셀 주문 교차검증.

    매칭 키: (channel, order_no).
    네이버: api_line_id = 상품주문번호 = order_transactions.order_no
    쿠팡:   api_order_id = 주문번호 = order_transactions.order_no

    Returns:
        dict: {channel, date_range, summary, amount_comparison, mismatches}
    """
    # API 주문 조회 (페이지네이션 지원)
    api_orders_raw = db.query_api_orders(
        channel=channel, date_from=date_from, date_to=date_to, limit=50000)

    # 취소/결제대기 등 제외
    excluded_count = 0
    api_orders = []
    for o in api_orders_raw:
        status = str(o.get('order_status', '')).upper()
        if status in EXCLUDE_STATUSES or status in {s.upper() for s in EXCLUDE_STATUSES}:
            excluded_count += 1
        else:
            api_orders.append(o)

    if excluded_count:
        logger.info(f'[검증] {channel}: 취소/대기 등 {excluded_count}건 제외')

    # 엑셀 주문 조회 (페이지네이션으로 전체 조회)
    excel_orders = _query_all_order_transactions(
        db, channel=channel, date_from=date_from, date_to=date_to)

    # 매칭 키 생성
    # 네이버: api_line_id(상품주문번호)로 1:1 매칭
    # 쿠팡/자사몰: api_order_id(주문번호)로 매칭 — 주문 내 여러 상품 합산
    api_by_key = {}
    for o in api_orders:
        if channel in ('스마트스토어', '해미애찬'):
            key = str(o.get('api_line_id', ''))
        else:
            key = str(o.get('api_order_id', ''))
        if key:
            if key not in api_by_key:
                api_by_key[key] = o
            else:
                # 같은 주문번호 → 수량/금액 합산
                api_by_key[key] = _merge_api_order(api_by_key[key], o)

    excel_by_key = {}
    for o in excel_orders:
        key = str(o.get('order_no', ''))
        if key:
            # 동일 주문번호 여러 행 가능 (line_no)
            if key not in excel_by_key:
                excel_by_key[key] = o
            else:
                # 수량/금액 합산
                excel_by_key[key] = _merge_excel_order(excel_by_key[key], o)

    # 매칭 실행
    matched = 0
    amount_mismatch = 0
    missing_excel = 0
    missing_api = 0
    mismatches = []

    # API에서 있는 주문 기준
    api_total_amount = 0
    api_settlement = 0
    api_commission = 0
    excel_total_amount = 0
    excel_settlement = 0
    excel_commission = 0

    for key, api_o in api_by_key.items():
        api_total_amount += int(api_o.get('total_amount', 0))
        api_settlement += int(api_o.get('settlement_amount', 0))
        api_commission += int(api_o.get('commission', 0))

        if key in excel_by_key:
            excel_o = excel_by_key[key]
            excel_total_amount += int(excel_o.get('total_amount', 0))
            excel_settlement += int(excel_o.get('settlement', 0))
            excel_commission += int(excel_o.get('commission', 0))

            # 금액 비교 (허용 오차: 10원)
            diffs = _compare_amounts(api_o, excel_o)
            if diffs:
                amount_mismatch += 1
                mismatches.append({
                    'order_no': key,
                    'type': 'amount_mismatch',
                    'api_data': _safe_order(api_o),
                    'excel_data': _safe_order(excel_o),
                    'diffs': diffs,
                })
            else:
                matched += 1

            # 매칭 결과 DB 업데이트
            match_status = 'matched' if not diffs else 'mismatch'
            if api_o.get('id'):
                db.update_api_order_match(api_o['id'], {
                    'match_status': match_status,
                    'matched_order_transaction_id': excel_o.get('id'),
                    'match_detail': diffs or {},
                })
        else:
            missing_excel += 1
            mismatches.append({
                'order_no': key,
                'type': 'missing_excel',
                'api_data': _safe_order(api_o),
                'excel_data': None,
                'diffs': None,
            })

    # 엑셀에만 있는 주문
    for key, excel_o in excel_by_key.items():
        if key not in api_by_key:
            missing_api += 1
            excel_total_amount += int(excel_o.get('total_amount', 0))
            excel_settlement += int(excel_o.get('settlement', 0))
            excel_commission += int(excel_o.get('commission', 0))
            mismatches.append({
                'order_no': key,
                'type': 'missing_api',
                'api_data': None,
                'excel_data': _safe_order(excel_o),
                'diffs': None,
            })

    total_api = len(api_by_key)
    total_excel = len(excel_by_key)
    match_rate = round(matched / total_api * 100, 1) if total_api > 0 else 0

    return {
        'channel': channel,
        'date_range': {'from': date_from, 'to': date_to},
        'summary': {
            'total_api': total_api,
            'total_excel': total_excel,
            'matched': matched,
            'amount_mismatch': amount_mismatch,
            'missing_excel': missing_excel,
            'missing_api': missing_api,
            'match_rate': match_rate,
            'excluded': excluded_count,  # 취소/대기 제외 건수
        },
        'amount_comparison': {
            'api_total_amount': api_total_amount,
            'excel_total_amount': excel_total_amount,
            'api_settlement': api_settlement,
            'excel_settlement': excel_settlement,
            'api_commission': api_commission,
            'excel_commission': excel_commission,
        },
        'mismatches': sorted(mismatches, key=lambda x: x['order_no']),
    }


def validate_settlements(db, channel, date_from, date_to):
    """API 정산 vs 계산 정산 교차검증.

    Returns:
        dict: {channel, summary, comparisons}
    """
    api_settlements = db.query_api_settlements(
        channel=channel, date_from=date_from, date_to=date_to)

    platform_settlements = db.query_platform_settlements(
        channel=channel, date_from=date_from, date_to=date_to)

    # settlement_date 기준 매칭
    api_by_date = {}
    for s in api_settlements:
        d = str(s.get('settlement_date', ''))[:10]
        if d:
            if d not in api_by_date:
                api_by_date[d] = s
            else:
                # 같은 날짜 합산
                api_by_date[d] = _merge_settlement(api_by_date[d], s)

    platform_by_date = {}
    for s in platform_settlements:
        d = str(s.get('settlement_date', ''))[:10]
        if d:
            platform_by_date[d] = s

    matched = 0
    mismatch = 0
    comparisons = []

    for date_key, api_s in api_by_date.items():
        if date_key in platform_by_date:
            plat_s = platform_by_date[date_key]
            api_net = int(api_s.get('net_settlement', 0))
            plat_net = int(plat_s.get('net_settlement', 0))
            diff = api_net - plat_net

            if abs(diff) <= 100:  # 100원 허용 오차
                matched += 1
            else:
                mismatch += 1

            comparisons.append({
                'date': date_key,
                'api_gross': int(api_s.get('gross_sales', 0)),
                'api_commission': int(api_s.get('total_commission', 0)),
                'api_net': api_net,
                'platform_gross': int(plat_s.get('gross_sales', 0)),
                'platform_fee': int(plat_s.get('platform_fee', 0)),
                'platform_net': plat_net,
                'diff': diff,
                'status': 'matched' if abs(diff) <= 100 else 'mismatch',
            })
        else:
            comparisons.append({
                'date': date_key,
                'api_gross': int(api_s.get('gross_sales', 0)),
                'api_commission': int(api_s.get('total_commission', 0)),
                'api_net': int(api_s.get('net_settlement', 0)),
                'platform_gross': 0,
                'platform_fee': 0,
                'platform_net': 0,
                'diff': int(api_s.get('net_settlement', 0)),
                'status': 'api_only',
            })

    return {
        'channel': channel,
        'summary': {
            'total_api': len(api_by_date),
            'total_platform': len(platform_by_date),
            'matched': matched,
            'mismatch': mismatch,
        },
        'comparisons': sorted(comparisons, key=lambda x: x['date']),
    }


def _compare_amounts(api_order, excel_order, tolerance=10):
    """금액 비교. 차이가 있으면 {field: {api_val, excel_val, diff}} 반환."""
    diffs = {}
    comparisons = [
        ('total_amount', 'total_amount'),
        ('qty', 'qty'),
    ]
    for api_field, excel_field in comparisons:
        api_val = int(api_order.get(api_field, 0) or 0)
        excel_val = int(excel_order.get(excel_field, 0) or 0)
        if abs(api_val - excel_val) > tolerance:
            diffs[api_field] = {
                'api_val': api_val,
                'excel_val': excel_val,
                'diff': api_val - excel_val,
            }

    # 수량은 정확 매칭
    api_qty = int(api_order.get('qty', 0) or 0)
    excel_qty = int(excel_order.get('qty', 0) or 0)
    if api_qty != excel_qty:
        diffs['qty'] = {
            'api_val': api_qty,
            'excel_val': excel_qty,
            'diff': api_qty - excel_qty,
        }

    return diffs if diffs else None


def _merge_api_order(existing, new_row):
    """동일 api_order_id의 여러 상품 합산 (쿠팡/자사몰)."""
    existing['qty'] = int(existing.get('qty', 0) or 0) + int(new_row.get('qty', 0) or 0)
    existing['total_amount'] = (int(existing.get('total_amount', 0) or 0) +
                                int(new_row.get('total_amount', 0) or 0))
    existing['settlement_amount'] = (int(existing.get('settlement_amount', 0) or 0) +
                                     int(new_row.get('settlement_amount', 0) or 0))
    existing['commission'] = (int(existing.get('commission', 0) or 0) +
                              int(new_row.get('commission', 0) or 0))
    # product_name 합치기
    names = [existing.get('product_name', ''), new_row.get('product_name', '')]
    existing['product_name'] = ' + '.join([n for n in names if n][:3])
    return existing


def _merge_excel_order(existing, new_row):
    """동일 주문번호의 여러 line 합산."""
    existing['qty'] = int(existing.get('qty', 0)) + int(new_row.get('qty', 0))
    existing['total_amount'] = (int(existing.get('total_amount', 0)) +
                                int(new_row.get('total_amount', 0)))
    existing['settlement'] = (int(existing.get('settlement', 0)) +
                              int(new_row.get('settlement', 0)))
    existing['commission'] = (int(existing.get('commission', 0)) +
                              int(new_row.get('commission', 0)))
    return existing


def _merge_settlement(existing, new_row):
    """동일 날짜의 정산 합산."""
    for field in ['gross_sales', 'total_commission', 'net_settlement',
                  'shipping_fee_income', 'shipping_fee_cost',
                  'coupon_discount', 'point_discount', 'other_deductions']:
        existing[field] = (int(existing.get(field, 0)) +
                           int(new_row.get(field, 0)))
    return existing


def _safe_order(order):
    """DB 행에서 직렬화 가능한 필드만 추출."""
    return {
        'order_no': order.get('order_no', order.get('api_order_id', '')),
        'product_name': order.get('product_name', ''),
        'qty': int(order.get('qty', 0) or 0),
        'total_amount': int(order.get('total_amount', 0) or 0),
        'settlement': int(order.get('settlement',
                                    order.get('settlement_amount', 0)) or 0),
        'commission': int(order.get('commission', 0) or 0),
    }
