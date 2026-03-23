"""
cj_shipping_service.py — CJ 택배 송장 자동 생성 + 예약접수 서비스.

플로우:
  1. DB 주문 조회 (송장 미배정 건)
  2. CJ 운송장 채번 (ReqInvcNo)
  3. CJ 예약접수 (RegBook) — 보내는분/받는분 정보 전송
  4. order_shipping.invoice_no 업데이트
  5. 마켓 push 대기 상태로 전환
"""
import logging
import os
import time

from services.courier.cj_client import CJCourierClient

logger = logging.getLogger(__name__)

# 보내는분 기본 정보 (배마마)
DEFAULT_SENDER = {
    'name': os.getenv('CJ_SENDER_NAME', '배마마'),
    'phone': os.getenv('CJ_SENDER_PHONE', '031-217-7979'),
    'zipcode': os.getenv('CJ_SENDER_ZIPCODE', '16827'),
    'address': os.getenv('CJ_SENDER_ADDRESS', '경기도 용인시 수지구'),
    'detail_address': os.getenv('CJ_SENDER_DETAIL', '배마마 본사'),
}


def _get_cj_client():
    """CJ 클라이언트 생성 (.env에서 설정 로드)."""
    cust_id = os.getenv('CJ_CUST_ID', '')
    biz_reg = os.getenv('CJ_BIZ_REG_NUM', '')
    use_prod = os.getenv('CJ_USE_PROD', 'false').lower() == 'true'

    client = CJCourierClient(
        cust_id=cust_id,
        biz_reg_num=biz_reg,
        test_mode=not bool(cust_id),
        use_prod=use_prod,
    )
    return client


def query_orders_without_invoice(db, channel=None, date_from=None, date_to=None, limit=500):
    """송장 미배정 주문 조회 (order_shipping에서 invoice_no 없는 건).

    Returns:
        [{channel, order_no, name, phone, address, memo, products: [{name, qty}]}]
    """
    try:
        q = db.client.table("order_shipping") \
            .select("channel, order_no, name, phone, address, memo, shipping_status")
        q = q.eq("shipping_status", "대기")
        q = q.or_("invoice_no.is.null,invoice_no.eq.")

        if channel:
            q = q.eq("channel", channel)

        rows = q.order("created_at", desc=True).limit(limit).execute()
        ships = rows.data or []

        if not ships:
            return []

        # order_transactions에서 상품 정보 가져오기
        order_nos = list(set(s['order_no'] for s in ships))
        products_map = {}  # order_no → [{product_name, qty}]

        for chunk_start in range(0, len(order_nos), 100):
            chunk = order_nos[chunk_start:chunk_start + 100]
            tx_q = db.client.table("order_transactions") \
                .select("order_no, product_name, qty") \
                .in_("order_no", chunk)
            if channel:
                tx_q = tx_q.eq("channel", channel)
            tx_res = tx_q.execute()
            for t in (tx_res.data or []):
                ono = t['order_no']
                if ono not in products_map:
                    products_map[ono] = []
                products_map[ono].append({
                    'product_name': t.get('product_name', ''),
                    'qty': t.get('qty', 1),
                })

        result = []
        for s in ships:
            ono = s['order_no']
            result.append({
                'channel': s['channel'],
                'order_no': ono,
                'name': s.get('name', ''),
                'phone': s.get('phone', ''),
                'address': s.get('address', ''),
                'memo': s.get('memo', ''),
                'products': products_map.get(ono, [{'product_name': '이유식', 'qty': 1}]),
            })
        return result

    except Exception as e:
        logger.error(f'[CJShipping] 미배정 주문 조회 오류: {e}', exc_info=True)
        return []


def generate_cj_invoices(db, orders: list, sender: dict = None):
    """주문 목록에 대해 CJ 운송장 채번 + 예약접수 일괄 처리.

    Args:
        db: SupabaseDB
        orders: query_orders_without_invoice() 결과
        sender: 보내는분 정보 (None이면 DEFAULT_SENDER)

    Returns:
        {total, success, failed, results: [{order_no, invoice_no, ok, error}]}
    """
    if not orders:
        return {'total': 0, 'success': 0, 'failed': 0, 'results': []}

    client = _get_cj_client()
    sender = sender or DEFAULT_SENDER

    results = []
    success = 0
    failed = 0
    db_updates = []

    for order in orders:
        try:
            # 1) 운송장 채번
            invoice_no = client.generate_invoice_no()

            # 2) 주소 파싱 (주소 + 상세주소 분리)
            addr_parts = _split_address(order.get('address', ''))

            # 3) 상품 목록
            items = [{'product_name': p['product_name'], 'qty': p['qty']}
                     for p in order.get('products', [])]
            if not items:
                items = [{'product_name': '이유식', 'qty': 1}]

            # 합포장 상품명 (배송메모에 포함)
            product_summary = ', '.join(
                f"{p['product_name']}x{p['qty']}" for p in items[:5]
            )

            # 4) 예약접수
            result = client.register_shipment(
                sender=sender,
                receiver={
                    'name': order.get('name', ''),
                    'phone': order.get('phone', ''),
                    'zipcode': addr_parts['zipcode'],
                    'address': addr_parts['address'],
                    'detail_address': addr_parts['detail_address'],
                },
                items=items,
                invoice_no=invoice_no,
                order_no=order['order_no'],
                memo=order.get('memo', '') or product_summary,
            )

            if result.get('ok'):
                success += 1
                db_updates.append({
                    'channel': order['channel'],
                    'order_no': order['order_no'],
                    'invoice_no': invoice_no,
                    'courier': 'CJ대한통운',
                })
                results.append({
                    'order_no': order['order_no'],
                    'name': order.get('name', ''),
                    'invoice_no': invoice_no,
                    'ok': True,
                })
            else:
                failed += 1
                results.append({
                    'order_no': order['order_no'],
                    'name': order.get('name', ''),
                    'invoice_no': invoice_no,
                    'ok': False,
                    'error': result.get('error', ''),
                })

            # CJ rate limit 대비
            time.sleep(0.3)

        except Exception as e:
            failed += 1
            results.append({
                'order_no': order['order_no'],
                'ok': False,
                'error': str(e),
            })
            logger.error(f'[CJShipping] {order["order_no"]} 처리 오류: {e}')

    # 5) DB 일괄 업데이트 (성공 건)
    if db_updates:
        db.bulk_update_shipping_invoices(db_updates)
        logger.info(f'[CJShipping] DB 업데이트: {len(db_updates)}건')

    return {
        'total': len(orders),
        'success': success,
        'failed': failed,
        'results': results,
    }


def check_cj_booking_status(db, channel=None, limit=50):
    """CJ 예약접수 후 상품추적으로 접수 확인.

    최근 송장 배정된 건 중 CJ에서 집화처리(11) 이상인 건 확인.

    Returns:
        {total, confirmed, pending, results: [{order_no, invoice_no, status}]}
    """
    client = _get_cj_client()

    try:
        # 최근 송장 배정 but 아직 발송 아닌 건
        q = db.client.table("order_shipping") \
            .select("channel, order_no, invoice_no") \
            .eq("shipping_status", "대기") \
            .neq("invoice_no", "").not_.is_("invoice_no", "null")
        if channel:
            q = q.eq("channel", channel)
        rows = q.order("created_at", desc=True).limit(limit).execute()
        ships = rows.data or []

        if not ships:
            return {'total': 0, 'confirmed': 0, 'pending': 0, 'results': []}

        results = []
        confirmed = 0
        pending = 0

        for s in ships:
            inv = s.get('invoice_no', '').replace('-', '')
            if not inv:
                continue

            track = client.get_tracking(inv)
            if track.get('ok') and track.get('steps'):
                status = track['status']
                confirmed += 1
                results.append({
                    'order_no': s['order_no'],
                    'invoice_no': s['invoice_no'],
                    'status': status,
                    'confirmed': True,
                })
            else:
                pending += 1
                results.append({
                    'order_no': s['order_no'],
                    'invoice_no': s['invoice_no'],
                    'status': '예약접수(미집화)',
                    'confirmed': False,
                })

            time.sleep(0.2)  # CJ rate limit

        return {
            'total': len(results),
            'confirmed': confirmed,
            'pending': pending,
            'results': results,
        }

    except Exception as e:
        logger.error(f'[CJShipping] 접수확인 오류: {e}', exc_info=True)
        return {'total': 0, 'confirmed': 0, 'pending': 0, 'error': str(e)}


def _split_address(full_address: str) -> dict:
    """전체 주소를 주소/상세주소/우편번호로 분리."""
    import re
    addr = str(full_address or '').strip()
    zipcode = ''

    # 우편번호 추출 (5자리 숫자)
    zip_match = re.search(r'\b(\d{5})\b', addr)
    if zip_match:
        zipcode = zip_match.group(1)
        addr = addr.replace(zip_match.group(0), '').strip()

    # 상세주소 분리 (아파트, 동/호 이후)
    detail = ''
    detail_patterns = [
        r'(\d+동\s*\d+호.*)',
        r'(\d+층.*)',
        r'(아파트\s*\d+.*)',
    ]
    for pat in detail_patterns:
        m = re.search(pat, addr)
        if m:
            detail = m.group(1).strip()
            addr = addr[:m.start()].strip()
            break

    if not detail and len(addr) > 30:
        # 공백 기준으로 뒤쪽을 상세주소로
        parts = addr.rsplit(' ', 1)
        if len(parts) == 2:
            addr = parts[0]
            detail = parts[1]

    return {
        'zipcode': zipcode,
        'address': addr,
        'detail_address': detail,
    }
