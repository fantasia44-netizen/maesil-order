"""
api_order_converter.py — API raw_data → 채널별 엑셀 DataFrame 변환.

marketplace.py, orders.py 양쪽에서 공유하는 변환 로직.
OrderProcessor가 인식하는 엑셀 컬럼명으로 변환하여
채널별 모든 차이점(옵션/단일상품/공백 등)이 동일하게 적용되도록 함.
"""
import pandas as pd

from services.channel_config import is_naver


def sanitize_receiver_name(name, fallback_name=''):
    """수취인명 검증 — CJ 택배 거부 방지.

    '집', '집.', '회사', 1글자 등 비정상 이름은 주문자명으로 대체.
    """
    _INVALID = {'집', '집.', '회사', '회사.', '사무실', '사무실.', '경비실',
                '문앞', '현관', '-', '.', '..', '본인', '자택'}
    n = str(name or '').strip()
    fb = str(fallback_name or '').strip()
    if not n or n in _INVALID or len(n) == 1:
        return fb or n
    return n


def api_orders_to_excel_df(orders, channel):
    """API raw_data → 채널별 엑셀 컬럼 형식 DataFrame.

    OrderProcessor가 인식하는 엑셀 컬럼명으로 변환하여
    채널별 모든 차이점(옵션/단일상품/공백 등)이 동일하게 적용되도록 함.
    """
    rows = []

    for o in orders:
        raw = o.get('raw_data', {})

        if is_naver(channel):
            po = raw.get('productOrder', {})
            if po.get('deliveryAttributeType') == 'ARRIVAL_GUARANTEE':
                continue
            sa = po.get('shippingAddress', {})
            rows.append({
                '상품주문번호': str(po.get('productOrderId', '')),
                '주문번호': str(raw.get('order', {}).get('orderId', '')),
                '주문일시': str(raw.get('order', {}).get('orderDate', ''))[:16],
                '상품명': po.get('productName', ''),
                '옵션정보': po.get('productOption', ''),
                '수량': int(po.get('quantity', 0)),
                '수취인명': sanitize_receiver_name(
                    sa.get('name', ''),
                    raw.get('order', {}).get('ordererName', '')),
                '수취인연락처1': sa.get('tel1', ''),
                '수취인연락처2': sa.get('tel2', ''),
                '기본배송지': sa.get('baseAddress', ''),
                '상세배송지': sa.get('detailedAddress', ''),
                '배송메세지': po.get('shippingMemo', ''),
                '주문상태': po.get('productOrderStatus', ''),
                '상품가격': int(po.get('unitPrice', 0)),
                '최종 상품별 총 주문금액': int(po.get('totalPaymentAmount', 0)),
                '정산예정금액': int(po.get('expectedSettlementAmount', 0)),
                '배송비 합계': int(po.get('deliveryFeeAmount', 0)),
                '배송속성': po.get('deliveryAttributeType', ''),
            })

        elif channel == '쿠팡':
            items = raw.get('orderItems', [])
            recv = raw.get('receiver', {})
            line_id = str(o.get('api_line_id', ''))
            matched_items = [it for it in items if str(it.get('vendorItemId', '')) == line_id] if line_id else []
            is_first_line = (items and str(items[0].get('vendorItemId', '')) == line_id)
            for item in (matched_items[:1] or items[:1]):
                rows.append({
                    '주문번호': str(raw.get('orderId', '')),
                    '묶음배송번호': str(raw.get('shipmentBoxId', '')),
                    '주문일': str(raw.get('orderedAt', ''))[:10],
                    '등록상품명': item.get('sellerProductName', item.get('vendorItemName', '')),
                    '등록옵션명': item.get('sellerProductItemName', ''),
                    '노출상품명': item.get('vendorItemName', ''),
                    '구매수(수량)': int(item.get('shippingCount', 0)),
                    '수취인이름': sanitize_receiver_name(
                        recv.get('name', ''),
                        raw.get('orderer', {}).get('name', '')),
                    '수취인전화번호': recv.get('safeNumber', recv.get('receiverNumber', '')),
                    '수취인 주소': f"{recv.get('addr1', '')} {recv.get('addr2', '')}".strip(),
                    '배송메세지': raw.get('parcelPrintMessage', ''),
                    '주문상태명': raw.get('status', ''),
                    '옵션판매가(판매단가)': int(item.get('salesPrice', 0)),
                    '결제액': int(item.get('orderPrice', 0)),
                    '배송비': int(raw.get('shippingPrice', 0)) if is_first_line else 0,
                })

        elif channel == '자사몰':
            item = raw.get('item', raw)
            order = raw.get('order', raw)
            receivers = order.get('receivers', [])
            rcv = receivers[0] if receivers else {}
            rows.append({
                '쇼핑몰번호': '1',
                '주문번호': str(order.get('order_id', '')),
                '발주일': str(order.get('order_date', ''))[:10],
                '주문상품명': item.get('product_name', ''),
                '옵션정보': item.get('option_value', '') or o.get('option_name', ''),
                '수량': int(item.get('quantity', item.get('qty', 1)) or 1),
                '수령인': sanitize_receiver_name(
                    rcv.get('name', rcv.get('receiver_name',
                             order.get('shipping_name', order.get('receiver_name', '')))),
                    order.get('buyer_name', order.get('member_name', ''))),
                '핸드폰': rcv.get('cellphone', rcv.get('receiver_cellphone',
                         order.get('shipping_phone', order.get('receiver_phone', '')))),
                '수령지전화': rcv.get('phone', rcv.get('receiver_phone',
                             order.get('shipping_cellphone', ''))),
                '주소': (rcv.get('address1', '') + ' ' + rcv.get('address2', '')).strip()
                       or order.get('shipping_address', order.get('receiver_address', '')),
                '비고': rcv.get('shipping_message', order.get('shipping_memo', '')),
                '판매가': int(float(item.get('product_price', 0) or 0)),
                '결제금액': int(float(item.get('payment_amount',
                               item.get('actual_payment_amount', 0)) or 0)),
                '배송비': int(float(order.get('shipping_fee', 0) or 0)),
            })

    return pd.DataFrame(rows) if rows else pd.DataFrame()
