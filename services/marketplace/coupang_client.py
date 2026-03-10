"""
coupang_client.py — 쿠팡 Wing (Open API) 클라이언트.

인증: HMAC-SHA256 per-request (토큰 없음, 매 요청마다 서명)
Base URL: https://api-gateway.coupang.com
"""
import hashlib
import hmac
import logging
from datetime import datetime, timezone

from .base_client import MarketplaceBaseClient

logger = logging.getLogger(__name__)


class CoupangWingClient(MarketplaceBaseClient):
    """쿠팡 Wing API."""

    CHANNEL_NAME = '쿠팡'
    BASE_URL = 'https://api-gateway.coupang.com'

    @property
    def is_ready(self) -> bool:
        return (self.is_active
                and bool(self.config.get('client_id'))      # access_key
                and bool(self.config.get('client_secret'))   # secret_key
                and bool(self.config.get('vendor_id')))

    # ── 인증 (HMAC-SHA256) ──

    def _generate_hmac_signature(self, method: str, url_with_query: str) -> dict:
        """HMAC-SHA256 서명 + Authorization 헤더 생성.

        쿠팡 인증 방식:
        message = datetime + method + path + querystring (? 제외)
        signature = HMAC-SHA256(secret_key, message)
        """
        access_key = self.config['client_id']
        secret_key = self.config['client_secret']

        from time import gmtime, strftime
        dt = strftime('%y%m%d', gmtime()) + 'T' + strftime('%H%M%S', gmtime()) + 'Z'

        # URL에서 path와 query 분리 (? 제외)
        path, *query = url_with_query.split('?')
        message = dt + method.upper() + path + (query[0] if query else '')

        signature = hmac.new(
            secret_key.encode('utf-8'),
            message.encode('utf-8'),
            hashlib.sha256,
        ).hexdigest()

        auth_header = (
            f'CEA algorithm=HmacSHA256, '
            f'access-key={access_key}, '
            f'signed-date={dt}, '
            f'signature={signature}'
        )

        return {
            'Authorization': auth_header,
            'Content-Type': 'application/json;charset=UTF-8',
        }

    def refresh_token(self, db) -> bool:
        """쿠팡은 HMAC 방식이므로 토큰 리프레시 불필요."""
        return True

    # ── 주문 조회 ──

    @staticmethod
    def _build_query(params: dict) -> str:
        """파라미터 dict → 정렬된 쿼리 문자열 (URL 인코딩 없이)."""
        parts = [f'{k}={v}' for k, v in sorted(params.items())]
        return '&'.join(parts)

    # 쿠팡 주문 상태 — 전체 조회를 위해 모든 상태 순회
    ORDER_STATUSES = [
        'ACCEPT',          # 발주확인
        'INSTRUCT',        # 상품준비중
        'DEPARTURE',       # 배송지시/출고
        'DELIVERING',      # 배송중
        'FINAL_DELIVERY',  # 배송완료
    ]

    def fetch_orders(self, date_from: str, date_to: str) -> list:
        """주문 목록 조회 — 모든 상태를 순회하여 전체 주문 수집."""
        vendor_id = self.config.get('vendor_id', '')
        if not vendor_id:
            logger.warning('[쿠팡] vendor_id 미설정')
            return []

        path = f'/v2/providers/openapi/apis/api/v4/vendors/{vendor_id}/ordersheets'
        all_orders = []

        for status in self.ORDER_STATUSES:
            next_token = ''
            status_count = 0

            while True:
                params = {
                    'createdAtFrom': date_from,
                    'createdAtTo': date_to,
                    'maxPerPage': 50,
                    'status': status,
                }
                if next_token:
                    params['nextToken'] = next_token

                query_str = self._build_query(params)
                full_url = f'{path}?{query_str}'
                headers = self._generate_hmac_signature('GET', full_url)

                try:
                    resp = self.session.get(
                        f'{self.BASE_URL}{full_url}',
                        headers=headers,
                        timeout=30,
                    )

                    if self._handle_rate_limit(resp):
                        continue

                    if resp.status_code != 200:
                        logger.error(f'[쿠팡] 주문 조회 실패 ({status}): '
                                     f'{resp.status_code} {resp.text[:200]}')
                        break

                    data = resp.json().get('data', [])
                    if not data:
                        break

                    for item in data:
                        all_orders.extend(self._normalize_order(item))
                    status_count += len(data)

                    next_token = resp.json().get('nextToken', '')
                    if not next_token:
                        break

                except Exception as e:
                    logger.error(f'[쿠팡] 주문 조회 오류 ({status}): {e}')
                    break

            if status_count:
                logger.info(f'[쿠팡] {status}: {status_count}건')

        logger.info(f'[쿠팡] 주문 총 {len(all_orders)}건 조회')
        return all_orders

    # ── 정산 조회 ──

    def fetch_settlements(self, date_from: str, date_to: str) -> list:
        """정산 내역 조회."""
        vendor_id = self.config.get('vendor_id', '')
        if not vendor_id:
            return []

        path = f'/v2/providers/openapi/apis/api/v4/vendors/{vendor_id}/settlements'
        settlements = []

        params = {'endDate': date_to, 'startDate': date_from}
        query_str = self._build_query(params)
        full_url = f'{path}?{query_str}'
        headers = self._generate_hmac_signature('GET', full_url)

        try:
            resp = self.session.get(
                f'{self.BASE_URL}{full_url}',
                headers=headers,
                timeout=30,
            )

            if resp.status_code != 200:
                logger.error(f'[쿠팡] 정산 조회 실패: {resp.status_code}')
                return []

            data = resp.json().get('data', [])
            for item in data:
                settlements.append(self._normalize_settlement(item))

        except Exception as e:
            logger.error(f'[쿠팡] 정산 조회 오류: {e}')

        logger.info(f'[쿠팡] 정산 {len(settlements)}건 조회')
        return settlements

    # ── 정규화 ──

    def _normalize_order(self, raw: dict) -> list:
        """API 응답 → api_orders 스키마 (주문당 여러 상품 → 여러 row).

        쿠팡 ordersheets는 shipmentBox 단위이며,
        각 box 안에 orderItems 리스트가 있음.
        """
        order_id = str(raw.get('orderId', ''))
        order_date = str(raw.get('orderedAt', ''))[:10]
        status = raw.get('status', '')
        shipping_fee = int(raw.get('shippingPrice', 0))

        items = raw.get('orderItems', [])
        rows = []

        for item in items:
            rows.append({
                'channel': self.CHANNEL_NAME,
                'api_order_id': order_id,
                'api_line_id': str(item.get('vendorItemId', '')),
                'order_date': order_date,
                'product_name': item.get('vendorItemName', ''),
                'option_name': item.get('sellerProductItemName', ''),
                'qty': int(item.get('shippingCount', 0)),
                'unit_price': int(item.get('salesPrice', 0)),
                'total_amount': int(item.get('orderPrice', 0)),
                'discount_amount': int(item.get('discountPrice', 0)),
                'settlement_amount': 0,  # ordersheets에는 정산금 미포함
                'commission': 0,
                'shipping_fee': shipping_fee if items.index(item) == 0 else 0,
                'fee_detail': {
                    'coupang_discount': int(item.get('coupangDiscount', 0)),
                    'instant_coupon': int(item.get('instantCouponDiscount', 0)),
                    'downloadable_coupon': int(item.get('downloadableCouponDiscount', 0)),
                },
                'order_status': status,
                'raw_data': raw,
                'raw_hash': self.compute_raw_hash(item),
            })

        return rows

    def _normalize_settlement(self, raw: dict) -> dict:
        """정산 API 응답 → api_settlements 스키마."""
        return {
            'channel': self.CHANNEL_NAME,
            'settlement_date': str(raw.get('settleDate', ''))[:10],
            'settlement_id': str(raw.get('settlementId', '')),
            'gross_sales': int(raw.get('salesAmount', 0)),
            'total_commission': int(raw.get('commissionAmount', 0)),
            'shipping_fee_income': int(raw.get('shippingFeeIncome', 0)),
            'shipping_fee_cost': int(raw.get('shippingFeeCost', 0)),
            'coupon_discount': int(raw.get('couponDiscount', 0)),
            'point_discount': int(raw.get('pointDiscount', 0)),
            'other_deductions': int(raw.get('otherDeductions', 0)),
            'net_settlement': int(raw.get('settleAmount', 0)),
            'fee_breakdown': {
                'coupang_fee_rate': float(raw.get('feeRate', 0)),
                'coupang_fee': int(raw.get('commissionAmount', 0)),
                'rocket_fee': int(raw.get('rocketServiceFee', 0)),
            },
            'raw_data': raw,
        }

    def test_connection(self, db) -> dict:
        """API 연결 테스트 — 간단한 주문 조회."""
        if not self.is_ready:
            return {'success': False, 'message': 'access_key/secret_key/vendor_id 미설정'}

        vendor_id = self.config['vendor_id']
        path = f'/v2/providers/openapi/apis/api/v4/vendors/{vendor_id}/ordersheets'

        today = datetime.now().strftime('%Y-%m-%d')
        params = {
            'createdAtFrom': today,
            'createdAtTo': today,
            'maxPerPage': 1,
            'status': 'ACCEPT',
        }
        query_str = self._build_query(params)
        full_url = f'{path}?{query_str}'
        headers = self._generate_hmac_signature('GET', full_url)

        try:
            resp = self.session.get(
                f'{self.BASE_URL}{full_url}',
                headers=headers,
                timeout=15,
            )
            if resp.status_code == 200:
                return {'success': True, 'message': '쿠팡 Wing API 연결 성공'}
            return {'success': False, 'message': f'HTTP {resp.status_code}: {resp.text[:200]}'}
        except Exception as e:
            return {'success': False, 'message': str(e)}
