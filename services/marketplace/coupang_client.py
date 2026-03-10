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

    def _generate_hmac_signature(self, method: str, path: str, query: str = '') -> dict:
        """HMAC-SHA256 서명 + Authorization 헤더 생성.

        쿠팡 인증 방식:
        message = datetime + method + path + query
        signature = HMAC-SHA256(secret_key, message)
        Authorization: CEA algorithm=HmacSHA256, access-key={}, signed-date={}, signature={}
        """
        access_key = self.config['client_id']
        secret_key = self.config['client_secret']

        dt = datetime.now(timezone.utc).strftime('%y%m%dT%H%M%SZ')

        # 메시지 생성
        message = dt + method.upper() + path
        if query:
            message += query

        # HMAC-SHA256 서명
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
            'X-Requested-By': 'autotool',
        }

    def refresh_token(self, db) -> bool:
        """쿠팡은 HMAC 방식이므로 토큰 리프레시 불필요."""
        return True

    # ── 주문 조회 ──

    def fetch_orders(self, date_from: str, date_to: str) -> list:
        """주문 목록 조회."""
        vendor_id = self.config.get('vendor_id', '')
        if not vendor_id:
            logger.warning('[쿠팡] vendor_id 미설정')
            return []

        path = f'/v2/providers/openapi/apis/api/v4/vendors/{vendor_id}/ordersheets'

        all_orders = []
        next_token = ''

        created_from = f'{date_from}T00:00:00'
        created_to = f'{date_to}T23:59:59'

        while True:
            params = {
                'createdAtFrom': created_from,
                'createdAtTo': created_to,
                'maxPerPage': 50,
                'status': 'ACCEPT',
            }
            if next_token:
                params['nextToken'] = next_token

            # 쿼리 문자열 생성 (서명용)
            query_parts = [f'{k}={v}' for k, v in sorted(params.items())]
            query_str = '?' + '&'.join(query_parts) if query_parts else ''

            headers = self._generate_hmac_signature('GET', path, query_str)

            try:
                resp = self.session.get(
                    f'{self.BASE_URL}{path}',
                    headers=headers,
                    params=params,
                    timeout=30,
                )

                if self._handle_rate_limit(resp):
                    continue

                if resp.status_code != 200:
                    logger.error(f'[쿠팡] 주문 조회 실패: {resp.status_code} {resp.text[:200]}')
                    break

                data = resp.json().get('data', [])
                if not data:
                    break

                for item in data:
                    all_orders.append(self._normalize_order(item))

                next_token = resp.json().get('nextToken', '')
                if not next_token:
                    break

            except Exception as e:
                logger.error(f'[쿠팡] 주문 조회 오류: {e}')
                break

        logger.info(f'[쿠팡] 주문 {len(all_orders)}건 조회')
        return all_orders

    # ── 정산 조회 ──

    def fetch_settlements(self, date_from: str, date_to: str) -> list:
        """정산 내역 조회."""
        vendor_id = self.config.get('vendor_id', '')
        if not vendor_id:
            return []

        path = f'/v2/providers/openapi/apis/api/v4/vendors/{vendor_id}/settlements'
        settlements = []

        params = {
            'startDate': date_from,
            'endDate': date_to,
        }
        query_parts = [f'{k}={v}' for k, v in sorted(params.items())]
        query_str = '?' + '&'.join(query_parts)

        headers = self._generate_hmac_signature('GET', path, query_str)

        try:
            resp = self.session.get(
                f'{self.BASE_URL}{path}',
                headers=headers,
                params=params,
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

    def _normalize_order(self, raw: dict) -> dict:
        """API 응답 → api_orders 스키마."""
        return {
            'channel': self.CHANNEL_NAME,
            'api_order_id': str(raw.get('orderId', '')),
            'api_line_id': str(raw.get('orderItemId', raw.get('shipmentBoxId', ''))),
            'order_date': str(raw.get('orderedAt', ''))[:10],
            'product_name': raw.get('vendorItemName', ''),
            'option_name': raw.get('sellerProductItemName', ''),
            'qty': int(raw.get('shippingCount', 0)),
            'unit_price': int(raw.get('unitPrice', 0)),
            'total_amount': int(raw.get('orderPrice', 0)),
            'discount_amount': int(raw.get('discountPrice', 0)),
            'settlement_amount': int(raw.get('settlementPrice', 0)),
            'commission': int(raw.get('commissionAmount', 0)),
            'shipping_fee': int(raw.get('shippingPrice', 0)),
            'fee_detail': {
                'coupang_discount': int(raw.get('coupangDiscount', 0)),
                'coupon_discount': int(raw.get('couponDiscount', 0)),
                'rocket_delivery': raw.get('isRocket', False),
            },
            'order_status': raw.get('status', ''),
            'raw_data': raw,
            'raw_hash': self.compute_raw_hash(raw),
        }

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
            'createdAtFrom': f'{today}T00:00:00',
            'createdAtTo': f'{today}T23:59:59',
            'maxPerPage': 1,
            'status': 'ACCEPT',
        }
        query_parts = [f'{k}={v}' for k, v in sorted(params.items())]
        query_str = '?' + '&'.join(query_parts)
        headers = self._generate_hmac_signature('GET', path, query_str)

        try:
            resp = self.session.get(
                f'{self.BASE_URL}{path}',
                headers=headers,
                params=params,
                timeout=15,
            )
            if resp.status_code == 200:
                return {'success': True, 'message': '쿠팡 Wing API 연결 성공'}
            return {'success': False, 'message': f'HTTP {resp.status_code}: {resp.text[:100]}'}
        except Exception as e:
            return {'success': False, 'message': str(e)}
