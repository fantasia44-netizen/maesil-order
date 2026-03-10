"""
naver_client.py — 네이버 스마트스토어 커머스 API 클라이언트.

인증: OAuth2 Client Credentials (client_id + timestamp HMAC 서명)
Base URL: https://api.commerce.naver.com
"""
import base64
import logging
import time
from datetime import datetime, timezone, timedelta

from .base_client import MarketplaceBaseClient

logger = logging.getLogger(__name__)

# bcrypt 라이브러리는 선택적 (네이버 인증에는 HMAC 사용)
try:
    import bcrypt
    HAS_BCRYPT = True
except ImportError:
    HAS_BCRYPT = False


class NaverCommerceClient(MarketplaceBaseClient):
    """네이버 스마트스토어 커머스 API."""

    CHANNEL_NAME = '스마트스토어'
    BASE_URL = 'https://api.commerce.naver.com'

    @property
    def is_ready(self) -> bool:
        return (self.is_active
                and bool(self.config.get('client_id'))
                and bool(self.config.get('client_secret')))

    # ── 인증 ──

    def _generate_signature(self) -> tuple:
        """네이버 커머스 API 서명 생성.

        client_secret은 bcrypt salt 형식($2a$04$...).
        message = client_id + "_" + timestamp(ms)
        signature = bcrypt.hashpw(message, client_secret)

        Returns:
            (timestamp_ms_str, signature_str)
        """
        client_id = self.config['client_id']
        client_secret = self.config['client_secret']
        timestamp = str(int(time.time() * 1000))

        message = f'{client_id}_{timestamp}'

        if not HAS_BCRYPT:
            raise RuntimeError('bcrypt 패키지가 필요합니다: pip install bcrypt')

        # client_secret 자체가 bcrypt salt — 직접 salt로 사용
        hashed = bcrypt.hashpw(
            message.encode('utf-8'),
            client_secret.encode('utf-8'),
        )
        # bcrypt 해시를 base64 인코딩
        signature = base64.b64encode(hashed).decode('utf-8')
        return timestamp, signature

    def refresh_token(self, db) -> bool:
        """OAuth2 토큰 발급/갱신."""
        if not self.is_ready:
            logger.warning('[네이버] client_id/secret 미설정')
            return False

        # 토큰이 아직 유효하면 스킵
        expires = self.config.get('token_expires_at')
        if expires:
            if isinstance(expires, str):
                try:
                    expires = datetime.fromisoformat(expires.replace('Z', '+00:00'))
                except ValueError:
                    expires = None
            if expires and expires > datetime.now(timezone.utc) + timedelta(minutes=5):
                return True

        timestamp, signature = self._generate_signature()

        try:
            resp = self.session.post(
                f'{self.BASE_URL}/external/v1/oauth2/token',
                data={
                    'client_id': self.config['client_id'],
                    'timestamp': timestamp,
                    'client_secret_sign': signature,
                    'grant_type': 'client_credentials',
                    'type': 'SELF',
                },
                timeout=15,
            )

            if resp.status_code != 200:
                logger.error(f'[네이버] 토큰 발급 실패: {resp.status_code} {resp.text}')
                return False

            data = resp.json()
            access_token = data.get('access_token', '')
            expires_in = int(data.get('expires_in', 86400))

            new_expires = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

            self.config['access_token'] = access_token
            self.config['token_expires_at'] = new_expires.isoformat()

            # DB 업데이트
            self.update_config(db, {
                'access_token': access_token,
                'token_expires_at': new_expires.isoformat(),
            })

            logger.info(f'[네이버] 토큰 발급 성공, 만료: {new_expires}')
            return True

        except Exception as e:
            logger.error(f'[네이버] 토큰 발급 오류: {e}')
            return False

    def _get_headers(self) -> dict:
        return {
            'Authorization': f'Bearer {self.config.get("access_token", "")}',
            'Content-Type': 'application/json',
        }

    # ── 주문 조회 ──

    def fetch_orders(self, date_from: str, date_to: str) -> list:
        """변경 주문 조회 + 상세 일괄조회."""
        if not self.config.get('access_token'):
            logger.warning('[네이버] 액세스 토큰 없음')
            return []

        # 1단계: 변경된 주문 ID 목록 가져오기
        product_order_ids = self._fetch_changed_order_ids(date_from, date_to)
        if not product_order_ids:
            return []

        # 2단계: 상세 일괄조회 (최대 300개씩)
        all_orders = []
        batch_size = 300
        for i in range(0, len(product_order_ids), batch_size):
            batch = product_order_ids[i:i + batch_size]
            details = self._fetch_order_details(batch)
            all_orders.extend(details)

        return all_orders

    def _fetch_changed_order_ids(self, date_from: str, date_to: str) -> list:
        """변경 주문 ID 목록 조회 (페이지네이션)."""
        ids = []
        last_changed_from = f'{date_from}T00:00:00.000+09:00'
        last_changed_to = f'{date_to}T23:59:59.999+09:00'

        url = f'{self.BASE_URL}/external/v1/pay-order/seller/orders/last-changed-statuses'
        params = {
            'lastChangedFrom': last_changed_from,
            'lastChangedTo': last_changed_to,
        }

        while True:
            try:
                resp = self.session.get(url, headers=self._get_headers(),
                                        params=params, timeout=30)
                if self._handle_rate_limit(resp):
                    continue
                if resp.status_code != 200:
                    logger.error(f'[네이버] 변경주문 조회 실패: {resp.status_code}')
                    break

                data = resp.json().get('data', {})
                statuses = data.get('lastChangeStatuses', [])
                for s in statuses:
                    pid = s.get('productOrderId', '')
                    if pid and pid not in ids:
                        ids.append(pid)

                # 다음 페이지
                more = data.get('more', {})
                if more.get('moreSequence'):
                    params['lastChangedFrom'] = more['moreSequence']
                else:
                    break

            except Exception as e:
                logger.error(f'[네이버] 변경주문 조회 오류: {e}')
                break

        logger.info(f'[네이버] 변경주문 {len(ids)}건 발견')
        return ids

    def _fetch_order_details(self, product_order_ids: list) -> list:
        """주문 상세 일괄조회."""
        url = f'{self.BASE_URL}/external/v1/pay-order/seller/orders/query'

        try:
            resp = self.session.post(
                url,
                headers=self._get_headers(),
                json={'productOrderIds': product_order_ids},
                timeout=30,
            )

            if self._handle_rate_limit(resp):
                resp = self.session.post(
                    url,
                    headers=self._get_headers(),
                    json={'productOrderIds': product_order_ids},
                    timeout=30,
                )

            if resp.status_code != 200:
                logger.error(f'[네이버] 주문상세 조회 실패: {resp.status_code}')
                return []

            data = resp.json().get('data', [])
            return [self._normalize_order(item) for item in data]

        except Exception as e:
            logger.error(f'[네이버] 주문상세 조회 오류: {e}')
            return []

    # ── 정산 조회 ──

    def fetch_settlements(self, date_from: str, date_to: str) -> list:
        """정산 내역 조회."""
        if not self.config.get('access_token'):
            return []

        url = f'{self.BASE_URL}/external/v1/pay-order/seller/settlements'
        settlements = []

        params = {
            'startDate': date_from,
            'endDate': date_to,
            'pageSize': 100,
            'page': 1,
        }

        while True:
            try:
                resp = self.session.get(url, headers=self._get_headers(),
                                        params=params, timeout=30)
                if self._handle_rate_limit(resp):
                    continue
                if resp.status_code != 200:
                    logger.error(f'[네이버] 정산 조회 실패: {resp.status_code}')
                    break

                data = resp.json().get('data', {})
                items = data.get('settlements', data.get('list', []))

                for item in items:
                    settlements.append(self._normalize_settlement(item))

                # 페이지네이션
                total_pages = data.get('totalPages', 1)
                if params['page'] >= total_pages:
                    break
                params['page'] += 1

            except Exception as e:
                logger.error(f'[네이버] 정산 조회 오류: {e}')
                break

        logger.info(f'[네이버] 정산 {len(settlements)}건 조회')
        return settlements

    # ── 정규화 ──

    def _normalize_order(self, raw: dict) -> dict:
        """API 응답 → api_orders 스키마."""
        order_info = raw.get('order', raw)
        product_order = raw.get('productOrder', raw)

        return {
            'channel': self.CHANNEL_NAME,
            'api_order_id': str(order_info.get('orderId', '')),
            'api_line_id': str(product_order.get('productOrderId', '')),
            'order_date': str(order_info.get('orderDate', ''))[:10],
            'product_name': product_order.get('productName', ''),
            'option_name': product_order.get('optionContent', ''),
            'qty': int(product_order.get('quantity', 0)),
            'unit_price': int(product_order.get('unitPrice', 0)),
            'total_amount': int(product_order.get('totalPaymentAmount', 0)),
            'discount_amount': int(product_order.get('totalDiscountAmount', 0)),
            'settlement_amount': int(product_order.get('expectedSettlementAmount', 0)),
            'commission': int(product_order.get('commissionAmount', 0)),
            'shipping_fee': int(product_order.get('deliveryFeeAmount', 0)),
            'fee_detail': {
                'naverpay_fee': int(product_order.get('naverPayFee', 0)),
                'sales_linked_fee': int(product_order.get('salesLinkedFee', 0)),
                'seller_burden_discount': int(product_order.get('sellerBurdenDiscount', 0)),
                'knowledge_shopping_fee': int(product_order.get('knowledgeShoppingFee', 0)),
            },
            'order_status': product_order.get('productOrderStatus', ''),
            'raw_data': raw,
            'raw_hash': self.compute_raw_hash(raw),
        }

    def _normalize_settlement(self, raw: dict) -> dict:
        """정산 API 응답 → api_settlements 스키마."""
        return {
            'channel': self.CHANNEL_NAME,
            'settlement_date': str(raw.get('settleDate', raw.get('settlementDate', '')))[:10],
            'settlement_id': str(raw.get('settlementId', '')),
            'gross_sales': int(raw.get('totalSalesAmount', raw.get('grossSales', 0))),
            'total_commission': int(raw.get('totalCommission',
                                            raw.get('platformFee', 0))),
            'shipping_fee_income': int(raw.get('deliveryFeeIncome', 0)),
            'shipping_fee_cost': int(raw.get('deliveryFeeCost', 0)),
            'coupon_discount': int(raw.get('couponDiscount', 0)),
            'point_discount': int(raw.get('pointDiscount', 0)),
            'other_deductions': int(raw.get('otherDeductions', 0)),
            'net_settlement': int(raw.get('settleAmount',
                                          raw.get('netSettlement', 0))),
            'fee_breakdown': {
                'naverpay_fee': int(raw.get('naverPayFee', 0)),
                'sales_linked_fee': int(raw.get('salesLinkedFee', 0)),
                'knowledge_shopping_fee': int(raw.get('knowledgeShoppingFee', 0)),
            },
            'raw_data': raw,
        }

    def test_connection(self, db) -> dict:
        """API 연결 테스트."""
        if not self.is_ready:
            return {'success': False, 'message': 'client_id/secret 미설정'}

        token_ok = self.refresh_token(db)
        if not token_ok:
            return {'success': False, 'message': '토큰 발급 실패'}

        return {'success': True, 'message': '네이버 커머스 API 연결 성공'}
