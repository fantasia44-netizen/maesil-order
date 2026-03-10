"""
cafe24_client.py — Cafe24 EC API 클라이언트.

인증: OAuth2 Authorization Code + Refresh Token
Base URL: https://{mallId}.cafe24api.com
정산 API 미제공 → 주문 교차검증 전용.
"""
import logging
from datetime import datetime, timezone, timedelta

from .base_client import MarketplaceBaseClient

logger = logging.getLogger(__name__)


class Cafe24Client(MarketplaceBaseClient):
    """Cafe24 EC API."""

    CHANNEL_NAME = '자사몰'

    @property
    def _base_url(self) -> str:
        mall_id = self.config.get('mall_id', '')
        return f'https://{mall_id}.cafe24api.com'

    @property
    def is_ready(self) -> bool:
        return (self.is_active
                and bool(self.config.get('client_id'))
                and bool(self.config.get('client_secret'))
                and bool(self.config.get('mall_id')))

    # ── 인증 ──

    def get_auth_url(self, redirect_uri: str, state: str = '') -> str:
        """OAuth2 인증 URL 생성 (최초 연결용, 사용자가 브라우저에서 승인)."""
        mall_id = self.config.get('mall_id', '')
        client_id = self.config.get('client_id', '')
        return (
            f'https://{mall_id}.cafe24api.com/api/v2/oauth/authorize'
            f'?response_type=code'
            f'&client_id={client_id}'
            f'&redirect_uri={redirect_uri}'
            f'&scope=mall.read_order,mall.read_product,mall.read_store'
            f'&state={state}'
        )

    def exchange_code(self, db, code: str, redirect_uri: str) -> bool:
        """인가 코드 → 액세스 토큰 교환."""
        try:
            resp = self.session.post(
                f'{self._base_url}/api/v2/oauth/token',
                data={
                    'grant_type': 'authorization_code',
                    'code': code,
                    'redirect_uri': redirect_uri,
                },
                auth=(self.config['client_id'], self.config['client_secret']),
                timeout=15,
            )

            if resp.status_code != 200:
                logger.error(f'[Cafe24] 코드 교환 실패: {resp.status_code}')
                return False

            data = resp.json()
            return self._save_tokens(db, data)

        except Exception as e:
            logger.error(f'[Cafe24] 코드 교환 오류: {e}')
            return False

    def refresh_token(self, db) -> bool:
        """리프레시 토큰으로 액세스 토큰 갱신."""
        if not self.config.get('refresh_token'):
            logger.warning('[Cafe24] refresh_token 없음 — OAuth 인증 필요')
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

        try:
            resp = self.session.post(
                f'{self._base_url}/api/v2/oauth/token',
                data={
                    'grant_type': 'refresh_token',
                    'refresh_token': self.config['refresh_token'],
                },
                auth=(self.config['client_id'], self.config['client_secret']),
                timeout=15,
            )

            if resp.status_code != 200:
                logger.error(f'[Cafe24] 토큰 갱신 실패: {resp.status_code}')
                return False

            data = resp.json()
            return self._save_tokens(db, data)

        except Exception as e:
            logger.error(f'[Cafe24] 토큰 갱신 오류: {e}')
            return False

    def _save_tokens(self, db, data: dict) -> bool:
        access_token = data.get('access_token', '')
        refresh_token = data.get('refresh_token', '')
        expires_in = int(data.get('expires_in', 7200))

        new_expires = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

        self.config['access_token'] = access_token
        if refresh_token:
            self.config['refresh_token'] = refresh_token
        self.config['token_expires_at'] = new_expires.isoformat()

        updates = {
            'access_token': access_token,
            'token_expires_at': new_expires.isoformat(),
        }
        if refresh_token:
            updates['refresh_token'] = refresh_token

        self.update_config(db, updates)
        logger.info(f'[Cafe24] 토큰 갱신 성공, 만료: {new_expires}')
        return True

    def _get_headers(self) -> dict:
        return {
            'Authorization': f'Bearer {self.config.get("access_token", "")}',
            'Content-Type': 'application/json',
            'X-Cafe24-Api-Version': '2024-03-01',
        }

    # ── 주문 조회 ──

    def fetch_orders(self, date_from: str, date_to: str) -> list:
        """주문 목록 조회."""
        if not self.config.get('access_token'):
            logger.warning('[Cafe24] 액세스 토큰 없음')
            return []

        all_orders = []
        offset = 0
        limit = 100

        while True:
            params = {
                'start_date': date_from,
                'end_date': date_to,
                'limit': limit,
                'offset': offset,
            }

            try:
                resp = self.session.get(
                    f'{self._base_url}/api/v2/admin/orders',
                    headers=self._get_headers(),
                    params=params,
                    timeout=30,
                )

                if self._handle_rate_limit(resp):
                    continue

                if resp.status_code == 401:
                    logger.warning('[Cafe24] 인증 만료')
                    break

                if resp.status_code != 200:
                    logger.error(f'[Cafe24] 주문 조회 실패: {resp.status_code}')
                    break

                data = resp.json()
                orders = data.get('orders', [])
                if not orders:
                    break

                for order in orders:
                    # Cafe24 주문은 items 안에 여러 상품
                    items = order.get('items', [order])
                    for item in items:
                        all_orders.append(self._normalize_order(order, item))

                if len(orders) < limit:
                    break
                offset += limit

            except Exception as e:
                logger.error(f'[Cafe24] 주문 조회 오류: {e}')
                break

        logger.info(f'[Cafe24] 주문 {len(all_orders)}건 조회')
        return all_orders

    # ── 정산 ──

    def fetch_settlements(self, date_from: str, date_to: str) -> list:
        """Cafe24는 정산 API 미제공."""
        logger.info('[Cafe24] 정산 API 미제공 — 엑셀/수동 유지')
        return []

    # ── 정규화 ──

    def _normalize_order(self, order: dict, item: dict = None) -> dict:
        """API 응답 → api_orders 스키마."""
        item = item or order

        return {
            'channel': self.CHANNEL_NAME,
            'api_order_id': str(order.get('order_id', '')),
            'api_line_id': str(item.get('order_item_code', '')),
            'order_date': str(order.get('order_date', ''))[:10],
            'product_name': item.get('product_name', ''),
            'option_name': item.get('option_value', ''),
            'qty': int(item.get('quantity', 0)),
            'unit_price': int(float(item.get('product_price', 0))),
            'total_amount': int(float(item.get('payment_amount',
                                               order.get('actual_payment_amount', 0)))),
            'discount_amount': int(float(item.get('discount_amount', 0))),
            'settlement_amount': 0,  # Cafe24 API 미제공
            'commission': 0,         # Cafe24 API 미제공
            'shipping_fee': int(float(order.get('shipping_fee', 0))),
            'fee_detail': {},
            'order_status': order.get('order_status', ''),
            'raw_data': {'order': order, 'item': item},
            'raw_hash': self.compute_raw_hash({'order': order, 'item': item}),
        }

    def test_connection(self, db) -> dict:
        """API 연결 테스트."""
        if not self.is_ready:
            return {'success': False, 'message': 'client_id/secret/mall_id 미설정'}

        if not self.config.get('refresh_token'):
            auth_url = self.get_auth_url('https://localhost/callback')
            return {
                'success': False,
                'message': 'OAuth 인증 필요',
                'auth_url': auth_url,
            }

        token_ok = self.refresh_token(db)
        if not token_ok:
            return {'success': False, 'message': '토큰 갱신 실패'}

        return {'success': True, 'message': 'Cafe24 API 연결 성공'}
