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
        from urllib.parse import quote
        mall_id = self.config.get('mall_id', '')
        client_id = self.config.get('client_id', '')
        # 앱에 등록된 scope — 쉼표 구분 (URL 인코딩하지 않음)
        scopes = ','.join([
            'mall.read_order', 'mall.write_order',
            'mall.read_product', 'mall.write_product',
            'mall.read_category', 'mall.write_category',
            'mall.read_store', 'mall.write_store',
            'mall.read_supply', 'mall.write_supply',
            'mall.read_shipping', 'mall.write_shipping',
            'mall.read_community', 'mall.write_community',
            'mall.read_salesreport',
            'mall.read_application', 'mall.write_application',
        ])
        encoded_uri = quote(redirect_uri, safe='')
        return (
            f'https://{mall_id}.cafe24api.com/api/v2/oauth/authorize'
            f'?response_type=code'
            f'&client_id={client_id}'
            f'&redirect_uri={encoded_uri}'
            f'&scope={scopes}'
            f'&state={state}'
        )

    def exchange_code(self, db, code: str, redirect_uri: str):
        """인가 코드 → 액세스 토큰 교환. 성공 시 True, 실패 시 에러 문자열."""
        try:
            logger.info(f'[Cafe24] 토큰 교환 시도: base={self._base_url}, redirect_uri={redirect_uri}')
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
                err_msg = f'HTTP {resp.status_code}: {resp.text[:300]}'
                logger.error(f'[Cafe24] 코드 교환 실패: {err_msg}')
                logger.error(f'[Cafe24] redirect_uri: {redirect_uri}')
                return err_msg

            data = resp.json()
            ok = self._save_tokens(db, data)
            return True if ok else '토큰 저장 실패'

        except Exception as e:
            logger.error(f'[Cafe24] 코드 교환 오류: {e}')
            return str(e)

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
            'X-Cafe24-Api-Version': '2025-12-01',
        }

    # ── 주문 조회 ──

    # 송장 대상 — 결제완료/상품준비중/배송보류 (출고 전)
    INVOICE_TARGET_STATUSES = ['N10', 'N20', 'N22']

    def fetch_orders(self, date_from: str, date_to: str,
                     status_filter: str = None) -> list:
        """주문 목록 조회.

        Args:
            status_filter: 'invoice_target' → N10/N20/N22만 (출고 전)
                           None → 전체 상태 수집 (기존 동작)
        """
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
                'embed': 'items,receivers',  # 상품 상세 + 배송정보 포함
            }
            if status_filter == 'invoice_target':
                params['order_status'] = ','.join(self.INVOICE_TARGET_STATUSES)

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

    # PG 수수료율 (결제수단별, extra_config에서 오버라이드 가능)
    DEFAULT_PG_FEE_RATES = {
        'card': 0.033,      # 카드: 3.3%
        'cash': 0.015,      # 계좌이체: 1.5%
        'tcash': 0.015,     # 실시간이체: 1.5%
        'cell': 0.05,       # 휴대폰결제: 5%
        'prepaid': 0.0,     # 선불결제: 0%
        'point': 0.0,       # 적립금: 0%
        'coupon': 0.0,      # 쿠폰: 0%
        'default': 0.033,   # 기타: 3.3%
    }

    def _get_pg_fee_rates(self) -> dict:
        """PG 수수료율 조회 (extra_config에서 오버라이드 가능)."""
        rates = dict(self.DEFAULT_PG_FEE_RATES)
        custom = self.config.get('extra_config', {}).get('pg_fee_rates', {})
        rates.update(custom)
        return rates

    def _estimate_pg_fee(self, payment_methods: list, payment_amount: float,
                         rates: dict) -> tuple:
        """결제수단별 PG 수수료 추정.

        카드+쿠폰 등 복합결제 시, 실결제 수단(card/cash/tcash/cell)에
        수수료율을 적용. 쿠폰/포인트는 수수료 없음.

        Returns:
            (estimated_fee, primary_method)
        """
        # 실결제 수단 찾기 (수수료 발생하는 것)
        paid_methods = [m for m in payment_methods
                        if m not in ('coupon', 'point', 'prepaid')]
        if not paid_methods:
            return 0, 'free'

        primary = paid_methods[0]
        rate = rates.get(primary, rates.get('default', 0.033))
        fee = int(payment_amount * rate)
        return fee, primary

    def fetch_settlements(self, date_from: str, date_to: str) -> list:
        """Cafe24 주문 데이터에서 일별 매출/할인/PG수수료 집계 → 정산 데이터.

        자사몰은 마켓 수수료 없음. PG 수수료를 결제수단별 수수료율로 추정 계산.
        수수료율은 DEFAULT_PG_FEE_RATES 기본값 또는 extra_config.pg_fee_rates로 설정.
        """
        if not self.config.get('access_token'):
            return []

        # 전체 주문 조회
        all_orders = []
        offset = 0
        limit = 100
        while True:
            try:
                resp = self.session.get(
                    f'{self._base_url}/api/v2/admin/orders',
                    headers=self._get_headers(),
                    params={'start_date': date_from, 'end_date': date_to,
                            'limit': limit, 'offset': offset},
                    timeout=30,
                )
                if self._handle_rate_limit(resp):
                    continue
                if resp.status_code != 200:
                    break
                orders = resp.json().get('orders', [])
                if not orders:
                    break
                all_orders.extend(orders)
                offset += len(orders)
                if len(orders) < limit:
                    break
            except Exception as e:
                logger.error(f'[Cafe24] 정산용 주문 조회 오류: {e}')
                break

        if not all_orders:
            return []

        pg_rates = self._get_pg_fee_rates()

        # 일별 집계
        daily = {}
        for o in all_orders:
            dt = str(o.get('order_date', ''))[:10]
            if not dt:
                continue
            amt = int(float(o.get('payment_amount', 0) or 0))
            actual = o.get('actual_order_amount', {})
            coupon = int(float(actual.get('coupon_discount_price', 0) or 0))
            points = int(float(actual.get('points_spent_amount', 0) or 0))
            shipping = int(float(actual.get('shipping_fee', 0) or 0))
            order_price = int(float(actual.get('order_price_amount', 0) or 0))
            methods = o.get('payment_method', []) or []

            # PG 수수료 추정
            pg_fee, primary_method = self._estimate_pg_fee(methods, amt, pg_rates)

            if dt not in daily:
                daily[dt] = {
                    'payment': 0, 'order_price': 0, 'shipping': 0,
                    'coupon': 0, 'points': 0, 'count': 0,
                    'pg_fee': 0, 'by_method': {},
                }
            d = daily[dt]
            d['payment'] += amt
            d['order_price'] += order_price
            d['shipping'] += shipping
            d['coupon'] += coupon
            d['points'] += points
            d['count'] += 1
            d['pg_fee'] += pg_fee

            # 결제수단별 집계
            method_key = primary_method
            if method_key not in d['by_method']:
                d['by_method'][method_key] = {'count': 0, 'amount': 0, 'fee': 0}
            d['by_method'][method_key]['count'] += 1
            d['by_method'][method_key]['amount'] += amt
            d['by_method'][method_key]['fee'] += pg_fee

        # api_settlements 형식으로 변환
        settlements = []
        for dt, d in sorted(daily.items()):
            settlements.append({
                'channel': self.CHANNEL_NAME,
                'settlement_date': dt,
                'settlement_id': f'cafe24_{dt}',
                'gross_sales': d['order_price'],
                'total_commission': d['pg_fee'],
                'shipping_fee_income': d['shipping'],
                'shipping_fee_cost': 0,
                'coupon_discount': d['coupon'],
                'point_discount': d['points'],
                'other_deductions': 0,
                'net_settlement': d['payment'] - d['pg_fee'],
                'fee_breakdown': {
                    'source': 'cafe24-orders',
                    'order_count': d['count'],
                    'pg_fee_estimated': d['pg_fee'],
                    'pg_fee_rates': {k: v for k, v in pg_rates.items()
                                     if k in d['by_method']},
                    'by_payment_method': d['by_method'],
                    'note': 'PG수수료 추정값 (실제 PG정산서와 차이 가능)',
                },
            })

        logger.info(f'[Cafe24] 정산 {len(settlements)}일 집계 '
                     f'(주문 {len(all_orders)}건)')
        return settlements

    # ── 정규화 ──

    def _normalize_order(self, order: dict, item: dict = None) -> dict:
        """API 응답 → api_orders 스키마.

        item이 있으면 상품별 행, 없으면 주문 레벨 행.
        Cafe24 item 필드: order_item_code, product_name, option_value,
                          quantity, product_price, payment_amount 등
        """
        item = item or order
        is_item = item is not order

        # 수량: item 레벨에서 quantity, 없으면 1
        qty = int(item.get('quantity', 0) or 0)
        if qty == 0 and is_item:
            qty = 1

        # 금액: item의 payment_amount 우선, 없으면 product_price * qty
        item_payment = item.get('payment_amount') or item.get('actual_payment_amount')
        if item_payment is not None:
            total_amount = int(float(item_payment))
        elif is_item:
            total_amount = int(float(item.get('product_price', 0) or 0)) * qty
        else:
            total_amount = int(float(order.get('actual_payment_amount', 0) or 0))

        return {
            'channel': self.CHANNEL_NAME,
            'api_order_id': str(order.get('order_id', '')),
            'api_line_id': str(item.get('order_item_code', '')
                               if is_item else order.get('order_id', '')),
            'order_date': str(order.get('order_date', ''))[:10],
            'product_name': item.get('product_name', '') or '',
            'option_name': item.get('option_value', '') or '',
            'qty': qty,
            'unit_price': int(float(item.get('product_price', 0) or 0)),
            'total_amount': total_amount,
            'discount_amount': int(float(item.get('discount_amount', 0) or 0)),
            'settlement_amount': 0,  # Cafe24 API 미제공
            'commission': 0,         # Cafe24 API 미제공
            'shipping_fee': int(float(order.get('shipping_fee', 0) or 0)),
            'fee_detail': {},
            'order_status': item.get('order_status', order.get('order_status', '')),
            'raw_data': {'order': order, 'item': item} if is_item else order,
            'raw_hash': self.compute_raw_hash(
                {'order_id': order.get('order_id'),
                 'item_code': item.get('order_item_code')} if is_item else order),
        }

    # ── 송장 등록 (발송처리) ──

    def register_invoice(self, orders: list) -> list:
        """Cafe24 배송 정보 등록.

        Cafe24 API: PUT /api/v2/admin/orders/{order_id}/items/{item_id}
        shipping_code(배송번호) + shipping_company_code(택배사코드) + tracking_no(송장번호) 전송.
        """
        results = []
        for o in orders:
            order_id = o.get('api_order_id', '')
            item_code = o.get('api_line_id', '')
            invoice_no = o.get('invoice_no', '')
            courier_code = o.get('courier_code', 'cj')

            if not order_id or not invoice_no:
                results.append({
                    'api_order_id': order_id,
                    'success': False,
                    'error': '주문ID 또는 송장번호 누락',
                })
                continue

            try:
                # Cafe24 배송처리: 주문 아이템 상태 업데이트
                url = f'{self._base_url}/api/v2/admin/orders/{order_id}/items'
                payload = {
                    'request': {
                        'items': [{
                            'order_item_code': item_code,
                            'shipping_company_code': courier_code,
                            'tracking_no': invoice_no,
                            'status': 'shipping',
                        }],
                    }
                }

                resp = self.session.put(
                    url,
                    headers=self._get_headers(),
                    json=payload,
                    timeout=15,
                )

                if self._handle_rate_limit(resp):
                    # 재시도
                    resp = self.session.put(
                        url, headers=self._get_headers(),
                        json=payload, timeout=15,
                    )

                if resp.status_code in (200, 201):
                    results.append({
                        'api_order_id': order_id,
                        'success': True,
                        'error': '',
                    })
                    logger.info(f'[Cafe24] 송장등록 성공: {order_id} → {invoice_no}')
                else:
                    err = resp.text[:200]
                    results.append({
                        'api_order_id': order_id,
                        'success': False,
                        'error': f'HTTP {resp.status_code}: {err}',
                    })
                    logger.warning(f'[Cafe24] 송장등록 실패: {order_id} → {err}')

            except Exception as e:
                results.append({
                    'api_order_id': order_id,
                    'success': False,
                    'error': str(e),
                })
                logger.error(f'[Cafe24] 송장등록 오류: {order_id} → {e}')

        return results

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
