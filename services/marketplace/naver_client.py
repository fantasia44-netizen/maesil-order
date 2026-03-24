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

    CHANNEL_NAME = '스마트스토어'  # 폴백용; 실제 채널명은 self.config['channel'] 우선
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

    # 송장 대상 상태 — 결제완료(송장 미입력) 주문만
    INVOICE_TARGET_STATUSES = ['PAYED']

    def fetch_orders(self, date_from: str, date_to: str,
                     status_filter: str = None) -> list:
        """네이버 권장 방식: last-changed-statuses → 개별 상세 조회.

        1단계: last-changed-statuses API로 변경된 주문 ID 수집 (누락 없는 방식)
        2단계: product-orders/query API로 상세 데이터 일괄 조회
        3단계: orderId별 전체 라인 보충 (같은 주문의 누락 라인 방지)

        Args:
            status_filter: 'invoice_target' → 결제완료(PAYED)만 필터링
                           'shipped' → 발송완료만 필터링
                           None → 전체 상태
        """
        if not self.config.get('access_token'):
            logger.warning('[네이버] 액세스 토큰 없음')
            return []

        # ── 1단계: 변경 주문 ID 수집 ──
        all_ids = []
        windows = self._split_date_range(date_from, date_to)
        for win_from, win_to in windows:
            ids = self._fetch_changed_order_ids_window(win_from, win_to)
            all_ids.extend(ids)

        # 중복 제거
        unique_ids = list(dict.fromkeys(all_ids))
        logger.info(f'[네이버] 1단계: 변경 주문 ID {len(unique_ids)}건 수집')

        if not unique_ids:
            return []

        # ── 2단계: 상세 데이터 일괄 조회 (100건씩 분할) ──
        all_orders = []
        seen_ids = set()
        BATCH_SIZE = 100

        for i in range(0, len(unique_ids), BATCH_SIZE):
            batch = unique_ids[i:i + BATCH_SIZE]
            details = self._fetch_order_details(batch)
            for o in details:
                pid = o.get('api_line_id', '')
                if pid and pid not in seen_ids:
                    seen_ids.add(pid)
                    all_orders.append(o)

        logger.info(f'[네이버] 2단계: 상세 조회 {len(all_orders)}건')

        # ── 3단계: orderId별 전체 라인 보충 (같은 주문의 누락 라인 방지) ──
        order_ids = set()
        for o in all_orders:
            oid = o.get('api_order_id', '')
            if oid:
                order_ids.add(oid)

        supplemented = 0
        for oid in order_ids:
            try:
                resp = self.session.get(
                    f'{self.BASE_URL}/external/v1/pay-order/seller/orders/{oid}/product-orders',
                    headers=self._get_headers(), timeout=30,
                )
                if self._handle_rate_limit(resp):
                    resp = self.session.get(
                        f'{self.BASE_URL}/external/v1/pay-order/seller/orders/{oid}/product-orders',
                        headers=self._get_headers(), timeout=30,
                    )
                if resp.status_code != 200:
                    continue
                items = resp.json().get('data', [])
                for item in items:
                    content = item.get('content', item)
                    po = content.get('productOrder', {})
                    pid = str(po.get('productOrderId', ''))
                    if pid and pid not in seen_ids:
                        seen_ids.add(pid)
                        all_orders.append(self._normalize_order(content))
                        supplemented += 1
            except Exception:
                pass

        if supplemented:
            logger.info(f'[네이버] 3단계: orderId별 보충 +{supplemented}건')

        # ── 상태 필터링 ──
        if status_filter == 'invoice_target':
            all_orders = [o for o in all_orders
                          if o.get('order_status') in self.INVOICE_TARGET_STATUSES]
        elif status_filter == 'shipped':
            all_orders = [o for o in all_orders
                          if o.get('order_status') in ('DELIVERING', 'DELIVERED')]

        logger.info(f'[네이버] 최종: {len(all_orders)}건 (필터={status_filter or "전체"})')
        return all_orders

    def _fetch_orders_window(self, from_str: str, to_str: str,
                             statuses: list = None) -> list:
        """단일 24시간 윈도우에서 주문 조회 (결제일 기준, 페이지네이션)."""
        url = f'{self.BASE_URL}/external/v1/pay-order/seller/product-orders'
        orders = []
        page = 1

        while True:
            try:
                params = {
                    'from': from_str,
                    'to': to_str,
                    'rangeType': 'PAYED_DATETIME',
                    'pageSize': 100,
                    'page': page,
                }
                if statuses:
                    params['productOrderStatuses'] = statuses
                resp = self.session.get(url, headers=self._get_headers(),
                                        params=params, timeout=30)
                if self._handle_rate_limit(resp):
                    continue
                if resp.status_code != 200:
                    logger.error(f'[네이버] 주문조회 실패: {resp.status_code} {resp.text[:200]}')
                    break

                data = resp.json().get('data', {})
                items = data.get('contents', [])

                for item in items:
                    # product-orders API: {productOrderId, content: {order, productOrder, delivery}}
                    content = item.get('content', item)
                    orders.append(self._normalize_order(content))

                # 페이지네이션: hasNext 방식
                pagination = data.get('pagination', {})
                if not pagination.get('hasNext', False):
                    break
                page += 1

            except Exception as e:
                logger.error(f'[네이버] 주문조회 오류: {e}')
                break

        return orders

    @staticmethod
    def _split_date_range(date_from: str, date_to: str) -> list:
        """날짜 범위를 24시간 윈도우로 분할."""
        from_dt = datetime.strptime(date_from, '%Y-%m-%d')
        to_dt = datetime.strptime(date_to, '%Y-%m-%d')
        # to_dt의 끝(23:59:59)까지 포함
        to_dt = to_dt.replace(hour=23, minute=59, second=59)

        windows = []
        cursor = from_dt
        while cursor < to_dt:
            win_end = min(cursor + timedelta(hours=23, minutes=59, seconds=59), to_dt)
            win_from = cursor.strftime('%Y-%m-%dT%H:%M:%S.000+09:00')
            win_to = win_end.strftime('%Y-%m-%dT%H:%M:%S.999+09:00')
            windows.append((win_from, win_to))
            cursor = win_end + timedelta(seconds=1)

        return windows

    def _fetch_changed_order_ids_window(self, from_str: str, to_str: str) -> list:
        """단일 24시간 윈도우에서 변경 주문 ID 조회 (페이지네이션)."""
        ids = []
        url = f'{self.BASE_URL}/external/v1/pay-order/seller/product-orders/last-changed-statuses'
        params = {
            'lastChangedFrom': from_str,
            'lastChangedTo': to_str,
        }

        while True:
            try:
                resp = self.session.get(url, headers=self._get_headers(),
                                        params=params, timeout=30)
                if self._handle_rate_limit(resp):
                    continue
                if resp.status_code != 200:
                    logger.error(f'[네이버] 변경주문 조회 실패: {resp.status_code} {resp.text[:200]}')
                    break

                data = resp.json().get('data', {})
                statuses = data.get('lastChangeStatuses', [])
                for s in statuses:
                    pid = s.get('productOrderId', '')
                    if pid and pid not in ids:
                        ids.append(pid)

                # 페이지네이션: moreFrom + moreSequence
                more = data.get('more', {})
                if more.get('moreSequence'):
                    params['lastChangedFrom'] = more['moreFrom']
                    params['moreSequence'] = more['moreSequence']
                else:
                    break

            except Exception as e:
                logger.error(f'[네이버] 변경주문 조회 오류: {e}')
                break

        return ids

    def _fetch_order_details(self, product_order_ids: list) -> list:
        """주문 상세 일괄조회."""
        url = f'{self.BASE_URL}/external/v1/pay-order/seller/product-orders/query'

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
        order_info = raw.get('order', {})
        po = raw.get('productOrder', {})

        # 수수료 합산 (결제수수료 + 판매수수료 + 지식쇼핑연동수수료 + 채널수수료)
        payment_comm = int(po.get('paymentCommission', 0))
        sale_comm = int(po.get('saleCommission', 0))
        knowledge_comm = int(po.get('knowledgeShoppingSellingInterlockCommission', 0))
        channel_comm = int(po.get('channelCommission', 0))
        total_commission = payment_comm + sale_comm + knowledge_comm + channel_comm

        return {
            'channel': self.channel_name,
            'api_order_id': str(order_info.get('orderId', '')),
            'api_line_id': str(po.get('productOrderId', '')),
            'order_date': str(order_info.get('orderDate', ''))[:10],
            'product_name': po.get('productName', ''),
            'option_name': po.get('productOption', ''),
            'qty': int(po.get('quantity', 0)),
            'unit_price': int(po.get('unitPrice', 0)),
            'total_amount': int(po.get('totalPaymentAmount', 0)),
            'discount_amount': int(po.get('productDiscountAmount', 0)),
            'settlement_amount': int(po.get('expectedSettlementAmount', 0)),
            'commission': total_commission,
            'shipping_fee': int(po.get('deliveryFeeAmount', 0)),
            'fee_detail': {
                'payment_commission': payment_comm,
                'sale_commission': sale_comm,
                'knowledge_shopping_commission': knowledge_comm,
                'channel_commission': channel_comm,
                'seller_burden_discount': int(po.get('sellerBurdenDiscountAmount', 0)),
                'delivery_type': po.get('deliveryAttributeType', ''),
            },
            'order_status': po.get('productOrderStatus', ''),
            'raw_data': raw,
            'raw_hash': self.compute_raw_hash(raw),
        }

    def _normalize_settlement(self, raw: dict) -> dict:
        """정산 API 응답 → api_settlements 스키마."""
        return {
            'channel': self.channel_name,
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

    # ── 주문 상태 조회 (배송 추적) ──

    def fetch_order_statuses(self, order_ids: list) -> list:
        """productOrderId 기반 주문 상태 배치 조회.

        POST /external/v1/pay-order/seller/product-orders/query
        최대 50건씩 배치.
        """
        if not self.config.get('access_token'):
            return []

        results = []
        for i in range(0, len(order_ids), 50):
            chunk = order_ids[i:i + 50]
            try:
                resp = self.session.post(
                    f'{self.BASE_URL}/external/v1/pay-order/seller/'
                    f'product-orders/query',
                    headers=self._get_headers(),
                    json={'productOrderIds': chunk},
                    timeout=30,
                )
                if resp.status_code == 200:
                    data = resp.json().get('data', [])
                    for item in data:
                        po = item.get('productOrder', {})
                        results.append({
                            'api_order_id': po.get('productOrderId', ''),
                            'status_raw': po.get('productOrderStatus', ''),
                        })
                else:
                    logger.warning(f'[네이버] 상태 조회 실패: {resp.status_code}')
            except Exception as e:
                logger.error(f'[네이버] 상태 조회 오류: {e}')

        return results

    # ── 송장 등록 (발송처리) ──

    def register_invoice(self, orders: list) -> list:
        """네이버 커머스 발송처리.

        엔드포인트: POST /external/v1/pay-order/seller/product-orders/dispatch
        같은 송장번호끼리 productOrderIds 배열로 묶어서 한 번에 발송.
        """
        if not self.config.get('access_token'):
            return [{'api_order_id': o.get('api_order_id', ''),
                      'success': False, 'error': '액세스 토큰 없음'}
                     for o in orders]

        # 같은 송장번호끼리 그룹핑
        groups = {}  # invoice_no → [orders]
        for o in orders:
            inv = o.get('invoice_no', '')
            if inv not in groups:
                groups[inv] = []
            groups[inv].append(o)

        results = []
        url = f'{self.BASE_URL}/external/v1/pay-order/seller/product-orders/dispatch'

        for invoice_no, group in groups.items():
            # productOrderIds: api_line_id 사용 (= productOrderId)
            product_order_ids = []
            for o in group:
                line_id = o.get('api_line_id', '')
                if line_id:
                    product_order_ids.append(line_id)

            if not product_order_ids:
                for o in group:
                    results.append({
                        'api_order_id': o.get('api_order_id', ''),
                        'success': False,
                        'error': 'productOrderId 누락',
                    })
                continue

            courier_code = group[0].get('courier_code', 'CJGLS')
            payload = {
                'productOrderIds': product_order_ids,
                'deliveryMethod': 'DELIVERY',
                'deliveryCompanyCode': courier_code,
                'trackingNumber': invoice_no,
            }

            try:
                resp = self.session.post(
                    url,
                    headers=self._get_headers(),
                    json=payload,
                    timeout=15,
                )

                if self._handle_rate_limit(resp):
                    resp = self.session.post(
                        url, headers=self._get_headers(),
                        json=payload, timeout=15,
                    )

                if resp.status_code in (200, 201):
                    data = resp.json()
                    # 성공 응답에서 개별 결과 확인
                    success_ids = set()
                    fail_data = data.get('data', {}).get('failProductOrderInfos', [])
                    fail_ids = {f.get('productOrderId') for f in fail_data}

                    for o in group:
                        lid = o.get('api_line_id', '')
                        if lid in fail_ids:
                            fail_info = next(
                                (f for f in fail_data if f.get('productOrderId') == lid),
                                {}
                            )
                            results.append({
                                'api_order_id': o.get('api_order_id', ''),
                                'success': False,
                                'error': fail_info.get('reason', '발송처리 실패'),
                            })
                        else:
                            results.append({
                                'api_order_id': o.get('api_order_id', ''),
                                'success': True,
                                'error': '',
                            })

                    succeeded = sum(1 for o in group
                                    if o.get('api_line_id') not in fail_ids)
                    logger.info(f'[네이버] 발송처리 {succeeded}/{len(group)}건 성공'
                                f' (송장: {invoice_no})')
                else:
                    err = resp.text[:200]
                    logger.warning(f'[네이버] 발송처리 실패: {resp.status_code} {err}')
                    for o in group:
                        results.append({
                            'api_order_id': o.get('api_order_id', ''),
                            'success': False,
                            'error': f'HTTP {resp.status_code}: {err}',
                        })

            except Exception as e:
                logger.error(f'[네이버] 발송처리 오류: {e}')
                for o in group:
                    results.append({
                        'api_order_id': o.get('api_order_id', ''),
                        'success': False,
                        'error': str(e),
                    })

        return results

    def test_connection(self, db) -> dict:
        """API 연결 테스트."""
        if not self.is_ready:
            return {'success': False, 'message': 'client_id/secret 미설정'}

        token_ok = self.refresh_token(db)
        if not token_ok:
            return {'success': False, 'message': '토큰 발급 실패'}

        return {'success': True, 'message': '네이버 커머스 API 연결 성공'}
