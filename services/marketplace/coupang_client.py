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

    # 쿠팡 주문 상태 — 전체 조회 (CANCEL은 ordersheets API 미지원)
    ORDER_STATUSES = [
        'ACCEPT',          # 발주확인
        'INSTRUCT',        # 상품준비중
        'DEPARTURE',       # 배송지시/출고
        'DELIVERING',      # 배송중
        'FINAL_DELIVERY',  # 배송완료
    ]
    # 송장 대상 — 결제완료(출고 전) 주문만
    INVOICE_TARGET_STATUSES = ['ACCEPT', 'INSTRUCT']

    def fetch_orders(self, date_from: str, date_to: str,
                     status_filter: str = None) -> list:
        """주문 목록 조회 — 상태를 순회하여 주문 수집.

        Args:
            status_filter: 'invoice_target' → ACCEPT/INSTRUCT만 (출고 전)
                           None → 전체 상태 수집 (기존 동작)
        """
        vendor_id = self.config.get('vendor_id', '')
        if not vendor_id:
            logger.warning('[쿠팡] vendor_id 미설정')
            return []

        path = f'/v2/providers/openapi/apis/api/v4/vendors/{vendor_id}/ordersheets'
        all_orders = []
        statuses = self.INVOICE_TARGET_STATUSES if status_filter == 'invoice_target' else self.ORDER_STATUSES

        for status in statuses:
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

    # ── 매출내역 조회 (revenue-history) — 주문별 수수료/정산 상세 ──

    def fetch_revenue_history(self, date_from: str, date_to: str) -> list:
        """매출내역 조회 — 주문별 수수료/정산 상세.

        recognitionDate (매출인식일 = 구매확정 또는 배송완료+3일) 기준.
        ordersheets에 없는 serviceFee, settlementAmount를 제공.

        Returns:
            list: [{orderId, saleDate, recognitionDate, settlementDate,
                    deliveryFee: {amount, fee, feeVat, feeRatio, settlementAmount},
                    items: [{vendorItemId, saleAmount, serviceFee, serviceFeeVat,
                             serviceFeeRatio, settlementAmount, couranteeFee, ...}]}]
        """
        vendor_id = self.config.get('vendor_id', '')
        if not vendor_id:
            return []

        path = '/v2/providers/openapi/apis/api/v1/revenue-history'
        all_records = []
        next_token = ''  # API requires token param, empty for first page

        while True:
            params = {
                'vendorId': vendor_id,
                'recognitionDateFrom': date_from,
                'recognitionDateTo': date_to,
                'token': next_token,
            }

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
                    logger.error(f'[쿠팡] 매출내역 조회 실패: '
                                 f'{resp.status_code} {resp.text[:200]}')
                    break

                data = resp.json()
                records = data.get('data', [])
                all_records.extend(records)

                has_next = data.get('hasNext', False)
                raw_token = data.get('nextToken', '')

                if not has_next or not raw_token:
                    break
                next_token = raw_token

            except Exception as e:
                logger.error(f'[쿠팡] 매출내역 조회 오류: {e}')
                break

        logger.info(f'[쿠팡] 매출내역 {len(all_records)}건 조회')
        return all_records

    # ── 정산 조회 (settlement-histories) — 월간 정산 요약 ──

    def fetch_settlements(self, date_from: str, date_to: str) -> list:
        """월간 정산 요약 조회.

        settlement-histories API: 매출인식월 기준 정산 요약.
        """
        vendor_id = self.config.get('vendor_id', '')
        if not vendor_id:
            return []

        # date_from에서 year-month 추출
        year_month = date_from[:7]  # 'YYYY-MM'

        path = '/v2/providers/marketplace_openapi/apis/api/v1/settlement-histories'
        settlements = []

        params = {
            'revenueRecognitionYearMonth': year_month,
            'vendorId': vendor_id,
        }
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
                logger.error(f'[쿠팡] 정산 조회 실패: {resp.status_code} {resp.text[:200]}')
                return []

            data = resp.json()
            # settlement-histories는 배열로 바로 반환
            items = data if isinstance(data, list) else data.get('data', [])
            for item in items:
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
                'product_name': item.get('sellerProductName', item.get('vendorItemName', '')),
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
        """settlement-histories API 응답 → api_settlements 스키마."""
        # settlement-histories 필드: totalSale, serviceFee, settlementTargetAmount,
        # settlementAmount, sellerDiscountCoupon, downloadableCoupon,
        # dedicatedDeliveryAmount, sellerServiceFee, couranteeFee, deductionAmount
        return {
            'channel': self.CHANNEL_NAME,
            'settlement_date': str(raw.get('settlementDate', ''))[:10],
            'settlement_id': (f"{raw.get('revenueRecognitionYearMonth', '')}_"
                              f"{raw.get('settlementType', '')}"),
            'gross_sales': int(raw.get('totalSale', 0)),
            'total_commission': int(raw.get('serviceFee', 0)),
            'shipping_fee_income': 0,
            'shipping_fee_cost': 0,
            'coupon_discount': (int(raw.get('sellerDiscountCoupon', 0)) +
                                int(raw.get('downloadableCoupon', 0))),
            'point_discount': 0,
            'other_deductions': int(raw.get('deductionAmount', 0)),
            'net_settlement': int(raw.get('settlementAmount', 0)),
            'fee_breakdown': {
                'service_fee': int(raw.get('serviceFee', 0)),
                'settlement_target': int(raw.get('settlementTargetAmount', 0)),
                'seller_service_fee': int(raw.get('sellerServiceFee', 0)),
                'courantee_fee': int(raw.get('couranteeFee', 0)),
                'courantee_reward': int(raw.get('couranteeCustomerReward', 0)),
                'dedicated_delivery': int(raw.get('dedicatedDeliveryAmount', 0)),
                'store_fee_discount': int(raw.get('storeFeeDiscount', 0)),
                'last_amount': int(raw.get('lastAmount', 0)),
                'settlement_type': raw.get('settlementType', ''),
                'status': raw.get('status', ''),
            },
            'raw_data': raw,
        }

    # ── 송장 등록 (발송처리) ──

    def register_invoice(self, orders: list) -> list:
        """쿠팡 송장업로드.

        엔드포인트: POST /v2/.../vendors/{vendorId}/orders/invoices
        raw_data에서 shipmentBoxId, orderId, vendorItemId 추출.
        배열로 일괄 전송 (orderSheetInvoiceApplyDtos).
        """
        vendor_id = self.config.get('vendor_id', '')
        if not vendor_id:
            return [{'api_order_id': o.get('api_order_id', ''),
                      'success': False, 'error': 'vendor_id 미설정'}
                     for o in orders]

        path = f'/v2/providers/openapi/apis/api/v4/vendors/{vendor_id}/orders/invoices'

        # body 구성
        dtos = []
        order_map = {}  # shipmentBoxId → api_order_id 매핑 (결과 추적용)
        for o in orders:
            raw = o.get('raw_data', {})
            shipment_box_id = raw.get('shipmentBoxId')
            order_id = raw.get('orderId')
            vendor_item_id = raw.get('vendorItemId')

            if not shipment_box_id or not order_id:
                continue

            dto = {
                'shipmentBoxId': shipment_box_id,
                'orderId': order_id,
                'deliveryCompanyCode': o.get('courier_code', 'CJGLS'),
                'invoiceNumber': o.get('invoice_no', ''),
            }
            if vendor_item_id:
                dto['vendorItemId'] = vendor_item_id

            dtos.append(dto)
            order_map[str(shipment_box_id)] = o.get('api_order_id', '')

        if not dtos:
            return [{'api_order_id': o.get('api_order_id', ''),
                      'success': False, 'error': 'shipmentBoxId/orderId 누락'}
                     for o in orders]

        # HMAC 서명 + POST
        headers = self._generate_hmac_signature('POST', path)
        body = {
            'vendorId': vendor_id,
            'orderSheetInvoiceApplyDtos': dtos,
        }

        try:
            resp = self.session.post(
                f'{self.BASE_URL}{path}',
                headers=headers,
                json=body,
                timeout=30,
            )

            if self._handle_rate_limit(resp):
                headers = self._generate_hmac_signature('POST', path)
                resp = self.session.post(
                    f'{self.BASE_URL}{path}',
                    headers=headers, json=body, timeout=30,
                )

            if resp.status_code != 200:
                err = resp.text[:300]
                logger.error(f'[쿠팡] 송장업로드 실패: {resp.status_code} {err}')
                return [{'api_order_id': order_map.get(str(d['shipmentBoxId']), ''),
                          'success': False, 'error': f'HTTP {resp.status_code}'}
                         for d in dtos]

            # 응답 파싱: responseList 순회
            data = resp.json().get('data', {})
            response_list = data.get('responseList', [])
            results = []
            for r in response_list:
                sbox_id = str(r.get('shipmentBoxId', ''))
                results.append({
                    'api_order_id': order_map.get(sbox_id, sbox_id),
                    'success': r.get('succeed', False),
                    'error': r.get('resultMessage', '') if not r.get('succeed') else '',
                })

            succeeded = sum(1 for r in results if r['success'])
            logger.info(f'[쿠팡] 송장업로드 {succeeded}/{len(results)}건 성공')
            return results

        except Exception as e:
            logger.error(f'[쿠팡] 송장업로드 오류: {e}')
            return [{'api_order_id': order_map.get(str(d['shipmentBoxId']), ''),
                      'success': False, 'error': str(e)}
                     for d in dtos]

    def test_connection(self, db) -> dict:
        """API 연결 테스트 — 간단한 주문 조회."""
        if not self.is_ready:
            return {'success': False, 'message': 'access_key/secret_key/vendor_id 미설정'}

        vendor_id = self.config['vendor_id']
        path = f'/v2/providers/openapi/apis/api/v4/vendors/{vendor_id}/ordersheets'

        from services.tz_utils import today_kst
        today = today_kst()
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
