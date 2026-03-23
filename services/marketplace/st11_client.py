"""
st11_client.py — 11번가 셀러 API 클라이언트 (스켈레톤).

API 문서: https://openapi.11st.co.kr
참고: https://skopenapi.readme.io/reference
인증: API Key → Header 'openapikey: {key}'
프로토콜: XML over HTTP (JSON 미지원 가능성)

API 키 발급:
  1. 11번가 셀러오피스 로그인 (https://soffice.11st.co.kr)
  2. 하단 "Open API" 클릭 → https://openapi.11st.co.kr/openapi/OpenApiFrontMain.tmall
  3. 담당업무: "개발", 사용용도: "주문 통합관리" → 즉시 발급
  ※ IP 화이트리스트 등록 필요할 수 있음

주요 API:
  - 주문조회: OrderService.getOrderList (startDate, endDate, orderStatus)
  - 송장등록: OrderService.setDeliveryInfo (ordNo, dlvCmpCd=01(CJ), invoiceNo)
  - 주문상태: 결제완료(202), 배송준비중(301), 배송중(302), 배송완료(303)
"""
import logging
from .base_client import MarketplaceBaseClient

logger = logging.getLogger(__name__)


class St11Client(MarketplaceBaseClient):
    """11번가 셀러 API 클라이언트."""

    CHANNEL_NAME = '11번가'
    BASE_URL = 'https://openapi.11st.co.kr'

    # TODO: API 키 신청 후 구현
    # 신청: https://openapi.11st.co.kr → 셀러 회원가입 → API 키 발급
    # 인증: Header에 openapikey 포함

    @property
    def is_ready(self) -> bool:
        return (self.is_active
                and bool(self.config.get('client_id')))  # api_key

    def refresh_token(self, db) -> bool:
        """11번가는 API Key 방식이라 토큰 갱신 불필요."""
        return True

    def _get_headers(self) -> dict:
        return {
            'openapikey': self.config.get('client_id', ''),
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        }

    def fetch_orders(self, date_from: str, date_to: str,
                     status_filter: str = None) -> list:
        """주문 조회.

        TODO: 구현 필요
        엔드포인트: GET /openapi/OpenApiService.tmall
        - method: OrderService.getOrderList
        - 파라미터: startDate, endDate, orderStatus
        - 주문상태: 결제완료(202), 배송준비중(301), 배송중(302), 배송완료(303)
        """
        if not self.is_ready:
            return []
        # TODO: 구현
        logger.warning('[11번가] fetch_orders 미구현')
        return []

    def fetch_settlements(self, date_from: str, date_to: str) -> list:
        """정산 조회. TODO: 구현"""
        return []

    def register_invoice(self, orders: list) -> list:
        """송장등록 (발송처리).

        TODO: 구현 필요
        엔드포인트: POST /openapi/OpenApiService.tmall
        - method: OrderService.setDeliveryInfo
        - 파라미터: ordNo, dlvCmpCd(CJ대한통운=01), invoiceNo
        """
        if not self.is_ready:
            return [{'api_order_id': o.get('api_order_id', ''),
                     'success': False, 'error': '11번가 API 미설정'}
                    for o in orders]
        # TODO: 구현
        logger.warning('[11번가] register_invoice 미구현')
        return [{'api_order_id': o.get('api_order_id', ''),
                 'success': False, 'error': '미구현'}
                for o in orders]

    def fetch_order_statuses(self, order_ids: list) -> list:
        """주문 상태 조회. TODO: 구현"""
        return []

    def _normalize_order(self, raw: dict) -> dict:
        """11번가 API 응답 정규화. TODO: 구현"""
        return {}
