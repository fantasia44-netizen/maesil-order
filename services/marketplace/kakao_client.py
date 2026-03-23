"""
kakao_client.py — 카카오 톡스토어(카카오쇼핑) API 클라이언트 (스켈레톤).

API 문서: https://shopping-developers.kakao.com
인증: Kakao OAuth (연동대행사 계정 + 판매자 계정 = 2개 필요)

API 연동 신청:
  1. https://shopping-developers.kakao.com 에서 연동 검토 요청서 제출
  2. 심사 후 개별 메일 회신 (선별 심사 — 모든 판매자에 제공하지 않음)
  3. 카카오쇼핑 API 연동 계약 체결
  4. Kakao Developers 앱 등록 → 연동
  ※ 판매자센터 > 판매채널 정보 > API 인증키 확인

주요 API:
  - 주문조회: GET /v2/shopping/orders/paidAt (v1은 2025.09.30 종료)
    ※ 최대 1일(24시간) 범위, 100건/요청
  - 송장등록: POST /v1/shopping/orders/deliveries/invoices (최대 100건, 비동기)
  - 택배사코드: GET /v1/shopping/deliveries/companies
  - 주문상태: ShippingWaiting, ShippingProgress, ShippingComplete
"""
import logging
from .base_client import MarketplaceBaseClient

logger = logging.getLogger(__name__)


class KakaoClient(MarketplaceBaseClient):
    """카카오 톡스토어 API 클라이언트."""

    CHANNEL_NAME = '카카오'
    BASE_URL = 'https://commerce-api.kakao.com'

    # TODO: 카카오 비즈니스 API 키 신청 후 구현
    # 신청: https://business.kakao.com → 톡스토어 판매자 → API 연동 신청
    # 인증: REST API Key + 판매자 인증

    @property
    def is_ready(self) -> bool:
        return (self.is_active
                and bool(self.config.get('client_id')))  # rest_api_key

    def refresh_token(self, db) -> bool:
        """카카오 토큰 발급.

        TODO: 구현 필요
        """
        if not self.is_ready:
            return False
        logger.warning('[카카오] refresh_token 미구현')
        return False

    def _get_headers(self) -> dict:
        return {
            'Authorization': f'KakaoAK {self.config.get("client_id", "")}',
            'Content-Type': 'application/json',
        }

    def fetch_orders(self, date_from: str, date_to: str,
                     status_filter: str = None) -> list:
        """주문 조회. TODO: 구현"""
        if not self.is_ready:
            return []
        logger.warning('[카카오] fetch_orders 미구현')
        return []

    def fetch_settlements(self, date_from: str, date_to: str) -> list:
        """정산 조회. TODO: 구현"""
        return []

    def register_invoice(self, orders: list) -> list:
        """송장등록. TODO: 구현"""
        if not self.is_ready:
            return [{'api_order_id': o.get('api_order_id', ''),
                     'success': False, 'error': '카카오 API 미설정'}
                    for o in orders]
        logger.warning('[카카오] register_invoice 미구현')
        return [{'api_order_id': o.get('api_order_id', ''),
                 'success': False, 'error': '미구현'}
                for o in orders]

    def fetch_order_statuses(self, order_ids: list) -> list:
        """주문 상태 조회. TODO: 구현"""
        return []

    def _normalize_order(self, raw: dict) -> dict:
        """카카오 API 응답 정규화. TODO: 구현"""
        return {}
