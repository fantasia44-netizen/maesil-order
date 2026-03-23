"""
kakao_client.py — 카카오 톡스토어 API 클라이언트 (스켈레톤).

API 문서: https://developers.kakao.com/docs/latest/ko/talk-store/common
인증: REST API Key + Admin Key (카카오 비즈니스 등록)
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
