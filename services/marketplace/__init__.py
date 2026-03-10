"""
marketplace/ — 마켓플레이스 API 연동 패키지.

MarketplaceManager: 채널별 API 클라이언트 싱글톤 관리.
app.py에서 app.marketplace = MarketplaceManager(app.db) 형태로 초기화.
"""
import logging

from .naver_client import NaverCommerceClient
from .coupang_client import CoupangWingClient
from .cafe24_client import Cafe24Client

logger = logging.getLogger(__name__)

# 채널 → 클라이언트 클래스 매핑
_CLIENT_MAP = {
    '스마트스토어': NaverCommerceClient,
    '쿠팡': CoupangWingClient,
    '자사몰': Cafe24Client,
}


class MarketplaceManager:
    """채널별 마켓플레이스 API 클라이언트 관리자."""

    def __init__(self, db=None):
        self.clients = {}
        if db:
            self._load_configs(db)

    def _load_configs(self, db):
        """DB에서 API 설정 로드 → 클라이언트 인스턴스 생성."""
        try:
            configs = db.query_marketplace_api_configs()
        except Exception as e:
            logger.warning(f'[Marketplace] config 로드 실패 (테이블 미생성?): {e}')
            configs = []

        for cfg in configs:
            channel = cfg.get('channel', '')
            cls = _CLIENT_MAP.get(channel)
            if cls:
                self.clients[channel] = cls(cfg)
                logger.info(f'[Marketplace] {channel} 클라이언트 로드 '
                            f'(active={cfg.get("is_active", False)})')

        # 미등록 채널 빈 config로 초기화
        for channel, cls in _CLIENT_MAP.items():
            if channel not in self.clients:
                self.clients[channel] = cls({
                    'channel': channel,
                    'is_active': False,
                })

    def get_client(self, channel):
        """채널명으로 클라이언트 반환."""
        return self.clients.get(channel)

    def get_active_channels(self) -> list:
        """활성화된 채널 목록."""
        return [ch for ch, c in self.clients.items() if c.is_active and c.is_ready]

    def get_all_channels(self) -> list:
        """전체 채널 상태 목록 (UI용)."""
        result = []
        for channel, client in self.clients.items():
            result.append({
                'channel': channel,
                'is_active': client.is_active,
                'is_ready': client.is_ready,
                'last_synced_at': client.config.get('last_synced_at'),
            })
        return result
