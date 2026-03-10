"""
base_client.py — 마켓플레이스 API 클라이언트 추상 베이스.

모든 마켓플레이스 클라이언트(네이버/쿠팡/Cafe24)가 상속하는 공통 인터페이스.
PopbillService/CodefService 싱글톤 패턴을 따름.
"""
import hashlib
import json
import logging
import time
from abc import ABC, abstractmethod

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class MarketplaceBaseClient(ABC):
    """마켓플레이스 API 클라이언트 추상 베이스."""

    CHANNEL_NAME = ''  # 서브클래스에서 오버라이드

    def __init__(self, config: dict):
        """
        Args:
            config: marketplace_api_config 테이블 row dict.
        """
        self.config = config or {}
        self.is_active = self.config.get('is_active', False)
        self._session = None

    @property
    def is_ready(self) -> bool:
        """API 호출 가능 상태인지."""
        return self.is_active and bool(self.config.get('client_id'))

    def _build_session(self) -> requests.Session:
        """재시도 + rate-limit 인식 HTTP 세션."""
        s = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=['GET', 'POST', 'PUT', 'DELETE'],
        )
        adapter = HTTPAdapter(max_retries=retry)
        s.mount('https://', adapter)
        s.mount('http://', adapter)
        return s

    @property
    def session(self) -> requests.Session:
        if self._session is None:
            self._session = self._build_session()
        return self._session

    def _handle_rate_limit(self, response: requests.Response):
        """429 응답 시 Retry-After 헤더 기반 대기."""
        if response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', 5))
            retry_after = min(retry_after, 60)  # 최대 60초
            logger.warning(f'[{self.CHANNEL_NAME}] Rate limited, waiting {retry_after}s')
            time.sleep(retry_after)
            return True
        return False

    @staticmethod
    def compute_raw_hash(data: dict) -> str:
        """원본 데이터 해시 (중복 감지용)."""
        raw = json.dumps(data, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(raw.encode()).hexdigest()

    # ── 추상 메서드 ──

    @abstractmethod
    def refresh_token(self, db) -> bool:
        """토큰 갱신. 성공 시 True."""
        ...

    @abstractmethod
    def fetch_orders(self, date_from: str, date_to: str) -> list:
        """주문 목록 조회.

        Args:
            date_from: 'YYYY-MM-DD'
            date_to: 'YYYY-MM-DD'

        Returns:
            정규화된 주문 dict 리스트.
        """
        ...

    @abstractmethod
    def fetch_settlements(self, date_from: str, date_to: str) -> list:
        """정산 데이터 조회.

        Returns:
            정규화된 정산 dict 리스트.
        """
        ...

    @abstractmethod
    def _normalize_order(self, raw: dict) -> dict:
        """API 원본 응답 → api_orders 스키마 변환."""
        ...

    def update_config(self, db, updates: dict):
        """marketplace_api_config 업데이트."""
        updates['channel'] = self.CHANNEL_NAME
        db.upsert_marketplace_api_config(updates)
        self.config.update(updates)
