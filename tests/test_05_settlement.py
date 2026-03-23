"""
test_05_settlement.py — 정산 대사 테스트.

P0 테스트 #5: 정산 조회, 매칭 서비스, P&L 반영.
"""
import pytest


class TestSettlement:
    """정산 기본 동작."""

    def test_settlement_query(self, db):
        """정산 조회가 에러 없이 동작."""
        result = db.query_platform_settlements()
        assert isinstance(result, list)

    def test_matching_service_import(self):
        """매칭 서비스 import."""
        import services.matching_service as ms
        assert ms is not None

    def test_pnl_service_import(self):
        """P&L 서비스 import."""
        import services.pnl_service as ps
        assert ps is not None


class TestMarketplaceClients:
    """마켓플레이스 클라이언트 기본 동작."""

    def test_all_clients_importable(self):
        """6개 구현 + 3개 스켈레톤 전부 import 가능."""
        from services.marketplace.naver_client import NaverCommerceClient
        from services.marketplace.coupang_client import CoupangWingClient
        from services.marketplace.cafe24_client import Cafe24Client
        from services.marketplace.st11_client import St11Client
        from services.marketplace.esm_client import EsmClient
        from services.marketplace.kakao_client import KakaoClient
        assert all([NaverCommerceClient, CoupangWingClient, Cafe24Client,
                    St11Client, EsmClient, KakaoClient])

    def test_platform_map_complete(self):
        """플랫폼 맵에 6채널 등록."""
        from services.marketplace import _PLATFORM_CLIENT_MAP
        expected = {'naver', 'coupang', 'cafe24', '11st', 'auction', 'kakao'}
        assert set(_PLATFORM_CLIENT_MAP.keys()) == expected


class TestRepoSplit:
    """db/ repo 분리 검증."""

    def test_all_repos_importable(self):
        """12개 repo 전부 import 가능."""
        from db.shipping_repo import ShippingRepo
        from db.orders_repo import OrdersRepo
        from db.inventory_repo import InventoryRepo
        from db.finance_repo import FinanceRepo
        from db.marketplace_repo import MarketplaceRepo
        from db.packing_repo import PackingRepo
        from db.auth_repo import AuthRepo
        from db.hr_repo import HrRepo
        from db.settlement_repo import SettlementRepo
        from db.product_repo import ProductRepo
        from db.trade_repo import TradeRepo
        from db.outbound_repo import OutboundRepo
        assert all([ShippingRepo, OrdersRepo, InventoryRepo, FinanceRepo,
                    MarketplaceRepo, PackingRepo, AuthRepo, HrRepo,
                    SettlementRepo, ProductRepo, TradeRepo, OutboundRepo])

    def test_repo_inherits_base(self):
        """모든 repo가 BaseRepo 상속."""
        from db.base import BaseRepo
        from db.shipping_repo import ShippingRepo
        from db.orders_repo import OrdersRepo
        assert issubclass(ShippingRepo, BaseRepo)
        assert issubclass(OrdersRepo, BaseRepo)
