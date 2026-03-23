"""
test_01_orders.py — 주문수집 + 중복방지 테스트.

P0 테스트 #1: 주문 생성, 중복 스킵, 옵션매칭 기본.
"""
import pytest


class TestOrderCollection:
    """주문수집 기본 동작."""

    def test_order_transactions_exist(self, db):
        """DB에 주문 데이터가 존재하는지."""
        result = db.query_order_transactions(limit=1)
        assert result is not None
        assert len(result) > 0, "order_transactions에 데이터 없음"

    def test_order_has_required_fields(self, db):
        """주문 레코드에 필수 필드가 있는지."""
        result = db.query_order_transactions(limit=1)
        order = result[0]
        required = ['channel', 'order_no', 'product_name', 'qty', 'status']
        for field in required:
            assert field in order, f"필수 필드 누락: {field}"

    def test_duplicate_prevention(self, supabase_client):
        """동일 order_no + channel + line_no는 중복 생성 안 됨."""
        # 기존 주문 1건 가져오기
        res = supabase_client.table('order_transactions') \
            .select('channel, order_no, line_no, raw_hash') \
            .limit(1).execute()
        assert res.data, "테스트 데이터 없음"
        existing = res.data[0]

        # 같은 order_no로 조회 → 1건만 있어야
        dupes = supabase_client.table('order_transactions') \
            .select('id') \
            .eq('channel', existing['channel']) \
            .eq('order_no', existing['order_no']) \
            .eq('line_no', existing['line_no']) \
            .execute()
        assert len(dupes.data) == 1, f"중복 주문 발견: {len(dupes.data)}건"

    def test_option_matcher_import(self):
        """옵션매칭 서비스 import 가능."""
        from services.option_matcher import match_option
        assert callable(match_option)


class TestOrderShipping:
    """주문-배송 연결."""

    def test_shipping_linked_to_order(self, supabase_client):
        """order_shipping에 order_no가 있으면 order_transactions에도 있어야."""
        ship = supabase_client.table('order_shipping') \
            .select('channel, order_no') \
            .limit(5).execute()

        for s in ship.data:
            tx = supabase_client.table('order_transactions') \
                .select('id').eq('order_no', s['order_no']) \
                .eq('channel', s['channel']).limit(1).execute()
            assert tx.data, f"order_shipping({s['order_no']})에 대응하는 order_transactions 없음"
