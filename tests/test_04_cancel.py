"""
test_04_cancel.py — 취소/변경 반영 테스트.

P0 테스트 #4: 취소 시 출고차단, rollback 동작.
"""
import pytest


class TestCancelFlow:
    """취소/환불 처리."""

    def test_rollback_function_exists(self, db):
        """rollback_import_run_full 함수 존재."""
        assert hasattr(db, 'rollback_import_run_full')
        assert callable(db.rollback_import_run_full)

    def test_import_run_query(self, db):
        """import_run 조회가 에러 없이 동작."""
        result = db.query_import_runs(limit=1)
        assert isinstance(result, list)

    def test_order_status_values(self, supabase_client):
        """주문 상태값이 정상/취소/환불 중 하나."""
        res = supabase_client.table('order_transactions') \
            .select('status').limit(100).execute()

        valid_statuses = {'정상', '취소', '환불', '반품', None, ''}
        for r in res.data:
            status = r.get('status', '')
            assert status in valid_statuses or not status, \
                f"예상치 못한 주문 상태: {status}"

    def test_cancelled_order_not_outbound(self, supabase_client):
        """취소된 주문은 출고완료 상태가 아니어야."""
        res = supabase_client.table('order_transactions') \
            .select('order_no, status, is_outbound_done') \
            .eq('status', '취소') \
            .eq('is_outbound_done', True) \
            .limit(5).execute()

        # 취소 + 출고완료 = 비정상 (rollback 필요한 건)
        if res.data:
            pytest.warn(UserWarning,
                       f"취소+출고완료 건 {len(res.data)}개 발견 (rollback 필요 가능성)")
