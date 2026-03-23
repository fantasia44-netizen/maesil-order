"""
db/outbound_repo.py — 출고/마감/출고통계 DB Repository.

db_supabase.py에서 분리 (2026-03-23).
메서드 6개.
"""
from .base import BaseRepo


class OutboundRepo(BaseRepo):
    """출고/마감/출고통계 DB Repository."""

    def get_closing_status(self, closing_date, closing_type):
        """특정 날짜+유형의 마감 상태 조회. None이면 미생성(open)."""
        res = (self.client.table("daily_closing")
               .select("*")
               .eq("closing_date", closing_date)
               .eq("closing_type", closing_type)
               .limit(1).execute())
        return res.data[0] if res.data else None


    def query_closing_list(self, date_from=None, date_to=None, closing_type=None):
        """마감 이력 조회."""
        q = self.client.table("daily_closing").select("*").order("closing_date", desc=True)
        if date_from:
            q = q.gte("closing_date", date_from)
        if date_to:
            q = q.lte("closing_date", date_to)
        if closing_type:
            q = q.eq("closing_type", closing_type)
        res = q.limit(200).execute()
        return res.data or []

    # --- master tables ---


    def query_pending_outbound_orders(self, date_from=None, date_to=None, channel=None):
        """미처리 주문 조회 (is_outbound_done=false, status='정상').
        collection_date 기준 필터 (stock_ledger/통합집계와 일관성 유지).
        collection_date가 NULL인 주문은 order_date로 fallback.
        """
        try:
            results = []
            # 1) collection_date가 있는 주문
            q = self.client.table("order_transactions").select("*") \
                .eq("is_outbound_done", False).eq("status", "정상") \
                .not_.is_("collection_date", "null")
            if date_from:
                q = q.gte("collection_date", date_from)
            if date_to:
                q = q.lte("collection_date", date_to)
            if channel:
                q = q.eq("channel", channel)
            q = q.order("collection_date").order("id")
            res = q.execute()
            results.extend(res.data or [])

            # 2) collection_date가 NULL인 주문 (기존 데이터 호환)
            q2 = self.client.table("order_transactions").select("*") \
                .eq("is_outbound_done", False).eq("status", "정상") \
                .is_("collection_date", "null")
            if date_from:
                q2 = q2.gte("order_date", date_from)
            if date_to:
                q2 = q2.lte("order_date", date_to)
            if channel:
                q2 = q2.eq("channel", channel)
            q2 = q2.order("order_date").order("id")
            res2 = q2.execute()
            results.extend(res2.data or [])

            return results
        except Exception as e:
            print(f"[DB] query_pending_outbound_orders error: {e}")
            return []


    def mark_orders_outbound_done(self, order_ids, outbound_date, revenue_category=None):
        """주문 출고 완료 표시."""
        try:
            update_data = {"is_outbound_done": True, "outbound_date": outbound_date}
            if revenue_category:
                update_data["revenue_category"] = revenue_category
            for chunk_start in range(0, len(order_ids), 50):
                chunk = order_ids[chunk_start:chunk_start + 50]
                self.client.table("order_transactions").update(update_data) \
                    .in_("id", chunk).execute()
        except Exception as e:
            print(f"[DB] mark_orders_outbound_done error: {e}")


    def query_outbound_summary(self, date_from=None, date_to=None):
        """출고 처리 현황 요약."""
        try:
            q_pending = self.client.table("order_transactions") \
                .select("id", count="exact") \
                .eq("is_outbound_done", False).eq("status", "정상")
            q_done = self.client.table("order_transactions") \
                .select("id", count="exact") \
                .eq("is_outbound_done", True)
            if date_from:
                q_pending = q_pending.gte("order_date", date_from)
                q_done = q_done.gte("order_date", date_from)
            if date_to:
                q_pending = q_pending.lte("order_date", date_to)
                q_done = q_done.lte("order_date", date_to)
            p_res = q_pending.execute()
            d_res = q_done.execute()
            return {
                "pending": p_res.count if p_res.count is not None else len(p_res.data or []),
                "done": d_res.count if d_res.count is not None else len(d_res.data or []),
            }
        except Exception as e:
            print(f"[DB] query_outbound_summary error: {e}")
            return {"pending": 0, "done": 0}

    # ================================================================
    # Phase 3: 대시보드 쿼리
    # ================================================================


    def reset_order_outbound(self, order_id, biz_id=None):
        """주문 출고 상태 초기화 (취소/환불 시)."""
        try:
            q = self.client.table("order_transactions").update({
                "is_outbound_done": False,
                "outbound_date": None,
                "revenue_category": None,
            }).eq("id", order_id)
            self._with_biz(q, biz_id).execute()
        except Exception as e:
            print(f"[DB] reset_order_outbound error: {e}")


