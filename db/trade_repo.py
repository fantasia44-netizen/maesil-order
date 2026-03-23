"""
db/trade_repo.py — 거래처/매입/입고 DB Repository.

db_supabase.py에서 분리 (2026-03-23).
메서드 9개.
"""
from .base import BaseRepo


class TradeRepo(BaseRepo):
    """거래처/매입/입고 DB Repository."""

    def query_manual_trades(self, date_from=None, date_to=None, partner_name=None):
        """수동 거래 조회 (날짜/거래처 필터)."""
        def builder(table):
            q = self.client.table(table).select("*").or_("is_deleted.is.null,is_deleted.eq.false").order("trade_date", desc=True)
            if date_from:
                q = q.gte("trade_date", date_from)
            if date_to:
                q = q.lte("trade_date", date_to)
            if partner_name and partner_name != "전체":
                q = q.eq("partner_name", partner_name)
            return q
        return self._paginate_query("manual_trades", builder)


    def insert_manual_trade(self, payload):
        """수동 거래 1건 등록."""
        self.client.table("manual_trades").insert(payload).execute()


    def query_manual_trade_by_id(self, trade_id):
        """수동 거래 1건 조회 (ID 기준)."""
        res = self.client.table("manual_trades").select("*").eq("id", trade_id).or_("is_deleted.is.null,is_deleted.eq.false").execute()
        return res.data[0] if res.data else None


    def delete_manual_trade(self, trade_id, biz_id=None):
        """수동 거래 1건 소프트 삭제."""
        q = self.client.table("manual_trades").update(
            {"is_deleted": True}
        ).eq("id", trade_id)
        self._with_biz(q, biz_id).execute()


    def insert_purchase_order(self, payload):
        """발주서 1건 저장."""
        self.client.table("purchase_orders").insert(payload).execute()


    def query_purchase_orders(self, date_from=None, date_to=None, partner_name=None):
        """발주서 이력 조회 (날짜/거래처 필터)."""
        def builder(table):
            q = self.client.table(table).select("*").or_("is_deleted.is.null,is_deleted.eq.false").order("order_date", desc=True)
            if date_from:
                q = q.gte("order_date", date_from)
            if date_to:
                q = q.lte("order_date", date_to)
            if partner_name and partner_name != "전체":
                q = q.eq("partner_name", partner_name)
            return q
        return self._paginate_query("purchase_orders", builder)


    def query_purchase_order_by_id(self, po_id):
        """발주서 1건 조회 (ID 기준)."""
        res = self.client.table("purchase_orders").select("*").eq("id", po_id).or_("is_deleted.is.null,is_deleted.eq.false").execute()
        return res.data[0] if res.data else None


    def update_purchase_order(self, po_id, update_data, biz_id=None):
        """발주서 1건 수정."""
        q = self.client.table("purchase_orders").update(update_data).eq("id", po_id)
        self._with_biz(q, biz_id).execute()


    def delete_purchase_order(self, po_id, biz_id=None):
        """발주서 1건 소프트 삭제."""
        q = self.client.table("purchase_orders").update(
            {"is_deleted": True}
        ).eq("id", po_id)
        self._with_biz(q, biz_id).execute()

    # --- 품목명 공백 정리 ---


