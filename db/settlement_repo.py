"""
db/settlement_repo.py — 정산/대사 DB Repository.

db_supabase.py에서 분리 (2026-03-23).
메서드 4개.
"""
from .base import BaseRepo


class SettlementRepo(BaseRepo):
    """정산/대사 DB Repository."""

    def query_platform_settlements(self, channel=None, match_status=None,
                                    date_from=None, date_to=None):
        """플랫폼 정산 조회."""
        try:
            q = self.client.table("platform_settlements") \
                .select("*").order("settlement_date", desc=True)
            if channel:
                q = q.eq("channel", channel)
            if match_status:
                q = q.eq("match_status", match_status)
            if date_from:
                q = q.gte("settlement_date", date_from)
            if date_to:
                q = q.lte("settlement_date", date_to)
            res = q.execute()
            return res.data or []
        except Exception as e:
            print(f"[DB] query_platform_settlements error: {e}")
            return []


    def insert_platform_settlement(self, payload):
        """플랫폼 정산 등록 (upsert)."""
        try:
            self.client.table("platform_settlements").upsert(
                payload, on_conflict="channel,settlement_date,api_reference"
            ).execute()
        except Exception as e:
            print(f"[DB] insert_platform_settlement error: {e}")


    def update_platform_settlement(self, settlement_id, update_data, biz_id=None):
        """플랫폼 정산 수정 (매칭 상태 등)."""
        try:
            q = self.client.table("platform_settlements").update(update_data).eq("id", settlement_id)
            self._with_biz(q, biz_id).execute()
        except Exception as e:
            print(f"[DB] update_platform_settlement error: {e}")


    def query_platform_settlement_by_id(self, settlement_id):
        """플랫폼 정산 1건 조회."""
        try:
            res = self.client.table("platform_settlements") \
                .select("*").eq("id", settlement_id).execute()
            return res.data[0] if res.data else None
        except Exception as e:
            print(f"[DB] query_platform_settlement_by_id error: {e}")
            return None

    # ── platform_fee_config ──


