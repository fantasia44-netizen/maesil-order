"""
db/packing_repo.py — 패킹센터 작업 DB Repository.

db_supabase.py에서 분리 (2026-03-23).
메서드 7개.
"""
from .base import BaseRepo


class PackingRepo(BaseRepo):
    """패킹센터 작업 DB Repository."""

    def insert_packing_job(self, payload):
        """패킹 작업 생성."""
        try:
            res = self.client.table("packing_jobs").insert(payload).execute()
            return res.data[0] if res.data else None
        except Exception as e:
            print(f"[DB] insert_packing_job error: {e}")
            return None


    def get_packing_job(self, job_id):
        """패킹 작업 단건 조회."""
        try:
            res = self.client.table("packing_jobs").select("*") \
                .eq("id", job_id).execute()
            return res.data[0] if res.data else None
        except Exception as e:
            print(f"[DB] get_packing_job error: {e}")
            return None


    def update_packing_job(self, job_id, update_data, biz_id=None):
        """패킹 작업 업데이트."""
        try:
            q = self.client.table("packing_jobs").update(update_data).eq("id", job_id)
            self._with_biz(q, biz_id).execute()
            return True
        except Exception as e:
            print(f"[DB] update_packing_job error: {e}")
            return False


    def query_packing_jobs(self, user_id=None, date_from=None, date_to=None,
                           search=None, limit=20, offset=0):
        """패킹 작업 목록 조회."""
        try:
            q = self.client.table("packing_jobs").select("*") \
                .eq("status", "completed")
            if user_id:
                q = q.eq("user_id", user_id)
            if date_from:
                q = q.gte("started_at", f"{date_from}T00:00:00+00:00")
            if date_to:
                q = q.lte("started_at", f"{date_to}T23:59:59+00:00")
            if search:
                q = q.or_(
                    f"scanned_barcode.ilike.%{search}%,"
                    f"product_name.ilike.%{search}%,"
                    f"order_no.ilike.%{search}%"
                )
            q = q.order("completed_at", desc=True)
            q = q.range(offset, offset + limit - 1)
            res = q.execute()
            return res.data or []
        except Exception as e:
            print(f"[DB] query_packing_jobs error: {e}")
            return []


    def count_packing_jobs(self, user_id=None, date_from=None, date_to=None,
                           search=None):
        """패킹 작업 건수."""
        try:
            q = self.client.table("packing_jobs").select("id", count="exact") \
                .eq("status", "completed")
            if user_id:
                q = q.eq("user_id", user_id)
            if date_from:
                q = q.gte("started_at", f"{date_from}T00:00:00+00:00")
            if date_to:
                q = q.lte("started_at", f"{date_to}T23:59:59+00:00")
            if search:
                q = q.or_(
                    f"scanned_barcode.ilike.%{search}%,"
                    f"product_name.ilike.%{search}%,"
                    f"order_no.ilike.%{search}%"
                )
            res = q.execute()
            return res.count if res.count is not None else 0
        except Exception as e:
            print(f"[DB] count_packing_jobs error: {e}")
            return 0

    # ── Packing Video Storage ─────────────────────────────


    def upload_packing_video(self, path, video_bytes):
        """패킹 영상 Supabase Storage 업로드."""
        try:
            self.client.storage.from_("packing-videos").upload(
                path, video_bytes,
                file_options={"content-type": "video/webm"}
            )
            return True
        except Exception as e:
            print(f"[DB] upload_packing_video error: {e}")
            raise


    def get_packing_video_signed_url(self, path, expires_in=3600):
        """패킹 영상 서명 URL 생성."""
        try:
            res = self.client.storage.from_("packing-videos") \
                .create_signed_url(path, expires_in)
            if isinstance(res, dict):
                return res.get('signedURL', '') or res.get('signedUrl', '')
            return ''
        except Exception as e:
            print(f"[DB] get_packing_video_signed_url error: {e}")
            return ''

    # ── 범용 File Storage (Supabase Storage) ─────────────
    # 버킷: order-outputs (출력), order-uploads (업로드), order-reports (PDF)

    STORAGE_BUCKETS = {
        'output': 'order-outputs',
        'upload': 'order-uploads',
        'report': 'order-reports',
    }


