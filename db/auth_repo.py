"""
db/auth_repo.py — 사용자/권한/감사로그 DB Repository.

db_supabase.py에서 분리 (2026-03-23).
메서드 14개.
"""
from .base import BaseRepo


class AuthRepo(BaseRepo):
    """사용자/권한/감사로그 DB Repository."""

    def query_user_by_id(self, user_id):
        """ID로 사용자 조회. dict or None."""
        try:
            res = self.client.table("app_users").select(self._USER_COLS) \
                .eq("id", user_id).limit(1).execute()
            return res.data[0] if res.data else None
        except Exception:
            return None


    def query_user_by_username(self, username):
        """username으로 사용자 조회. dict or None."""
        try:
            res = self.client.table("app_users").select(self._USER_COLS) \
                .eq("username", username).limit(1).execute()
            return res.data[0] if res.data else None
        except Exception:
            return None


    def insert_user(self, payload):
        """사용자 등록."""
        self.client.table("app_users").insert(payload).execute()


    def update_user(self, user_id, update_data, biz_id=None):
        """사용자 정보 수정."""
        q = self.client.table("app_users").update(update_data).eq("id", user_id)
        self._with_biz(q, biz_id).execute()


    def query_all_users(self):
        """전체 사용자 목록."""
        def builder(table):
            return self.client.table(table).select("*").order("created_at", desc=True)
        return self._paginate_query("app_users", builder)


    def count_pending_users(self):
        """승인 대기 사용자 수 (60초 TTL 캐시)."""
        now = time.time()
        if now - self._pending_cache['ts'] < 60:
            return self._pending_cache['count']
        try:
            res = self.client.table("app_users").select("id", count="exact") \
                .eq("is_approved", False).eq("is_active_user", True).execute()
            cnt = res.count if res.count is not None else len(res.data)
            self._pending_cache.update(count=cnt, ts=now)
            return cnt
        except Exception:
            return self._pending_cache.get('count', 0)

    # ================================================================
    # 감사 로그 (audit_logs)
    # ================================================================


    def insert_audit_log(self, payload):
        """감사 로그 기록 (old_value/new_value JSON 지원)."""
        import json
        # JSONB 필드는 dict → JSON string 변환
        for key in ('old_value', 'new_value'):
            if key in payload and payload[key] is not None:
                if isinstance(payload[key], (dict, list)):
                    payload[key] = json.dumps(payload[key], ensure_ascii=False)
        self.client.table("audit_logs").insert(payload).execute()


    def query_audit_logs(self, page=1, per_page=50, action_filter=None,
                         user_filter=None, date_from=None, date_to=None):
        """감사 로그 페이지네이션 조회 (필터 지원). returns (items, total_count)."""
        try:
            # 전체 건수 (필터 적용)
            count_q = self.client.table("audit_logs").select("id", count="exact")
            if action_filter:
                count_q = count_q.ilike("action", f"%{action_filter}%")
            if user_filter:
                count_q = count_q.ilike("user_name", f"%{user_filter}%")
            if date_from:
                count_q = count_q.gte("created_at", date_from)
            if date_to:
                count_q = count_q.lte("created_at", date_to + 'T23:59:59')
            count_res = count_q.limit(1).execute()
            total = count_res.count if count_res.count is not None else 0

            # 페이지 데이터
            offset = (page - 1) * per_page
            data_q = self.client.table("audit_logs").select("*") \
                .order("created_at", desc=True)
            if action_filter:
                data_q = data_q.ilike("action", f"%{action_filter}%")
            if user_filter:
                data_q = data_q.ilike("user_name", f"%{user_filter}%")
            if date_from:
                data_q = data_q.gte("created_at", date_from)
            if date_to:
                data_q = data_q.lte("created_at", date_to + 'T23:59:59')
            data_res = data_q.range(offset, offset + per_page - 1).execute()
            items = data_res.data or []

            return items, total
        except Exception:
            return [], 0


    def query_audit_log_by_id(self, log_id):
        """감사 로그 1건 조회."""
        try:
            res = self.client.table("audit_logs").select("*") \
                .eq("id", log_id).limit(1).execute()
            return res.data[0] if res.data else None
        except Exception:
            return None


    def update_audit_log(self, log_id, update_data, biz_id=None):
        """감사 로그 수정 (롤백 상태 기록용)."""
        q = self.client.table("audit_logs").update(update_data).eq("id", log_id)
        self._with_biz(q, biz_id).execute()

    # --- 소프트 삭제 / 블라인드 / 교체 지원 ---


    def query_role_permissions(self, use_cache=True):
        """권한 전체 조회 → {role: {page_key: bool}}. TTL 캐시."""
        now = time.time()
        if use_cache and self._perm_cache['data'] and (now - self._perm_cache['ts']) < self._perm_cache['ttl']:
            return self._perm_cache['data']
        try:
            res = self.client.table("role_permissions").select("role,page_key,is_allowed").execute()
            perms = {}
            for row in (res.data or []):
                role = row['role']
                if role not in perms:
                    perms[role] = {}
                perms[role][row['page_key']] = row['is_allowed']
            self._perm_cache['data'] = perms
            self._perm_cache['ts'] = time.time()
            return perms
        except Exception:
            return self._perm_cache['data'] or {}


    def upsert_role_permissions(self, role, perms_dict):
        """한 역할의 권한 일괄 저장. perms_dict = {page_key: bool}."""
        from datetime import datetime, timezone
        now_str = datetime.now(timezone.utc).isoformat()
        payload = [
            {'role': role, 'page_key': pk, 'is_allowed': allowed, 'updated_at': now_str}
            for pk, allowed in perms_dict.items()
        ]
        if payload:
            self.client.table("role_permissions").upsert(
                payload, on_conflict="role,page_key"
            ).execute()
        self._invalidate_perm_cache()


    def seed_default_permissions(self, page_registry):
        """테이블이 비어있으면 PAGE_REGISTRY 기본값으로 초기화."""
        try:
            res = self.client.table("role_permissions").select("id").limit(1).execute()
            if res.data:
                return  # 이미 데이터 있음
        except Exception:
            return
        from datetime import datetime, timezone
        from config import Config
        now_str = datetime.now(timezone.utc).isoformat()
        payload = []
        for page_key, name, icon, url, default_roles, *_ in page_registry:
            for role in Config.ROLES.keys():
                payload.append({
                    'role': role,
                    'page_key': page_key,
                    'is_allowed': role in default_roles,
                    'updated_at': now_str,
                })
        # 500건씩 배치 upsert
        for i in range(0, len(payload), 500):
            self.client.table("role_permissions").upsert(
                payload[i:i+500], on_conflict="role,page_key"
            ).execute()
        self._invalidate_perm_cache()


    def upload_user_file(self, path, file_bytes, content_type=None):
        return self._storage_upload('upload', path, file_bytes, content_type)


