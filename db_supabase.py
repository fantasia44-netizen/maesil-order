"""
db_supabase.py — Supabase 구현 (CRUD/조회). 보고서용 조회는 raw rows까지만.
+ 사용자 인증/관리 CRUD (app_users, audit_logs)
"""
import pandas as pd
from supabase import create_client, Client
from db_base import DBBase
from config import SUPABASE_URL, SUPABASE_KEY


class SupabaseDB(DBBase):
    def __init__(self):
        self.client: Client = None
        self._db_cols = None

    def connect(self):
        try:
            self.client = create_client(SUPABASE_URL, SUPABASE_KEY)
            try:
                self.client.rpc("ensure_stock_ledger_columns", {}).execute()
            except Exception as col_err:
                print(f"Column migration note: {col_err}")
            self._db_cols = self.get_db_columns()
            return True
        except Exception:
            return False

    def get_db_columns(self):
        try:
            sample = self.client.table("stock_ledger").select("*").limit(1).execute()
            if sample.data:
                return set(sample.data[0].keys())
            return None
        except:
            return None

    def _filter_payload(self, payload_list):
        """DB에 없는 컬럼을 payload에서 자동 제거."""
        if not self._db_cols or not payload_list:
            return payload_list
        return [{k: v for k, v in row.items() if k in self._db_cols} for row in payload_list]

    def _paginate_query(self, table, query_builder):
        """페이지네이션으로 전체 데이터 조회."""
        all_data = []
        offset = 0
        while True:
            res = query_builder(table).range(offset, offset + 999).execute()
            if not res.data:
                break
            all_data.extend(res.data)
            if len(res.data) < 1000:
                break
            offset += 1000
        return all_data

    # --- stock_ledger CRUD ---

    @staticmethod
    def _normalize_product_names(payload_list):
        """품목명 공백 정규화 — '(수)건해삼채 200g' → '(수)건해삼채200g'."""
        for row in payload_list:
            pn = row.get('product_name', '')
            if pn:
                row['product_name'] = str(pn).replace(' ', '').strip()
        return payload_list

    def insert_stock_ledger(self, payload_list):
        if not payload_list:
            return
        payload_list = self._normalize_product_names(payload_list)
        filtered = self._filter_payload(payload_list)
        self.client.table("stock_ledger").insert(filtered).execute()

    def delete_stock_ledger_all(self):
        res = self.client.table("stock_ledger").delete().neq("id", 0).execute()
        return len(res.data) if res.data else 0

    def delete_stock_ledger_by(self, date_str, record_type, location=None):
        q = self.client.table("stock_ledger").delete() \
            .eq("transaction_date", date_str).eq("type", record_type)
        if location:
            q = q.eq("location", location)
        res = q.execute()
        return len(res.data) if res.data else 0

    def query_stock_ledger(self, date_to, date_from=None, location=None,
                            category=None, type_list=None, order_desc=False):
        def builder(table):
            q = self.client.table(table).select("*")
            q = q.lte("transaction_date", date_to)
            if date_from:
                q = q.gte("transaction_date", date_from)
            if location and location != "전체":
                q = q.eq("location", location)
            if category and category != "전체":
                q = q.eq("category", category)
            if type_list:
                q = q.in_("type", type_list)
            if order_desc:
                q = q.order("transaction_date", desc=True)
            return q
        return self._paginate_query("stock_ledger", builder)

    def query_stock_by_location(self, location, select_fields=None):
        base_fields = ["product_name", "qty", "category", "expiry_date",
                        "storage_method", "unit", "lot_number", "grade"]
        opt_fields = ["origin", "manufacture_date"]
        if select_fields is None:
            sel = base_fields + [f for f in opt_fields if not self._db_cols or f in self._db_cols]
        else:
            sel = select_fields
        sel_str = ",".join(sel)

        def builder(table):
            return self.client.table(table).select(sel_str).eq("location", location)
        return self._paginate_query("stock_ledger", builder)

    def query_filter_options(self):
        def builder(table):
            return self.client.table(table).select("location,category")
        all_vals = self._paginate_query("stock_ledger", builder)
        locs = sorted(set(r['location'] for r in all_vals if r.get('location')))
        cats = sorted(set(r['category'] for r in all_vals if r.get('category')))
        return locs, cats

    def query_unit_for_product(self, product_name):
        try:
            res = self.client.table("stock_ledger").select("unit") \
                .eq("product_name", product_name).limit(1).execute()
            if res.data:
                return res.data[0].get('unit') or ''
            return None
        except:
            return None

    def update_stock_ledger(self, row_id, update_data):
        if self._db_cols:
            update_data = {k: v for k, v in update_data.items() if k in self._db_cols}
        self.client.table("stock_ledger").update(update_data).eq("id", row_id).execute()

    def delete_stock_ledger_by_id(self, row_id):
        self.client.table("stock_ledger").delete().eq("id", row_id).execute()

    # --- daily_revenue ---

    def upsert_revenue(self, payload_list):
        if not payload_list:
            return
        self.client.table("daily_revenue").upsert(
            payload_list, on_conflict="revenue_date,product_name,category"
        ).execute()

    def query_revenue(self, date_from=None, date_to=None, category=None):
        def builder(table):
            q = self.client.table(table).select("*").order("revenue_date", desc=True)
            if date_from:
                q = q.gte("revenue_date", date_from)
            if date_to:
                q = q.lte("revenue_date", date_to)
            if category and category != "전체":
                q = q.eq("category", category)
            return q
        return self._paginate_query("daily_revenue", builder)

    def delete_revenue_all(self):
        res = self.client.table("daily_revenue").delete().neq("id", 0).execute()
        return len(res.data) if res.data else 0

    def delete_revenue_by_date(self, date_from=None, date_to=None):
        query = self.client.table("daily_revenue").delete()
        if date_from:
            query = query.gte("revenue_date", date_from)
        if date_to:
            query = query.lte("revenue_date", date_to)
        res = query.execute()
        return len(res.data) if res.data else 0

    def delete_revenue_by_id(self, revenue_id):
        """daily_revenue 1건 삭제 (ID 기준)."""
        self.client.table("daily_revenue").delete().eq("id", revenue_id).execute()

    # --- master tables ---

    def sync_master_table(self, table_name, payload_list, batch_size=500):
        self.client.table(table_name).delete().neq("id", 0).execute()
        for i in range(0, len(payload_list), batch_size):
            self.client.table(table_name).insert(payload_list[i:i + batch_size]).execute()

    def query_master_table(self, table_name):
        def builder(table):
            return self.client.table(table).select("*")
        return self._paginate_query(table_name, builder)

    def count_master_table(self, table_name):
        try:
            res = self.client.table(table_name).select("id", count="exact").limit(1).execute()
            return res.count if res.count is not None else len(res.data)
        except:
            return -1

    # --- business_partners ---

    def query_partners(self):
        """거래처 전체 조회 (이름순)."""
        def builder(table):
            return self.client.table(table).select("*").order("partner_name")
        return self._paginate_query("business_partners", builder)

    def insert_partner(self, payload):
        """거래처 1건 등록."""
        self.client.table("business_partners").insert(payload).execute()

    def insert_partners_batch(self, payload_list):
        """거래처 엑셀 일괄 등록."""
        if not payload_list:
            return
        for i in range(0, len(payload_list), 500):
            self.client.table("business_partners").insert(
                payload_list[i:i + 500]).execute()

    def delete_partner(self, partner_id):
        """거래처 1건 삭제."""
        self.client.table("business_partners").delete().eq("id", partner_id).execute()

    # --- my_business ---

    def query_my_business(self):
        """내 사업장 전체 조회."""
        def builder(table):
            return self.client.table(table).select("*").order("id")
        return self._paginate_query("my_business", builder)

    def upsert_my_business(self, payload):
        """내 사업장 등록/수정."""
        self.client.table("my_business").upsert(payload).execute()

    def delete_my_business(self, biz_id):
        """내 사업장 삭제."""
        self.client.table("my_business").delete().eq("id", biz_id).execute()

    def set_default_business(self, biz_id):
        """기본 사업장 지정 (나머지 해제)."""
        all_biz = self.query_my_business()
        for b in all_biz:
            self.client.table("my_business").update(
                {"is_default": b["id"] == biz_id}
            ).eq("id", b["id"]).execute()

    def query_default_business(self):
        """기본 사업장 1건 반환. 없으면 첫 번째 레코드."""
        all_biz = self.query_my_business()
        if not all_biz:
            return {}
        for b in all_biz:
            if b.get("is_default"):
                return b
        return all_biz[0]

    # --- manual_trades ---

    def query_manual_trades(self, date_from=None, date_to=None, partner_name=None):
        """수동 거래 조회 (날짜/거래처 필터)."""
        def builder(table):
            q = self.client.table(table).select("*").order("trade_date", desc=True)
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
        res = self.client.table("manual_trades").select("*").eq("id", trade_id).execute()
        return res.data[0] if res.data else None

    def delete_manual_trade(self, trade_id):
        """수동 거래 1건 삭제."""
        self.client.table("manual_trades").delete().eq("id", trade_id).execute()

    def delete_revenue_specific(self, revenue_date, product_name, category):
        """daily_revenue에서 특정 조건의 레코드 삭제."""
        res = (self.client.table("daily_revenue")
               .delete()
               .eq("revenue_date", revenue_date)
               .eq("product_name", product_name)
               .eq("category", category)
               .execute())
        return len(res.data) if res.data else 0

    # --- 품목명 공백 정리 ---

    def fix_product_name_spaces(self):
        """stock_ledger + daily_revenue의 품목명에서 공백을 제거하여 통합.
        예: '(수)건해삼채 200g' → '(수)건해삼채200g'
        returns: (fixed_count, duplicate_groups) 수정된 건수와 중복 그룹 목록
        """
        # stock_ledger
        all_rows = self._paginate_query("stock_ledger",
            lambda t: self.client.table(t).select("id,product_name"))
        fixed = 0
        dupes = {}
        for r in all_rows:
            pn = r.get('product_name', '')
            norm = str(pn).replace(' ', '').strip()
            if norm != pn:
                # 공백이 있는 이름 → 정규화된 이름으로 UPDATE
                self.client.table("stock_ledger").update(
                    {"product_name": norm}).eq("id", r['id']).execute()
                fixed += 1
                if norm not in dupes:
                    dupes[norm] = set()
                dupes[norm].add(pn)
                dupes[norm].add(norm)

        # daily_revenue
        rev_rows = self._paginate_query("daily_revenue",
            lambda t: self.client.table(t).select("id,product_name"))
        for r in rev_rows:
            pn = r.get('product_name', '')
            norm = str(pn).replace(' ', '').strip()
            if norm != pn:
                self.client.table("daily_revenue").update(
                    {"product_name": norm}).eq("id", r['id']).execute()
                fixed += 1

        return fixed, dupes

    # ================================================================
    # 사용자 관리 (app_users) CRUD
    # ================================================================

    def query_user_by_id(self, user_id):
        """ID로 사용자 조회. dict or None."""
        try:
            res = self.client.table("app_users").select("*") \
                .eq("id", user_id).limit(1).execute()
            return res.data[0] if res.data else None
        except Exception:
            return None

    def query_user_by_username(self, username):
        """username으로 사용자 조회. dict or None."""
        try:
            res = self.client.table("app_users").select("*") \
                .eq("username", username).limit(1).execute()
            return res.data[0] if res.data else None
        except Exception:
            return None

    def insert_user(self, payload):
        """사용자 등록."""
        self.client.table("app_users").insert(payload).execute()

    def update_user(self, user_id, update_data):
        """사용자 정보 수정."""
        self.client.table("app_users").update(update_data) \
            .eq("id", user_id).execute()

    def query_all_users(self):
        """전체 사용자 목록."""
        def builder(table):
            return self.client.table(table).select("*").order("created_at", desc=True)
        return self._paginate_query("app_users", builder)

    def count_pending_users(self):
        """승인 대기 사용자 수."""
        try:
            res = self.client.table("app_users").select("id", count="exact") \
                .eq("is_approved", False).eq("is_active_user", True).execute()
            return res.count if res.count is not None else len(res.data)
        except Exception:
            return 0

    # ================================================================
    # 감사 로그 (audit_logs)
    # ================================================================

    def insert_audit_log(self, payload):
        """감사 로그 기록."""
        self.client.table("audit_logs").insert(payload).execute()

    def query_audit_logs(self, page=1, per_page=50):
        """감사 로그 페이지네이션 조회. returns (items, total_count)."""
        try:
            # 전체 건수
            count_res = self.client.table("audit_logs").select("id", count="exact") \
                .limit(1).execute()
            total = count_res.count if count_res.count is not None else 0

            # 페이지 데이터
            offset = (page - 1) * per_page
            data_res = self.client.table("audit_logs").select("*") \
                .order("created_at", desc=True) \
                .range(offset, offset + per_page - 1).execute()
            items = data_res.data or []

            return items, total
        except Exception:
            return [], 0
