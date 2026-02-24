"""
db_supabase.py — Supabase 구현 (CRUD/조회). 보고서용 조회는 raw rows까지만.
+ 사용자 인증/관리 CRUD (app_users, audit_logs)
"""
import time
import pandas as pd
from supabase import create_client, Client
from db_base import DBBase
from config import SUPABASE_URL, SUPABASE_KEY

# 옵션마스터 메모리 캐시 (TTL 기반)
_option_cache = {
    'data': None,         # 전체 옵션 목록
    'data_list': None,    # OrderProcessor 호환 dict list
    'ts': 0,              # 마지막 로드 시간
    'ttl': 300,           # 5분 캐시 (초)
}


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

    def query_unique_product_names(self):
        """stock_ledger에서 고유 품목명 목록 반환 (양수 재고 기준)."""
        def builder(table):
            return self.client.table(table).select("product_name,qty")
        all_data = self._paginate_query("stock_ledger", builder)
        totals = {}
        for r in all_data:
            name = r.get('product_name', '')
            if name:
                totals[name] = totals.get(name, 0) + (r.get('qty', 0) or 0)
        return sorted([n for n, q in totals.items() if q > 0])

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

    def delete_stock_ledger_sales_out(self, date_str, product_name, location, qty):
        """특정 SALES_OUT 레코드 삭제 (거래 삭제 시 재고 복원용).

        FIFO 차감으로 생성된 여러 레코드 중, 합계가 qty와 일치하는 것들을 삭제.
        삭제되면 stock_ledger 기반 잔고가 자동 복원됨.
        """
        q = (self.client.table("stock_ledger").select("id,qty")
             .eq("transaction_date", date_str)
             .eq("type", "SALES_OUT")
             .eq("product_name", product_name)
             .eq("location", location))
        res = q.execute()

        if not res.data:
            return 0

        # qty 합산이 일치하는 레코드들 삭제 (SALES_OUT qty는 음수)
        target_qty = -abs(qty)
        candidates = sorted(res.data, key=lambda r: r['id'], reverse=True)

        # 최신 레코드부터 합산하여 목표 수량에 맞는 그룹 찾기
        to_delete = []
        running = 0
        for rec in candidates:
            to_delete.append(rec['id'])
            running += rec['qty']
            if running == target_qty:
                break

        if running != target_qty:
            # 정확히 일치하지 않으면 안전을 위해 삭제하지 않음
            return 0

        deleted = 0
        for rid in to_delete:
            self.client.table("stock_ledger").delete().eq("id", rid).execute()
            deleted += 1
        return deleted

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

    def delete_revenue_by_date(self, date_from=None, date_to=None, exclude_categories=None):
        query = self.client.table("daily_revenue").delete()
        if date_from:
            query = query.gte("revenue_date", date_from)
        if date_to:
            query = query.lte("revenue_date", date_to)
        if exclude_categories:
            for cat in exclude_categories:
                query = query.neq("category", cat)
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

    def query_price_table(self):
        """가격표(master_prices) 전체 조회 → aggregator.price_map 호환 dict 반환."""
        import unicodedata
        def _n(t): return unicodedata.normalize('NFC', str(t).strip())

        rows = self.query_master_table('master_prices')
        price_map = {}
        for r in rows:
            nm = _n(r.get('품목명', '') or r.get('product_name', ''))
            if not nm:
                continue
            price_map[nm] = {
                'SKU': str(r.get('SKU', r.get('sku', ''))),
                '네이버판매가': float(r.get('네이버판매가', r.get('naver_price', 0)) or 0),
                '쿠팡판매가': float(r.get('쿠팡판매가', r.get('coupang_price', 0)) or 0),
                '로켓판매가': float(r.get('로켓판매가', r.get('rocket_price', 0)) or 0),
            }
        return price_map

    # --- product_costs (품목별 매입단가) ---

    def query_product_costs(self):
        """product_costs 전체 조회 → {product_name: {cost_price, unit, memo}} dict."""
        try:
            rows = self._paginate_query("product_costs",
                lambda t: self.client.table(t).select("*").order("product_name"))
            return {r['product_name']: r for r in rows}
        except Exception:
            return {}

    def upsert_product_cost(self, product_name, cost_price, unit='', memo=''):
        """품목 매입단가 1건 등록/수정 (upsert)."""
        from datetime import datetime, timezone
        payload = {
            'product_name': product_name,
            'cost_price': float(cost_price),
            'unit': unit,
            'memo': memo,
            'updated_at': datetime.now(timezone.utc).isoformat(),
        }
        self.client.table("product_costs").upsert(
            payload, on_conflict="product_name"
        ).execute()

    def upsert_product_costs_batch(self, items):
        """품목 매입단가 일괄 등록/수정.
        items: [{product_name, cost_price, unit?, memo?}]
        """
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()
        payload = []
        for item in items:
            payload.append({
                'product_name': item['product_name'],
                'cost_price': float(item.get('cost_price', 0)),
                'unit': item.get('unit', ''),
                'memo': item.get('memo', ''),
                'updated_at': now,
            })
        if not payload:
            return
        for i in range(0, len(payload), 500):
            self.client.table("product_costs").upsert(
                payload[i:i + 500], on_conflict="product_name"
            ).execute()

    def delete_product_cost(self, product_name):
        """품목 매입단가 1건 삭제."""
        self.client.table("product_costs").delete().eq(
            "product_name", product_name
        ).execute()

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
    # 옵션마스터 (option_master) CRUD
    # ================================================================

    def _invalidate_option_cache(self):
        """옵션마스터 캐시 무효화 (데이터 변경 시 호출)."""
        _option_cache['data'] = None
        _option_cache['data_list'] = None
        _option_cache['ts'] = 0

    def query_option_master(self, use_cache=True):
        """옵션마스터 전체 조회 (sort_order 정렬, 메모리 캐시 적용)."""
        now = time.time()
        if use_cache and _option_cache['data'] is not None and (now - _option_cache['ts']) < _option_cache['ttl']:
            return _option_cache['data']

        def builder(table):
            return self.client.table(table).select("*").order("sort_order")
        data = self._paginate_query("option_master", builder)

        # 캐시 갱신
        _option_cache['data'] = data
        _option_cache['data_list'] = None  # list 캐시도 다시 생성
        _option_cache['ts'] = now
        return data

    def query_option_master_as_list(self):
        """옵션마스터를 OrderProcessor 호환 dict list로 반환 (캐시)."""
        # list 캐시가 유효하면 바로 반환
        now = time.time()
        if _option_cache['data_list'] is not None and (now - _option_cache['ts']) < _option_cache['ttl']:
            return _option_cache['data_list']

        all_rows = self.query_option_master()
        result = []
        for r in all_rows:
            result.append({
                '원문명': r.get('original_name', ''),
                '품목명': r.get('product_name', ''),
                '라인코드': r.get('line_code', '0'),
                '출력순서': float(r.get('sort_order', 999)),
                '바코드': r.get('barcode', ''),
                'Key': r.get('match_key', ''),
            })
        _option_cache['data_list'] = result
        return result

    def search_option_master(self, keyword):
        """옵션마스터 검색 (품목명 or 원문명 부분 일치, 캐시 활용)."""
        keyword_upper = keyword.replace(' ', '').upper()
        all_rows = self.query_option_master()  # 캐시 사용
        return [r for r in all_rows
                if keyword_upper in (r.get('match_key', '') or '').upper()
                or keyword_upper in (r.get('product_name', '') or '').replace(' ', '').upper()]

    def insert_option_master(self, payload):
        """옵션마스터 1건 등록 (match_key 자동 계산)."""
        orig = payload.get('original_name', '')
        payload['match_key'] = str(orig).replace(' ', '').upper()
        self.client.table("option_master").insert(payload).execute()
        self._invalidate_option_cache()

    def insert_option_master_batch(self, payload_list, batch_size=500):
        """옵션마스터 일괄 등록 (중복 시 upsert)."""
        for row in payload_list:
            orig = row.get('original_name', '')
            row['match_key'] = str(orig).replace(' ', '').upper()
        for i in range(0, len(payload_list), batch_size):
            chunk = payload_list[i:i + batch_size]
            try:
                self.client.table("option_master").upsert(
                    chunk, on_conflict="match_key"
                ).execute()
            except Exception:
                for row in chunk:
                    try:
                        self.client.table("option_master").upsert(
                            row, on_conflict="match_key"
                        ).execute()
                    except Exception:
                        pass
        self._invalidate_option_cache()

    def update_option_master(self, option_id, update_data):
        """옵션마스터 1건 수정."""
        if 'original_name' in update_data:
            update_data['match_key'] = str(update_data['original_name']).replace(' ', '').upper()
        self.client.table("option_master").update(update_data).eq("id", option_id).execute()
        self._invalidate_option_cache()

    def delete_option_master(self, option_id):
        """옵션마스터 1건 삭제."""
        self.client.table("option_master").delete().eq("id", option_id).execute()
        self._invalidate_option_cache()

    def count_option_master(self):
        """옵션마스터 건수 (캐시 활용)."""
        cached = _option_cache.get('data')
        if cached is not None and (time.time() - _option_cache['ts']) < _option_cache['ttl']:
            return len(cached)
        try:
            res = self.client.table("option_master").select("id", count="exact").limit(1).execute()
            return res.count if res.count is not None else len(res.data)
        except Exception:
            return -1

    def sync_option_master(self, payload_list, batch_size=500):
        """옵션마스터 전체 교체 (엑셀 내 중복 match_key 자동 제거)."""
        # match_key 생성 + 중복 제거 (뒤에 나오는 행이 우선)
        seen = {}
        for row in payload_list:
            orig = row.get('original_name', '')
            row['match_key'] = str(orig).replace(' ', '').upper()
            seen[row['match_key']] = row
        deduped = list(seen.values())

        # 기존 데이터 전체 삭제 후 삽입
        self.client.table("option_master").delete().neq("id", 0).execute()
        for i in range(0, len(deduped), batch_size):
            self.client.table("option_master").insert(
                deduped[i:i + batch_size]
            ).execute()
        self._invalidate_option_cache()

    def touch_option_matched(self, match_keys):
        """매칭 성공한 옵션들의 last_matched_at 타임스탬프 갱신."""
        if not match_keys:
            return 0
        from datetime import datetime, timezone
        now_str = datetime.now(timezone.utc).isoformat()
        updated = 0
        for i in range(0, len(match_keys), 50):
            chunk = match_keys[i:i + 50]
            try:
                self.client.table("option_master").update(
                    {"last_matched_at": now_str}
                ).in_("match_key", chunk).execute()
                updated += len(chunk)
            except Exception:
                for mk in chunk:
                    try:
                        self.client.table("option_master").update(
                            {"last_matched_at": now_str}
                        ).eq("match_key", mk).execute()
                        updated += 1
                    except Exception:
                        pass
        self._invalidate_option_cache()
        return updated

    def query_stale_options(self, days=30):
        """N일 이상 매칭되지 않은 옵션 조회 (last_matched_at이 NULL이거나 오래된 것)."""
        from datetime import datetime, timezone, timedelta
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        stale = []
        try:
            # last_matched_at이 NULL인 항목 (한 번도 매칭 안 됨)
            res1 = self.client.table("option_master").select("*") \
                .is_("last_matched_at", "null").execute()
            stale.extend(res1.data or [])
            # last_matched_at이 cutoff보다 오래된 항목
            res2 = self.client.table("option_master").select("*") \
                .lt("last_matched_at", cutoff).execute()
            stale.extend(res2.data or [])
        except Exception:
            pass
        # 중복 제거 (id 기준)
        seen = set()
        result = []
        for r in stale:
            if r['id'] not in seen:
                seen.add(r['id'])
                result.append(r)
        return result

    def delete_stale_options(self, days=30):
        """N일 이상 매칭되지 않은 옵션 일괄 삭제."""
        stale = self.query_stale_options(days)
        if not stale:
            return 0
        ids = [r['id'] for r in stale]
        deleted = 0
        for i in range(0, len(ids), 50):
            chunk = ids[i:i + 50]
            try:
                self.client.table("option_master").delete() \
                    .in_("id", chunk).execute()
                deleted += len(chunk)
            except Exception:
                for rid in chunk:
                    try:
                        self.client.table("option_master").delete() \
                            .eq("id", rid).execute()
                        deleted += 1
                    except Exception:
                        pass
        self._invalidate_option_cache()
        return deleted

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
