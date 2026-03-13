"""
db_supabase.py — Supabase 구현 (CRUD/조회). 보고서용 조회는 raw rows까지만.
+ 사용자 인증/관리 CRUD (app_users, audit_logs)
"""
import time
import pandas as pd
from supabase import create_client, Client
from db_base import DBBase
from config import SUPABASE_URL, SUPABASE_KEY
from services.tz_utils import today_kst, days_ago_kst

# 옵션마스터 메모리 캐시 (TTL 기반)
_option_cache = {
    'data': None,         # 전체 옵션 목록
    'data_list': None,    # OrderProcessor 호환 dict list
    'ts': 0,              # 마지막 로드 시간
    'ttl': 1800,          # 30분 캐시 (초) — 옵션마스터는 자주 안 바뀜
}

# 권한 메모리 캐시 (TTL 기반)
_perm_cache = {
    'data': None,         # {role: {page_key: bool}}
    'ts': 0,
    'ttl': 600,           # 10분 캐시 (초)
}

# 가격표 메모리 캐시 (TTL 기반)
_price_cache = {
    'data': None,         # {product_name: {SKU, 네이버판매가, 쿠팡판매가, 로켓판매가}}
    'ts': 0,
    'ttl': 1800,          # 30분 캐시 (초)
}


class SupabaseDB(DBBase):
    def __init__(self):
        self.client: Client = None
        self._db_cols = None

    def connect(self, url=None, key=None):
        try:
            self._url = url or SUPABASE_URL
            self._key = key or SUPABASE_KEY
            self.client = create_client(self._url, self._key)
            try:
                self.client.rpc("ensure_stock_ledger_columns", {}).execute()
            except Exception as col_err:
                print(f"Column migration note: {col_err}")
            self._db_cols = self.get_db_columns()
            return True
        except Exception as e:
            print(f"[DB connect error] {e}")
            return False

    def _reconnect(self):
        """HTTP/2 연결 풀 재생성 (Server disconnected 오류 복구)."""
        try:
            print("[DB] Supabase 재연결 시도...")
            self.client = create_client(
                getattr(self, '_url', SUPABASE_URL),
                getattr(self, '_key', SUPABASE_KEY),
            )
            print("[DB] Supabase 재연결 성공")
            return True
        except Exception as e:
            print(f"[DB] Supabase 재연결 실패: {e}")
            return False

    def _is_connection_error(self, exc):
        """httpx 연결 오류 여부 판별."""
        err_name = type(exc).__name__
        err_msg = str(exc).lower()
        return ('RemoteProtocolError' in err_name or
                'ConnectError' in err_name or
                'server disconnected' in err_msg or
                'connection reset' in err_msg)

    def _retry_on_disconnect(self, fn, *args, **kwargs):
        """연결 오류 시 재연결 후 1회 재시도하는 범용 래퍼."""
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            if self._is_connection_error(e):
                print(f"[DB] 연결 오류 감지, 재시도: {type(e).__name__}")
                self._reconnect()
                return fn(*args, **kwargs)
            raise

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

    def _paginate_query(self, table, query_builder, max_retries=3):
        """페이지네이션으로 전체 데이터 조회 (페이지별 재시도)."""
        all_data = []
        offset = 0
        page_size = 1000
        while True:
            rows = None
            for attempt in range(max_retries):
                try:
                    res = query_builder(table).range(
                        offset, offset + page_size - 1
                    ).execute()
                    rows = res.data or []
                    break
                except Exception as e:
                    print(f"[DB] _paginate_query({table}) page {offset // page_size} "
                          f"attempt {attempt + 1}/{max_retries} error: {e}")
                    if self._is_connection_error(e) and attempt < max_retries - 1:
                        self._reconnect()
            if rows is None:
                import logging
                msg = (f"[DB] _paginate_query({table}) 페이지 {offset // page_size} "
                       f"최종 실패, 현재까지 {len(all_data)}건 — 부분 데이터 반환 주의!")
                logging.getLogger(__name__).error(msg)
                print(msg)
                break
            all_data.extend(rows)
            if len(rows) < page_size:
                break
            offset += page_size
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
            return {'inserted': 0, 'failed': 0, 'errors': []}
        payload_list = self._normalize_product_names(payload_list)
        filtered = self._filter_payload(payload_list)
        # 배치 삽입 시도 → 실패 시 개별 삽입 fallback
        try:
            self.client.table("stock_ledger").insert(filtered).execute()
            return {'inserted': len(filtered), 'failed': 0, 'errors': []}
        except Exception as batch_err:
            print(f"[stock_ledger] 배치 삽입 실패, 개별 삽입 시도: {batch_err}")
            inserted = 0
            failed = 0
            errors = []
            for row in filtered:
                try:
                    self.client.table("stock_ledger").insert(row).execute()
                    inserted += 1
                except Exception as row_err:
                    failed += 1
                    pname = row.get('product_name', '?')
                    err_msg = f"{pname}: {row_err}"
                    errors.append(err_msg)
                    print(f"[stock_ledger] 개별 삽입 실패 — {err_msg}")
            return {'inserted': inserted, 'failed': failed, 'errors': errors}

    def upsert_stock_ledger_idempotent(self, payload_list):
        """event_uid 기반 중복 방지 insert.
        event_uid가 이미 존재하면 스킵(무시).
        event_uid가 없는 레코드는 일반 insert.
        Returns: (inserted_count, skipped_count)
        """
        if not payload_list:
            return 0, 0
        payload_list = self._normalize_product_names(payload_list)
        filtered = self._filter_payload(payload_list)

        # event_uid가 있는 것과 없는 것 분리
        with_uid = [p for p in filtered if p.get('event_uid')]
        without_uid = [p for p in filtered if not p.get('event_uid')]

        inserted = 0
        skipped = 0

        # event_uid 있는 것: 배치 upsert (on_conflict → 스킵)
        if with_uid:
            BATCH = 200
            for i in range(0, len(with_uid), BATCH):
                chunk = with_uid[i:i + BATCH]
                try:
                    self.client.table("stock_ledger").upsert(
                        chunk, on_conflict="event_uid",
                        ignore_duplicates=True
                    ).execute()
                    inserted += len(chunk)
                except Exception as e1:
                    # ignore_duplicates 미지원 시 개별 fallback
                    print(f"[DB] stock_ledger batch upsert failed: {e1}")
                    for p in chunk:
                        try:
                            self.client.table("stock_ledger").insert(p).execute()
                            inserted += 1
                        except Exception:
                            skipped += 1

        # event_uid 없는 것: 배치 insert
        if without_uid:
            BATCH = 200
            for i in range(0, len(without_uid), BATCH):
                chunk = without_uid[i:i + BATCH]
                self.client.table("stock_ledger").insert(chunk).execute()
                inserted += len(chunk)

        return inserted, skipped

    def delete_stock_ledger_all(self):
        res = self.client.table("stock_ledger").delete().neq("id", 0).execute()
        return len(res.data) if res.data else 0

    def delete_stock_ledger_by(self, date_str, record_type, location=None,
                               product_names=None):
        q = self.client.table("stock_ledger").delete() \
            .eq("transaction_date", date_str).eq("type", record_type)
        if location:
            q = q.eq("location", location)
        if product_names:
            q = q.in_("product_name", list(product_names))
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
                q = q.order("transaction_date", desc=True).order("id", desc=True)
            else:
                # ★ 페이지네이션 시 ORDER BY 없으면 행 중복/누락 발생 방지
                q = q.order("id")
            return q
        return self._paginate_query("stock_ledger", builder)

    def query_stock_by_location(self, location, select_fields=None):
        sel_str = ",".join(select_fields) if select_fields else "*"

        def builder(table):
            return self.client.table(table).select(sel_str).eq("location", location).order("id")
        return self._paginate_query("stock_ledger", builder)

    def query_filter_options(self):
        def builder(table):
            return self.client.table(table).select("location,category").order("id")
        all_vals = self._paginate_query("stock_ledger", builder)
        locs = sorted(set(r['location'] for r in all_vals if r.get('location')))
        cats = sorted(set(r['category'] for r in all_vals if r.get('category')))
        return locs, cats

    def query_product_categories(self):
        """stock_ledger에서 product_name → category 매핑 조회.
        가장 최근 레코드의 category를 사용.
        공백 포함/미포함 이름 모두 매핑하여 product_costs와의 호환성 보장.
        Returns: {product_name: category} dict.
        """
        import logging
        logger = logging.getLogger(__name__)
        try:
            rows = self._paginate_query("stock_ledger",
                lambda t: self.client.table(t).select("product_name,category").order("id", desc=True))
            cat_map = {}
            for r in rows:
                name = (r.get('product_name') or '').strip()
                cat = (r.get('category') or '').strip()
                if not name or not cat:
                    continue
                # stock_ledger 원본 이름 (공백 제거된 상태)
                if name not in cat_map:
                    cat_map[name] = cat
                # 공백 제거 정규화 버전도 추가 (이미 동일할 수 있음)
                norm = name.replace(' ', '')
                if norm not in cat_map:
                    cat_map[norm] = cat
            logger.info(f"query_product_categories: {len(cat_map)} entries from {len(rows)} rows")
            return cat_map
        except Exception as e:
            logger.error(f"query_product_categories failed: {e}")
            return {}

    def query_unique_product_names(self):
        """stock_ledger에서 고유 품목명+단위 목록 반환 (양수 재고 기준)."""
        def builder(table):
            return self.client.table(table).select("product_name,qty,unit").order("id")
        all_data = self._paginate_query("stock_ledger", builder)
        totals = {}
        units = {}
        for r in all_data:
            name = r.get('product_name', '')
            if name:
                totals[name] = totals.get(name, 0) + (r.get('qty', 0) or 0)
                if not units.get(name):
                    units[name] = r.get('unit') or '개'
        names = sorted([n for n, q in totals.items() if q > 0])
        return [{'name': n, 'unit': units.get(n, '개') or '개'} for n in names]

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
        # channel 필드 없는 레코드에 기본값 보장
        for p in payload_list:
            if 'channel' not in p:
                p['channel'] = ''
        self.client.table("daily_revenue").upsert(
            payload_list, on_conflict="revenue_date,product_name,category,channel"
        ).execute()

    def query_revenue(self, date_from=None, date_to=None, category=None, channel=None):
        """매출 조회 (order_transactions + daily_revenue 합산).

        - order_transactions: 온라인 채널 매출 (스마트스토어, 쿠팡, 자사몰, 카카오 등)
        - daily_revenue: 거래처매출, 로켓 등 order_transactions에 없는 매출

        반환 형식: revenue_date, product_name, category,
            channel, qty, unit_price, revenue, settlement, commission
        """
        from services.channel_config import (
            CHANNEL_REVENUE_MAP, normalize_channel_display,
            DAILY_REVENUE_ONLY_CATEGORIES, DB_CUTOFF_DATE,
            LEGACY_CATEGORY_TO_CHANNEL,
        )

        agg = {}

        # ── 1. order_transactions (온라인 채널 매출) ──
        # daily_revenue 전용 카테고리 필터 시에는 order_transactions 스킵
        skip_ot = (category and category != "전체"
                   and category in DAILY_REVENUE_ONLY_CATEGORIES)

        if not skip_ot:
            def ot_builder(table):
                q = self.client.table(table).select(
                    "order_date,channel,product_name,qty,unit_price,"
                    "total_amount,settlement,commission,discount_amount,shipping_fee"
                ).eq("status", "정상").order("order_date", desc=True)
                if date_from:
                    q = q.gte("order_date", date_from)
                if date_to:
                    q = q.lte("order_date", date_to)
                if channel and channel != "전체":
                    q = q.eq("channel", channel)
                return q

            ot_rows = self._paginate_query("order_transactions", ot_builder)

            for r in ot_rows:
                pn = (r.get("product_name") or "").replace(' ', '').strip()
                if not pn:
                    continue  # 상품명 없는 레코드 제외
                ch = normalize_channel_display(r.get("channel", ""))
                actual_cat = CHANNEL_REVENUE_MAP.get(ch, "일반매출")
                if category and category != "전체" and actual_cat != category:
                    continue

                key = (r.get("order_date", ""), pn, ch)
                if key not in agg:
                    agg[key] = {
                        "revenue_date": r.get("order_date", ""),
                        "product_name": pn,
                        "category": actual_cat,
                        "channel": ch,
                        "qty": 0, "revenue": 0, "settlement": 0,
                        "commission": 0, "discount_amount": 0, "shipping_fee": 0,
                    }
                agg[key]["qty"] += (r.get("qty") or 0)
                agg[key]["revenue"] += (r.get("total_amount") or 0)
                agg[key]["settlement"] += (r.get("settlement") or 0)
                agg[key]["commission"] += (r.get("commission") or 0)
                agg[key]["discount_amount"] += (r.get("discount_amount") or 0)
                agg[key]["shipping_fee"] += (r.get("shipping_fee") or 0)

        # ── 2. daily_revenue ──
        # DB 전환일(DB_CUTOFF_DATE) 이전: 모든 카테고리 조회 (레거시 데이터)
        # DB 전환일 이후: 거래처매출/로켓만 조회 (온라인 채널은 order_transactions)
        need_legacy = (not date_from or date_from < DB_CUTOFF_DATE)

        dr_cats = DAILY_REVENUE_ONLY_CATEGORIES
        if category and category != "전체":
            if category not in dr_cats:
                dr_cats = set()
            else:
                dr_cats = {category}

        def dr_builder(table):
            q = self.client.table(table).select(
                "revenue_date,product_name,category,channel,"
                "qty,unit_price,revenue"
            ).order("revenue_date", desc=True)
            if date_from:
                q = q.gte("revenue_date", date_from)
            if date_to:
                q = q.lte("revenue_date", date_to)
            return q

        # 2-A. 레거시 데이터 (cutoff 이전, 모든 카테고리)
        if need_legacy:
            def dr_legacy_builder(table):
                q = dr_builder(table).lt("revenue_date", DB_CUTOFF_DATE)
                if category and category != "전체":
                    # 채널명으로 필터: 레거시 카테고리 → 채널 역매핑
                    rev_map = {v: k for k, v in LEGACY_CATEGORY_TO_CHANNEL.items()}
                    legacy_cat = rev_map.get(category)
                    if legacy_cat:
                        q = q.eq("category", legacy_cat)
                    elif category in DAILY_REVENUE_ONLY_CATEGORIES:
                        q = q.eq("category", category)
                return q.order("id")

            legacy_rows = self._paginate_query("daily_revenue", dr_legacy_builder)
            for r in legacy_rows:
                pn_dr = (r.get("product_name") or "").strip()
                if not pn_dr:
                    continue
                cat = r.get("category", "기타")
                # 레거시 카테고리 정규화 (N배송(용인) → N배송)
                if cat == "N배송(용인)":
                    cat = "N배송"
                # 레거시 카테고리 → 현재 채널명 매핑
                ch = LEGACY_CATEGORY_TO_CHANNEL.get(cat)
                if not ch:
                    ch = normalize_channel_display(r.get("channel", "") or cat)
                key = (r.get("revenue_date", ""), pn_dr, ch)
                if key not in agg:
                    agg[key] = {
                        "revenue_date": r.get("revenue_date", ""),
                        "product_name": pn_dr,
                        "category": cat,
                        "channel": ch,
                        "qty": 0, "revenue": 0, "settlement": 0,
                        "commission": 0, "discount_amount": 0, "shipping_fee": 0,
                    }
                agg[key]["qty"] += (r.get("qty") or 0)
                agg[key]["revenue"] += (r.get("revenue") or 0)

        # 2-B. 전용 카테고리 (cutoff 이후, 거래처매출/로켓만)
        if dr_cats:
            def dr_current_builder(table):
                q = dr_builder(table).gte("revenue_date", DB_CUTOFF_DATE)
                if len(dr_cats) == 1:
                    q = q.eq("category", list(dr_cats)[0])
                else:
                    q = q.in_("category", list(dr_cats))
                return q.order("id")

            dr_rows = self._paginate_query("daily_revenue", dr_current_builder)
            for r in dr_rows:
                pn_dr = (r.get("product_name") or "").strip()
                if not pn_dr:
                    continue
                cat = r.get("category", "기타")
                # 레거시 카테고리 정규화 (N배송(용인) → N배송)
                if cat == "N배송(용인)":
                    cat = "N배송"
                ch = normalize_channel_display(r.get("channel", "") or cat)
                key = (r.get("revenue_date", ""), pn_dr, ch)
                if key not in agg:
                    agg[key] = {
                        "revenue_date": r.get("revenue_date", ""),
                        "product_name": pn_dr,
                        "category": cat,
                        "channel": ch,
                        "qty": 0, "revenue": 0, "settlement": 0,
                        "commission": 0, "discount_amount": 0, "shipping_fee": 0,
                    }
                agg[key]["qty"] += (r.get("qty") or 0)
                agg[key]["revenue"] += (r.get("revenue") or 0)
                agg[key]["settlement"] += (r.get("settlement") or 0)
                agg[key]["commission"] += (r.get("commission") or 0)

        result = sorted(agg.values(), key=lambda x: x["revenue_date"], reverse=True)
        return result

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

    # --- daily_closing (일일마감) ---

    def get_closing_status(self, closing_date, closing_type):
        """특정 날짜+유형의 마감 상태 조회. None이면 미생성(open)."""
        res = (self.client.table("daily_closing")
               .select("*")
               .eq("closing_date", closing_date)
               .eq("closing_type", closing_type)
               .limit(1).execute())
        return res.data[0] if res.data else None

    def is_closed(self, closing_date, closing_type):
        """해당 날짜+유형이 마감되었는지 여부."""
        row = self.get_closing_status(closing_date, closing_type)
        return row is not None and row.get('status') == 'closed'

    def close_day(self, closing_date, closing_type, closed_by, cutoff_time='15:05', memo=''):
        """일일마감 실행."""
        from datetime import datetime, timezone
        payload = {
            'closing_date': closing_date,
            'closing_type': closing_type,
            'status': 'closed',
            'cutoff_time': cutoff_time,
            'closed_by': closed_by,
            'closed_at': datetime.now(timezone.utc).isoformat(),
            'memo': memo,
        }
        self.client.table("daily_closing").upsert(
            payload, on_conflict="closing_date,closing_type"
        ).execute()

    def reopen_day(self, closing_date, closing_type, reopened_by, memo=''):
        """마감 해제 (재오픈)."""
        from datetime import datetime, timezone
        row = self.get_closing_status(closing_date, closing_type)
        if not row:
            return
        self.client.table("daily_closing").update({
            'status': 'open',
            'reopened_by': reopened_by,
            'reopened_at': datetime.now(timezone.utc).isoformat(),
            'memo': memo,
        }).eq("id", row['id']).execute()

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

    def sync_master_table(self, table_name, payload_list, batch_size=500):
        self.client.table(table_name).delete().neq("id", 0).execute()
        for i in range(0, len(payload_list), batch_size):
            self.client.table(table_name).insert(payload_list[i:i + batch_size]).execute()

    def query_master_table(self, table_name):
        def builder(table):
            return self.client.table(table).select("*").order("id")
        return self._paginate_query(table_name, builder)

    def count_master_table(self, table_name):
        try:
            res = self.client.table(table_name).select("id", count="exact").limit(1).execute()
            return res.count if res.count is not None else len(res.data)
        except:
            return -1

    def query_price_table(self, use_cache=True):
        """가격표(master_prices) 전체 조회 → aggregator.price_map 호환 dict 반환. (캐시 적용)"""
        import unicodedata
        now = time.time()
        if use_cache and _price_cache['data'] is not None and (now - _price_cache['ts']) < _price_cache['ttl']:
            return _price_cache['data']

        def _n(t): return unicodedata.normalize('NFC', str(t).strip())

        rows = self.query_master_table('master_prices')
        price_map = {}
        for r in rows:
            nm = _n(r.get('품목명', '') or r.get('product_name', ''))
            if not nm:
                continue
            naver = float(r.get('네이버판매가', r.get('naver_price', 0)) or 0)
            price_map[nm] = {
                'SKU': str(r.get('SKU', r.get('sku', ''))),
                '네이버판매가': naver,
                '자사몰판매가': float(r.get('자사몰판매가', r.get('jasa_price', 0)) or 0) or naver,
                '쿠팡판매가': float(r.get('쿠팡판매가', r.get('coupang_price', 0)) or 0),
                '로켓판매가': float(r.get('로켓판매가', r.get('rocket_price', 0)) or 0),
            }

        _price_cache['data'] = price_map
        _price_cache['ts'] = now
        return price_map

    # --- product_costs (품목별 단가: 매입/생산 구분) ---

    def query_product_costs(self):
        """product_costs 전체 조회 → {product_name: {cost_price, unit, memo, cost_type, weight, weight_unit}} dict."""
        try:
            rows = self._paginate_query("product_costs",
                lambda t: self.client.table(t).select("*").order("product_name"))
            return {r['product_name']: r for r in rows}
        except Exception:
            return {}

    def upsert_product_cost(self, product_name, cost_price, unit='', memo='',
                            weight=0, weight_unit='g', cost_type='매입',
                            material_type='원료',
                            purchase_unit='', standard_unit='',
                            conversion_ratio=1, food_type=''):
        """품목 단가 1건 등록/수정 (upsert).
        cost_type: '매입' = 원재료 매입단가, '생산' = 완제품 생산단가
        material_type: '원료', '부재료', '반제품', '완제품', '포장재'
        purchase_unit: 매입단위 (박스, 포대, kg 등)
        standard_unit: 사용단위 (g, 개, kg 등)
        conversion_ratio: 1 매입단위 = X 사용단위
        food_type: '농산물', '수산물', '축산물', '' (미지정)
        """
        from datetime import datetime, timezone
        product_name = str(product_name).replace(' ', '').strip()
        payload = {
            'product_name': product_name,
            'cost_price': float(cost_price),
            'unit': unit,
            'memo': memo,
            'weight': float(weight or 0),
            'weight_unit': weight_unit or 'g',
            'cost_type': cost_type or '매입',
            'material_type': material_type or '원료',
            'purchase_unit': purchase_unit or '',
            'standard_unit': standard_unit or '',
            'conversion_ratio': float(conversion_ratio or 1),
            'food_type': food_type or '',
            'updated_at': datetime.now(timezone.utc).isoformat(),
        }
        self.client.table("product_costs").upsert(
            payload, on_conflict="product_name"
        ).execute()

    def upsert_product_costs_batch(self, items):
        """품목 단가 일괄 등록/수정.
        items: [{product_name, cost_price, unit?, memo?, weight?, weight_unit?,
                 cost_type?, material_type?, purchase_unit?, standard_unit?,
                 conversion_ratio?, food_type?}]
        """
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()
        payload = []
        for item in items:
            payload.append({
                'product_name': str(item['product_name']).replace(' ', '').strip(),
                'cost_price': float(item.get('cost_price', 0)),
                'unit': item.get('unit', ''),
                'memo': item.get('memo', ''),
                'weight': float(item.get('weight', 0) or 0),
                'weight_unit': item.get('weight_unit', 'g') or 'g',
                'cost_type': item.get('cost_type', '매입') or '매입',
                'material_type': item.get('material_type', '원료') or '원료',
                'purchase_unit': item.get('purchase_unit', '') or '',
                'standard_unit': item.get('standard_unit', '') or '',
                'conversion_ratio': float(item.get('conversion_ratio', 1) or 1),
                'food_type': item.get('food_type', '') or '',
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

    # --- product_cost_history (매입단가 이력) ---

    def insert_cost_history(self, product_name, old_cost_price, new_cost_price,
                            old_conversion_ratio=1, new_conversion_ratio=1,
                            changed_by='', change_reason='', effective_date=None):
        """매입단가 변경 이력 1건 저장."""
        from datetime import datetime, timezone
        if effective_date is None:
            effective_date = today_kst()
        payload = {
            'product_name': str(product_name).strip(),
            'old_cost_price': float(old_cost_price or 0),
            'new_cost_price': float(new_cost_price or 0),
            'old_conversion_ratio': float(old_conversion_ratio or 1),
            'new_conversion_ratio': float(new_conversion_ratio or 1),
            'changed_by': changed_by or '',
            'change_reason': change_reason or '',
            'effective_date': str(effective_date),
            'created_at': datetime.now(timezone.utc).isoformat(),
        }
        self.client.table("product_cost_history").insert(payload).execute()

    def query_cost_history(self, product_name=None, limit=100):
        """매입단가 변경 이력 조회.
        product_name이 주어지면 해당 품목만, 없으면 최신순 전체.
        Returns: list of dict.
        """
        try:
            q = self.client.table("product_cost_history").select("*")
            if product_name:
                # 공백 제거 정규화 양쪽 모두 검색
                norm = product_name.replace(' ', '')
                q = q.or_(f"product_name.eq.{product_name},product_name.eq.{norm}")
            q = q.order("created_at", desc=True).limit(limit)
            res = q.execute()
            return res.data if res.data else []
        except Exception:
            return []

    # --- channel_costs (채널별 비용) ---

    def query_channel_costs(self):
        """채널별 비용 전체 조회 → {channel: {fee_rate, shipping, packaging, other_cost, memo}}."""
        try:
            rows = self._paginate_query("channel_costs",
                lambda t: self.client.table(t).select("*").order("channel"))
            return {r['channel']: r for r in rows}
        except Exception:
            return {}

    def upsert_channel_cost(self, channel, fee_rate=0, shipping=0,
                            packaging=0, other_cost=0, memo=''):
        """채널 비용 1건 등록/수정 (upsert on channel)."""
        from datetime import datetime, timezone
        payload = {
            'channel': channel,
            'fee_rate': float(fee_rate),
            'shipping': float(shipping),
            'packaging': float(packaging),
            'other_cost': float(other_cost),
            'memo': memo,
            'updated_at': datetime.now(timezone.utc).isoformat(),
        }
        self.client.table("channel_costs").upsert(
            payload, on_conflict="channel"
        ).execute()

    def delete_channel_cost(self, channel):
        """채널 비용 1건 삭제."""
        self.client.table("channel_costs").delete().eq(
            "channel", channel
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

    def update_partner(self, partner_id, payload):
        """거래처 1건 수정."""
        self.client.table("business_partners").update(payload).eq("id", partner_id).execute()

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

    # --- 발주서 이력 ---

    def insert_purchase_order(self, payload):
        """발주서 1건 저장."""
        self.client.table("purchase_orders").insert(payload).execute()

    def query_purchase_orders(self, date_from=None, date_to=None, partner_name=None):
        """발주서 이력 조회 (날짜/거래처 필터)."""
        def builder(table):
            q = self.client.table(table).select("*").order("order_date", desc=True)
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
        res = self.client.table("purchase_orders").select("*").eq("id", po_id).execute()
        return res.data[0] if res.data else None

    def update_purchase_order(self, po_id, update_data):
        """발주서 1건 수정."""
        self.client.table("purchase_orders").update(update_data).eq("id", po_id).execute()

    def delete_purchase_order(self, po_id):
        """발주서 1건 삭제."""
        self.client.table("purchase_orders").delete().eq("id", po_id).execute()

    # --- 품목명 공백 정리 ---

    def fix_product_name_spaces(self):
        """stock_ledger + daily_revenue의 품목명에서 공백을 제거하여 통합.
        예: '(수)건해삼채 200g' → '(수)건해삼채200g'
        returns: (fixed_count, duplicate_groups) 수정된 건수와 중복 그룹 목록
        """
        # stock_ledger
        all_rows = self._paginate_query("stock_ledger",
            lambda t: self.client.table(t).select("id,product_name").order("id"))
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
            lambda t: self.client.table(t).select("id,product_name").order("id"))
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
            return self.client.table(table).select("*").order("sort_order").order("id")
        data = self._paginate_query("option_master", builder)

        # 캐시 갱신
        _option_cache['data'] = data
        _option_cache['data_list'] = None  # list 캐시도 다시 생성
        _option_cache['ts'] = now
        return data

    def query_option_master_as_list(self, use_cache=True):
        """옵션마스터를 OrderProcessor 호환 dict list로 반환 (캐시).

        Args:
            use_cache: False면 캐시 완전 우회, DB 직접 조회.
        """
        # list 캐시가 유효하면 바로 반환
        now = time.time()
        if use_cache and _option_cache['data_list'] is not None and (now - _option_cache['ts']) < _option_cache['ttl']:
            return _option_cache['data_list']

        all_rows = self.query_option_master(use_cache=use_cache)
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
        """옵션마스터 1건 등록/갱신 (match_key 자동 계산, 중복 시 upsert)."""
        orig = payload.get('original_name', '')
        payload['match_key'] = str(orig).replace(' ', '').upper()
        self.client.table("option_master").upsert(
            payload, on_conflict="match_key"
        ).execute()
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

    _USER_COLS = (
        "id,username,name,password_hash,role,is_active_user,is_approved,"
        "failed_login_count,locked_until,last_login,company_name"
    )

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

    def update_user(self, user_id, update_data):
        """사용자 정보 수정."""
        self.client.table("app_users").update(update_data) \
            .eq("id", user_id).execute()

    def query_all_users(self):
        """전체 사용자 목록."""
        def builder(table):
            return self.client.table(table).select("*").order("created_at", desc=True)
        return self._paginate_query("app_users", builder)

    _pending_cache = {'count': 0, 'ts': 0}

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

    def update_audit_log(self, log_id, update_data):
        """감사 로그 수정 (롤백 상태 기록용)."""
        self.client.table("audit_logs").update(update_data) \
            .eq("id", log_id).execute()

    # --- 소프트 삭제 지원 ---

    def soft_delete_stock_ledger(self, row_id, deleted_by=None):
        """stock_ledger 소프트 삭제 (is_deleted=True). 원본 데이터 보존."""
        from datetime import datetime, timezone
        update = {
            'is_deleted': True,
            'deleted_at': datetime.now(timezone.utc).isoformat(),
        }
        if deleted_by:
            update['deleted_by'] = deleted_by
        self.client.table("stock_ledger").update(update) \
            .eq("id", row_id).execute()

    def restore_stock_ledger(self, row_id):
        """stock_ledger 소프트 삭제 복원."""
        self.client.table("stock_ledger").update({
            'is_deleted': False,
            'deleted_at': None,
            'deleted_by': None,
        }).eq("id", row_id).execute()

    def query_stock_ledger_by_id(self, row_id):
        """stock_ledger 1건 조회 (ID 기준)."""
        try:
            res = self.client.table("stock_ledger").select("*") \
                .eq("id", row_id).limit(1).execute()
            return res.data[0] if res.data else None
        except Exception:
            return None

    # ================================================================
    # 권한 관리 (role_permissions)
    # ================================================================

    def query_role_permissions(self, use_cache=True):
        """권한 전체 조회 → {role: {page_key: bool}}. TTL 캐시."""
        now = time.time()
        if use_cache and _perm_cache['data'] and (now - _perm_cache['ts']) < _perm_cache['ttl']:
            return _perm_cache['data']
        try:
            res = self.client.table("role_permissions").select("role,page_key,is_allowed").execute()
            perms = {}
            for row in (res.data or []):
                role = row['role']
                if role not in perms:
                    perms[role] = {}
                perms[role][row['page_key']] = row['is_allowed']
            _perm_cache['data'] = perms
            _perm_cache['ts'] = time.time()
            return perms
        except Exception:
            return _perm_cache['data'] or {}

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

    def _invalidate_perm_cache(self):
        """권한 캐시 즉시 무효화."""
        _perm_cache['data'] = None
        _perm_cache['ts'] = 0

    # ================================================================
    # 행사 관리 (promotions)
    # ================================================================

    def query_promotions(self, product_name=None, category=None,
                         date_from=None, date_to=None, active_only=False):
        """행사 목록 조회."""
        try:
            q = self.client.table("promotions").select("*")
            if product_name:
                q = q.ilike("product_name", f"%{product_name}%")
            if category:
                q = q.eq("category", category)
            if date_from:
                q = q.gte("end_date", date_from)
            if date_to:
                q = q.lte("start_date", date_to)
            if active_only:
                q = q.eq("is_active", True)
            q = q.order("start_date", desc=True)
            res = q.execute()
            return res.data or []
        except Exception:
            return []

    def query_active_promotion(self, product_name, category, target_date):
        """특정 품목+채널+일자에 활성 행사 조회 (가장 최근 등록 우선)."""
        try:
            res = self.client.table("promotions").select("*") \
                .eq("product_name", product_name) \
                .eq("category", category) \
                .eq("is_active", True) \
                .lte("start_date", target_date) \
                .gte("end_date", target_date) \
                .order("created_at", desc=True) \
                .limit(1).execute()
            return res.data[0] if res.data else None
        except Exception:
            return None

    def insert_promotion(self, payload):
        """행사 1건 등록."""
        self.client.table("promotions").insert(payload).execute()

    def update_promotion(self, promo_id, update_data):
        """행사 1건 수정."""
        from datetime import datetime, timezone
        update_data['updated_at'] = datetime.now(timezone.utc).isoformat()
        self.client.table("promotions").update(update_data) \
            .eq("id", promo_id).execute()

    def delete_promotion(self, promo_id):
        """행사 1건 삭제."""
        self.client.table("promotions").delete().eq("id", promo_id).execute()

    # ================================================================
    # 쿠폰 관리 (coupons)
    # ================================================================

    def query_coupons(self, product_name=None, category=None,
                      date_from=None, date_to=None, active_only=False):
        """쿠폰 목록 조회."""
        try:
            q = self.client.table("coupons").select("*")
            if product_name:
                q = q.ilike("product_name", f"%{product_name}%")
            if category:
                q = q.eq("category", category)
            if date_from:
                q = q.gte("end_date", date_from)
            if date_to:
                q = q.lte("start_date", date_to)
            if active_only:
                q = q.eq("is_active", True)
            q = q.order("start_date", desc=True)
            res = q.execute()
            return res.data or []
        except Exception:
            return []

    def query_active_coupon(self, product_name, category, target_date):
        """특정 품목+채널+일자에 활성 쿠폰 조회."""
        try:
            res = self.client.table("coupons").select("*") \
                .eq("product_name", product_name) \
                .eq("category", category) \
                .eq("is_active", True) \
                .lte("start_date", target_date) \
                .gte("end_date", target_date) \
                .order("created_at", desc=True) \
                .limit(1).execute()
            return res.data[0] if res.data else None
        except Exception:
            return None

    def insert_coupon(self, payload):
        """쿠폰 1건 등록."""
        self.client.table("coupons").insert(payload).execute()

    def update_coupon(self, coupon_id, update_data):
        """쿠폰 1건 수정."""
        from datetime import datetime, timezone
        update_data['updated_at'] = datetime.now(timezone.utc).isoformat()
        self.client.table("coupons").update(update_data) \
            .eq("id", coupon_id).execute()

    def delete_coupon(self, coupon_id):
        """쿠폰 1건 삭제."""
        self.client.table("coupons").delete().eq("id", coupon_id).execute()

    # ================================================================
    # 단가 결정 (행사 > 쿠폰 > 기본가)
    # ================================================================

    def resolve_unit_price(self, product_name, category, target_date, price_map=None):
        """매출 단가 결정: 행사가 > 쿠폰할인가 > 기본 판매가.
        Returns: (unit_price, source) — source: 'promotion'/'coupon'/'master'/'none'
        """
        import unicodedata
        _CATEGORY_PRICE_COL = {
            "일반매출": "네이버판매가",
            "자사몰매출": "자사몰판매가",
            "쿠팡매출": "쿠팡판매가",
            "로켓": "로켓판매가",
            "N배송": "네이버판매가",
        }

        # 공백 정규화된 이름 (가격표 조회용)
        norm_name = unicodedata.normalize('NFC', str(product_name).replace(' ', '').strip())

        def _price_lookup(pn, col):
            if not price_map or not col:
                return 0
            entry = price_map.get(pn) or price_map.get(norm_name) or {}
            return float(entry.get(col, 0) or 0)

        # 1) 행사가 확인
        promo = self.query_active_promotion(product_name, category, target_date)
        if promo:
            return float(promo['promo_price']), 'promotion'

        # 2) 쿠폰 확인
        coupon = self.query_active_coupon(product_name, category, target_date)
        if coupon:
            price_col = _CATEGORY_PRICE_COL.get(category)
            base_price = _price_lookup(product_name, price_col)
            if base_price > 0:
                if coupon['discount_type'] == '%':
                    discount = base_price * float(coupon['discount_value']) / 100
                else:
                    discount = float(coupon['discount_value'])
                return max(0, base_price - discount), 'coupon'

        # 3) 기본 판매가
        price_col = _CATEGORY_PRICE_COL.get(category)
        base_price = _price_lookup(product_name, price_col)
        if base_price > 0:
            return base_price, 'master'

        return 0, 'none'

    # ================================================================
    # Phase 1: 주문 수집 파이프라인
    # ================================================================

    def create_import_run(self, channel, filename, file_hash, uploaded_by, total_rows):
        """import_runs 레코드 생성. 반환: (import_run_id, error_msg)"""
        try:
            res = self.client.table("import_runs").insert({
                "channel": channel,
                "filename": filename,
                "file_hash": file_hash,
                "uploaded_by": uploaded_by,
                "total_rows": total_rows,
                "status": "processing",
            }).execute()
            if res.data:
                return res.data[0]["id"], None
            return None, "INSERT OK but no ID returned"
        except Exception as e:
            print(f"[DB] create_import_run error: {e}")
            return None, str(e)

    def update_import_run(self, run_id, update_data):
        """import_runs 결과 갱신."""
        try:
            self.client.table("import_runs").update(update_data) \
                .eq("id", run_id).execute()
        except Exception as e:
            print(f"[DB] update_import_run error: {e}")

    def query_import_runs(self, limit=50):
        """최근 import_runs 목록 조회."""
        try:
            res = self.client.table("import_runs").select("*") \
                .order("created_at", desc=True).limit(limit).execute()
            return res.data or []
        except Exception:
            return []

    def query_import_run_by_id(self, run_id):
        """import_runs 상세 조회."""
        try:
            res = self.client.table("import_runs").select("*") \
                .eq("id", run_id).execute()
            return res.data[0] if res.data else None
        except Exception:
            return None

    def upsert_order_batch(self, import_run_id, orders):
        """주문 배치 upsert (RPC 호출).
        orders: [{transaction: {...}, shipping: {...}}, ...]
        반환: {inserted, updated, skipped, failed, errors, rpc_error}
        """
        import json
        try:
            res = self.client.rpc("rpc_upsert_order_batch", {
                "p_import_run_id": import_run_id,
                "p_orders": orders,
            }).execute()
            if res.data:
                return res.data
            return {"inserted": 0, "updated": 0, "skipped": 0, "failed": len(orders),
                    "rpc_error": "RPC OK but no data returned"}
        except Exception as e:
            rpc_err = str(e)
            print(f"[DB] upsert_order_batch RPC error: {rpc_err}")
            # RPC 실패 시 fallback: 개별 upsert (REST API)
            result = self._upsert_order_batch_fallback(import_run_id, orders)
            result["rpc_error"] = rpc_err
            return result

    def _upsert_order_batch_fallback(self, import_run_id, orders):
        """RPC 실패 시 REST API 배치 upsert (최적화: 50건씩 배치 처리)."""
        inserted, updated, skipped, failed = 0, 0, 0, 0
        errors = []
        from datetime import datetime, timedelta, timezone

        BATCH = 50
        for batch_start in range(0, len(orders), BATCH):
            batch = orders[batch_start:batch_start + BATCH]

            # 1단계: 배치 내 기존 주문 한번에 조회 (채널별 .in_() 사용)
            by_channel = {}
            for order in batch:
                txn = order.get("transaction", {})
                ch = txn.get("channel", "")
                ono = txn.get("order_no", "")
                by_channel.setdefault(ch, set()).add(ono)

            existing_map = {}  # (channel, order_no, line_no) → {id, raw_hash, status}
            for ch, order_nos in by_channel.items():
                try:
                    res = self.client.table("order_transactions") \
                        .select("id,channel,order_no,line_no,raw_hash,status") \
                        .eq("channel", ch) \
                        .in_("order_no", list(order_nos)) \
                        .execute()
                    for rec in (res.data or []):
                        key = (rec.get('channel', ''), rec.get('order_no', ''), rec.get('line_no', 1))
                        existing_map[key] = rec
                except Exception as e:
                    print(f"[DB] fallback batch lookup error: {e}")

            # 1-b단계: 크로스 채널 중복 체크 (raw_hash + order_no 양방향)
            batch_hashes = [o.get("transaction", {}).get("raw_hash", "") for o in batch]
            batch_hashes = [h for h in batch_hashes if h]
            cross_channel_hashes = {}  # raw_hash → 기존 채널명
            if batch_hashes:
                for hi in range(0, len(batch_hashes), 200):
                    h_chunk = batch_hashes[hi:hi + 200]
                    try:
                        xres = self.client.table("order_transactions") \
                            .select("raw_hash,channel") \
                            .in_("raw_hash", h_chunk) \
                            .execute()
                        for xr in (xres.data or []):
                            cross_channel_hashes[xr["raw_hash"]] = xr.get("channel", "")
                    except Exception:
                        pass  # 조회 실패 시 기존 로직으로 진행

            # 1-c단계: order_no 기반 크로스 채널 중복 체크 (같은 주문번호가 다른 채널에 존재)
            batch_order_nos = set()
            for order in batch:
                txn = order.get("transaction", {})
                ono = txn.get("order_no", "")
                if ono:
                    batch_order_nos.add(ono)
            cross_channel_orders = {}  # order_no → 기존 채널명
            if batch_order_nos:
                for oi in range(0, len(batch_order_nos), 200):
                    o_chunk = list(batch_order_nos)[oi:oi + 200]
                    try:
                        xores = self.client.table("order_transactions") \
                            .select("order_no,channel") \
                            .in_("order_no", o_chunk) \
                            .execute()
                        for xor in (xores.data or []):
                            xor_ono = xor.get("order_no", "")
                            xor_ch = xor.get("channel", "")
                            if xor_ono not in cross_channel_orders:
                                cross_channel_orders[xor_ono] = set()
                            cross_channel_orders[xor_ono].add(xor_ch)
                    except Exception:
                        pass

            # 2단계: 분류 (insert / update / skip)
            to_insert = []
            to_update = []  # (id, txn_update)
            ship_batch = []
            cross_skipped = 0  # 크로스 채널 중복 스킵 카운트

            for i, order in enumerate(batch, batch_start + 1):
                txn = order.get("transaction", {})
                ship = order.get("shipping", {})
                key = (txn.get("channel", ""), txn.get("order_no", ""), txn.get("line_no", 1))
                rec = existing_map.get(key)

                if rec:
                    if rec.get("status") in ("취소", "환불"):
                        skipped += 1
                        continue
                    if rec.get("raw_hash") and rec.get("raw_hash") == txn.get("raw_hash"):
                        skipped += 1
                        continue
                    # UPDATE 대상 (collection_date는 최초 수집일 보존 — 덮어쓰기 방지)
                    txn_update = {k: v for k, v in txn.items()
                                  if k not in ("raw_data", "collection_date")}
                    txn_update["import_run_id"] = import_run_id
                    if "raw_data" in txn:
                        txn_update["raw_data"] = txn["raw_data"]
                    to_update.append((rec["id"], txn_update, i))
                else:
                    # 크로스 채널 중복 체크 1: 같은 raw_hash가 다른 채널에 이미 존재
                    t_hash = txn.get("raw_hash", "")
                    existing_ch = cross_channel_hashes.get(t_hash)
                    if t_hash and existing_ch and existing_ch != txn.get("channel", ""):
                        cross_skipped += 1
                        skipped += 1
                        continue
                    # 크로스 채널 중복 체크 2: 같은 order_no가 다른 채널에 이미 존재
                    t_ono = txn.get("order_no", "")
                    t_ch = txn.get("channel", "")
                    existing_chs = cross_channel_orders.get(t_ono, set())
                    other_chs = existing_chs - {t_ch}
                    if t_ono and other_chs:
                        cross_skipped += 1
                        skipped += 1
                        continue
                    # INSERT 대상
                    txn["import_run_id"] = import_run_id
                    to_insert.append((txn, i))

                # shipping 수집
                if ship and ship.get("name"):
                    ship_data = {
                        "channel": txn.get("channel", ""),
                        "order_no": txn.get("order_no", ""),
                        **{k: v for k, v in ship.items() if k not in ("channel", "order_no")},
                        "expires_at": (datetime.now(timezone.utc) + timedelta(days=180)).isoformat(),
                    }
                    ship_batch.append(ship_data)

            # 3단계: 배치 INSERT
            if to_insert:
                try:
                    rows = [t[0] for t in to_insert]
                    self.client.table("order_transactions").insert(rows).execute()
                    inserted += len(rows)
                except Exception as e:
                    # 배치 실패 시 개별 재시도
                    for txn_data, row_i in to_insert:
                        try:
                            self.client.table("order_transactions").insert(txn_data).execute()
                            inserted += 1
                        except Exception as e2:
                            failed += 1
                            errors.append({"row": row_i, "order_no": txn_data.get("order_no", ""), "error": str(e2)})

            # 4단계: UPDATE (개별 — id 기반이라 배치 불가)
            for rec_id, txn_update, row_i in to_update:
                try:
                    self.client.table("order_transactions").update(txn_update) \
                        .eq("id", rec_id).execute()
                    updated += 1
                except Exception as e:
                    failed += 1
                    errors.append({"row": row_i, "order_no": txn_update.get("order_no", ""), "error": str(e)})
                    if failed <= 3:
                        print(f"[DB] fallback update row {row_i}: {str(e)[:200]}")

            # 5단계: shipping 배치 upsert
            if ship_batch:
                try:
                    self.client.table("order_shipping").upsert(
                        ship_batch, on_conflict="channel,order_no"
                    ).execute()
                except Exception:
                    # 배치 실패 시 개별 재시도
                    for sd in ship_batch:
                        try:
                            self.client.table("order_shipping").upsert(
                                sd, on_conflict="channel,order_no"
                            ).execute()
                        except Exception:
                            pass

        # import_runs 결과 갱신
        status = "completed" if failed == 0 else ("partial" if inserted + updated > 0 else "failed")
        self.update_import_run(import_run_id, {
            "success_count": inserted + updated,
            "changed_count": updated,
            "fail_count": failed,
            "error_summary": errors if errors else None,
            "status": status,
        })
        result = {"inserted": inserted, "updated": updated, "skipped": skipped, "failed": failed, "errors": errors}
        if cross_skipped > 0:
            result["cross_channel_skipped"] = cross_skipped
        return result

    def cancel_or_edit_order(self, order_id, change_type, payload, reason, user):
        """주문 수정/취소/환불 (RPC 호출).
        반환: {success, change_type, order_id} or {success: false, error}
        """
        try:
            res = self.client.rpc("rpc_cancel_or_edit_order", {
                "p_order_id": order_id,
                "p_change_type": change_type,
                "p_payload": payload or {},
                "p_reason": reason or "",
                "p_user": user or "",
            }).execute()
            return res.data if res.data else {"success": False, "error": "RPC 응답 없음"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def query_order_transactions(self, date_from=None, date_to=None, channel=None,
                                  status=None, search=None, limit=100, offset=0):
        """주문 목록 조회 (필터 지원)."""
        try:
            q = self.client.table("order_transactions").select("*")
            if date_from:
                q = q.gte("order_date", date_from)
            if date_to:
                q = q.lte("order_date", date_to)
            if channel:
                q = q.eq("channel", channel)
            if status:
                q = q.eq("status", status)
            if search:
                q = q.or_(f"order_no.ilike.%{search}%,product_name.ilike.%{search}%")
            q = q.order("order_date", desc=True).order("id", desc=True)
            q = q.range(offset, offset + limit - 1)
            res = q.execute()
            return res.data or []
        except Exception:
            return []

    def query_order_transaction_by_id(self, order_id):
        """주문 상세 조회."""
        try:
            res = self.client.table("order_transactions").select("*") \
                .eq("id", order_id).execute()
            return res.data[0] if res.data else None
        except Exception:
            return None

    def query_order_shipping(self, channel, order_no):
        """배송 정보 조회."""
        try:
            res = self.client.table("order_shipping").select("*") \
                .eq("channel", channel).eq("order_no", order_no).execute()
            return res.data[0] if res.data else None
        except Exception:
            return None

    def query_order_change_log(self, order_transaction_id):
        """주문 변경 이력 조회."""
        try:
            res = self.client.table("order_change_log").select("*") \
                .eq("order_transaction_id", order_transaction_id) \
                .order("changed_at", desc=True).execute()
            return res.data or []
        except Exception:
            return []

    def anonymize_expired_shipping(self):
        """만료된 배송 개인정보 익명화 (6개월 경과).
        반환: 익명화 처리 건수
        """
        from datetime import datetime, timezone
        try:
            now_iso = datetime.now(timezone.utc).isoformat()
            # 대상 조회
            res = self.client.table("order_shipping").select("id") \
                .lt("expires_at", now_iso) \
                .eq("is_anonymized", False).execute()

            if not res.data:
                return 0

            count = 0
            for rec in res.data:
                self.client.table("order_shipping").update({
                    "name": "***",
                    "phone": "***",
                    "phone2": "***",
                    "address": "***",
                    "memo": "***",
                    "is_anonymized": True,
                    "anonymized_at": now_iso,
                }).eq("id", rec["id"]).execute()
                count += 1

            return count
        except Exception as e:
            print(f"[DB] anonymize_expired_shipping error: {e}")
            return 0

    def query_order_shipping_for_invoice(self, channel=None, date_from=None, date_to=None):
        """송장 생성용: 정상 주문의 배송정보 조회 (대기 상태만).
        order_transactions와 order_shipping 조인이 필요하므로
        transactions에서 대상 주문번호를 먼저 조회 후 shipping에서 검색.
        """
        try:
            # 1. 대상 거래 조회
            q = self.client.table("order_transactions") \
                .select("channel,order_no,order_date,product_name,barcode,line_code,sort_order,qty,unit_price")
            q = q.eq("status", "정상")
            if channel:
                q = q.eq("channel", channel)
            if date_from:
                q = q.gte("order_date", date_from)
            if date_to:
                q = q.lte("order_date", date_to)
            txns = self._paginate_query("order_transactions", lambda t: q.order("id"))

            if not txns:
                return []

            # 2. 해당 주문들의 배송정보 조회
            order_nos = list(set(t["order_no"] for t in txns))
            # Supabase in_ 최대 제한 있으므로 분할
            shipping_map = {}
            for chunk_start in range(0, len(order_nos), 100):
                chunk = order_nos[chunk_start:chunk_start + 100]
                sq = self.client.table("order_shipping").select("*") \
                    .eq("shipping_status", "대기") \
                    .in_("order_no", chunk)
                if channel:
                    sq = sq.eq("channel", channel)
                ship_res = sq.execute()
                for s in (ship_res.data or []):
                    key = (s["channel"], s["order_no"])
                    shipping_map[key] = s

            # 3. 조인
            result = []
            for t in txns:
                key = (t["channel"], t["order_no"])
                ship = shipping_map.get(key)
                if ship:
                    result.append({**t, "shipping": ship})

            return result
        except Exception as e:
            print(f"[DB] query_order_shipping_for_invoice error: {e}")
            return []

    # ================================================================
    # Phase 2: 출고/매출 자동처리
    # ================================================================

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

    def count_orders_by_date(self, date_str):
        """특정 날짜의 주문 건수."""
        try:
            res = self.client.table("order_transactions").select("id", count="exact") \
                .eq("order_date", date_str).execute()
            return res.count if res.count is not None else len(res.data or [])
        except Exception:
            return 0

    def sum_revenue_by_date(self, date_str):
        """특정 날짜의 매출 합계 (order_transactions 기반).
        Returns: dict {total_amount, settlement, commission, qty}
        """
        try:
            res = self.client.table("order_transactions") \
                .select("total_amount,settlement,commission,qty") \
                .eq("order_date", date_str) \
                .eq("status", "정상").execute()
            total_amount = sum(r.get("total_amount", 0) or 0 for r in (res.data or []))
            settlement = sum(r.get("settlement", 0) or 0 for r in (res.data or []))
            commission = sum(r.get("commission", 0) or 0 for r in (res.data or []))
            qty = sum(r.get("qty", 0) or 0 for r in (res.data or []))
            return {
                'total_amount': total_amount,
                'settlement': settlement,
                'commission': commission,
                'qty': qty,
            }
        except Exception:
            return {'total_amount': 0, 'settlement': 0, 'commission': 0, 'qty': 0}

    def query_revenue_trend(self, days=7):
        """최근 N일 매출 추이 (order_transactions + daily_revenue 합산).

        DB_CUTOFF_DATE 이전 기간은 daily_revenue(레거시)에서 조회,
        이후 기간은 order_transactions에서 조회.
        """
        from services.channel_config import DB_CUTOFF_DATE
        try:
            date_from = days_ago_kst(days)
            today = today_kst()
            daily = {}

            # ── 1. order_transactions (cutoff 이후) ──
            ot_start = max(date_from, DB_CUTOFF_DATE)
            if ot_start <= today:
                def ot_builder(table):
                    return self.client.table(table).select(
                        "order_date,total_amount,settlement"
                    ).gte("order_date", ot_start) \
                     .lte("order_date", today) \
                     .eq("status", "정상") \
                     .order("order_date")

                ot_rows = self._paginate_query("order_transactions", ot_builder)
                for r in ot_rows:
                    d = r.get("order_date", "")
                    if not d:
                        continue
                    ta = r.get("total_amount", 0) or 0
                    st = r.get("settlement", 0) or 0
                    if d not in daily:
                        daily[d] = {"date": d, "total": 0, "settlement": 0}
                    daily[d]["total"] += ta
                    daily[d]["settlement"] += st

            # ── 2. daily_revenue (cutoff 이전) ──
            if date_from < DB_CUTOFF_DATE:
                from datetime import datetime, timedelta
                cutoff_dt = datetime.strptime(DB_CUTOFF_DATE, '%Y-%m-%d')
                legacy_end = (cutoff_dt - timedelta(days=1)).strftime('%Y-%m-%d')

                def builder(table):
                    return self.client.table(table).select(
                        'revenue_date,category,revenue'
                    ).gte('revenue_date', date_from) \
                     .lte('revenue_date', legacy_end) \
                     .order('revenue_date')

                rows = self._paginate_query('daily_revenue', builder)
                for r in rows:
                    d = r.get("revenue_date", "")
                    cat = (r.get("category") or "").strip()
                    # 거래처매출/로켓 제외 (B2B)
                    if cat in ("거래처매출", "로켓"):
                        continue
                    rev = r.get("revenue", 0) or 0
                    if d not in daily:
                        daily[d] = {"date": d, "total": 0, "settlement": 0}
                    daily[d]["total"] += rev

            return sorted(daily.values(), key=lambda x: x["date"])
        except Exception:
            return []

    def query_orders_by_channel(self, date_from=None, date_to=None):
        """채널별 주문 통계 (채널 표시명 정규화 적용)."""
        from services.channel_config import normalize_channel_display
        try:
            def builder(table):
                q = self.client.table(table) \
                    .select("channel,qty,total_amount") \
                    .eq("status", "정상")
                if date_from:
                    q = q.gte("order_date", date_from)
                if date_to:
                    q = q.lte("order_date", date_to)
                return q.order("id")

            rows = self._paginate_query("order_transactions", builder)
            channels = {}
            for r in rows:
                ch = normalize_channel_display(r.get("channel", "기타"))
                if ch not in channels:
                    channels[ch] = {"channel": ch, "count": 0, "qty": 0, "amount": 0}
                channels[ch]["count"] += 1
                channels[ch]["qty"] += (r.get("qty") or 0)
                channels[ch]["amount"] += (r.get("total_amount") or 0)
            return sorted(channels.values(), key=lambda x: x["count"], reverse=True)
        except Exception:
            return []

    def query_stock_summary_by_location(self, exclude_products=None):
        """창고별 재고 품목 수 요약 (양수 재고만)."""
        try:
            today = today_kst()

            def builder(table):
                return self.client.table(table) \
                    .select("product_name,location,qty") \
                    .lte("transaction_date", today) \
                    .order("id")

            all_data = self._paginate_query("stock_ledger", builder)
            # 품목+창고별 합산
            _excl = exclude_products or set()
            stock = {}
            for r in all_data:
                pn = r.get("product_name", "")
                if pn in _excl:
                    continue
                key = (pn, r.get("location", ""))
                stock[key] = stock.get(key, 0) + (r.get("qty") or 0)
            # 창고별 집계 (양수 재고 품목만)
            locations = {}
            for (pn, loc), total_qty in stock.items():
                if total_qty > 0:
                    if loc not in locations:
                        locations[loc] = {"location": loc, "product_count": 0, "total_qty": 0}
                    locations[loc]["product_count"] += 1
                    locations[loc]["total_qty"] += total_qty
            return sorted(locations.values(), key=lambda x: x["product_count"], reverse=True)
        except Exception:
            return []

    def query_top_products_by_revenue(self, days=30, limit=10):
        """매출 TOP N 상품 (order_transactions 기반, 최근 N일)."""
        try:
            date_from = days_ago_kst(days)

            def builder(table):
                return self.client.table(table) \
                    .select("product_name,qty,total_amount,settlement") \
                    .gte("order_date", date_from) \
                    .eq("status", "정상") \
                    .order("id")

            all_data = self._paginate_query("order_transactions", builder)
            products = {}
            for r in all_data:
                pn = (r.get("product_name") or "").replace(' ', '').strip()
                if not pn:
                    continue  # 상품명 없는 레코드 제외
                if pn not in products:
                    products[pn] = {"product_name": pn, "qty": 0,
                                    "revenue": 0, "settlement": 0}
                products[pn]["qty"] += (r.get("qty") or 0)
                products[pn]["revenue"] += (r.get("total_amount") or 0)
                products[pn]["settlement"] += (r.get("settlement") or 0)
            ranked = sorted(products.values(), key=lambda x: x["revenue"], reverse=True)
            return ranked[:limit]
        except Exception:
            return []

    def query_recent_activity(self, limit=20):
        """최근 활동 (stock_ledger + order_transactions 통합)."""
        try:
            # 최근 주문
            orders = self.client.table("order_transactions") \
                .select("id,channel,order_date,product_name,qty,processed_at") \
                .order("processed_at", desc=True).limit(limit).execute()
            # 최근 재고 변동
            stock = self.client.table("stock_ledger") \
                .select("id,type,product_name,qty,location,transaction_date") \
                .order("id", desc=True).limit(limit).execute()

            activities = []
            for o in (orders.data or []):
                activities.append({
                    "type": "order",
                    "date": o.get("processed_at", o.get("order_date", "")),
                    "desc": f"{o.get('channel','')} 주문: {o.get('product_name','')} x{o.get('qty',0)}",
                })
            for s in (stock.data or []):
                type_label = {
                    "INBOUND": "입고", "SALES_OUT": "출고", "PRODUCTION": "생산",
                    "PROD_OUT": "생산출고", "ADJUST": "조정", "SET_OUT": "세트출고",
                    "SET_IN": "세트입고", "REPACK_OUT": "소분출고", "REPACK_IN": "소분입고",
                    "MOVE_OUT": "이동출고", "MOVE_IN": "이동입고",
                }.get(s.get("type", ""), s.get("type", ""))
                activities.append({
                    "type": "stock",
                    "date": s.get("transaction_date", ""),
                    "desc": f"[{type_label}] {s.get('product_name','')} {s.get('qty',0)} ({s.get('location','')})",
                })
            activities.sort(key=lambda x: x.get("date", ""), reverse=True)
            return activities[:limit]
        except Exception:
            return []

    # ================================================================
    # Phase 2: BOM 마스터 조회 (자동 처리용)
    # ================================================================

    def query_bom_master_all(self):
        """bom_master 전체 조회. Returns: list of {channel, set_name, components}."""
        try:
            res = self.client.table("bom_master").select("*").execute()
            return res.data or []
        except Exception as e:
            print(f"[DB] query_bom_master_all error: {e}")
            return []

    # ================================================================
    # 실시간 주문처리: 추가 메서드
    # ================================================================

    def query_orders_by_import_run(self, import_run_id, outbound_done=None):
        """특정 import_run에 속한 주문 조회 (실시간 처리용)."""
        try:
            q = self.client.table("order_transactions").select("*") \
                .eq("import_run_id", import_run_id).eq("status", "정상")
            if outbound_done is not None:
                q = q.eq("is_outbound_done", outbound_done)
            q = q.order("id")
            res = q.execute()
            return res.data or []
        except Exception as e:
            print(f"[DB] query_orders_by_import_run error: {e}")
            return []

    def get_import_run_impact(self, run_id):
        """import_run 취소 시 영향 범위 미리보기.
        반환: {run: {...}, order_count, outbound_count, cancelled_count, error}
        """
        try:
            # import_run 정보
            run = self.query_import_run_by_id(run_id)
            if not run:
                return {"error": "import_run을 찾을 수 없습니다."}

            # 해당 run_id의 order_transactions 전체 (상태 무관)
            all_orders = self.client.table("order_transactions") \
                .select("id,status,is_outbound_done") \
                .eq("import_run_id", run_id).execute()
            all_list = all_orders.data or []

            total_count = len(all_list)
            active_count = sum(1 for o in all_list if o.get("status") == "정상")
            outbound_count = sum(1 for o in all_list
                                if o.get("status") == "정상" and o.get("is_outbound_done"))
            already_cancelled = sum(1 for o in all_list if o.get("status") in ("취소", "환불"))

            return {
                "run": run,
                "total_count": total_count,
                "active_count": active_count,
                "outbound_count": outbound_count,
                "already_cancelled": already_cancelled,
            }
        except Exception as e:
            print(f"[DB] get_import_run_impact error: {e}")
            return {"error": str(e)}

    def cancel_import_run(self, run_id, cancelled_by):
        """import_run 단위 롤백: run status → cancelled, 정상 주문 → 취소 처리.
        출고 처리된 주문은 건너뛰고, 미출고 정상 주문만 취소.
        반환: {cancelled_orders, skipped_outbound, error}
        """
        from datetime import datetime, timezone
        try:
            # 1) import_run 상태 확인
            run = self.query_import_run_by_id(run_id)
            if not run:
                return {"error": "import_run을 찾을 수 없습니다."}
            if run.get("status") == "cancelled":
                return {"error": "이미 취소된 import_run입니다."}

            # 2) 해당 run의 정상 주문 조회
            res = self.client.table("order_transactions") \
                .select("id,is_outbound_done,order_no,channel,product_name,qty") \
                .eq("import_run_id", run_id) \
                .eq("status", "정상").execute()
            active_orders = res.data or []

            cancelled_orders = 0
            skipped_outbound = 0
            now_iso = datetime.now(timezone.utc).isoformat()

            for order in active_orders:
                # 출고 완료된 주문은 건너뜀 (연쇄 취소는 다음 단계)
                if order.get("is_outbound_done"):
                    skipped_outbound += 1
                    continue

                # 주문 상태 → 취소
                self.client.table("order_transactions").update({
                    "status": "취소",
                    "status_reason": f"import_run 일괄취소 (run_id={run_id})",
                    "updated_at": now_iso,
                }).eq("id", order["id"]).execute()

                # 변경 이력 기록 (order_change_log)
                try:
                    self.client.table("order_change_log").insert({
                        "order_transaction_id": order["id"],
                        "change_type": "status_change",
                        "field_name": "status",
                        "before_value": "정상",
                        "after_value": "취소",
                        "change_reason": f"import_run 일괄취소 (run_id={run_id})",
                        "changed_by": cancelled_by,
                    }).execute()
                except Exception:
                    pass  # change_log 실패해도 취소는 계속 진행

                cancelled_orders += 1

            # 3) import_runs 상태 업데이트
            new_status = "cancelled"
            if skipped_outbound > 0:
                new_status = "partially_cancelled"

            self.update_import_run(run_id, {
                "status": new_status,
                "cancelled_by": cancelled_by,
                "cancelled_at": now_iso,
            })

            return {
                "cancelled_orders": cancelled_orders,
                "skipped_outbound": skipped_outbound,
            }
        except Exception as e:
            print(f"[DB] cancel_import_run error: {e}")
            import traceback; traceback.print_exc()
            return {"error": str(e)}

    def reset_order_outbound(self, order_id):
        """주문 출고 상태 초기화 (취소/환불 시)."""
        try:
            self.client.table("order_transactions").update({
                "is_outbound_done": False,
                "outbound_date": None,
                "revenue_category": None,
            }).eq("id", order_id).execute()
        except Exception as e:
            print(f"[DB] reset_order_outbound error: {e}")

    def search_order_shipping(self, keyword, field='all'):
        """order_shipping 검색 (송장번호/수취인명).
        송장번호는 하이픈 유무와 관계없이 매칭.

        Args:
            keyword: 검색어
            field: 'all', 'invoice', 'name'

        Returns: list of {channel, order_no, name, phone, invoice_no, ...}
        """
        try:
            def _invoice_search(kw):
                """송장번호 검색 — 하이픈 유무 모두 시도"""
                kw_clean = kw.replace('-', '')
                # 1차: 원본 키워드로 검색
                q = self.client.table("order_shipping").select("*") \
                    .eq("is_anonymized", False) \
                    .ilike("invoice_no", f"%{kw}%") \
                    .order("created_at", desc=True).limit(100)
                results = (q.execute()).data or []
                if results:
                    return results
                # 2차: 하이픈 제거 값이 다르면 재시도
                if kw_clean != kw:
                    q2 = self.client.table("order_shipping").select("*") \
                        .eq("is_anonymized", False) \
                        .ilike("invoice_no", f"%{kw_clean}%") \
                        .order("created_at", desc=True).limit(100)
                    results = (q2.execute()).data or []
                    if results:
                        return results
                # 3차: 12자리 숫자 → 하이픈 포맷(XXXX-XXXX-XXXX) 변환 후 재시도
                if len(kw_clean) == 12 and kw_clean.isdigit():
                    kw_fmt = f"{kw_clean[:4]}-{kw_clean[4:8]}-{kw_clean[8:]}"
                    q3 = self.client.table("order_shipping").select("*") \
                        .eq("is_anonymized", False) \
                        .ilike("invoice_no", f"%{kw_fmt}%") \
                        .order("created_at", desc=True).limit(100)
                    results = (q3.execute()).data or []
                return results

            if field == 'invoice':
                return _invoice_search(keyword)
            elif field == 'name':
                q = self.client.table("order_shipping").select("*") \
                    .eq("is_anonymized", False) \
                    .ilike("name", f"%{keyword}%")
            else:
                # 'all': 이름 검색 + 송장 검색 합산
                invoice_results = _invoice_search(keyword)
                name_q = self.client.table("order_shipping").select("*") \
                    .eq("is_anonymized", False) \
                    .ilike("name", f"%{keyword}%") \
                    .order("created_at", desc=True).limit(100)
                name_results = (name_q.execute()).data or []
                # 중복 제거 (id 기준)
                seen = set()
                merged = []
                for r in invoice_results + name_results:
                    if r['id'] not in seen:
                        seen.add(r['id'])
                        merged.append(r)
                return merged[:100]

            q = q.order("created_at", desc=True).limit(100)
            res = q.execute()
            return res.data or []
        except Exception as e:
            print(f"[DB] search_order_shipping error: {e}")
            return []

    def update_order_shipping_invoice(self, channel, order_no,
                                       invoice_no, courier=None,
                                       shipping_status=None):
        """order_shipping 송장번호 업데이트."""
        try:
            update = {"invoice_no": invoice_no}
            if courier:
                update["courier"] = courier
            if shipping_status:
                update["shipping_status"] = shipping_status
            self.client.table("order_shipping").update(update) \
                .eq("channel", channel).eq("order_no", order_no).execute()
            return True
        except Exception as e:
            print(f"[DB] update_order_shipping_invoice error: {e}")
            return False

    def bulk_update_shipping_invoices(self, updates):
        """송장번호 일괄 업데이트.

        Args:
            updates: list of {channel, order_no, invoice_no, courier}
        Returns:
            int: 업데이트 건수
        """
        count = 0
        for u in updates:
            if self.update_order_shipping_invoice(
                u['channel'], u['order_no'],
                u['invoice_no'], u.get('courier')
            ):
                count += 1
        return count

    def query_pending_invoice_push(self, channel=None, date_from=None, date_to=None):
        """송장 push 대기건 조회: order_shipping (invoice_no 있음 + shipping_status='대기')
        + api_orders (api_order_id, api_line_id, raw_data) 매핑.

        Returns:
            [{channel, order_no, invoice_no, courier,
              api_order_id, api_line_id, raw_data}, ...]
        """
        try:
            # 1. order_shipping: 송장번호 있고 대기 상태
            q = self.client.table("order_shipping") \
                .select("channel,order_no,invoice_no,courier")
            q = q.eq("shipping_status", "대기") \
                .neq("invoice_no", "").not_.is_("invoice_no", "null")
            if channel:
                q = q.eq("channel", channel)
            ship_res = q.order("created_at", desc=True).limit(500).execute()
            ships = ship_res.data or []
            if not ships:
                return []

            # 2. api_orders에서 매핑 정보 조회
            order_nos = list(set(s['order_no'] for s in ships))
            api_map = {}  # (channel, order_no) → [{api_order_id, api_line_id, raw_data}]
            for chunk_start in range(0, len(order_nos), 100):
                chunk = order_nos[chunk_start:chunk_start + 100]
                aq = self.client.table("api_orders") \
                    .select("channel,api_order_id,api_line_id,raw_data") \
                    .in_("api_order_id", chunk)
                if channel:
                    aq = aq.eq("channel", channel)
                api_res = aq.execute()
                for a in (api_res.data or []):
                    key = (a['channel'], a['api_order_id'])
                    if key not in api_map:
                        api_map[key] = []
                    api_map[key].append(a)

            # 3. 조인 결과
            result = []
            for s in ships:
                ch = s['channel']
                ono = s['order_no']
                api_rows = api_map.get((ch, ono), [])
                if api_rows:
                    for ar in api_rows:
                        result.append({
                            'channel': ch,
                            'order_no': ono,
                            'invoice_no': s['invoice_no'],
                            'courier': s.get('courier', ''),
                            'api_order_id': ar['api_order_id'],
                            'api_line_id': ar['api_line_id'],
                            'raw_data': ar.get('raw_data') or {},
                        })
                else:
                    # api_orders에 없는 경우 (수동 주문 등)
                    result.append({
                        'channel': ch,
                        'order_no': ono,
                        'invoice_no': s['invoice_no'],
                        'courier': s.get('courier', ''),
                        'api_order_id': '',
                        'api_line_id': '',
                        'raw_data': {},
                    })

            return result
        except Exception as e:
            print(f"[DB] query_pending_invoice_push error: {e}")
            return []

    def bulk_update_shipping_status(self, updates):
        """shipping_status 일괄 업데이트.

        Args:
            updates: [{channel, order_no, shipping_status}]
        Returns:
            int: 업데이트 건수
        """
        count = 0
        for u in updates:
            try:
                self.client.table("order_shipping").update({
                    "shipping_status": u['shipping_status'],
                }).eq("channel", u['channel']).eq("order_no", u['order_no']).execute()
                count += 1
            except Exception as e:
                print(f"[DB] bulk_update_shipping_status error: {u} → {e}")
        return count

    def _batch_query_orders_by_keys(self, order_keys, date_from=None, date_to=None,
                                      channel_filter=None, status=None, limit=200):
        """(channel, order_no) 쌍 목록으로 order_transactions 배치 조회.
        채널별 .in_() 사용하여 N+1 제거.
        """
        by_channel = {}
        for ch, ono in order_keys:
            by_channel.setdefault(ch, set()).add(ono)

        results = []
        for ch, order_nos in by_channel.items():
            if channel_filter and ch != channel_filter:
                continue
            for i in range(0, len(order_nos), 200):
                batch_nos = list(order_nos)[i:i+200]
                try:
                    q = self.client.table("order_transactions").select("*") \
                        .eq("channel", ch).in_("order_no", batch_nos)
                    if date_from:
                        q = q.gte("order_date", date_from)
                    if date_to:
                        q = q.lte("order_date", date_to)
                    if status:
                        q = q.eq("status", status)
                    res = q.execute()
                    if res.data:
                        results.extend(res.data)
                except Exception:
                    pass
        results.sort(key=lambda x: x.get('order_date', ''), reverse=True)
        return results[:limit]

    def query_order_transactions_extended(self, date_from=None, date_to=None,
                                           channel=None, status=None,
                                           outbound=None,
                                           search=None, search_field=None,
                                           limit=100, offset=0):
        """주문 확장 검색 (송장번호/수취인명 검색 포함).
        최적화: 채널별 배치 .in_() 조회 (N+1 제거).

        search_field: 'all'(기본), 'order_no', 'product', 'invoice', 'recipient'
        """
        try:
            # 송장번호/수취인명 검색 → order_shipping에서 order_no 매칭
            if search and search_field in ('invoice', 'recipient'):
                sf = 'invoice' if search_field == 'invoice' else 'name'
                shipping = self.search_order_shipping(search, field=sf)
                if not shipping:
                    return []
                order_keys = [(s['channel'], s['order_no']) for s in shipping]
                results = self._batch_query_orders_by_keys(
                    order_keys[:200], date_from, date_to, channel, status, limit
                )
                if results:
                    self._merge_invoice_no(results)
                return results

            # 기본 검색 (기존 로직 확장)
            q = self.client.table("order_transactions").select("*")
            if date_from:
                q = q.gte("order_date", date_from)
            if date_to:
                q = q.lte("order_date", date_to)
            if channel:
                q = q.eq("channel", channel)
            if status:
                q = q.eq("status", status)
            if outbound == 'done':
                q = q.eq("is_outbound_done", True)
            elif outbound == 'not_done':
                q = q.eq("is_outbound_done", False)
            if search:
                if search_field == 'order_no':
                    q = q.ilike("order_no", f"%{search}%")
                elif search_field in ('product', 'product_name'):
                    q = q.ilike("product_name", f"%{search}%")
                else:
                    q = q.or_(
                        f"order_no.ilike.%{search}%,"
                        f"product_name.ilike.%{search}%"
                    )
            q = q.order("order_date", desc=True).order("id", desc=True)
            q = q.range(offset, offset + limit - 1)
            res = q.execute()
            results = res.data or []

            # "전체" 검색이면 수취인명 검색 결과도 병합 (배치 조회)
            if search and search_field in ('all', '', None):
                try:
                    shipping = self.search_order_shipping(search, field='name')
                    if shipping:
                        existing_ids = {r['id'] for r in results}
                        order_keys = [(s['channel'], s['order_no']) for s in shipping]
                        extra = self._batch_query_orders_by_keys(
                            order_keys[:100], date_from, date_to, channel, status, limit
                        )
                        for row in extra:
                            if row['id'] not in existing_ids:
                                results.append(row)
                                existing_ids.add(row['id'])
                        results.sort(key=lambda x: x.get('order_date', ''), reverse=True)
                        results = results[:limit]
                except Exception:
                    pass

            # 결과에 invoice_no 병합 (order_shipping 조인)
            if results:
                self._merge_invoice_no(results)
            return results
        except Exception as e:
            print(f"[DB] query_order_transactions_extended error: {e}")
            return []

    def _merge_invoice_no(self, orders):
        """주문 목록에 order_shipping + packing_jobs 정보 병합 (in-place).
        최적화: 채널별 order_no 배치 .in_() 조회 (N+1 제거).
        """
        try:
            keys = list({(o.get('channel', ''), o.get('order_no', '')) for o in orders})
            shipping_map = {}  # (channel, order_no) → {invoice_no, courier, name, shipping_status}

            # 채널별로 그룹화하여 .in_() 배치 조회
            by_channel = {}
            for ch, ono in keys:
                by_channel.setdefault(ch, []).append(ono)

            for ch, order_nos in by_channel.items():
                # 200개씩 배치 (Supabase .in_() 제한 고려)
                for i in range(0, len(order_nos), 200):
                    batch_nos = order_nos[i:i+200]
                    try:
                        res = self.client.table("order_shipping") \
                            .select("channel,order_no,invoice_no,courier,name,shipping_status") \
                            .eq("channel", ch) \
                            .in_("order_no", batch_nos) \
                            .execute()
                        for s in (res.data or []):
                            shipping_map[(s.get('channel', ''), s.get('order_no', ''))] = {
                                'invoice_no': s.get('invoice_no', ''),
                                'courier': s.get('courier', ''),
                                'name': s.get('name', ''),
                                'shipping_status': s.get('shipping_status', ''),
                            }
                    except Exception:
                        pass

            # 패킹 상태 배치 조회 (order_no 기준)
            packing_map = {}  # order_no → {status, completed_at}
            all_order_nos = list({o.get('order_no', '') for o in orders if o.get('order_no')})
            if all_order_nos:
                for i in range(0, len(all_order_nos), 200):
                    batch = all_order_nos[i:i+200]
                    try:
                        pj = self.client.table("packing_jobs") \
                            .select("order_no,status,completed_at") \
                            .in_("order_no", batch) \
                            .eq("status", "completed") \
                            .execute()
                        for p in (pj.data or []):
                            ono = p.get('order_no', '')
                            if ono:
                                packing_map[ono] = {
                                    'packing_status': 'completed',
                                    'packing_completed_at': p.get('completed_at', ''),
                                }
                    except Exception:
                        pass

            # 병합
            for o in orders:
                key = (o.get('channel', ''), o.get('order_no', ''))
                si = shipping_map.get(key, {})
                o['invoice_no'] = si.get('invoice_no', '')
                o['courier'] = si.get('courier', '')
                o['recipient_name'] = si.get('name', '')
                o['shipping_status'] = si.get('shipping_status', '')
                # 패킹 상태
                pk = packing_map.get(o.get('order_no', ''), {})
                o['packing_status'] = pk.get('packing_status', '')
                o['packing_completed_at'] = pk.get('packing_completed_at', '')
        except Exception as e:
            print(f"[DB] _merge_invoice_no error: {e}")
            import traceback; traceback.print_exc()
            for o in orders:
                o.setdefault('invoice_no', '')
                o.setdefault('courier', '')
                o.setdefault('recipient_name', '')
                o.setdefault('shipping_status', '')
                o.setdefault('packing_status', '')
                o.setdefault('packing_completed_at', '')

    # ── Packing Jobs ──────────────────────────────────────

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

    def update_packing_job(self, job_id, update_data):
        """패킹 작업 업데이트."""
        try:
            self.client.table("packing_jobs").update(update_data) \
                .eq("id", job_id).execute()
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

    def _storage_upload(self, bucket_key, path, file_bytes, content_type=None):
        """범용 Storage 업로드. bucket_key: 'output'|'upload'|'report'"""
        bucket = self.STORAGE_BUCKETS.get(bucket_key, bucket_key)
        if content_type is None:
            ext = path.rsplit('.', 1)[-1].lower() if '.' in path else ''
            content_type = {
                'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                'xls': 'application/vnd.ms-excel',
                'csv': 'text/csv',
                'pdf': 'application/pdf',
            }.get(ext, 'application/octet-stream')
        try:
            self.client.storage.from_(bucket).upload(
                path, file_bytes, file_options={"content-type": content_type}
            )
            return True
        except Exception as e:
            if '409' in str(e) or 'Duplicate' in str(e) or 'already exists' in str(e):
                try:
                    self.client.storage.from_(bucket).update(
                        path, file_bytes, file_options={"content-type": content_type}
                    )
                    return True
                except Exception as e2:
                    print(f"[DB] _storage_upload({bucket}) update error: {e2}")
            else:
                print(f"[DB] _storage_upload({bucket}) error: {e}")
            return False

    def _storage_signed_url(self, bucket_key, path, expires_in=3600):
        """범용 서명 URL 생성."""
        bucket = self.STORAGE_BUCKETS.get(bucket_key, bucket_key)
        try:
            res = self.client.storage.from_(bucket).create_signed_url(path, expires_in)
            if isinstance(res, dict):
                return res.get('signedURL', '') or res.get('signedUrl', '')
            return ''
        except Exception as e:
            print(f"[DB] _storage_signed_url({bucket}) error: {e}")
            return ''

    def _storage_list(self, bucket_key, prefix='', limit=100):
        """범용 파일 목록."""
        bucket = self.STORAGE_BUCKETS.get(bucket_key, bucket_key)
        try:
            res = self.client.storage.from_(bucket).list(
                prefix, {"limit": limit, "sortBy": {"column": "created_at", "order": "desc"}}
            )
            return res or []
        except Exception as e:
            print(f"[DB] _storage_list({bucket}) error: {e}")
            return []

    # 편의 함수
    def upload_output_file(self, path, file_bytes, content_type=None):
        return self._storage_upload('output', path, file_bytes, content_type)

    def get_output_file_signed_url(self, path, expires_in=3600):
        return self._storage_signed_url('output', path, expires_in)

    def list_output_files(self, prefix='', limit=100):
        return self._storage_list('output', prefix, limit)

    def upload_user_file(self, path, file_bytes, content_type=None):
        return self._storage_upload('upload', path, file_bytes, content_type)

    def upload_report_file(self, path, file_bytes, content_type=None):
        return self._storage_upload('report', path, file_bytes, content_type)

    def get_report_signed_url(self, path, expires_in=3600):
        return self._storage_signed_url('report', path, expires_in)

    # ── expenses (간접비/비용 관리) ──

    def query_expenses(self, month=None, category=None):
        """비용 목록 조회. month='2026-03', category='인건비' 등 필터 가능."""
        try:
            q = self.client.table("expenses").select("*")
            if month:
                q = q.eq("expense_month", month)
            if category:
                q = q.eq("category", category)
            q = q.order("expense_date", desc=True)
            res = q.execute()
            return res.data or []
        except Exception as e:
            print(f"[DB] query_expenses error: {e}")
            return []

    def insert_expense(self, data):
        """비용 1건 등록. data: {expense_date, expense_month, category, ...}."""
        res = self.client.table("expenses").insert(data).execute()
        return res.data[0] if res.data else None

    def update_expense(self, expense_id, data):
        """비용 1건 수정."""
        res = self.client.table("expenses").update(data).eq(
            "id", int(expense_id)
        ).execute()
        return res.data[0] if res.data else None

    def delete_expense(self, expense_id):
        """비용 1건 삭제."""
        self.client.table("expenses").delete().eq(
            "id", int(expense_id)
        ).execute()

    def query_expense_categories(self):
        """비용 카테고리 목록 조회 (활성만, sort_order 순)."""
        try:
            res = self.client.table("expense_categories").select("*") \
                .eq("is_active", True) \
                .order("sort_order") \
                .execute()
            return res.data or []
        except Exception as e:
            print(f"[DB] query_expense_categories error: {e}")
            return []

    def generate_recurring_expenses(self, target_month):
        """반복 비용 자동 생성: 직전월의 is_recurring=True 항목을 target_month로 복사.
        target_month: '2026-03' 형식.
        이미 해당 월에 반복 비용이 존재하면 건너뜀(중복 방지).
        """
        # 직전월 계산
        parts = target_month.split('-')
        year, mon = int(parts[0]), int(parts[1])
        if mon == 1:
            prev_month = f"{year - 1}-12"
        else:
            prev_month = f"{year}-{mon - 1:02d}"

        # 직전월 반복 비용 조회
        prev_rows = self.query_expenses(month=prev_month)
        recurring = [r for r in prev_rows if r.get('is_recurring')]
        if not recurring:
            return 0

        # 이미 해당 월에 반복 비용이 있는지 확인 (카테고리+서브카테고리 기준)
        existing = self.query_expenses(month=target_month)
        existing_keys = {
            (r.get('category', ''), r.get('subcategory', ''))
            for r in existing if r.get('is_recurring')
        }

        inserted = 0
        for row in recurring:
            key = (row.get('category', ''), row.get('subcategory', ''))
            if key in existing_keys:
                continue

            # expense_date를 target_month의 1일로 설정
            new_date = f"{target_month}-01"
            new_row = {
                'expense_date': new_date,
                'expense_month': target_month,
                'category': row.get('category', ''),
                'subcategory': row.get('subcategory', ''),
                'amount': row.get('amount', 0),
                'is_recurring': True,
                'memo': row.get('memo', ''),
                'registered_by': row.get('registered_by', ''),
            }
            self.insert_expense(new_row)
            inserted += 1

        return inserted

    # ── employees (직원 관리) ──

    def query_employees(self, status=None):
        """직원 목록 조회. status='재직'/'퇴직' 필터 가능."""
        try:
            q = self.client.table("employees").select("*")
            if status:
                q = q.eq("status", status)
            q = q.order("name")
            res = q.execute()
            return res.data or []
        except Exception as e:
            print(f"[DB] query_employees error: {e}")
            return []

    def insert_employee(self, data):
        """직원 1명 등록."""
        res = self.client.table("employees").insert(data).execute()
        return res.data[0] if res.data else None

    def update_employee(self, emp_id, data):
        """직원 정보 수정."""
        res = self.client.table("employees").update(data).eq(
            "id", int(emp_id)
        ).execute()
        return res.data[0] if res.data else None

    def delete_employee(self, emp_id):
        """직원 삭제."""
        self.client.table("employees").delete().eq(
            "id", int(emp_id)
        ).execute()

    # ── payroll_monthly (급여 관리) ──

    def query_payroll(self, pay_month=None):
        """급여 목록 조회. pay_month='2026-03' 필터."""
        try:
            q = self.client.table("payroll_monthly").select("*")
            if pay_month:
                q = q.eq("pay_month", pay_month)
            q = q.order("employee_id")
            res = q.execute()
            return res.data or []
        except Exception as e:
            print(f"[DB] query_payroll error: {e}")
            return []

    def insert_payroll(self, data):
        """급여 1건 등록."""
        res = self.client.table("payroll_monthly").insert(data).execute()
        return res.data[0] if res.data else None

    def update_payroll(self, payroll_id, data):
        """급여 1건 수정."""
        res = self.client.table("payroll_monthly").update(data).eq(
            "id", int(payroll_id)
        ).execute()
        return res.data[0] if res.data else None

    def generate_monthly_payroll(self, pay_month):
        """재직 직원의 기본급으로 월 급여 자동 생성.
        이미 해당 월에 급여가 있는 직원은 건너뜀.
        Returns: 생성 건수.
        """
        employees = self.query_employees(status='재직')
        if not employees:
            return 0

        existing = self.query_payroll(pay_month=pay_month)
        existing_emp_ids = {r.get('employee_id') for r in existing}

        inserted = 0
        for emp in employees:
            emp_id = emp.get('id')
            if emp_id in existing_emp_ids:
                continue
            base = float(emp.get('base_salary', 0))
            payload = {
                'employee_id': emp_id,
                'pay_month': pay_month,
                'base_salary': base,
                'allowances': 0,
                'total_cost': base,
                'memo': '',
            }
            self.insert_payroll(payload)
            inserted += 1

        return inserted

    def sync_payroll_to_expenses(self, pay_month):
        """payroll 합계를 expenses에 자동 반영.

        - 인건비 (급여합계): total_cost 합계
        - 보험료 (4대보험 회사부담): 국민연금+건강+장기요양+고용+산재 사업주분

        해당 월의 기존 항목이 있으면 금액 업데이트, 없으면 신규 등록.
        Returns: dict {total_cost, insurance_cost, actions: [...]}.
        """
        payroll = self.query_payroll(pay_month=pay_month)
        if not payroll:
            return {'total_cost': 0, 'insurance_cost': 0, 'actions': ['no_data']}

        total_cost = sum(float(r.get('total_cost', 0)) for r in payroll)

        # 4대보험 회사부담분 합계
        insurance_fields = [
            'national_pension_employer',      # 국민연금
            'health_insurance_employer',      # 건강보험
            'long_term_care_employer',        # 장기요양
            'employment_insurance_employer',  # 고용보험
            'industrial_accident_insurance',  # 산재보험
        ]
        insurance_cost = 0
        for r in payroll:
            for field in insurance_fields:
                insurance_cost += float(r.get(field, 0) or 0)

        expense_date = f"{pay_month}-25"  # 급여 지급일 기준 25일
        actions = []

        # ── 1. 인건비 (급여합계) ──
        existing_labor = self.query_expenses(month=pay_month, category='인건비')
        payroll_expense = None
        for ex in existing_labor:
            if ex.get('subcategory') == '급여합계':
                payroll_expense = ex
                break

        if payroll_expense:
            self.update_expense(payroll_expense['id'], {
                'amount': total_cost,
                'memo': f'{pay_month} 급여 합계 (자동반영)',
            })
            actions.append('labor_updated')
        else:
            self.insert_expense({
                'expense_date': expense_date,
                'expense_month': pay_month,
                'category': '인건비',
                'subcategory': '급여합계',
                'amount': total_cost,
                'is_recurring': False,
                'memo': f'{pay_month} 급여 합계 (자동반영)',
                'registered_by': 'system',
            })
            actions.append('labor_inserted')

        # ── 2. 보험료 (4대보험 회사부담) ──
        if insurance_cost > 0:
            existing_ins = self.query_expenses(month=pay_month, category='보험료')
            ins_expense = None
            for ex in existing_ins:
                if ex.get('subcategory') == '4대보험':
                    ins_expense = ex
                    break

            if ins_expense:
                self.update_expense(ins_expense['id'], {
                    'amount': round(insurance_cost),
                    'memo': f'{pay_month} 4대보험 회사부담 (자동반영)',
                })
                actions.append('insurance_updated')
            else:
                self.insert_expense({
                    'expense_date': expense_date,
                    'expense_month': pay_month,
                    'category': '보험료',
                    'subcategory': '4대보험',
                    'amount': round(insurance_cost),
                    'is_recurring': False,
                    'memo': f'{pay_month} 4대보험 회사부담 (자동반영)',
                    'registered_by': 'system',
                })
                actions.append('insurance_inserted')

        return {
            'total_cost': total_cost,
            'insurance_cost': round(insurance_cost),
            'actions': actions,
        }

    # ── annual_leave / leave_records (연차 관리) ──

    def query_annual_leave(self, employee_id=None, year=None):
        """연차 현황 조회."""
        try:
            q = self.client.table("annual_leave").select("*")
            if employee_id:
                q = q.eq("employee_id", int(employee_id))
            if year:
                q = q.eq("leave_year", int(year))
            res = q.execute()
            return res.data or []
        except Exception as e:
            print(f"[DB] query_annual_leave error: {e}")
            return []

    def update_annual_leave(self, employee_id, year, data):
        """연차 현황 upsert (없으면 생성, 있으면 수정)."""
        existing = self.query_annual_leave(
            employee_id=employee_id, year=year
        )
        if existing:
            res = self.client.table("annual_leave").update(data).eq(
                "employee_id", int(employee_id)
            ).eq("leave_year", int(year)).execute()
            return res.data[0] if res.data else None
        else:
            data['employee_id'] = int(employee_id)
            data['leave_year'] = int(year)
            res = self.client.table("annual_leave").insert(data).execute()
            return res.data[0] if res.data else None

    def insert_leave_record(self, data):
        """연차 사용 기록 등록 + used_days 자동 업데이트."""
        res = self.client.table("leave_records").insert(data).execute()
        record = res.data[0] if res.data else None

        if record:
            # used_days 자동 업데이트
            emp_id = data.get('employee_id')
            leave_date = data.get('leave_date', '')
            year = int(leave_date[:4]) if leave_date else None
            days = float(data.get('days', 1))
            if emp_id and year:
                al = self.query_annual_leave(employee_id=emp_id, year=year)
                if al:
                    new_used = float(al[0].get('used_days', 0)) + days
                    self.update_annual_leave(emp_id, year, {
                        'used_days': new_used,
                    })
                else:
                    # annual_leave 레코드가 없으면 생성
                    self.update_annual_leave(emp_id, year, {
                        'granted_days': 0,
                        'used_days': days,
                    })

        return record

    def query_leave_records(self, employee_id=None, year=None):
        """연차 사용 기록 조회."""
        try:
            q = self.client.table("leave_records").select("*")
            if employee_id:
                q = q.eq("employee_id", int(employee_id))
            if year:
                date_from = f"{year}-01-01"
                date_to = f"{year}-12-31"
                q = q.gte("leave_date", date_from).lte("leave_date", date_to)
            q = q.order("leave_date", desc=True)
            res = q.execute()
            return res.data or []
        except Exception as e:
            print(f"[DB] query_leave_records error: {e}")
            return []

    @staticmethod
    def calculate_legal_leave_days(hire_date_str):
        """입사일 기반 법정 연차일수 계산.
        - 1년 미만: 매월 1일씩 (최대 11일)
        - 1년 이상: 15일 + 2년마다 1일 추가 (최대 25일)

        Args:
            hire_date_str: 'YYYY-MM-DD' 형식 입사일

        Returns:
            int: 법정 연차일수
        """
        from datetime import date
        if not hire_date_str:
            return 0
        try:
            hire = date.fromisoformat(str(hire_date_str)[:10])
        except (ValueError, TypeError):
            return 0

        today = date.today()
        delta = today - hire
        total_months = (today.year - hire.year) * 12 + (today.month - hire.month)

        if delta.days < 365:
            # 1년 미만: 매월 1일씩, 최대 11일
            return min(total_months, 11)
        else:
            # 1년 이상: 15일 기본
            years_worked = delta.days // 365
            # 2년마다 1일 추가 (최대 25일)
            extra = (years_worked - 1) // 2
            return min(15 + extra, 25)

    # ── salary_components (급여 항목 관리) ──

    def query_salary_components(self, employee_id, active_only=True):
        """직원의 급여 항목 목록 조회.

        Args:
            employee_id: 직원 ID
            active_only: True이면 현재 유효한 항목만 (effective_to IS NULL)

        Returns:
            list of dict
        """
        try:
            q = self.client.table("salary_components").select("*").eq(
                "employee_id", int(employee_id)
            )
            if active_only:
                q = q.is_("effective_to", "null")
            q = q.order("component_type")
            res = q.execute()
            return res.data or []
        except Exception as e:
            print(f"[DB] query_salary_components error: {e}")
            return []

    def upsert_salary_component(self, data):
        """급여 항목 추가/수정.
        id가 있으면 수정, 없으면 신규 추가.

        Args:
            data: dict with employee_id, component_type, component_name,
                  amount, is_taxable, is_fixed, effective_from, etc.

        Returns:
            dict: 저장된 레코드
        """
        comp_id = data.get('id')
        from datetime import datetime, timezone
        data['updated_at'] = datetime.now(timezone.utc).isoformat()

        if comp_id:
            # 수정
            update_data = {k: v for k, v in data.items() if k != 'id'}
            res = self.client.table("salary_components").update(
                update_data
            ).eq("id", int(comp_id)).execute()
        else:
            # 신규
            res = self.client.table("salary_components").insert(data).execute()
        return res.data[0] if res.data else None

    def delete_salary_component(self, comp_id):
        """급여 항목 삭제 (종료일 설정으로 비활성화).

        Args:
            comp_id: salary_component ID
        """
        from datetime import date, datetime, timezone
        self.client.table("salary_components").update({
            'effective_to': date.today().isoformat(),
            'updated_at': datetime.now(timezone.utc).isoformat(),
        }).eq("id", int(comp_id)).execute()

    def bulk_set_salary_components(self, employee_id, components):
        """직원의 급여 항목 일괄 설정.
        기존 활성 항목 중 전달되지 않은 것은 비활성화.

        Args:
            employee_id: 직원 ID
            components: list of dict [{component_type, component_name,
                        amount, is_taxable, is_fixed}, ...]

        Returns:
            int: 설정된 항목 수
        """
        from datetime import date
        today = date.today().isoformat()

        # 기존 활성 항목 조회
        existing = self.query_salary_components(employee_id, active_only=True)
        existing_map = {c['component_type']: c for c in existing}

        count = 0
        new_types = set()

        for comp in components:
            ctype = comp.get('component_type', '')
            if not ctype:
                continue
            new_types.add(ctype)

            if ctype in existing_map:
                # 기존 항목 업데이트
                self.upsert_salary_component({
                    'id': existing_map[ctype]['id'],
                    'component_name': comp.get('component_name', ctype),
                    'amount': int(comp.get('amount', 0)),
                    'is_taxable': comp.get('is_taxable', True),
                    'is_fixed': comp.get('is_fixed', True),
                })
            else:
                # 신규 항목 추가
                self.upsert_salary_component({
                    'employee_id': int(employee_id),
                    'component_type': ctype,
                    'component_name': comp.get('component_name', ctype),
                    'amount': int(comp.get('amount', 0)),
                    'is_taxable': comp.get('is_taxable', True),
                    'is_fixed': comp.get('is_fixed', True),
                    'effective_from': today,
                })
            count += 1

        # 전달되지 않은 기존 항목은 비활성화
        for ctype, existing_comp in existing_map.items():
            if ctype not in new_types:
                self.delete_salary_component(existing_comp['id'])

        return count

    # ── insurance_rates (4대보험 요율 관리) ──

    def query_insurance_rates(self, year=None):
        """4대보험 요율 조회.

        Args:
            year: 연도 (int). None이면 전체.

        Returns:
            list of dict
        """
        try:
            q = self.client.table("insurance_rates").select("*")
            if year:
                q = q.eq("year", int(year))
            q = q.order("insurance_type")
            res = q.execute()
            return res.data or []
        except Exception as e:
            print(f"[DB] query_insurance_rates error: {e}")
            return []

    def update_insurance_rates(self, year, rates):
        """4대보험 요율 일괄 업데이트 (upsert).

        Args:
            year: 연도 (int)
            rates: list of dict [{insurance_type, employee_rate,
                    employer_rate, min_base, max_base, notes}, ...]

        Returns:
            int: 업데이트된 건수
        """
        count = 0
        for rate in rates:
            ins_type = rate.get('insurance_type', '')
            if not ins_type:
                continue
            payload = {
                'year': int(year),
                'insurance_type': ins_type,
                'employee_rate': float(rate.get('employee_rate', 0)),
                'employer_rate': float(rate.get('employer_rate', 0)),
                'min_base': int(rate.get('min_base', 0)),
                'max_base': int(rate.get('max_base', 0)),
                'notes': rate.get('notes', ''),
            }
            try:
                res = self.client.table("insurance_rates").upsert(
                    payload, on_conflict="year,insurance_type"
                ).execute()
                if res.data:
                    count += 1
            except Exception as e:
                print(f"[DB] update_insurance_rates error ({ins_type}): {e}")
        return count

    # ── nontaxable_limits (비과세 한도 관리) ──

    def query_nontaxable_limits(self, year=None):
        """비과세 한도 조회.

        Args:
            year: 연도 (int). None이면 전체.

        Returns:
            list of dict
        """
        try:
            q = self.client.table("nontaxable_limits").select("*")
            if year:
                q = q.eq("year", int(year))
            res = q.execute()
            return res.data or []
        except Exception as e:
            print(f"[DB] query_nontaxable_limits error: {e}")
            return []

    # ── 개인별 보험요율 오버라이드 ──

    def query_employee_insurance_overrides(self, employee_id):
        """직원의 개인별 보험요율 오버라이드 조회.
        Returns: list of dict [{insurance_type, employee_rate, employer_rate, notes}, ...]
        """
        try:
            res = self.client.table("employee_insurance_overrides") \
                .select("*").eq("employee_id", employee_id).execute()
            return res.data or []
        except Exception as e:
            print(f"[DB] query_employee_insurance_overrides error: {e}")
            return []

    def upsert_employee_insurance_override(self, employee_id, insurance_type,
                                            employee_rate, employer_rate, notes=''):
        """직원 보험요율 오버라이드 설정 (upsert).
        기존 레코드가 있으면 업데이트, 없으면 생성.
        """
        try:
            # 기존 레코드 확인
            res = self.client.table("employee_insurance_overrides") \
                .select("id").eq("employee_id", employee_id) \
                .eq("insurance_type", insurance_type).execute()
            payload = {
                'employee_id': employee_id,
                'insurance_type': insurance_type,
                'employee_rate': float(employee_rate),
                'employer_rate': float(employer_rate),
                'notes': notes,
            }
            if res.data:
                self.client.table("employee_insurance_overrides") \
                    .update(payload).eq("id", res.data[0]['id']).execute()
            else:
                self.client.table("employee_insurance_overrides") \
                    .insert(payload).execute()
        except Exception as e:
            print(f"[DB] upsert_employee_insurance_override error: {e}")
            raise

    def delete_employee_insurance_override(self, employee_id, insurance_type):
        """직원 보험요율 오버라이드 삭제 (기본값으로 복원)."""
        try:
            self.client.table("employee_insurance_overrides") \
                .delete().eq("employee_id", employee_id) \
                .eq("insurance_type", insurance_type).execute()
        except Exception as e:
            print(f"[DB] delete_employee_insurance_override error: {e}")

    # ── enhanced payroll generation ──

    def generate_monthly_payroll_v2(self, pay_month):
        """한국 급여체계 기반 월 급여 자동 생성.
        각 직원의 salary_components, insurance_rates 기반 자동 계산.
        hire_date/retire_date 기반 대상자 필터 + 일할계산 + 근태차감.
        이미 해당 월에 급여가 있는 직원은 재계산하여 UPDATE.

        Args:
            pay_month: 'YYYY-MM' 형식 대상 월

        Returns:
            dict: {inserted: 신규건수, updated: 갱신건수, skipped: 스킵건수}
        """
        from services.hr_service import (
            calculate_payroll, calculate_proration_ratio,
            calculate_attendance_deductions
        )
        from datetime import datetime, timezone
        import calendar as cal_mod

        year = int(pay_month[:4])
        month = int(pay_month[5:7])
        cal_days = cal_mod.monthrange(year, month)[1]
        month_start = f'{year}-{month:02d}-01'
        month_end = f'{year}-{month:02d}-{cal_days:02d}'

        # 해당 월 재직 대상자: hire_date <= 월말 AND (retire_date IS NULL OR retire_date >= 월초)
        all_employees = self.query_employees()
        eligible = []
        for emp in all_employees:
            status = emp.get('status', '')
            hire = emp.get('hire_date', '')
            retire = emp.get('retire_date') or ''

            # 재직 또는 퇴사자 중 해당 월에 근무한 직원
            if status not in ('재직', '퇴사', '퇴직'):
                continue
            if not hire:
                if status == '재직':
                    eligible.append(emp)
                continue
            if hire > month_end:
                continue  # 아직 입사 전
            if retire and retire < month_start:
                continue  # 이미 퇴사
            eligible.append(emp)

        if not eligible:
            return {'inserted': 0, 'updated': 0, 'skipped': 0}

        existing = self.query_payroll(pay_month=pay_month)
        existing_map = {r.get('employee_id'): r for r in existing}

        # 보험 요율 조회
        insurance_rates = self.query_insurance_rates(year=year)
        rate_map = {r['insurance_type']: r for r in insurance_rates}

        # 비과세 한도 조회
        nontax_limits = self.query_nontaxable_limits(year=year)
        nontax_map = {r['limit_type']: r['monthly_limit'] for r in nontax_limits}

        inserted = 0
        updated = 0
        skipped = 0
        for emp in eligible:
            emp_id = emp.get('id')

            # 일할비율 계산
            proration = calculate_proration_ratio(
                emp.get('hire_date'), emp.get('retire_date'), year, month)
            if proration['ratio'] <= 0:
                skipped += 1
                continue

            # 근태 차감 계산 (결근/조퇴/무급/지각)
            leave_recs = self._query_leave_records_for_month(emp_id, year, month)
            att_result = calculate_attendance_deductions(
                leave_recs,
                int(float(emp.get('base_salary', 0))) * proration['ratio'],
                proration['calendar_days'])

            # 직원의 급여 항목 조회
            components = self.query_salary_components(emp_id, active_only=True)

            # 개인별 보험요율 오버라이드 조회
            overrides = self.query_employee_insurance_overrides(emp_id)

            # 급여 계산 (일할 + 근태차감 반영)
            result = calculate_payroll(
                emp, components, rate_map, nontax_map,
                insurance_overrides=overrides,
                proration_ratio=proration['ratio'],
                attendance_deduction=att_result['total_deduction'],
                attendance_detail=att_result['detail'],
                proration_days=proration['work_days'],
                calendar_days=proration['calendar_days'])

            payroll_data = {
                'base_salary': result['base_salary'],
                'allowances': result['total_allowances'],
                'total_cost': result['gross_salary'],
                'position_allowance': result['position_allowance'],
                'responsibility_allowance': result['responsibility_allowance'],
                'longevity_allowance': result['longevity_allowance'],
                'meal_allowance': result['meal_allowance'],
                'vehicle_allowance': result['vehicle_allowance'],
                'overtime_pay': result['overtime_pay'],
                'night_pay': result['night_pay'],
                'holiday_pay': result['holiday_pay'],
                'bonus': result['bonus'],
                'other_allowance': result['other_allowance'],
                'other_allowance_detail': result.get('other_allowance_detail', {}),
                'gross_salary': result['gross_salary'],
                'taxable_amount': result['taxable_amount'],
                'nontaxable_amount': result['nontaxable_amount'],
                'national_pension': result['national_pension'],
                'health_insurance': result['health_insurance'],
                'long_term_care': result['long_term_care'],
                'employment_insurance': result['employment_insurance'],
                'income_tax': result['income_tax'],
                'local_income_tax': result['local_income_tax'],
                'total_deductions': result['total_deductions'],
                'net_salary': result['net_salary'],
                'national_pension_employer': result['national_pension_employer'],
                'health_insurance_employer': result['health_insurance_employer'],
                'long_term_care_employer': result['long_term_care_employer'],
                'employment_insurance_employer': result['employment_insurance_employer'],
                'industrial_accident_insurance': result['industrial_accident_insurance'],
                'total_employer_cost': result['total_employer_cost'],
                'proration_ratio': result['proration_ratio'],
                'proration_days': result['proration_days'],
                'calendar_days': result['calendar_days'],
                'attendance_deduction': result['attendance_deduction'],
                'attendance_detail': result['attendance_detail'],
            }

            existing_payroll = existing_map.get(emp_id)
            if existing_payroll:
                # 기존 레코드가 confirmed가 아니면 재계산 UPDATE
                if existing_payroll.get('status') != 'confirmed':
                    payroll_data['updated_at'] = datetime.now(timezone.utc).isoformat()
                    self.update_payroll(existing_payroll['id'], payroll_data)
                    updated += 1
                else:
                    skipped += 1
            else:
                # 신규 레코드 INSERT
                payroll_data['employee_id'] = emp_id
                payroll_data['pay_month'] = pay_month
                payroll_data['status'] = 'draft'
                payroll_data['memo'] = ''
                self.insert_payroll(payroll_data)
                inserted += 1

        # 급여 → expenses 자동 동기화
        if inserted > 0 or updated > 0:
            try:
                self.sync_payroll_to_expenses(pay_month)
            except Exception:
                pass  # 동기화 실패해도 급여 생성 결과는 유지

        return {'inserted': inserted, 'updated': updated, 'skipped': skipped}

    def _query_leave_records_for_month(self, employee_id, year, month):
        """특정 직원의 해당 월 leave_records 조회 (결근/조퇴/무급/지각 포함)."""
        month_start = f'{year}-{month:02d}-01'
        import calendar as cal_mod
        cal_days = cal_mod.monthrange(year, month)[1]
        month_end = f'{year}-{month:02d}-{cal_days:02d}'
        try:
            res = self.client.table('leave_records') \
                .select('*') \
                .eq('employee_id', employee_id) \
                .gte('leave_date', month_start) \
                .lte('leave_date', month_end) \
                .execute()
            return res.data or []
        except Exception:
            return []

    def generate_bulk_payroll(self, from_month, to_month):
        """여러 월 급여 일괄 생성.

        Args:
            from_month: 시작월 'YYYY-MM'
            to_month: 종료월 'YYYY-MM'

        Returns:
            dict: {months: [{month, inserted, updated, skipped}, ...], total_inserted, total_updated}
        """
        from datetime import date
        results = []
        total_ins = 0
        total_upd = 0

        # 월 목록 생성
        fy, fm = int(from_month[:4]), int(from_month[5:7])
        ty, tm = int(to_month[:4]), int(to_month[5:7])
        y, m = fy, fm
        while (y, m) <= (ty, tm):
            pay_month = f'{y}-{m:02d}'
            r = self.generate_monthly_payroll_v2(pay_month)
            results.append({
                'month': pay_month,
                'inserted': r['inserted'],
                'updated': r['updated'],
                'skipped': r.get('skipped', 0),
            })
            total_ins += r['inserted']
            total_upd += r['updated']
            m += 1
            if m > 12:
                m = 1
                y += 1

        return {
            'months': results,
            'total_inserted': total_ins,
            'total_updated': total_upd,
        }

    def recalculate_payroll(self, payroll_id):
        """기존 급여 1건 재계산 (급여 항목/보험 요율 변경 시).

        Args:
            payroll_id: payroll_monthly ID

        Returns:
            dict: 업데이트된 급여 레코드
        """
        from services.hr_service import calculate_payroll
        from datetime import datetime, timezone

        # 기존 급여 조회
        try:
            res = self.client.table("payroll_monthly").select("*").eq(
                "id", int(payroll_id)
            ).execute()
            if not res.data:
                return None
            payroll = res.data[0]
        except Exception:
            return None

        emp_id = payroll.get('employee_id')
        pay_month = payroll.get('pay_month', '')
        year = int(pay_month[:4]) if pay_month else 2025

        # 직원 정보
        employees = self.query_employees()
        emp = next((e for e in employees if e['id'] == emp_id), None)
        if not emp:
            return None

        # 급여 항목 & 요율
        components = self.query_salary_components(emp_id, active_only=True)
        insurance_rates = self.query_insurance_rates(year=year)
        rate_map = {r['insurance_type']: r for r in insurance_rates}
        nontax_limits = self.query_nontaxable_limits(year=year)
        nontax_map = {r['limit_type']: r['monthly_limit'] for r in nontax_limits}

        # 개인별 보험요율 오버라이드
        overrides = self.query_employee_insurance_overrides(emp_id)

        result = calculate_payroll(emp, components, rate_map, nontax_map,
                                   insurance_overrides=overrides)

        update_data = {
            'base_salary': result['base_salary'],
            'allowances': result['total_allowances'],
            'total_cost': result['gross_salary'],
            'position_allowance': result['position_allowance'],
            'responsibility_allowance': result['responsibility_allowance'],
            'longevity_allowance': result['longevity_allowance'],
            'meal_allowance': result['meal_allowance'],
            'vehicle_allowance': result['vehicle_allowance'],
            'overtime_pay': result['overtime_pay'],
            'night_pay': result['night_pay'],
            'holiday_pay': result['holiday_pay'],
            'bonus': result['bonus'],
            'other_allowance': result['other_allowance'],
            'other_allowance_detail': result.get('other_allowance_detail', {}),
            'gross_salary': result['gross_salary'],
            'taxable_amount': result['taxable_amount'],
            'nontaxable_amount': result['nontaxable_amount'],
            'national_pension': result['national_pension'],
            'health_insurance': result['health_insurance'],
            'long_term_care': result['long_term_care'],
            'employment_insurance': result['employment_insurance'],
            'income_tax': result['income_tax'],
            'local_income_tax': result['local_income_tax'],
            'total_deductions': result['total_deductions'],
            'net_salary': result['net_salary'],
            'national_pension_employer': result['national_pension_employer'],
            'health_insurance_employer': result['health_insurance_employer'],
            'long_term_care_employer': result['long_term_care_employer'],
            'employment_insurance_employer': result['employment_insurance_employer'],
            'industrial_accident_insurance': result['industrial_accident_insurance'],
            'total_employer_cost': result['total_employer_cost'],
            'updated_at': datetime.now(timezone.utc).isoformat(),
        }

        return self.update_payroll(payroll_id, update_data)

    # ══════════════════════════════════════════════════════════
    # 회계 ERP 메서드 (은행/세금계산서/매칭/정산)
    # ══════════════════════════════════════════════════════════

    # ── codef_connections ──

    def insert_codef_connection(self, payload):
        """CODEF 연결 정보 저장 (upsert)."""
        try:
            self.client.table("codef_connections").upsert(
                payload, on_conflict="connected_id"
            ).execute()
        except Exception as e:
            print(f"[DB] insert_codef_connection error: {e}")

    def query_codef_connections(self):
        """CODEF 연결 목록."""
        try:
            res = self.client.table("codef_connections") \
                .select("*").order("created_at", desc=True).execute()
            return res.data or []
        except Exception as e:
            print(f"[DB] query_codef_connections error: {e}")
            return []

    # ── bank_accounts ──

    def query_bank_accounts(self):
        """은행 계좌 전체 조회."""
        try:
            res = self.client.table("bank_accounts") \
                .select("*").order("bank_name").execute()
            return res.data or []
        except Exception as e:
            print(f"[DB] query_bank_accounts error: {e}")
            return []

    def query_bank_account_by_id(self, account_id):
        """은행 계좌 1건 조회."""
        try:
            res = self.client.table("bank_accounts") \
                .select("*").eq("id", account_id).execute()
            return res.data[0] if res.data else None
        except Exception as e:
            print(f"[DB] query_bank_account_by_id error: {e}")
            return None

    def insert_bank_account(self, payload):
        """은행 계좌 등록."""
        try:
            self.client.table("bank_accounts").insert(payload).execute()
        except Exception as e:
            print(f"[DB] insert_bank_account error: {e}")
            raise

    def update_bank_account(self, account_id, update_data):
        """은행 계좌 수정."""
        try:
            self.client.table("bank_accounts") \
                .update(update_data).eq("id", account_id).execute()
        except Exception as e:
            print(f"[DB] update_bank_account error: {e}")

    def delete_bank_account(self, account_id):
        """은행 계좌 삭제."""
        try:
            self.client.table("bank_accounts") \
                .delete().eq("id", account_id).execute()
        except Exception as e:
            print(f"[DB] delete_bank_account error: {e}")

    # ── bank_transactions ──

    def query_bank_transactions(self, date_from=None, date_to=None,
                                 bank_account_id=None, transaction_type=None,
                                 category=None, unmatched_only=False):
        """은행 거래내역 조회 (필터)."""
        try:
            q = self.client.table("bank_transactions") \
                .select("*, bank_accounts(bank_name, account_number)") \
                .order("transaction_date", desc=True) \
                .order("transaction_time", desc=True)
            if date_from:
                q = q.gte("transaction_date", date_from)
            if date_to:
                q = q.lte("transaction_date", date_to)
            if bank_account_id:
                q = q.eq("bank_account_id", bank_account_id)
            if transaction_type:
                q = q.eq("transaction_type", transaction_type)
            if category:
                q = q.eq("category", category)
            if unmatched_only:
                q = q.is_("matched_invoice_id", "null")
                q = q.is_("matched_settlement_id", "null")
            res = q.execute()
            return res.data or []
        except Exception as e:
            print(f"[DB] query_bank_transactions error: {e}")
            return []

    def query_bank_transaction_by_id(self, tx_id):
        """은행 거래내역 1건 조회."""
        try:
            res = self.client.table("bank_transactions") \
                .select("*").eq("id", tx_id).execute()
            return res.data[0] if res.data else None
        except Exception as e:
            print(f"[DB] query_bank_transaction_by_id error: {e}")
            return None

    def insert_bank_transaction(self, payload):
        """은행 거래내역 1건 등록."""
        self.client.table("bank_transactions").insert(payload).execute()

    def update_bank_transaction(self, tx_id, update_data):
        """은행 거래내역 수정 (카테고리 분류 등)."""
        try:
            self.client.table("bank_transactions") \
                .update(update_data).eq("id", tx_id).execute()
        except Exception as e:
            print(f"[DB] update_bank_transaction error: {e}")

    # ── tax_invoices ──

    def query_tax_invoices(self, direction=None, status=None,
                            date_from=None, date_to=None,
                            partner_name=None, unmatched_only=False):
        """세금계산서 목록 조회."""
        try:
            q = self.client.table("tax_invoices") \
                .select("*").order("write_date", desc=True)
            if direction:
                q = q.eq("direction", direction)
            if status:
                q = q.eq("status", status)
            if date_from:
                q = q.gte("write_date", date_from)
            if date_to:
                q = q.lte("write_date", date_to)
            if partner_name:
                q = q.or_(
                    f"supplier_corp_name.ilike.%{partner_name}%,"
                    f"buyer_corp_name.ilike.%{partner_name}%"
                )
            if unmatched_only:
                q = q.is_("matched_transaction_id", "null")
                q = q.neq("status", "cancelled")  # 취소건 제외
            res = q.execute()
            return res.data or []
        except Exception as e:
            print(f"[DB] query_tax_invoices error: {e}")
            return []

    def query_tax_invoice_by_id(self, invoice_id):
        """세금계산서 1건 조회."""
        try:
            res = self.client.table("tax_invoices") \
                .select("*").eq("id", invoice_id).execute()
            return res.data[0] if res.data else None
        except Exception as e:
            print(f"[DB] query_tax_invoice_by_id error: {e}")
            return None

    def insert_tax_invoice(self, payload):
        """세금계산서 등록."""
        try:
            res = self.client.table("tax_invoices").insert(payload).execute()
            return res.data[0]['id'] if res.data else None
        except Exception as e:
            print(f"[DB] insert_tax_invoice error: {e}")
            return None

    def batch_insert_tax_invoices(self, payloads):
        """세금계산서 일괄 등록 (배치). Returns: 삽입된 건수."""
        if not payloads:
            return 0
        try:
            res = self.client.table("tax_invoices").insert(payloads).execute()
            return len(res.data) if res.data else 0
        except Exception as e:
            print(f"[DB] batch_insert_tax_invoices error: {e}")
            return 0

    def query_existing_invoice_numbers(self, invoice_numbers):
        """승인번호 목록으로 기존 세금계산서 일괄 조회. Returns: set of existing numbers."""
        if not invoice_numbers:
            return set()
        try:
            nums = [n for n in invoice_numbers if n]
            if not nums:
                return set()
            # Supabase in_ 필터로 한번에 조회
            res = self.client.table("tax_invoices") \
                .select("invoice_number") \
                .in_("invoice_number", nums) \
                .execute()
            return {r['invoice_number'] for r in (res.data or [])}
        except Exception as e:
            print(f"[DB] query_existing_invoice_numbers error: {e}")
            return set()

    def update_tax_invoice(self, invoice_id, update_data):
        """세금계산서 수정."""
        try:
            self.client.table("tax_invoices") \
                .update(update_data).eq("id", invoice_id).execute()
        except Exception as e:
            print(f"[DB] update_tax_invoice error: {e}")

    def delete_tax_invoice(self, invoice_id):
        """세금계산서 삭제."""
        try:
            self.client.table("tax_invoices") \
                .delete().eq("id", invoice_id).execute()
        except Exception as e:
            print(f"[DB] delete_tax_invoice error: {e}")

    # ── payment_matches ──

    def query_payment_matches(self, date_from=None, date_to=None, status=None):
        """매출-입금 매칭 목록 조회."""
        try:
            q = self.client.table("payment_matches") \
                .select("*").order("matched_at", desc=True)
            if date_from:
                q = q.gte("matched_at", date_from)
            if date_to:
                q = q.lte("matched_at", date_to)
            if status:
                q = q.eq("match_status", status)
            res = q.execute()
            return res.data or []
        except Exception as e:
            print(f"[DB] query_payment_matches error: {e}")
            return []

    def query_payment_match_by_id(self, match_id):
        """매칭 1건 조회."""
        try:
            res = self.client.table("payment_matches") \
                .select("*").eq("id", match_id).execute()
            return res.data[0] if res.data else None
        except Exception as e:
            print(f"[DB] query_payment_match_by_id error: {e}")
            return None

    def insert_payment_match(self, payload):
        """매칭 레코드 등록. Returns: match_id (int) or None."""
        try:
            res = self.client.table("payment_matches").insert(payload).execute()
            if res.data:
                return res.data[0].get('id')
            return None
        except Exception as e:
            print(f"[DB] insert_payment_match error: {e}")
            return None

    def delete_payment_match(self, match_id):
        """매칭 해제."""
        try:
            self.client.table("payment_matches") \
                .delete().eq("id", match_id).execute()
        except Exception as e:
            print(f"[DB] delete_payment_match error: {e}")

    # ── account_codes ──

    def query_account_codes(self, category=None):
        """계정과목 조회."""
        try:
            q = self.client.table("account_codes") \
                .select("*").order("sort_order")
            if category:
                q = q.eq("category", category)
            res = q.execute()
            return res.data or []
        except Exception as e:
            print(f"[DB] query_account_codes error: {e}")
            return []

    # ── platform_settlements ──

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

    def update_platform_settlement(self, settlement_id, update_data):
        """플랫폼 정산 수정 (매칭 상태 등)."""
        try:
            self.client.table("platform_settlements") \
                .update(update_data).eq("id", settlement_id).execute()
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

    def query_platform_fee_config(self, channel=None):
        """플랫폼 수수료 설정 조회."""
        try:
            q = self.client.table("platform_fee_config") \
                .select("*").order("channel")
            if channel:
                q = q.eq("channel", channel)
            res = q.execute()
            return res.data or []
        except Exception as e:
            print(f"[DB] query_platform_fee_config error: {e}")
            return []

    # ── 거래처 조회 헬퍼 (회계 ERP용) ──

    def query_partner_by_id(self, partner_id):
        """거래처 1건 조회."""
        try:
            res = self.client.table("business_partners") \
                .select("*").eq("id", partner_id).execute()
            return res.data[0] if res.data else None
        except Exception as e:
            print(f"[DB] query_partner_by_id error: {e}")
            return None

    def query_business_info(self):
        """사업장 정보 조회 (business_info 테이블 - 세금계산서 발행용)."""
        try:
            res = self.client.table("business_info") \
                .select("*").limit(1).execute()
            return res.data[0] if res.data else None
        except Exception:
            return None

    # ══════════════════════════════════════════
    # 은행 거래 삭제 (재동기화용)
    # ══════════════════════════════════════════

    def delete_all_bank_transactions(self, bank_account_id=None):
        """은행 거래내역 전체 삭제 (재동기화용)."""
        def _do():
            q = self.client.table("bank_transactions")
            if bank_account_id:
                q = q.delete().eq("bank_account_id", bank_account_id)
            else:
                q = q.delete().neq("id", 0)
            q.execute()
        try:
            self._retry_on_disconnect(_do)
        except Exception as e:
            print(f"[DB] delete_all_bank_transactions error: {e}")

    def delete_bank_account(self, account_id):
        """은행 계좌 삭제."""
        try:
            self.client.table("bank_accounts") \
                .delete().eq("id", account_id).execute()
        except Exception as e:
            print(f"[DB] delete_bank_account error: {e}")
            raise

    # ══════════════════════════════════════════
    # 세금계산서 중복 확인
    # ══════════════════════════════════════════

    def check_tax_invoice_exists(self, invoice_number=None, mgt_key=None):
        """세금계산서 중복 확인 (국세청 승인번호 또는 관리번호).
        Returns: 기존 레코드 id or None
        """
        def _do():
            if invoice_number:
                res = self.client.table("tax_invoices") \
                    .select("id") \
                    .eq("invoice_number", invoice_number) \
                    .limit(1).execute()
                if res.data:
                    return res.data[0]['id']
            if mgt_key:
                res = self.client.table("tax_invoices") \
                    .select("id") \
                    .eq("mgt_key", mgt_key) \
                    .limit(1).execute()
                if res.data:
                    return res.data[0]['id']
            return None
        try:
            return self._retry_on_disconnect(_do)
        except Exception:
            return None

    # ══════════════════════════════════════════
    # 카드 이용내역 (card_transactions)
    # ══════════════════════════════════════════

    def query_card_transactions(self, date_from=None, date_to=None,
                                 bank_account_id=None, category=None,
                                 search=None):
        """카드 이용내역 목록 조회."""
        def _do():
            q = self.client.table("card_transactions") \
                .select("*, bank_accounts(bank_name, account_number)") \
                .order("approval_date", desc=True) \
                .order("approval_time", desc=True)
            if date_from:
                q = q.gte("approval_date", date_from)
            if date_to:
                q = q.lte("approval_date", date_to)
            if bank_account_id:
                q = q.eq("bank_account_id", bank_account_id)
            if category and category != '전체':
                q = q.eq("category", category)
            if search:
                q = q.ilike("merchant_name", f"%{search}%")
            res = q.execute()
            return res.data or []
        try:
            return self._retry_on_disconnect(_do)
        except Exception as e:
            print(f"[DB] query_card_transactions error: {e}")
            return []

    def check_card_transaction_exists(self, approval_date, approval_no, amount):
        """카드 거래 중복 확인 (승인번호 기준)."""
        def _do():
            if not approval_no:
                return False
            res = self.client.table("card_transactions") \
                .select("id") \
                .eq("approval_date", approval_date) \
                .eq("approval_no", approval_no) \
                .eq("amount", amount) \
                .limit(1).execute()
            return len(res.data or []) > 0
        try:
            return self._retry_on_disconnect(_do)
        except Exception:
            return False

    def insert_card_transaction(self, payload):
        """카드 이용내역 1건 등록."""
        def _do():
            self.client.table("card_transactions").insert(payload).execute()
        self._retry_on_disconnect(_do)

    def update_card_transaction(self, tx_id, update_data):
        """카드 이용내역 수정 (카테고리 분류 등)."""
        def _do():
            self.client.table("card_transactions") \
                .update(update_data).eq("id", tx_id).execute()
        try:
            self._retry_on_disconnect(_do)
        except Exception as e:
            print(f"[DB] update_card_transaction error: {e}")

    def delete_all_card_transactions(self, bank_account_id=None):
        """카드 이용내역 전체 삭제."""
        try:
            q = self.client.table("card_transactions")
            if bank_account_id:
                q = q.delete().eq("bank_account_id", bank_account_id)
            else:
                q = q.delete().neq("id", 0)
            q.execute()
        except Exception as e:
            print(f"[DB] delete_all_card_transactions error: {e}")

    # ══════════════════════════════════════════
    # CODEF 연결 정보
    # ══════════════════════════════════════════

    def query_codef_connections(self):
        """CODEF 연결 정보 목록 조회."""
        try:
            res = self.client.table("codef_connections") \
                .select("*").order("created_at", desc=True).execute()
            return res.data or []
        except Exception as e:
            print(f"[DB] query_codef_connections error: {e}")
            return []

    def insert_codef_connection(self, payload):
        """CODEF 연결 정보 등록."""
        try:
            self.client.table("codef_connections").insert(payload).execute()
        except Exception as e:
            print(f"[DB] insert_codef_connection error: {e}")
            raise

    # ══════════════════════════════════════════
    # journal_entries / journal_lines (전표 시스템)
    # ══════════════════════════════════════════

    def insert_journal_entry(self, payload):
        """전표 헤더 등록. Returns: entry_id (int) or None."""
        try:
            res = self.client.table("journal_entries").insert(payload).execute()
            if res.data:
                return res.data[0]['id']
            return None
        except Exception as e:
            print(f"[DB] insert_journal_entry error: {e}")
            return None

    def insert_journal_line(self, payload):
        """전표 라인 1건 등록."""
        try:
            self.client.table("journal_lines").insert(payload).execute()
        except Exception as e:
            print(f"[DB] insert_journal_line error: {e}")

    def query_journal_entries(self, date_from=None, date_to=None,
                              journal_type=None, status=None,
                              ref_type=None, ref_id=None):
        """전표 목록 조회."""
        try:
            q = self.client.table("journal_entries") \
                .select("*").order("journal_date", desc=True) \
                .order("id", desc=True)
            if date_from:
                q = q.gte("journal_date", date_from)
            if date_to:
                q = q.lte("journal_date", date_to)
            if journal_type:
                q = q.eq("journal_type", journal_type)
            if status:
                q = q.eq("status", status)
            if ref_type:
                q = q.eq("ref_type", ref_type)
            if ref_id is not None:
                q = q.eq("ref_id", ref_id)
            res = q.limit(500).execute()
            return res.data or []
        except Exception as e:
            print(f"[DB] query_journal_entries error: {e}")
            return []

    def query_journal_entry_by_id(self, entry_id):
        """전표 1건 조회."""
        try:
            res = self.client.table("journal_entries") \
                .select("*").eq("id", entry_id).execute()
            return res.data[0] if res.data else None
        except Exception as e:
            print(f"[DB] query_journal_entry_by_id error: {e}")
            return None

    def update_journal_entry(self, entry_id, update_data):
        """전표 수정 (status 변경 등)."""
        try:
            self.client.table("journal_entries") \
                .update(update_data).eq("id", entry_id).execute()
        except Exception as e:
            print(f"[DB] update_journal_entry error: {e}")

    def query_journal_lines_by_entry(self, entry_id):
        """전표 라인 조회."""
        try:
            res = self.client.table("journal_lines") \
                .select("*").eq("journal_entry_id", entry_id) \
                .order("line_no").execute()
            return res.data or []
        except Exception as e:
            print(f"[DB] query_journal_lines_by_entry error: {e}")
            return []

    # ── event_account_mapping ──

    def query_event_account_mapping(self, event_type):
        """이벤트→계정 매핑 1건 조회."""
        try:
            res = self.client.table("event_account_mapping") \
                .select("*").eq("event_type", event_type) \
                .eq("is_active", True).execute()
            return res.data[0] if res.data else None
        except Exception as e:
            print(f"[DB] query_event_account_mapping error: {e}")
            return None

    def query_all_event_account_mappings(self):
        """이벤트→계정 매핑 전체 조회."""
        try:
            res = self.client.table("event_account_mapping") \
                .select("*").order("event_type").execute()
            return res.data or []
        except Exception as e:
            print(f"[DB] query_all_event_account_mappings error: {e}")
            return []

    # ══════════════════════════════════════════
    # 마켓플레이스 API 연동
    # ══════════════════════════════════════════

    # ── marketplace_api_config ──

    def query_marketplace_api_configs(self, channel=None):
        """마켓플레이스 API 설정 조회."""
        try:
            q = self.client.table("marketplace_api_config").select("*")
            if channel:
                q = q.eq("channel", channel)
            res = q.execute()
            return res.data or []
        except Exception as e:
            print(f"[DB] query_marketplace_api_configs error: {e}")
            return []

    def upsert_marketplace_api_config(self, payload):
        """마켓플레이스 API 설정 upsert."""
        try:
            from datetime import datetime, timezone
            payload['updated_at'] = datetime.now(timezone.utc).isoformat()
            self.client.table("marketplace_api_config").upsert(
                payload, on_conflict="channel"
            ).execute()
        except Exception as e:
            print(f"[DB] upsert_marketplace_api_config error: {e}")

    # ── api_sync_log ──

    def insert_api_sync_log(self, payload):
        """API 동기화 로그 생성. Returns: 생성된 row (id 포함)."""
        try:
            res = self.client.table("api_sync_log").insert(payload).execute()
            return res.data[0] if res.data else None
        except Exception as e:
            print(f"[DB] insert_api_sync_log error: {e}")
            return None

    def update_api_sync_log(self, log_id, update_data):
        """API 동기화 로그 업데이트."""
        try:
            self.client.table("api_sync_log") \
                .update(update_data).eq("id", log_id).execute()
        except Exception as e:
            print(f"[DB] update_api_sync_log error: {e}")

    def query_api_sync_logs(self, channel=None, limit=50):
        """API 동기화 로그 조회."""
        try:
            q = self.client.table("api_sync_log") \
                .select("*").order("started_at", desc=True).limit(limit)
            if channel:
                q = q.eq("channel", channel)
            res = q.execute()
            return res.data or []
        except Exception as e:
            print(f"[DB] query_api_sync_logs error: {e}")
            return []

    # ── work_logs (작업 이력) ──

    def insert_work_log(self, payload):
        """작업 이력 기록. Returns: 생성된 row (id 포함) or None."""
        import json as _json
        try:
            for key in ('meta',):
                if key in payload and payload[key] is not None:
                    if isinstance(payload[key], (dict, list)):
                        payload[key] = _json.dumps(payload[key], ensure_ascii=False)
            res = self.client.table("work_logs").insert(payload).execute()
            return res.data[0] if res.data else None
        except Exception as e:
            print(f"[DB] insert_work_log error: {e}")
            return None

    def update_work_log(self, log_id, update_data):
        """작업 이력 업데이트 (완료시간, 결과 등)."""
        import json as _json
        try:
            for key in ('meta',):
                if key in update_data and update_data[key] is not None:
                    if isinstance(update_data[key], (dict, list)):
                        update_data[key] = _json.dumps(update_data[key], ensure_ascii=False)
            self.client.table("work_logs").update(update_data).eq("id", log_id).execute()
        except Exception as e:
            print(f"[DB] update_work_log error: {e}")

    def query_work_logs(self, page=1, per_page=50, action_filter=None,
                        user_filter=None, channel_filter=None,
                        category_filter=None, date_from=None, date_to=None):
        """작업 이력 페이지네이션 조회. Returns: (items, total_count)."""
        try:
            count_q = self.client.table("work_logs").select("id", count="exact")
            data_q = self.client.table("work_logs").select("*").order("created_at", desc=True)
            for q_ref in (count_q, data_q):
                if action_filter:
                    q_ref = q_ref.ilike("action", f"%{action_filter}%")
                if user_filter:
                    q_ref = q_ref.ilike("user_name", f"%{user_filter}%")
                if channel_filter:
                    q_ref = q_ref.eq("channel", channel_filter)
                if category_filter:
                    q_ref = q_ref.eq("category", category_filter)
                if date_from:
                    q_ref = q_ref.gte("created_at", date_from)
                if date_to:
                    q_ref = q_ref.lte("created_at", date_to + 'T23:59:59')
            # re-apply filters (postgrest builder is mutable)
            count_q2 = self.client.table("work_logs").select("id", count="exact")
            data_q2 = self.client.table("work_logs").select("*").order("created_at", desc=True)
            if action_filter:
                count_q2 = count_q2.ilike("action", f"%{action_filter}%")
                data_q2 = data_q2.ilike("action", f"%{action_filter}%")
            if user_filter:
                count_q2 = count_q2.ilike("user_name", f"%{user_filter}%")
                data_q2 = data_q2.ilike("user_name", f"%{user_filter}%")
            if channel_filter:
                count_q2 = count_q2.eq("channel", channel_filter)
                data_q2 = data_q2.eq("channel", channel_filter)
            if category_filter:
                count_q2 = count_q2.eq("category", category_filter)
                data_q2 = data_q2.eq("category", category_filter)
            if date_from:
                count_q2 = count_q2.gte("created_at", date_from)
                data_q2 = data_q2.gte("created_at", date_from)
            if date_to:
                count_q2 = count_q2.lte("created_at", date_to + 'T23:59:59')
                data_q2 = data_q2.lte("created_at", date_to + 'T23:59:59')

            count_res = count_q2.limit(1).execute()
            total = count_res.count if count_res.count is not None else 0

            offset = (page - 1) * per_page
            data_res = data_q2.range(offset, offset + per_page - 1).execute()
            return data_res.data or [], total
        except Exception as e:
            print(f"[DB] query_work_logs error: {e}")
            return [], 0

    # ── api_orders ──

    def upsert_api_orders_batch(self, orders):
        """API 주문 배치 upsert. Returns: {new, updated, skipped}."""
        new = 0
        updated = 0
        skipped = 0
        batch_size = 100

        # 기존 키 조회 (channel, api_order_id, api_line_id) — 날짜 범위 제한
        existing_keys = set()
        try:
            # 주문 날짜 범위 추출
            dates = [o.get('order_date', '')[:10] for o in orders if o.get('order_date')]
            date_min = min(dates) if dates else None
            date_max = max(dates) if dates else None
            channels = list({o.get('channel', '') for o in orders})

            for ch in channels:
                offset = 0
                while True:
                    q = self.client.table("api_orders") \
                        .select("channel,api_order_id,api_line_id") \
                        .eq("channel", ch)
                    if date_min:
                        q = q.gte("order_date", date_min)
                    if date_max:
                        q = q.lte("order_date", date_max)
                    rows = q.range(offset, offset + 999).execute().data
                    for r in rows:
                        existing_keys.add((r['channel'], r['api_order_id'], r.get('api_line_id', '')))
                    if len(rows) < 1000:
                        break
                    offset += 1000
        except Exception as e:
            print(f"[DB] existing keys lookup error: {e}")

        for i in range(0, len(orders), batch_size):
            batch = orders[i:i + batch_size]
            # 재시도 로직 (최대 3회)
            success = False
            for attempt in range(3):
                try:
                    self.client.table("api_orders").upsert(
                        batch, on_conflict="channel,api_order_id,api_line_id"
                    ).execute()
                    success = True
                    break
                except Exception as e:
                    print(f"[DB] upsert batch {i} attempt {attempt+1} error: {e}")
                    if attempt < 2:
                        import time
                        time.sleep(1)

            if success:
                for o in batch:
                    key = (o.get('channel', ''), o.get('api_order_id', ''), o.get('api_line_id', ''))
                    if key in existing_keys:
                        updated += 1
                    else:
                        new += 1
            else:
                print(f"[DB] upsert batch {i} failed after 3 attempts, {len(batch)} skipped")
                skipped += len(batch)

        return {'new': new, 'updated': updated, 'skipped': skipped}

    def query_api_orders(self, channel=None, date_from=None, date_to=None,
                         match_status=None, limit=50000, columns=None):
        """API 주문 조회 (페이지네이션으로 Supabase 1000행 제한 우회).

        Args:
            columns: 조회할 컬럼 목록 (None이면 전체). 예: "channel,order_date,total_amount"
        """
        all_rows = []
        page_size = 1000
        offset = 0
        max_retries = 3
        select_cols = columns or "*"

        while offset < limit:
            rows = None
            for attempt in range(max_retries):
                try:
                    q = self.client.table("api_orders") \
                        .select(select_cols).order("order_date", desc=True) \
                        .range(offset, offset + page_size - 1)
                    if channel:
                        q = q.eq("channel", channel)
                    if date_from:
                        q = q.gte("order_date", date_from)
                    if date_to:
                        q = q.lte("order_date", date_to)
                    if match_status:
                        q = q.eq("match_status", match_status)
                    res = q.execute()
                    rows = res.data or []
                    break  # 성공
                except Exception as e:
                    print(f"[DB] query_api_orders page {offset//page_size} "
                          f"attempt {attempt+1}/{max_retries} error: {e}")
                    if self._is_connection_error(e) and attempt < max_retries - 1:
                        self._reconnect()
                    elif attempt >= max_retries - 1:
                        print(f"[DB] query_api_orders 페이지 {offset//page_size} "
                              f"최종 실패, 현재까지 {len(all_rows)}건 반환")

            if rows is None:
                break  # 재시도 모두 실패
            all_rows.extend(rows)
            if len(rows) < page_size:
                break
            offset += page_size

        return all_rows

    def update_api_order_match(self, api_order_id, match_data):
        """API 주문 매칭 결과 업데이트."""
        try:
            self.client.table("api_orders") \
                .update(match_data).eq("id", api_order_id).execute()
        except Exception as e:
            print(f"[DB] update_api_order_match error: {e}")

    def update_api_order_fee(self, channel, api_order_id, api_line_id, fee_data):
        """API 주문의 수수료/정산 데이터 업데이트 (revenue-history 연동).

        Args:
            channel: 채널명
            api_order_id: 주문번호
            api_line_id: 상품주문번호 (vendorItemId)
            fee_data: {commission, settlement_amount, fee_detail}
        """
        try:
            self.client.table("api_orders") \
                .update(fee_data) \
                .eq("channel", channel) \
                .eq("api_order_id", api_order_id) \
                .eq("api_line_id", api_line_id) \
                .execute()
        except Exception as e:
            print(f"[DB] update_api_order_fee error: {e}")

    # ── api_settlements ──

    def upsert_api_settlements_batch(self, settlements):
        """API 정산 배치 upsert."""
        batch_size = 50
        for i in range(0, len(settlements), batch_size):
            batch = settlements[i:i + batch_size]
            try:
                self.client.table("api_settlements").upsert(
                    batch, on_conflict="channel,settlement_date,settlement_id"
                ).execute()
            except Exception as e:
                print(f"[DB] upsert_api_settlements_batch error: {e}")

    def query_api_settlements(self, channel=None, date_from=None, date_to=None,
                              limit=1000):
        """API 정산 조회."""
        for attempt in range(3):
            try:
                q = self.client.table("api_settlements") \
                    .select("*").order("settlement_date", desc=True).limit(limit)
                if channel:
                    q = q.eq("channel", channel)
                if date_from:
                    q = q.gte("settlement_date", date_from)
                if date_to:
                    q = q.lte("settlement_date", date_to)
                res = q.execute()
                return res.data or []
            except Exception as e:
                print(f"[DB] query_api_settlements attempt {attempt+1}/3 error: {e}")
                if self._is_connection_error(e) and attempt < 2:
                    self._reconnect()
        return []
