"""
db/product_repo.py — 상품/옵션/마스터/BOM DB Repository.

db_supabase.py에서 분리 (2026-03-23).
메서드 34개.
"""
from .base import BaseRepo


class ProductRepo(BaseRepo):
    """상품/옵션/마스터/BOM DB Repository."""

    def _normalize_product_names(payload_list):
        """품목명 공백 정규화 — '(수)건해삼채 200g' → '(수)건해삼채200g'."""
        for row in payload_list:
            pn = row.get('product_name', '')
            if pn:
                row['product_name'] = str(pn).replace(' ', '').strip()
        return payload_list


    def query_filter_options(self):
        def builder(table):
            return self.client.table(table).select("location,category") \
                .eq("status", "active").order("id")
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
                lambda t: self.client.table(t).select("product_name,category")
                    .eq("status", "active").order("id", desc=True))
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
            return self.client.table(table).select("product_name,qty,unit") \
                .eq("status", "active").order("id")
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
                .eq("status", "active").eq("product_name", product_name).limit(1).execute()
            if res.data:
                return res.data[0].get('unit') or ''
            return None
        except Exception:
            return None


    def sync_master_table(self, table_name, payload_list, batch_size=500):
        self.client.table(table_name).update({"is_deleted": True}).neq("id", 0).execute()
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
        except Exception:
            return -1


    def query_price_table(self, use_cache=True):
        """가격표(master_prices) 전체 조회 → aggregator.price_map 호환 dict 반환. (캐시 적용)"""
        import unicodedata
        now = time.time()
        if use_cache and self._price_cache['data'] is not None and (now - self._price_cache['ts']) < self._price_cache['ttl']:
            return self._price_cache['data']

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

        self._price_cache['data'] = price_map
        self._price_cache['ts'] = now
        return price_map

    # --- product_costs (품목별 단가: 매입/생산 구분) ---


    def query_product_costs(self):
        """product_costs 전체 조회 → {product_name: {cost_price, unit, memo, cost_type, weight, weight_unit}} dict."""
        try:
            rows = self._paginate_query("product_costs",
                lambda t: self.client.table(t).select("*").or_("is_deleted.is.null,is_deleted.eq.false").order("product_name"))
            return {r['product_name']: r for r in rows}
        except Exception:
            return {}


    def upsert_product_cost(self, product_name, cost_price, unit='', memo='',
                            weight=0, weight_unit='g', cost_type='매입',
                            material_type='원료',
                            purchase_unit='', standard_unit='',
                            conversion_ratio=1, food_type='',
                            category='', storage_method=''):
        """품목 단가 1건 등록/수정 (upsert).
        cost_type: '매입' = 원재료 매입단가, '생산' = 완제품 생산단가
        material_type: '원료', '부재료', '반제품', '완제품', '포장재'
        purchase_unit: 매입단위 (박스, 포대, kg 등)
        standard_unit: 사용단위 (g, 개, kg 등)
        conversion_ratio: 1 매입단위 = X 사용단위
        food_type: '농산물', '수산물', '축산물', '' (미지정)
        category: '완제품', '반제품', '원료' 등 (재고 분류)
        storage_method: '냉동', '냉장', '실온', '' (미지정)
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
            'category': category or '',
            'storage_method': storage_method or '',
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
                'category': item.get('category', '') or '',
                'storage_method': item.get('storage_method', '') or '',
                'updated_at': now,
            })
        if not payload:
            return
        for i in range(0, len(payload), 500):
            self.client.table("product_costs").upsert(
                payload[i:i + 500], on_conflict="product_name"
            ).execute()


    def delete_product_cost(self, product_name, biz_id=None):
        """품목 매입단가 1건 소프트 삭제."""
        q = self.client.table("product_costs").update(
            {"is_deleted": True}
        ).eq("product_name", product_name)
        self._with_biz(q, biz_id).execute()

    # --- product_cost_history (매입단가 이력) ---


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
        self._option_cache['data'] = None
        self._option_cache['data_list'] = None
        self._option_cache['ts'] = 0


    def query_option_master(self, use_cache=True):
        """옵션마스터 전체 조회 (sort_order 정렬, 메모리 캐시 적용)."""
        now = time.time()
        if use_cache and self._option_cache['data'] is not None and (now - self._option_cache['ts']) < self._option_cache['ttl']:
            return self._option_cache['data']

        def builder(table):
            return self.client.table(table).select("*").or_("is_deleted.is.null,is_deleted.eq.false").order("sort_order").order("id")
        data = self._paginate_query("option_master", builder)

        # 캐시 갱신
        self._option_cache['data'] = data
        self._option_cache['data_list'] = None  # list 캐시도 다시 생성
        self._option_cache['ts'] = now
        return data


    def query_option_master_as_list(self, use_cache=True):
        """옵션마스터를 OrderProcessor 호환 dict list로 반환 (캐시).

        Args:
            use_cache: False면 캐시 완전 우회, DB 직접 조회.
        """
        # list 캐시가 유효하면 바로 반환
        now = time.time()
        if use_cache and self._option_cache['data_list'] is not None and (now - self._option_cache['ts']) < self._option_cache['ttl']:
            return self._option_cache['data_list']

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
        self._option_cache['data_list'] = result
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
        payload['match_key'] = normalize_match_key(orig)
        self.client.table("option_master").upsert(
            payload, on_conflict="match_key"
        ).execute()
        self._invalidate_option_cache()


    def insert_option_master_batch(self, payload_list, batch_size=500):
        """옵션마스터 일괄 등록 (중복 시 upsert)."""
        for row in payload_list:
            orig = row.get('original_name', '')
            row['match_key'] = normalize_match_key(orig)
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


    def update_option_master(self, option_id, update_data, biz_id=None):
        """옵션마스터 1건 수정."""
        if 'original_name' in update_data:
            update_data['match_key'] = normalize_match_key(update_data['original_name'])
        q = self.client.table("option_master").update(update_data).eq("id", option_id)
        self._with_biz(q, biz_id).execute()
        self._invalidate_option_cache()


    def delete_option_master(self, option_id, biz_id=None):
        """옵션마스터 1건 소프트 삭제."""
        q = self.client.table("option_master").update(
            {"is_deleted": True}
        ).eq("id", option_id)
        self._with_biz(q, biz_id).execute()
        self._invalidate_option_cache()


    def count_option_master(self):
        """옵션마스터 건수 (캐시 활용)."""
        cached = self._option_cache.get('data')
        if cached is not None and (time.time() - self._option_cache['ts']) < self._option_cache['ttl']:
            return len(cached)
        try:
            res = self.client.table("option_master").select("id", count="exact").or_("is_deleted.is.null,is_deleted.eq.false").limit(1).execute()
            return res.count if res.count is not None else len(res.data)
        except Exception:
            return -1


    def sync_option_master(self, payload_list, batch_size=500, biz_id=None):
        """옵션마스터 전체 교체 (엑셀 내 중복 match_key 자동 제거)."""
        # match_key 생성 + 중복 제거 (뒤에 나오는 행이 우선)
        seen = {}
        for row in payload_list:
            orig = row.get('original_name', '')
            row['match_key'] = normalize_match_key(orig)
            seen[row['match_key']] = row
        deduped = list(seen.values())

        # 기존 데이터 전체 소프트 삭제 후 삽입
        q = self.client.table("option_master").update({"is_deleted": True}).neq("id", 0)
        self._with_biz(q, biz_id).execute()
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
                .is_("last_matched_at", "null").or_("is_deleted.is.null,is_deleted.eq.false").execute()
            stale.extend(res1.data or [])
            # last_matched_at이 cutoff보다 오래된 항목
            res2 = self.client.table("option_master").select("*") \
                .lt("last_matched_at", cutoff).or_("is_deleted.is.null,is_deleted.eq.false").execute()
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


    def delete_stale_options(self, days=30, biz_id=None):
        """N일 이상 매칭되지 않은 옵션 일괄 삭제."""
        stale = self.query_stale_options(days)
        if not stale:
            return 0
        ids = [r['id'] for r in stale]
        deleted = 0
        for i in range(0, len(ids), 50):
            chunk = ids[i:i + 50]
            try:
                q = self.client.table("option_master").update(
                    {"is_deleted": True}
                ).in_("id", chunk)
                self._with_biz(q, biz_id).execute()
                deleted += len(chunk)
            except Exception:
                for rid in chunk:
                    try:
                        q2 = self.client.table("option_master").update(
                            {"is_deleted": True}
                        ).eq("id", rid)
                        self._with_biz(q2, biz_id).execute()
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


    def query_promotions(self, product_name=None, category=None,
                         date_from=None, date_to=None, active_only=False):
        """행사 목록 조회."""
        try:
            q = self.client.table("promotions").select("*").or_("is_deleted.is.null,is_deleted.eq.false")
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
                .or_("is_deleted.is.null,is_deleted.eq.false") \
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


    def update_promotion(self, promo_id, update_data, biz_id=None):
        """행사 1건 수정."""
        from datetime import datetime, timezone
        update_data['updated_at'] = datetime.now(timezone.utc).isoformat()
        q = self.client.table("promotions").update(update_data).eq("id", promo_id)
        self._with_biz(q, biz_id).execute()


    def delete_promotion(self, promo_id, biz_id=None):
        """행사 1건 소프트 삭제."""
        q = self.client.table("promotions").update(
            {"is_deleted": True}
        ).eq("id", promo_id)
        self._with_biz(q, biz_id).execute()

    # ================================================================
    # 쿠폰 관리 (coupons)
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


