"""
db/finance_repo.py — 매출/비용/은행/카드/세금계산서 DB Repository.

db_supabase.py에서 분리 (2026-03-23).
메서드 46개.
"""
from .base import BaseRepo


class FinanceRepo(BaseRepo):
    """매출/비용/은행/카드/세금계산서 DB Repository."""

    def upsert_revenue(self, payload_list):
        if not payload_list:
            return
        from services.product_name import canonical
        # channel 필드 없는 레코드에 기본값 보장 + product_name canonical 통일
        for p in payload_list:
            if 'channel' not in p:
                p['channel'] = ''
            if p.get('product_name'):
                p['product_name'] = canonical(p['product_name'])
        self.client.table("daily_revenue").upsert(
            payload_list, on_conflict="revenue_date,product_name,category,channel"
        ).execute()

        # ── 로켓/거래처매출 gap-filler: stock_ledger SALES_OUT 자동 생성 ──
        # 같은 (date, product, warehouse) 에 SALES_OUT 이 이미 있으면 스킵.
        # event_uid 'DR_AUTO:{...}' 로 idempotent. 호출자가 별도로 stock_ledger 를
        # 쓰는 경우 (api_rocket_manual 등) 그 기록이 있으면 여기선 자동 생성하지 않음.
        try:
            self._auto_stock_from_revenue(payload_list)
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(
                f'[upsert_revenue] 자동 stock_ledger 생성 실패: {e}')

    def _auto_stock_from_revenue(self, payload_list):
        """daily_revenue 로켓/거래처매출 행에 대해 stock_ledger SALES_OUT 누락분을 채움.

        목적: daily_revenue 에만 기록되고 stock_ledger 에 반영 안 된 '유령 재고' 방지.
        안전성:
          - 같은 (date, canonical_product, warehouse) 에 SALES_OUT 존재 시 스킵
          - event_uid='DR_AUTO:...' 로 중복 insert 방지
          - 이미 manual 경로가 stock_ledger 를 썼다면 그 기록이 감지되어 스킵
        """
        from services.product_name import canonical
        targets = []
        for p in payload_list:
            cat = p.get('category') or ''
            if cat not in ('로켓', '거래처매출'):
                continue
            qty = int(p.get('qty') or 0)
            if qty <= 0:
                continue
            date = p.get('revenue_date') or ''
            pn = canonical(p.get('product_name') or '')
            wh = (p.get('warehouse') or '').strip() or '넥스원'
            if not date or not pn:
                continue
            targets.append({'date': date, 'pn': pn, 'wh': wh, 'qty': qty, 'cat': cat})
        if not targets:
            return

        # 기존 SALES_OUT 확인 — (date, pn, wh) 조합별 1회
        checked = set()
        to_insert = []
        for t in targets:
            key = (t['date'], t['pn'], t['wh'])
            if key in checked:
                continue
            checked.add(key)
            try:
                existing = self.client.table('stock_ledger') \
                    .select('id') \
                    .eq('transaction_date', t['date']) \
                    .eq('product_name', t['pn']) \
                    .eq('location', t['wh']) \
                    .eq('type', 'SALES_OUT') \
                    .limit(1) \
                    .execute()
                if existing.data:
                    continue  # 이미 있음 — 호출자가 써둠
            except Exception:
                continue

            # 같은 키의 payload 합계
            total_qty = sum(
                x['qty'] for x in targets
                if x['date'] == t['date'] and x['pn'] == t['pn'] and x['wh'] == t['wh']
            )
            to_insert.append({
                'transaction_date': t['date'],
                'type': 'SALES_OUT',
                'product_name': t['pn'],
                'qty': -total_qty,
                'location': t['wh'],
                'unit': '개',
                'memo': f"daily_revenue 자동연동 ({t['cat']})",
                'event_uid': f"DR_AUTO:{t['date']}:{t['wh']}:{t['pn']}:{t['cat']}",
            })

        if to_insert:
            try:
                # insert_stock_ledger 경유 → canonical 자동 적용
                self.insert_stock_ledger(to_insert)
                import logging
                logging.getLogger(__name__).info(
                    f'[upsert_revenue] daily_revenue→stock_ledger 자동연동: {len(to_insert)}건'
                )
            except Exception as e:
                import logging
                logging.getLogger(__name__).warning(
                    f'[upsert_revenue] 자동연동 insert 실패: {e}'
                )


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
                        "source": "주문",
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
            ).or_("is_deleted.is.null,is_deleted.eq.false").order("revenue_date", desc=True)
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
                        "source": "정산",
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
                        "source": "정산",
                    }
                agg[key]["qty"] += (r.get("qty") or 0)
                agg[key]["revenue"] += (r.get("revenue") or 0)
                agg[key]["settlement"] += (r.get("settlement") or 0)
                agg[key]["commission"] += (r.get("commission") or 0)

        result = sorted(agg.values(), key=lambda x: x["revenue_date"], reverse=True)
        return result


    def delete_revenue_all(self, biz_id=None):
        q = self.client.table("daily_revenue").update(
            {"is_deleted": True}
        ).neq("id", 0)
        res = self._with_biz(q, biz_id).execute()
        return len(res.data) if res.data else 0


    def delete_revenue_by_date(self, date_from=None, date_to=None, exclude_categories=None, biz_id=None):
        query = self.client.table("daily_revenue").update({"is_deleted": True})
        if date_from:
            query = query.gte("revenue_date", date_from)
        if date_to:
            query = query.lte("revenue_date", date_to)
        if exclude_categories:
            for cat in exclude_categories:
                query = query.neq("category", cat)
        res = self._with_biz(query, biz_id).execute()
        return len(res.data) if res.data else 0


    def delete_revenue_by_id(self, revenue_id, biz_id=None):
        """daily_revenue 1건 소프트 삭제."""
        q = self.client.table("daily_revenue").update(
            {"is_deleted": True}
        ).eq("id", revenue_id)
        self._with_biz(q, biz_id).execute()

    # --- daily_closing (일일마감) ---


    def delete_revenue_specific(self, revenue_date, product_name, category, biz_id=None):
        """daily_revenue에서 특정 조건의 레코드 소프트 삭제."""
        # 먼저 대상 조회
        q = (self.client.table("daily_revenue").select("id")
             .eq("revenue_date", revenue_date)
             .eq("product_name", product_name)
             .eq("category", category))
        res = self._with_biz(q, biz_id).execute()
        if not res.data:
            return 0
        count = 0
        for row in res.data:
            dq = self.client.table("daily_revenue").update(
                {"is_deleted": True}
            ).eq("id", row["id"])
            self._with_biz(dq, biz_id).execute()
            count += 1
        return count

    # --- 발주서 이력 ---


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
                    ).or_("is_deleted.is.null,is_deleted.eq.false") \
                     .gte('revenue_date', date_from) \
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


    def query_expenses(self, month=None, category=None,
                        date_from=None, date_to=None):
        """비용 목록 조회. month 또는 date_from/date_to 기간 필터, category 필터."""
        try:
            q = self.client.table("expenses").select("*")
            q = q.or_("is_deleted.is.null,is_deleted.eq.false")
            if date_from and date_to:
                q = q.gte("expense_date", date_from).lte("expense_date", date_to)
            elif date_from:
                q = q.gte("expense_date", date_from)
            elif date_to:
                q = q.lte("expense_date", date_to)
            elif month:
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


    def update_expense(self, expense_id, data, biz_id=None):
        """비용 1건 수정."""
        q = self.client.table("expenses").update(data).eq("id", int(expense_id))
        res = self._with_biz(q, biz_id).execute()
        return res.data[0] if res.data else None


    def delete_expense(self, expense_id, deleted_by=None, biz_id=None):
        """비용 1건 블라인드 처리 (소프트 삭제)."""
        data = {"is_deleted": True}
        if deleted_by:
            data["deleted_by"] = deleted_by
        q = self.client.table("expenses").update(data).eq("id", int(expense_id))
        self._with_biz(q, biz_id).execute()


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


    def insert_codef_connection(self, payload):
        """CODEF 연결 정보 저장 (upsert — connected_id 기준 중복 방지)."""
        try:
            self.client.table("codef_connections").upsert(
                payload, on_conflict="connected_id"
            ).execute()
        except Exception as e:
            print(f"[DB] insert_codef_connection error: {e}")
            raise


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
                .select("*").or_("is_deleted.is.null,is_deleted.eq.false").order("bank_name").execute()
            return res.data or []
        except Exception as e:
            print(f"[DB] query_bank_accounts error: {e}")
            return []


    def query_bank_account_by_id(self, account_id):
        """은행 계좌 1건 조회."""
        try:
            res = self.client.table("bank_accounts") \
                .select("*").eq("id", account_id).or_("is_deleted.is.null,is_deleted.eq.false").execute()
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


    def update_bank_account(self, account_id, update_data, biz_id=None):
        """은행 계좌 수정."""
        try:
            q = self.client.table("bank_accounts").update(update_data).eq("id", account_id)
            self._with_biz(q, biz_id).execute()
        except Exception as e:
            print(f"[DB] update_bank_account error: {e}")

    # ── bank_transactions ──


    def query_bank_transactions(self, date_from=None, date_to=None,
                                 bank_account_id=None, transaction_type=None,
                                 category=None, unmatched_only=False):
        """은행 거래내역 조회 (필터)."""
        try:
            q = self.client.table("bank_transactions") \
                .select("*, bank_accounts(bank_name, account_number)") \
                .or_("is_deleted.is.null,is_deleted.eq.false") \
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
                .select("*").eq("id", tx_id).or_("is_deleted.is.null,is_deleted.eq.false").execute()
            return res.data[0] if res.data else None
        except Exception as e:
            print(f"[DB] query_bank_transaction_by_id error: {e}")
            return None


    def insert_bank_transaction(self, payload):
        """은행 거래내역 1건 등록."""
        self.client.table("bank_transactions").insert(payload).execute()


    def update_bank_transaction(self, tx_id, update_data, biz_id=None):
        """은행 거래내역 수정 (카테고리 분류 등)."""
        try:
            q = self.client.table("bank_transactions").update(update_data).eq("id", tx_id)
            self._with_biz(q, biz_id).execute()
        except Exception as e:
            print(f"[DB] update_bank_transaction error: {e}")

    # ── tax_invoices ──


    def query_tax_invoices(self, direction=None, status=None,
                            date_from=None, date_to=None,
                            partner_name=None, unmatched_only=False):
        """세금계산서 목록 조회."""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                q = self.client.table("tax_invoices") \
                    .select("*").or_("is_deleted.is.null,is_deleted.eq.false").order("write_date", desc=True)
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
                print(f"[DB] query_tax_invoices attempt {attempt+1}/{max_retries} error: {e}")
                if self._is_connection_error(e) and attempt < max_retries - 1:
                    time.sleep(1)
                    self._reconnect()
                elif attempt >= max_retries - 1:
                    return []
        return []


    def query_tax_invoice_by_id(self, invoice_id):
        """세금계산서 1건 조회."""
        try:
            res = self.client.table("tax_invoices") \
                .select("*").eq("id", invoice_id).or_("is_deleted.is.null,is_deleted.eq.false").execute()
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


    def update_tax_invoice(self, invoice_id, update_data, biz_id=None):
        """세금계산서 수정."""
        try:
            q = self.client.table("tax_invoices").update(update_data).eq("id", invoice_id)
            self._with_biz(q, biz_id).execute()
        except Exception as e:
            print(f"[DB] update_tax_invoice error: {e}")


    def delete_tax_invoice(self, invoice_id, biz_id=None):
        """세금계산서 소프트 삭제."""
        try:
            q = self.client.table("tax_invoices").update({"is_deleted": True}).eq("id", invoice_id)
            self._with_biz(q, biz_id).execute()
        except Exception as e:
            print(f"[DB] delete_tax_invoice error: {e}")

    # ── payment_matches ──


    def delete_all_bank_transactions(self, bank_account_id=None, biz_id=None):
        """은행 거래내역 전체 소프트 삭제 (재동기화용)."""
        def _do():
            q = self.client.table("bank_transactions")
            if bank_account_id:
                q = q.update({"is_deleted": True}).eq("bank_account_id", bank_account_id)
            else:
                q = q.update({"is_deleted": True}).neq("id", 0)
            self._with_biz(q, biz_id).execute()
        try:
            self._retry_on_disconnect(_do)
        except Exception as e:
            print(f"[DB] delete_all_bank_transactions error: {e}")


    def delete_bank_account(self, account_id, biz_id=None):
        """은행 계좌 소프트 삭제."""
        try:
            q = self.client.table("bank_accounts").update({"is_deleted": True}).eq("id", account_id)
            self._with_biz(q, biz_id).execute()
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
                    .or_("is_deleted.is.null,is_deleted.eq.false") \
                    .limit(1).execute()
                if res.data:
                    return res.data[0]['id']
            if mgt_key:
                res = self.client.table("tax_invoices") \
                    .select("id") \
                    .eq("mgt_key", mgt_key) \
                    .or_("is_deleted.is.null,is_deleted.eq.false") \
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
                .or_("is_deleted.is.null,is_deleted.eq.false") \
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
                .or_("is_deleted.is.null,is_deleted.eq.false") \
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


    def update_card_transaction(self, tx_id, update_data, biz_id=None):
        """카드 이용내역 수정 (카테고리 분류 등)."""
        def _do():
            q = self.client.table("card_transactions").update(update_data).eq("id", tx_id)
            self._with_biz(q, biz_id).execute()
        try:
            self._retry_on_disconnect(_do)
        except Exception as e:
            print(f"[DB] update_card_transaction error: {e}")


    def delete_all_card_transactions(self, bank_account_id=None, biz_id=None):
        """카드 이용내역 전체 소프트 삭제."""
        try:
            q = self.client.table("card_transactions")
            if bank_account_id:
                q = q.update({"is_deleted": True}).eq("bank_account_id", bank_account_id)
            else:
                q = q.update({"is_deleted": True}).neq("id", 0)
            self._with_biz(q, biz_id).execute()
        except Exception as e:
            print(f"[DB] delete_all_card_transactions error: {e}")

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


    def update_journal_entry(self, entry_id, update_data, biz_id=None):
        """전표 수정 (status 변경 등)."""
        try:
            q = self.client.table("journal_entries").update(update_data).eq("id", entry_id)
            self._with_biz(q, biz_id).execute()
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


