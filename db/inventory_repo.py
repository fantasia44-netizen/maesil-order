"""
db/inventory_repo.py — 재고/수불장 관련 DB Repository.

db_supabase.py에서 분리 (2026-03-23).
메서드 15개.
"""
from .base import BaseRepo


class InventoryRepo(BaseRepo):
    """재고/수불장 관련 DB Repository."""

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

        # event_uid 있는 것: 중복 체크 후 insert (유니크 제약 없어도 안전)
        if with_uid:
            existing_uids = set()
            try:
                uid_list = [p['event_uid'] for p in with_uid]
                res = self.client.table("stock_ledger").select("event_uid") \
                    .in_("event_uid", uid_list).execute()
                existing_uids = {r['event_uid'] for r in (res.data or [])}
            except Exception:
                pass

            new_items = [p for p in with_uid if p['event_uid'] not in existing_uids]
            skipped += len(with_uid) - len(new_items)

            if new_items:
                BATCH = 200
                for i in range(0, len(new_items), BATCH):
                    chunk = new_items[i:i + BATCH]
                    try:
                        self.client.table("stock_ledger").insert(chunk).execute()
                        inserted += len(chunk)
                    except Exception as e1:
                        print(f"[DB] stock_ledger batch insert failed: {e1}")
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


    def delete_stock_ledger_all(self, biz_id=None):
        q = self.client.table("stock_ledger").update(
            {"is_deleted": True, "status": "cancelled"}
        ).neq("id", 0)
        res = self._with_biz(q, biz_id).execute()
        return len(res.data) if res.data else 0


    def delete_stock_ledger_by(self, date_str, record_type, location=None,
                               product_names=None, biz_id=None):
        q = self.client.table("stock_ledger").update(
            {"is_deleted": True, "status": "cancelled"}
        ).eq("transaction_date", date_str).eq("type", record_type)
        if location:
            q = q.eq("location", location)
        if product_names:
            q = q.in_("product_name", list(product_names))
        res = self._with_biz(q, biz_id).execute()
        return len(res.data) if res.data else 0


    def query_stock_ledger(self, date_to, date_from=None, location=None,
                            category=None, type_list=None, order_desc=False,
                            include_blind=False):
        def builder(table):
            q = self.client.table(table).select("*")
            if not include_blind:
                q = q.eq("status", "active")
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
            return self.client.table(table).select(sel_str) \
                .eq("status", "active").eq("location", location).order("id")
        return self._paginate_query("stock_ledger", builder)


    def update_stock_ledger(self, row_id, update_data, biz_id=None):
        if self._db_cols:
            update_data = {k: v for k, v in update_data.items() if k in self._db_cols}
        q = self.client.table("stock_ledger").update(update_data).eq("id", row_id)
        self._with_biz(q, biz_id).execute()


    def delete_stock_ledger_by_id(self, row_id, biz_id=None):
        """stock_ledger 레코드 소프트 삭제 (is_deleted=True)."""
        q = self.client.table("stock_ledger").update(
            {"is_deleted": True, "status": "cancelled"}
        ).eq("id", row_id)
        self._with_biz(q, biz_id).execute()


    def delete_stock_ledger_sales_out(self, date_str, product_name, location, qty, biz_id=None):
        """특정 SALES_OUT 레코드 삭제 (거래 삭제 시 재고 복원용).

        FIFO 차감으로 생성된 여러 레코드 중, 합계가 qty와 일치하는 것들을 삭제.
        삭제되면 stock_ledger 기반 잔고가 자동 복원됨.
        """
        sq = (self.client.table("stock_ledger").select("id,qty")
              .eq("transaction_date", date_str)
              .eq("type", "SALES_OUT")
              .eq("product_name", product_name)
              .eq("location", location))
        res = self._with_biz(sq, biz_id).execute()

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
            dq = self.client.table("stock_ledger").update(
                {"is_deleted": True, "status": "cancelled"}
            ).eq("id", rid)
            self._with_biz(dq, biz_id).execute()
            deleted += 1
        return deleted

    # --- daily_revenue ---


    def soft_delete_stock_ledger(self, row_id, deleted_by=None, biz_id=None):
        """stock_ledger 소프트 삭제 (is_deleted=True). 원본 데이터 보존."""
        from datetime import datetime, timezone
        update = {
            'is_deleted': True,
            'status': 'cancelled',
            'deleted_at': datetime.now(timezone.utc).isoformat(),
        }
        if deleted_by:
            update['deleted_by'] = deleted_by
        q = self.client.table("stock_ledger").update(update).eq("id", row_id)
        self._with_biz(q, biz_id).execute()


    def blind_stock_ledger(self, row_id, blinded_by=None, biz_id=None):
        """stock_ledger 블라인드 처리 (status='cancelled'). 원본 이력 보존."""
        from datetime import datetime, timezone
        update = {
            'status': 'cancelled',
            'is_deleted': True,
            'deleted_at': datetime.now(timezone.utc).isoformat(),
        }
        if blinded_by:
            update['deleted_by'] = blinded_by
        q = self.client.table("stock_ledger").update(update).eq("id", row_id)
        self._with_biz(q, biz_id).execute()


    def replace_stock_ledger(self, old_id, new_payload, replaced_by_user=None, biz_id=None):
        """수정: 원본을 블라인드(replaced) 처리하고 새 레코드 INSERT. 양방향 링크.

        Returns:
            int: 새 레코드 ID
        """
        from datetime import datetime, timezone
        now_iso = datetime.now(timezone.utc).isoformat()

        # 1) 원본 status → 'replaced'
        q = self.client.table("stock_ledger").update({
            'status': 'replaced',
            'updated_at': now_iso,
            'updated_by': replaced_by_user,
        }).eq("id", old_id)
        self._with_biz(q, biz_id).execute()

        # 2) 새 레코드 INSERT
        new_payload['replaces'] = old_id
        new_payload['created_by'] = replaced_by_user
        new_payload['status'] = 'active'
        filtered = self._filter_payload([new_payload])
        res = self.client.table("stock_ledger").insert(filtered).execute()
        new_id = res.data[0]['id'] if res.data else None

        # 3) 원본에 replaced_by 링크
        if new_id:
            self.client.table("stock_ledger").update({
                'replaced_by': new_id,
            }).eq("id", old_id).execute()

        return new_id


    def restore_stock_ledger(self, row_id):
        """stock_ledger 블라인드/삭제 복원 (status → 'active')."""
        self.client.table("stock_ledger").update({
            'status': 'active',
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


    def query_stock_summary_by_location(self, exclude_products=None):
        """창고별 재고 품목 수 요약 (양수 재고만). 최근 180일만 조회."""
        try:
            today = today_kst()
            date_from = days_ago_kst(180)

            def builder(table):
                return self.client.table(table) \
                    .select("product_name,location,qty") \
                    .gte("transaction_date", date_from) \
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


