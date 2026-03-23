"""
db/orders_repo.py — 주문/import_run 관련 DB Repository.

db_supabase.py에서 분리 (2026-03-23).
메서드 13개.
"""
from .base import BaseRepo


class OrdersRepo(BaseRepo):
    """주문/import_run 관련 DB Repository."""

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


    def rollback_import_run_full(self, run_id, cancelled_by):
        """import_run 전체 롤백: 재고(stock_ledger) 복원 + 주문 취소 + run 상태 변경.

        API 주문수집 실패 시 원상복구용. 순서:
        1) 해당 run의 출고완료 주문 → stock_ledger SALES_OUT 삭제 + is_outbound_done 리셋
        2) cancel_import_run으로 미출고 주문 취소
        """
        from datetime import datetime, timezone
        try:
            # 1) 해당 run의 출고 완료 주문 조회
            res = self.client.table("order_transactions") \
                .select("id,order_no,channel,product_name,qty,outbound_date,is_outbound_done") \
                .eq("import_run_id", run_id) \
                .eq("status", "정상") \
                .eq("is_outbound_done", True).execute()
            outbound_orders = res.data or []

            stock_restored = 0
            for order in outbound_orders:
                oid = order["id"]
                # stock_ledger에서 event_uid에 order_id가 포함된 SALES_OUT 삭제
                try:
                    sl_res = self.client.table("stock_ledger") \
                        .select("id") \
                        .eq("type", "SALES_OUT") \
                        .like("event_uid", f"%{oid}%").execute()
                    sl_ids = [r["id"] for r in (sl_res.data or [])]
                    for sl_id in sl_ids:
                        # 하드 삭제 → 소프트 삭제로 변경 (데이터 보존)
                        self.client.table("stock_ledger").update(
                            {"is_deleted": True, "status": "cancelled"}
                        ).eq("id", sl_id).execute()
                        stock_restored += 1
                except Exception:
                    pass

                # is_outbound_done 리셋
                self.reset_order_outbound(oid)

            # 2) 나머지 미출고 주문 취소 + run 상태 변경
            cancel_result = self.cancel_import_run(run_id, cancelled_by)
            cancel_result['stock_ledger_deleted'] = stock_restored
            cancel_result['outbound_reset'] = len(outbound_orders)
            return cancel_result
        except Exception as e:
            print(f"[DB] rollback_import_run_full error: {e}")
            import traceback; traceback.print_exc()
            return {"error": str(e)}


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


