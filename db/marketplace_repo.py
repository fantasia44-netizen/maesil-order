"""
db/marketplace_repo.py — API설정/동기화/api_orders DB Repository.

db_supabase.py에서 분리 (2026-03-23).
메서드 14개.
"""
from .base import BaseRepo


class MarketplaceRepo(BaseRepo):
    """API설정/동기화/api_orders DB Repository."""

    def query_channel_costs(self):
        """채널별 비용 전체 조회 → {channel: {fee_rate, shipping, packaging, other_cost, memo}}."""
        try:
            rows = self._paginate_query("channel_costs",
                lambda t: self.client.table(t).select("*").or_("is_deleted.is.null,is_deleted.eq.false").order("channel"))
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


    def delete_channel_cost(self, channel, biz_id=None):
        """채널 비용 1건 소프트 삭제."""
        q = self.client.table("channel_costs").update(
            {"is_deleted": True}
        ).eq("channel", channel)
        self._with_biz(q, biz_id).execute()

    # --- business_partners ---


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


    def update_api_sync_log(self, log_id, update_data, biz_id=None):
        """API 동기화 로그 업데이트."""
        try:
            q = self.client.table("api_sync_log").update(update_data).eq("id", log_id)
            self._with_biz(q, biz_id).execute()
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


    def upsert_api_orders_batch(self, orders):
        """API 주문 배치 upsert. Returns: {new, updated, skipped}."""
        # ★ 마켓 API → api_orders 저장 choke point — canonical 통일
        from services.product_name import canonical
        for o in orders or []:
            if isinstance(o, dict) and o.get('product_name'):
                o['product_name'] = canonical(o['product_name'])
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
                        time.sleep(1 + attempt)  # backoff: 1초, 2초
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


    def update_api_order_match(self, api_order_id, match_data, biz_id=None):
        """API 주문 매칭 결과 업데이트."""
        try:
            q = self.client.table("api_orders").update(match_data).eq("id", api_order_id)
            self._with_biz(q, biz_id).execute()
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
                    time.sleep(1 + attempt)
                    self._reconnect()
        return []

