"""
db/shipping_repo.py — 송장/배송 관련 DB Repository.

db_supabase.py에서 분리 (2026-03-23, AI 코드리뷰 기반).
배송 상태, 송장번호 매칭, 마켓 push 대기, 배송추적 관련 메서드.
"""
from .base import BaseRepo


class ShippingRepo(BaseRepo):
    """송장/배송 DB Repository."""

    # ── 송장번호 업데이트 ──

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

    # ── shipping_status 업데이트 ──

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

    # ── delivery_status (배송추적) ──

    def bulk_update_delivery_status(self, updates):
        """배송 상태 일괄 갱신.

        Args:
            updates: [{channel, order_no, delivery_status, delivery_status_raw,
                       delivery_status_updated_at}]
        Returns:
            int: 업데이트 건수
        """
        count = 0
        for u in updates:
            try:
                self.client.table("order_shipping").update({
                    "delivery_status": u['delivery_status'],
                    "delivery_status_raw": u.get('delivery_status_raw', ''),
                    "delivery_status_updated_at": u.get('delivery_status_updated_at'),
                }).eq("channel", u['channel']).eq("order_no", u['order_no']).execute()
                count += 1
            except Exception as e:
                print(f"[DB] bulk_update_delivery_status error: {u} → {e}")
        return count

    def query_delivery_status_summary(self, channel=None):
        """배송 상태별 건수 집계.

        Returns:
            {channel: {status: count, ...}, ...}
        """
        try:
            q = self.client.table("order_shipping") \
                .select("channel, delivery_status, shipping_status")
            q = q.or_("shipping_status.eq.대기,shipping_status.eq.발송")
            if channel:
                q = q.eq("channel", channel)
            res = q.limit(10000).execute()

            summary = {}
            for r in (res.data or []):
                ch = r['channel']
                ds = r.get('delivery_status') or r.get('shipping_status') or '대기'
                if ch not in summary:
                    summary[ch] = {}
                summary[ch][ds] = summary[ch].get(ds, 0) + 1
            return summary
        except Exception as e:
            print(f"[DB] query_delivery_status_summary error: {e}")
            return {}

    # ── push 대기 조회 ──

    def query_pending_invoice_push(self, channel=None, date_from=None, date_to=None):
        """송장 push 대기건 조회: order_shipping (invoice_no 있음 + shipping_status='대기')
        + api_orders (api_order_id, api_line_id, raw_data) 매핑.

        Returns:
            [{channel, order_no, invoice_no, courier,
              api_order_id, api_line_id, raw_data}, ...]
        """
        try:
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

            # api_orders에서 매핑 (api_order_id + api_line_id 이중 조회)
            order_nos = list(set(s['order_no'] for s in ships))
            api_map = {}
            for chunk_start in range(0, len(order_nos), 100):
                chunk = order_nos[chunk_start:chunk_start + 100]

                # api_order_id로 조회 (쿠팡/자사몰)
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

                # api_line_id로 조회 (네이버)
                unmapped = [n for n in chunk if not any(
                    (s['channel'], n) in api_map for s in ships if s['order_no'] == n)]
                if unmapped:
                    aq2 = self.client.table("api_orders") \
                        .select("channel,api_order_id,api_line_id,raw_data") \
                        .in_("api_line_id", unmapped)
                    if channel:
                        aq2 = aq2.eq("channel", channel)
                    api_res2 = aq2.execute()
                    for a in (api_res2.data or []):
                        key = (a['channel'], a['api_line_id'])
                        if key not in api_map:
                            api_map[key] = []
                        api_map[key].append(a)

            # 조인 결과
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

    # ── 배송추적 대상 ──

    def query_shipped_orders_for_tracking(self, channel=None, limit=200):
        """배송 추적 대상 조회: 발송완료 but 배송완료/구매확정 아닌 건.

        Returns:
            [{channel, order_no, api_order_id, delivery_status, shipping_status}]
        """
        try:
            q = self.client.table("order_shipping") \
                .select("channel,order_no,shipping_status,delivery_status")
            q = q.eq("shipping_status", "발송")
            if channel:
                q = q.eq("channel", channel)
            q = q.or_("delivery_status.is.null,delivery_status.not.in.(배송완료,구매확정,취소,반품완료)")
            ship_res = q.order("created_at", desc=True).limit(limit).execute()
            ships = ship_res.data or []

            if not ships:
                return []

            # api_orders에서 api_order_id 매핑 (이중 조회)
            order_nos = list(set(s['order_no'] for s in ships))
            api_map = {}
            for chunk_start in range(0, len(order_nos), 100):
                chunk = order_nos[chunk_start:chunk_start + 100]
                aq = self.client.table("api_orders") \
                    .select("channel,api_order_id,api_line_id") \
                    .in_("api_order_id", chunk)
                if channel:
                    aq = aq.eq("channel", channel)
                api_res = aq.execute()
                for a in (api_res.data or []):
                    api_map[(a['channel'], a['api_order_id'])] = a

                unmapped = [n for n in chunk if not any(
                    (s['channel'], n) in api_map for s in ships if s['order_no'] == n)]
                if unmapped:
                    aq2 = self.client.table("api_orders") \
                        .select("channel,api_order_id,api_line_id") \
                        .in_("api_line_id", unmapped)
                    if channel:
                        aq2 = aq2.eq("channel", channel)
                    api_res2 = aq2.execute()
                    for a in (api_res2.data or []):
                        api_map[(a['channel'], a['api_line_id'])] = a

            result = []
            for s in ships:
                ch = s['channel']
                ono = s['order_no']
                api_row = api_map.get((ch, ono))
                api_oid = ''
                if api_row:
                    api_oid = api_row.get('api_line_id') or api_row.get('api_order_id', '')
                result.append({
                    'channel': ch,
                    'order_no': ono,
                    'shipping_status': s.get('shipping_status', ''),
                    'delivery_status': s.get('delivery_status'),
                    'api_order_id': api_oid,
                })
            return result
        except Exception as e:
            print(f"[DB] query_shipped_orders_for_tracking error: {e}")
            return []
