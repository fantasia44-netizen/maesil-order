"""
reconciliation_service.py -- 숫자 대조표(Reconciliation) 비즈니스 로직.
주문-출고, 출고-매출, 매출 요약, 재고 정합성 검증 함수 제공.
"""


def validate_order_outbound(db, date_from, date_to):
    """주문(출고완료) 수량 vs stock_ledger SALES_OUT 수량 대조.

    - order_transactions: is_outbound_done=True, status='정상' 주문의 qty 합계
    - stock_ledger: SALES_OUT 레코드의 abs(qty) 합계
    - 품목별 비교하여 불일치 항목 리스트 반환

    Returns:
        {
            "match": True/False,
            "order_qty": int,
            "outbound_qty": int,
            "diff_items": [
                {"product_name": str, "order_qty": int, "outbound_qty": int, "diff": int},
                ...
            ]
        }
    """
    try:
        # ── 1. order_transactions: 출고완료 주문 수량 ──
        order_rows = _paginate_all(
            db, "order_transactions",
            lambda t: db.client.table(t)
                .select("product_name,qty")
                .eq("is_outbound_done", True)
                .eq("status", "정상")
                .gte("order_date", date_from)
                .lte("order_date", date_to)
        )

        order_by_product = {}
        for r in order_rows:
            pn = (r.get("product_name") or "").strip()
            if not pn:
                continue
            order_by_product[pn] = order_by_product.get(pn, 0) + abs(int(r.get("qty") or 0))

        # ── 2. stock_ledger: SALES_OUT 수량 ──
        outbound_rows = db.query_stock_ledger(
            date_from=date_from, date_to=date_to,
            type_list=["SALES_OUT"]
        )

        outbound_by_product = {}
        for r in outbound_rows:
            pn = (r.get("product_name") or "").strip()
            if not pn:
                continue
            outbound_by_product[pn] = outbound_by_product.get(pn, 0) + abs(int(r.get("qty") or 0))

        # ── 3. 대조 ──
        all_products = sorted(set(order_by_product.keys()) | set(outbound_by_product.keys()))
        diff_items = []
        total_order = 0
        total_outbound = 0

        for pn in all_products:
            oq = order_by_product.get(pn, 0)
            sq = outbound_by_product.get(pn, 0)
            total_order += oq
            total_outbound += sq
            if oq != sq:
                diff_items.append({
                    "product_name": pn,
                    "order_qty": oq,
                    "outbound_qty": sq,
                    "diff": oq - sq,
                })

        return {
            "match": len(diff_items) == 0,
            "order_qty": total_order,
            "outbound_qty": total_outbound,
            "diff_items": diff_items,
        }
    except Exception as e:
        return {"match": False, "order_qty": 0, "outbound_qty": 0,
                "diff_items": [], "error": str(e)}


def validate_outbound_revenue(db, date_from, date_to):
    """stock_ledger SALES_OUT 수량 vs daily_revenue 수량 대조.

    - stock_ledger: SALES_OUT 품목별 abs(qty)
    - daily_revenue: 품목별 qty
    - 품목별 비교하여 불일치 항목 리스트 반환

    Returns:
        {
            "match": True/False,
            "outbound_total": int,
            "revenue_total": int,
            "diff_items": [
                {"product_name": str, "outbound_qty": int, "revenue_qty": int, "diff": int},
                ...
            ]
        }
    """
    try:
        # ── 1. stock_ledger: SALES_OUT ──
        outbound_rows = db.query_stock_ledger(
            date_from=date_from, date_to=date_to,
            type_list=["SALES_OUT"]
        )

        outbound_by_product = {}
        for r in outbound_rows:
            pn = (r.get("product_name") or "").strip()
            if not pn:
                continue
            outbound_by_product[pn] = outbound_by_product.get(pn, 0) + abs(int(r.get("qty") or 0))

        # ── 2. daily_revenue: 품목별 수량 ──
        revenue_rows = _paginate_all(
            db, "daily_revenue",
            lambda t: db.client.table(t)
                .select("product_name,qty")
                .gte("revenue_date", date_from)
                .lte("revenue_date", date_to)
        )

        revenue_by_product = {}
        for r in revenue_rows:
            pn = (r.get("product_name") or "").strip()
            if not pn:
                continue
            revenue_by_product[pn] = revenue_by_product.get(pn, 0) + abs(int(r.get("qty") or 0))

        # ── 3. 대조 ──
        all_products = sorted(set(outbound_by_product.keys()) | set(revenue_by_product.keys()))
        diff_items = []
        total_outbound = 0
        total_revenue = 0

        for pn in all_products:
            sq = outbound_by_product.get(pn, 0)
            rq = revenue_by_product.get(pn, 0)
            total_outbound += sq
            total_revenue += rq
            if sq != rq:
                diff_items.append({
                    "product_name": pn,
                    "outbound_qty": sq,
                    "revenue_qty": rq,
                    "diff": sq - rq,
                })

        return {
            "match": len(diff_items) == 0,
            "outbound_total": total_outbound,
            "revenue_total": total_revenue,
            "diff_items": diff_items,
        }
    except Exception as e:
        return {"match": False, "outbound_total": 0, "revenue_total": 0,
                "diff_items": [], "error": str(e)}


def validate_revenue_summary(db, date_str):
    """특정 날짜의 매출 요약 (daily_revenue 기준).

    Returns:
        {
            "date": "2025-03-07",
            "total_revenue": int,
            "by_channel": {"스마트스토어": int, "쿠팡": int, ...},
            "item_count": int
        }
    """
    try:
        revenue_rows = _paginate_all(
            db, "daily_revenue",
            lambda t: db.client.table(t)
                .select("product_name,category,channel,qty,revenue")
                .eq("revenue_date", date_str)
        )

        total_revenue = 0
        by_channel = {}
        product_set = set()

        for r in revenue_rows:
            pn = (r.get("product_name") or "").strip()
            if pn:
                product_set.add(pn)
            rev = float(r.get("revenue") or 0)
            total_revenue += rev
            ch = (r.get("channel") or r.get("category") or "기타").strip()
            by_channel[ch] = by_channel.get(ch, 0) + rev

        return {
            "date": date_str,
            "total_revenue": int(total_revenue),
            "by_channel": {k: int(v) for k, v in sorted(by_channel.items())},
            "item_count": len(product_set),
        }
    except Exception as e:
        return {"date": date_str, "total_revenue": 0,
                "by_channel": {}, "item_count": 0, "error": str(e)}


def validate_stock_integrity(db, date_str):
    """재고 정합성 검증 — 입고총합 - 출고총합 = 현재고.

    stock_ledger에서 date_str까지의 전체 거래를 품목별로 합산하여
    마이너스 재고 / 제로 재고 품목을 탐지.

    Returns:
        {
            "negative_stock_items": [
                {"product_name": str, "total_qty": int},
                ...
            ],
            "zero_stock_items": [
                {"product_name": str, "total_qty": int},
                ...
            ],
            "total_products": int
        }
    """
    try:
        all_rows = db.query_stock_ledger(date_to=date_str)

        qty_by_product = {}
        for r in all_rows:
            pn = (r.get("product_name") or "").strip()
            if not pn:
                continue
            qty_by_product[pn] = qty_by_product.get(pn, 0) + int(r.get("qty") or 0)

        negative_items = []
        zero_items = []

        for pn in sorted(qty_by_product.keys()):
            total = qty_by_product[pn]
            if total < 0:
                negative_items.append({"product_name": pn, "total_qty": total})
            elif total == 0:
                zero_items.append({"product_name": pn, "total_qty": total})

        return {
            "negative_stock_items": negative_items,
            "zero_stock_items": zero_items,
            "total_products": len(qty_by_product),
        }
    except Exception as e:
        return {"negative_stock_items": [], "zero_stock_items": [],
                "total_products": 0, "error": str(e)}


# ─── 헬퍼 ───

def _paginate_all(db, table_name, query_builder_fn):
    """db._paginate_query 래퍼 — 서비스에서 직접 페이지네이션 호출."""
    return db._paginate_query(table_name, query_builder_fn)
