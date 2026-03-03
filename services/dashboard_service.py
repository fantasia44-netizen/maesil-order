"""
dashboard_service.py — 대시보드 데이터 오케스트레이터.
기존 revenue_service, stock 쿼리를 조합하여 대시보드 KPI/차트 데이터 제공.
"""
from datetime import datetime, timedelta
from services.tz_utils import today_kst


def get_dashboard_data(db, date=None):
    """대시보드 전체 데이터 수집.

    Args:
        db: SupabaseDB instance
        date: 기준일 (YYYY-MM-DD), None이면 오늘 (KST)

    Returns:
        dict: kpi, revenue_trend, channel_breakdown, warehouse_stock,
              top_products, recent_activity
    """
    if date is None:
        date = today_kst()

    # 이번 달 범위
    today = datetime.strptime(date, '%Y-%m-%d')
    month_start = today.replace(day=1).strftime('%Y-%m-%d')

    return {
        "date": date,
        "kpi": _get_kpi(db, date, month_start),
        "revenue_trend": db.query_revenue_trend(days=30),
        "channel_breakdown": db.query_orders_by_channel(
            date_from=month_start, date_to=date),
        "warehouse_stock": db.query_stock_summary_by_location(),
        "top_products": db.query_top_products_by_revenue(days=30, limit=10),
        "recent_activity": db.query_recent_activity(limit=15),
    }


def _get_kpi(db, date, month_start):
    """KPI 카드 데이터.
    매출은 order_transactions 기반: 총매출(total_amount), 순매출(settlement).
    """
    today_orders = db.count_orders_by_date(date)
    today_rev = db.sum_revenue_by_date(date)  # dict

    # 이번 달 매출 합계
    month_total = 0
    month_settlement = 0
    try:
        rev_data = db.query_revenue(date_from=month_start, date_to=date)
        month_total = sum(r.get("revenue", 0) or 0 for r in (rev_data or []))
        month_settlement = sum(r.get("settlement", 0) or 0 for r in (rev_data or []))
    except Exception:
        pass

    # 미처리 출고
    outbound_summary = db.query_outbound_summary(date_from=month_start, date_to=date)

    # 재고 품목 수
    stock_summary = db.query_stock_summary_by_location()
    total_products = sum(s.get("product_count", 0) for s in stock_summary)

    return {
        "today_orders": today_orders,
        "today_revenue": today_rev.get("total_amount", 0),
        "today_settlement": today_rev.get("settlement", 0),
        "today_commission": today_rev.get("commission", 0),
        "month_revenue": month_total,
        "month_settlement": month_settlement,
        "pending_outbound": outbound_summary.get("pending", 0),
        "done_outbound": outbound_summary.get("done", 0),
        "stock_products": total_products,
    }


def get_revenue_chart_data(db, days=30):
    """매출 차트용 데이터 (일별 + 카테고리별)."""
    return db.query_revenue_trend(days=days)


def get_channel_chart_data(db, date_from=None, date_to=None):
    """채널 분포 차트용 데이터."""
    return db.query_orders_by_channel(date_from=date_from, date_to=date_to)
