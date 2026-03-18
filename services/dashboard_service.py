"""
dashboard_service.py — 대시보드 데이터 오케스트레이터.
기존 revenue_service, stock 쿼리를 조합하여 대시보드 KPI/차트 데이터 제공.
+ 캐싱: 동일 데이터 5분간 재사용 (Supabase API 호출 최소화)
"""
import time
from datetime import datetime, timedelta
from services.tz_utils import today_kst

# ── 모듈 레벨 캐시 (gunicorn worker당 1개) ──
_dashboard_cache = {'data': None, 'ts': 0, 'ttl': 300}  # 5분


def _get_stock_exclude_set(db):
    """product_costs에서 is_stock_managed=false인 품목 set."""
    from services.stock_service import _get_stock_unmanaged_set
    return _get_stock_unmanaged_set(db)


def get_dashboard_data(db, date=None, force_refresh=False):
    """대시보드 전체 데이터 수집 (5분 캐시).

    Args:
        db: SupabaseDB instance
        date: 기준일 (YYYY-MM-DD), None이면 오늘 (KST)
        force_refresh: True면 캐시 무시

    Returns:
        dict: kpi, revenue_trend, channel_breakdown, warehouse_stock,
              top_products, recent_activity
    """
    if date is None:
        date = today_kst()

    # 캐시 확인 (날짜 지정 없는 기본 요청만 캐시)
    now = time.time()
    if (not force_refresh
            and _dashboard_cache['data']
            and (now - _dashboard_cache['ts']) < _dashboard_cache['ttl']
            and _dashboard_cache['data'].get('date') == date):
        return _dashboard_cache['data']

    # 이번 달 범위
    today = datetime.strptime(date, '%Y-%m-%d')
    month_start = today.replace(day=1).strftime('%Y-%m-%d')

    excl = _get_stock_exclude_set(db)

    # ── stock_summary 1회만 호출 (KPI + warehouse_stock 공유) ──
    warehouse_stock = db.query_stock_summary_by_location(exclude_products=excl)
    total_products = sum(s.get("product_count", 0) for s in warehouse_stock)

    kpi = _get_kpi(db, date, month_start, total_products)

    result = {
        "date": date,
        "month": today.month,
        "month_start": month_start,
        "kpi": kpi,
        "revenue_trend": db.query_revenue_trend(days=7),
        "channel_breakdown": db.query_orders_by_channel(
            date_from=month_start, date_to=date),
        "warehouse_stock": warehouse_stock,
        "top_products": db.query_top_products_by_revenue(days=30, limit=10),
        "recent_activity": db.query_recent_activity(limit=15),
    }

    # 캐시 저장
    _dashboard_cache['data'] = result
    _dashboard_cache['ts'] = now

    return result


def _get_kpi(db, date, month_start, stock_product_count=0):
    """KPI 카드 데이터.
    매출은 order_transactions 기반: 총매출(total_amount), 순매출(settlement).
    stock_product_count: 외부에서 이미 계산된 값 (중복 쿼리 방지).
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

    return {
        "today_orders": today_orders,
        "today_revenue": today_rev.get("total_amount", 0),
        "today_settlement": today_rev.get("settlement", 0),
        "today_commission": today_rev.get("commission", 0),
        "month_revenue": month_total,
        "month_settlement": month_settlement,
        "pending_outbound": outbound_summary.get("pending", 0),
        "done_outbound": outbound_summary.get("done", 0),
        "stock_products": stock_product_count,
    }


def get_revenue_chart_data(db, days=30):
    """매출 차트용 데이터 (일별 + 카테고리별)."""
    return db.query_revenue_trend(days=days)


def get_channel_chart_data(db, date_from=None, date_to=None):
    """채널 분포 차트용 데이터."""
    return db.query_orders_by_channel(date_from=date_from, date_to=date_to)
