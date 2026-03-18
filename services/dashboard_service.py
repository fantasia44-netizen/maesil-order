"""
dashboard_service.py — 대시보드 데이터 오케스트레이터.
기존 revenue_service, stock 쿼리를 조합하여 대시보드 KPI/차트 데이터 제공.
+ 캐싱: 동일 데이터 5분간 재사용 (Supabase API 호출 최소화)
+ 병렬: 독립 쿼리를 ThreadPoolExecutor로 동시 실행
"""
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from services.tz_utils import today_kst

# ── 모듈 레벨 캐시 (gunicorn worker당 1개) ──
_dashboard_cache = {'data': None, 'ts': 0, 'ttl': 300}  # 5분


def _get_stock_exclude_set(db):
    """product_costs에서 is_stock_managed=false인 품목 set."""
    from services.stock_service import _get_stock_unmanaged_set
    return _get_stock_unmanaged_set(db)


def _safe_call(fn, *args, **kwargs):
    """쿼리 실패 시 기본값 반환."""
    try:
        return fn(*args, **kwargs)
    except Exception as e:
        print(f"[Dashboard] {fn.__name__} error: {e}")
        return None


def get_dashboard_data(db, date=None, force_refresh=False):
    """대시보드 전체 데이터 수집 (5분 캐시 + 병렬 쿼리).

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

    # 캐시 확인
    now = time.time()
    if (not force_refresh
            and _dashboard_cache['data']
            and (now - _dashboard_cache['ts']) < _dashboard_cache['ttl']
            and _dashboard_cache['data'].get('date') == date):
        return _dashboard_cache['data']

    # 이번 달 범위
    today_dt = datetime.strptime(date, '%Y-%m-%d')
    month_start = today_dt.replace(day=1).strftime('%Y-%m-%d')

    excl = _get_stock_exclude_set(db)

    # ── 독립 쿼리 6개를 병렬 실행 ──
    results = {}
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {
            pool.submit(_safe_call, db.query_stock_summary_by_location,
                        exclude_products=excl): 'warehouse_stock',
            pool.submit(_safe_call, db.query_revenue_trend, days=7): 'revenue_trend',
            pool.submit(_safe_call, db.query_orders_by_channel,
                        date_from=month_start, date_to=date): 'channel_breakdown',
            pool.submit(_safe_call, db.query_top_products_by_revenue,
                        days=30, limit=10): 'top_products',
            pool.submit(_safe_call, db.query_recent_activity, limit=15): 'recent_activity',
            pool.submit(_get_kpi_parallel, db, date, month_start): 'kpi_raw',
        }
        for future in as_completed(futures):
            key = futures[future]
            try:
                results[key] = future.result()
            except Exception as e:
                print(f"[Dashboard] {key} thread error: {e}")
                results[key] = None

    # stock_summary에서 재고 품목 수 계산 → KPI에 주입
    warehouse_stock = results.get('warehouse_stock') or []
    total_products = sum(s.get("product_count", 0) for s in warehouse_stock)

    kpi = results.get('kpi_raw') or {}
    kpi['stock_products'] = total_products

    result = {
        "date": date,
        "month": today_dt.month,
        "month_start": month_start,
        "kpi": kpi,
        "revenue_trend": results.get('revenue_trend') or [],
        "channel_breakdown": results.get('channel_breakdown') or [],
        "warehouse_stock": warehouse_stock,
        "top_products": results.get('top_products') or [],
        "recent_activity": results.get('recent_activity') or [],
    }

    # 캐시 저장
    _dashboard_cache['data'] = result
    _dashboard_cache['ts'] = now

    return result


def _get_kpi_parallel(db, date, month_start):
    """KPI 카드 데이터 (내부 쿼리도 병렬).
    매출은 order_transactions 기반: 총매출(total_amount), 순매출(settlement).
    """
    results = {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {
            pool.submit(_safe_call, db.count_orders_by_date, date): 'orders',
            pool.submit(_safe_call, db.sum_revenue_by_date, date): 'today_rev',
            pool.submit(_safe_call, db.query_revenue,
                        date_from=month_start, date_to=date): 'month_rev',
            pool.submit(_safe_call, db.query_outbound_summary,
                        date_from=month_start, date_to=date): 'outbound',
        }
        for future in as_completed(futures):
            key = futures[future]
            try:
                results[key] = future.result()
            except Exception:
                results[key] = None

    today_rev = results.get('today_rev') or {}
    month_rev_data = results.get('month_rev') or []
    outbound = results.get('outbound') or {}

    month_total = sum(r.get("revenue", 0) or 0 for r in month_rev_data)
    month_settlement = sum(r.get("settlement", 0) or 0 for r in month_rev_data)

    return {
        "today_orders": results.get('orders') or 0,
        "today_revenue": today_rev.get("total_amount", 0),
        "today_settlement": today_rev.get("settlement", 0),
        "today_commission": today_rev.get("commission", 0),
        "month_revenue": month_total,
        "month_settlement": month_settlement,
        "pending_outbound": outbound.get("pending", 0),
        "done_outbound": outbound.get("done", 0),
        "stock_products": 0,  # 호출 측에서 주입
    }


def get_revenue_chart_data(db, days=30):
    """매출 차트용 데이터 (일별 + 카테고리별)."""
    return db.query_revenue_trend(days=days)


def get_channel_chart_data(db, date_from=None, date_to=None):
    """채널 분포 차트용 데이터."""
    return db.query_orders_by_channel(date_from=date_from, date_to=date_to)
