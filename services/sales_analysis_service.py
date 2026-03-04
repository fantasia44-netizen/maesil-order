"""
services/sales_analysis_service.py — 월간 판매분석 엔진.

order_transactions 기반 품목별 월간 판매 집계 + 전월 비교.
DB_CUTOFF_DATE 이전 월은 daily_revenue(레거시)에서 조회.
"""
from datetime import datetime, date
from calendar import monthrange
from collections import defaultdict
from services.tz_utils import today_kst
from services.channel_config import (
    DB_CUTOFF_DATE, LEGACY_CATEGORY_TO_CHANNEL,
)


# ─── 채널 그룹핑 (엑셀과 동일: 네이버 vs 쿠팡) ───
COUPANG_CHANNELS = {'쿠팡', 'Coupang', 'coupang'}

def _channel_group(ch):
    """채널명을 네이버/쿠팡 2그룹으로 분류."""
    ch = (ch or '').strip()
    if ch in COUPANG_CHANNELS:
        return '쿠팡'
    return '네이버'  # 스마트스토어, 자사몰, 카카오, 해미애찬 등


# ─── 월 범위 계산 ───
def _month_range(year, month):
    """해당 월의 (first_day, last_day, total_days) 반환."""
    first = date(year, month, 1)
    _, days = monthrange(year, month)
    last = date(year, month, days)
    return first.isoformat(), last.isoformat(), days


def _prev_month(year, month):
    """전월 (year, month) 반환."""
    if month == 1:
        return year - 1, 12
    return year, month - 1


# ═══════════════════════════════════════════════════════════════
# 핵심: 월간 판매 집계
# ═══════════════════════════════════════════════════════════════

def _fetch_month_sales(db, year, month):
    """특정 월의 품목별 채널별 판매 데이터 집계.

    DB_CUTOFF_DATE 이전 월 → daily_revenue(레거시)에서 조회.
    DB_CUTOFF_DATE 이후 월 → order_transactions에서 조회.

    Returns:
        dict: {product_name: {
            'naver_qty': int, 'coupang_qty': int, 'total_qty': int,
            'naver_amount': int, 'coupang_amount': int, 'total_amount': int,
        }}
    """
    first, last, _ = _month_range(year, month)

    agg = defaultdict(lambda: {
        'naver_qty': 0, 'coupang_qty': 0, 'total_qty': 0,
        'naver_amount': 0, 'coupang_amount': 0, 'total_amount': 0,
    })

    # cutoff 이전 월이면 daily_revenue(레거시)에서 조회
    if last < DB_CUTOFF_DATE:
        rows = _fetch_legacy_sales(db, first, last)
    elif first >= DB_CUTOFF_DATE:
        rows = _fetch_order_sales(db, first, last)
    else:
        # 월이 cutoff를 걸치는 경우 (예: 3월 1일~31일, cutoff=3/2)
        # cutoff 이전은 레거시, 이후는 order_transactions
        from datetime import timedelta
        cutoff = datetime.strptime(DB_CUTOFF_DATE, '%Y-%m-%d').date()
        prev_day = (cutoff - timedelta(days=1)).isoformat()
        rows = _fetch_legacy_sales(db, first, prev_day) + \
               _fetch_order_sales(db, cutoff.isoformat(), last)

    for r in rows:
        pn = (r.get('product_name') or '').strip()
        if not pn:
            continue
        qty = int(r.get('qty', 0) or 0)
        amt = int(r.get('amount', 0) or 0)
        grp = _channel_group(r.get('channel', ''))

        item = agg[pn]
        item['total_qty'] += qty
        item['total_amount'] += amt
        if grp == '쿠팡':
            item['coupang_qty'] += qty
            item['coupang_amount'] += amt
        else:
            item['naver_qty'] += qty
            item['naver_amount'] += amt

    return dict(agg)


def _fetch_legacy_sales(db, first, last):
    """daily_revenue(레거시)에서 품목별 판매 데이터 조회."""
    def builder(table):
        return db.client.table(table).select(
            'category,product_name,qty,revenue'
        ).gte('revenue_date', first).lte('revenue_date', last)

    raw = db._paginate_query('daily_revenue', builder)

    rows = []
    for r in raw:
        cat = (r.get('category') or '').strip()
        # 거래처매출/로켓은 판매분석 대상 아님 (B2B)
        if cat in ('거래처매출', '로켓'):
            continue
        ch = LEGACY_CATEGORY_TO_CHANNEL.get(cat, cat)
        rows.append({
            'product_name': r.get('product_name', ''),
            'channel': ch,
            'qty': r.get('qty', 0),
            'amount': r.get('revenue', 0),
        })
    return rows


def _fetch_order_sales(db, first, last):
    """order_transactions에서 품목별 판매 데이터 조회."""
    def builder(table):
        return db.client.table(table).select(
            'channel,product_name,qty,total_amount'
        ).eq('status', '정상') \
         .gte('order_date', first) \
         .lte('order_date', last)

    raw = db._paginate_query('order_transactions', builder)

    return [{
        'product_name': r.get('product_name', ''),
        'channel': r.get('channel', ''),
        'qty': r.get('qty', 0),
        'amount': r.get('total_amount', 0),
    } for r in raw]


def get_monthly_sales_analysis(db, year=None, month=None):
    """월간 판매분석 메인 함수.

    Args:
        db: SupabaseDB instance
        year, month: 분석 대상월 (기본: 이번달)

    Returns:
        dict: {
            current_month: {year, month, days, elapsed_days},
            prev_month: {year, month, days},
            items: [{
                product_name, sales_category,
                cur_qty, cur_naver_qty, cur_coupang_qty, cur_amount,
                prev_qty, prev_naver_qty, prev_coupang_qty, prev_amount,
                qty_change_rate, amount_change_rate,
                daily_avg_cur, daily_avg_prev,
                achievement_rate,  -- 일평균 기준 달성율
                share_pct,  -- 이번달 수량 비중
            }],
            totals: {cur_qty, prev_qty, ...},
            category_totals: [{category, cur_qty, prev_qty, ...}],
        }
    """
    # 기본: 이번달
    today = datetime.strptime(today_kst(), '%Y-%m-%d')
    if year is None:
        year = today.year
    if month is None:
        month = today.month

    _, _, cur_days = _month_range(year, month)
    elapsed = today.day if (year == today.year and month == today.month) else cur_days

    py, pm = _prev_month(year, month)
    _, _, prev_days = _month_range(py, pm)

    # DB 조회 (2회)
    cur_sales = _fetch_month_sales(db, year, month)
    prev_sales = _fetch_month_sales(db, py, pm)

    # product_costs에서 sales_category 매핑
    cost_map = db.query_product_costs()
    def get_category(pn):
        info = cost_map.get(pn, {})
        cat = info.get('sales_category') or info.get('material_type') or ''
        return cat if cat else '미분류'

    # 전체 품목 합집합
    all_products = set(cur_sales.keys()) | set(prev_sales.keys())

    items = []
    grand = {
        'cur_qty': 0, 'prev_qty': 0, 'cur_amount': 0, 'prev_amount': 0,
        'cur_naver_qty': 0, 'cur_coupang_qty': 0,
        'prev_naver_qty': 0, 'prev_coupang_qty': 0,
    }

    for pn in all_products:
        cur = cur_sales.get(pn, {})
        prev = prev_sales.get(pn, {})
        cq = cur.get('total_qty', 0)
        pq = prev.get('total_qty', 0)
        ca = cur.get('total_amount', 0)
        pa = prev.get('total_amount', 0)

        # 일평균
        daily_cur = cq / elapsed if elapsed > 0 else 0
        daily_prev = pq / prev_days if prev_days > 0 else 0

        # 달성율 (일평균 기준: 이번달 일평균 / 전월 일평균)
        if daily_prev > 0:
            achievement = round(daily_cur / daily_prev * 100, 1)
        elif daily_cur > 0:
            achievement = None  # 전월 0 → 신규
        else:
            achievement = None

        # 수량 증감율 (단순 비교)
        if pq > 0:
            qty_rate = round((cq - pq) / pq * 100, 1)
        else:
            qty_rate = None

        if pa > 0:
            amt_rate = round((ca - pa) / pa * 100, 1)
        else:
            amt_rate = None

        items.append({
            'product_name': pn,
            'sales_category': get_category(pn),
            'cur_qty': cq,
            'cur_naver_qty': cur.get('naver_qty', 0),
            'cur_coupang_qty': cur.get('coupang_qty', 0),
            'cur_amount': ca,
            'cur_naver_amount': cur.get('naver_amount', 0),
            'cur_coupang_amount': cur.get('coupang_amount', 0),
            'prev_qty': pq,
            'prev_naver_qty': prev.get('naver_qty', 0),
            'prev_coupang_qty': prev.get('coupang_qty', 0),
            'prev_amount': pa,
            'prev_naver_amount': prev.get('naver_amount', 0),
            'prev_coupang_amount': prev.get('coupang_amount', 0),
            'daily_avg_cur': round(daily_cur, 1),
            'daily_avg_prev': round(daily_prev, 1),
            'achievement_rate': achievement,
            'qty_change_rate': qty_rate,
            'amount_change_rate': amt_rate,
        })

        grand['cur_qty'] += cq
        grand['prev_qty'] += pq
        grand['cur_amount'] += ca
        grand['prev_amount'] += pa
        grand['cur_naver_qty'] += cur.get('naver_qty', 0)
        grand['cur_coupang_qty'] += cur.get('coupang_qty', 0)
        grand['prev_naver_qty'] += prev.get('naver_qty', 0)
        grand['prev_coupang_qty'] += prev.get('coupang_qty', 0)

    # 비중 계산
    total_cur = grand['cur_qty'] or 1
    for item in items:
        item['share_pct'] = round(item['cur_qty'] / total_cur * 100, 2)

    # 정렬: 이번달 수량 내림차순
    items.sort(key=lambda x: x['cur_qty'], reverse=True)

    # 카테고리별 소계
    cat_agg = defaultdict(lambda: {
        'cur_qty': 0, 'prev_qty': 0, 'cur_amount': 0, 'prev_amount': 0,
        'count': 0,
    })
    for item in items:
        cat = item['sales_category']
        c = cat_agg[cat]
        c['cur_qty'] += item['cur_qty']
        c['prev_qty'] += item['prev_qty']
        c['cur_amount'] += item['cur_amount']
        c['prev_amount'] += item['prev_amount']
        c['count'] += 1

    cat_totals = []
    for cat, c in sorted(cat_agg.items()):
        daily_c = c['cur_qty'] / elapsed if elapsed > 0 else 0
        daily_p = c['prev_qty'] / prev_days if prev_days > 0 else 0
        ach = round(daily_c / daily_p * 100, 1) if daily_p > 0 else None
        cat_totals.append({
            'category': cat,
            'cur_qty': c['cur_qty'],
            'prev_qty': c['prev_qty'],
            'cur_amount': c['cur_amount'],
            'prev_amount': c['prev_amount'],
            'achievement_rate': ach,
            'count': c['count'],
        })

    # 총계 달성율
    g_daily_c = grand['cur_qty'] / elapsed if elapsed > 0 else 0
    g_daily_p = grand['prev_qty'] / prev_days if prev_days > 0 else 0
    grand['achievement_rate'] = round(g_daily_c / g_daily_p * 100, 1) if g_daily_p > 0 else None
    grand['daily_avg_cur'] = round(g_daily_c, 1)
    grand['daily_avg_prev'] = round(g_daily_p, 1)

    return {
        'current_month': {
            'year': year, 'month': month,
            'days': cur_days, 'elapsed_days': elapsed,
            'label': f'{year}년 {month:02d}월',
        },
        'prev_month': {
            'year': py, 'month': pm,
            'days': prev_days,
            'label': f'{py}년 {pm:02d}월',
        },
        'items': items,
        'totals': grand,
        'category_totals': cat_totals,
        'generated_at': datetime.now().isoformat(),
    }
