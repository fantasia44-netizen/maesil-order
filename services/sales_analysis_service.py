"""
services/sales_analysis_service.py — 월간 판매분석 엔진.

order_transactions 기반 품목별 월간 판매 집계 + 전월 비교.
DB_CUTOFF_DATE 이전 월은 daily_revenue(레거시)에서 조회.
분류: cost_type(생산/OEM/소분/매입) + food_type(농산물/수산물/축산물).
"""
from datetime import datetime, date
from calendar import monthrange
from collections import defaultdict
from services.tz_utils import today_kst
from services.channel_config import (
    DB_CUTOFF_DATE, LEGACY_CATEGORY_TO_CHANNEL,
)


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
    """특정 월의 품목별 판매 데이터 집계 (합계만).

    DB_CUTOFF_DATE 이전 월 → daily_revenue(레거시)에서 조회.
    DB_CUTOFF_DATE 이후 월 → order_transactions에서 조회.

    Returns:
        dict: {product_name: {'total_qty': int, 'total_amount': int}}
    """
    first, last, _ = _month_range(year, month)

    agg = defaultdict(lambda: {'total_qty': 0, 'total_amount': 0})

    # cutoff 이전 월이면 daily_revenue(레거시)에서 조회
    if last < DB_CUTOFF_DATE:
        rows = _fetch_legacy_sales(db, first, last)
    elif first >= DB_CUTOFF_DATE:
        rows = _fetch_order_sales(db, first, last)
    else:
        # 월이 cutoff를 걸치는 경우
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

        item = agg[pn]
        item['total_qty'] += qty
        item['total_amount'] += amt

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
        # 거래처매출은 판매분석 대상 아님 (B2B)
        if cat in ('거래처매출',):
            continue
        rows.append({
            'product_name': r.get('product_name', ''),
            'qty': r.get('qty', 0),
            'amount': r.get('revenue', 0),
        })
    return rows


def _fetch_order_sales(db, first, last):
    """order_transactions + daily_revenue(로켓)에서 품목별 판매 데이터 조회."""
    def builder(table):
        return db.client.table(table).select(
            'product_name,qty,total_amount'
        ).eq('status', '정상') \
         .gte('order_date', first) \
         .lte('order_date', last)

    raw = db._paginate_query('order_transactions', builder)

    rows = [{
        'product_name': r.get('product_name', ''),
        'qty': r.get('qty', 0),
        'amount': r.get('total_amount', 0),
    } for r in raw]

    # 로켓매출은 daily_revenue에만 존재 → 별도 조회
    def rocket_builder(table):
        return db.client.table(table).select(
            'product_name,qty,revenue'
        ).eq('category', '로켓') \
         .gte('revenue_date', first) \
         .lte('revenue_date', last)

    rocket_raw = db._paginate_query('daily_revenue', rocket_builder)
    for r in rocket_raw:
        rows.append({
            'product_name': r.get('product_name', ''),
            'qty': r.get('qty', 0),
            'amount': r.get('revenue', 0),
        })

    return rows


# ═══════════════════════════════════════════════════════════════
# product_costs 매핑 헬퍼 (공백 정규화 대응)
# ═══════════════════════════════════════════════════════════════

def _build_cost_lookup(cost_map):
    """product_costs 조회 결과를 공백 정규화 키도 포함하여 반환."""
    lookup = {}
    for name, info in cost_map.items():
        lookup[name] = info
        norm = name.replace(' ', '')
        if norm != name and norm not in lookup:
            lookup[norm] = info
    return lookup


def get_monthly_sales_analysis(db, year=None, month=None):
    """월간 판매분석 메인 함수.

    분류: cost_type(생산/OEM/소분/매입) + food_type(농산물/수산물/축산물).
    채널별 분리 없이 합계만 표시.
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

    # DB 조회
    cur_sales_raw = _fetch_month_sales(db, year, month)
    prev_sales_raw = _fetch_month_sales(db, py, pm)

    # product_costs에서 cost_type, food_type 매핑
    cost_map = db.query_product_costs()
    lookup = _build_cost_lookup(cost_map)

    # 공백 차이 품목 병합: product_costs 이름을 정본으로, 없으면 첫 등장 이름 사용
    canon_map = {}  # norm_name → canonical_name
    for name in cost_map:
        canon_map[name.replace(' ', '')] = name
    # product_costs에 없는 품목도 정본 등록 (첫 등장 기준)
    for pn in list(cur_sales_raw.keys()) + list(prev_sales_raw.keys()):
        norm = pn.replace(' ', '')
        if norm not in canon_map:
            canon_map[norm] = pn

    def _normalize_sales(raw):
        merged = defaultdict(lambda: {'total_qty': 0, 'total_amount': 0})
        for pn, data in raw.items():
            norm = pn.replace(' ', '')
            canonical = canon_map.get(norm, pn)
            merged[canonical]['total_qty'] += data['total_qty']
            merged[canonical]['total_amount'] += data['total_amount']
        return dict(merged)

    cur_sales = _normalize_sales(cur_sales_raw)
    prev_sales = _normalize_sales(prev_sales_raw)

    def get_cost_type(pn):
        info = lookup.get(pn, {})
        return info.get('cost_type', '') or '미분류'

    def get_food_type(pn):
        info = lookup.get(pn, {})
        return info.get('food_type', '') or ''

    # 전체 품목 합집합
    all_products = set(cur_sales.keys()) | set(prev_sales.keys())

    items = []
    grand = {'cur_qty': 0, 'prev_qty': 0, 'cur_amount': 0, 'prev_amount': 0}

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

        # 달성율
        if daily_prev > 0:
            achievement = round(daily_cur / daily_prev * 100, 1)
        elif daily_cur > 0:
            achievement = None
        else:
            achievement = None

        # 증감율
        qty_rate = round((cq - pq) / pq * 100, 1) if pq > 0 else None
        amt_rate = round((ca - pa) / pa * 100, 1) if pa > 0 else None

        items.append({
            'product_name': pn,
            'cost_type': get_cost_type(pn),
            'food_type': get_food_type(pn),
            'cur_qty': cq,
            'cur_amount': ca,
            'prev_qty': pq,
            'prev_amount': pa,
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

    # 비중 계산
    total_cur = grand['cur_qty'] or 1
    for item in items:
        item['share_pct'] = round(item['cur_qty'] / total_cur * 100, 2)

    # 정렬: 이번달 수량 내림차순
    items.sort(key=lambda x: x['cur_qty'], reverse=True)

    # cost_type별 소계
    ct_agg = defaultdict(lambda: {
        'cur_qty': 0, 'prev_qty': 0, 'cur_amount': 0, 'prev_amount': 0, 'count': 0,
    })
    # food_type별 소계
    ft_agg = defaultdict(lambda: {
        'cur_qty': 0, 'prev_qty': 0, 'cur_amount': 0, 'prev_amount': 0, 'count': 0,
    })

    for item in items:
        ct = item['cost_type']
        c = ct_agg[ct]
        c['cur_qty'] += item['cur_qty']
        c['prev_qty'] += item['prev_qty']
        c['cur_amount'] += item['cur_amount']
        c['prev_amount'] += item['prev_amount']
        c['count'] += 1

        ft = item['food_type']
        if ft:
            f = ft_agg[ft]
            f['cur_qty'] += item['cur_qty']
            f['prev_qty'] += item['prev_qty']
            f['cur_amount'] += item['cur_amount']
            f['prev_amount'] += item['prev_amount']
            f['count'] += 1

    def _build_totals(agg_dict, key_name):
        totals = []
        for k, c in sorted(agg_dict.items()):
            daily_c = c['cur_qty'] / elapsed if elapsed > 0 else 0
            daily_p = c['prev_qty'] / prev_days if prev_days > 0 else 0
            ach = round(daily_c / daily_p * 100, 1) if daily_p > 0 else None
            totals.append({
                'category': k,
                'cur_qty': c['cur_qty'],
                'prev_qty': c['prev_qty'],
                'cur_amount': c['cur_amount'],
                'prev_amount': c['prev_amount'],
                'achievement_rate': ach,
                'count': c['count'],
            })
        return totals

    cost_type_totals = _build_totals(ct_agg, 'cost_type')
    food_type_totals = _build_totals(ft_agg, 'food_type')

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
        'cost_type_totals': cost_type_totals,
        'food_type_totals': food_type_totals,
        # 하위 호환: category_totals = cost_type_totals
        'category_totals': cost_type_totals,
        'generated_at': datetime.now().isoformat(),
    }
