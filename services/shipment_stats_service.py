"""
shipment_stats_service.py — 출고 통계 비즈니스 로직.
stock_ledger SALES_OUT 데이터를 수량 기준으로 집계.
revenue_service.py의 get_revenue_stats() 패턴과 동일 구조.
"""


def get_shipment_stats(db, date_from=None, date_to=None, location=None):
    """출고 통계 데이터 산출 (메인 오케스트레이터).

    Args:
        db: SupabaseDB instance
        date_from: 시작일 (YYYY-MM-DD) or None
        date_to: 종료일 (YYYY-MM-DD) or None
        location: 창고 필터 ('넥스원', '해서' 등) or None

    Returns:
        dict: summary, daily_totals, monthly_totals, location_breakdown,
              category_breakdown, daily_location_totals, monthly_location_totals,
              top_products
    """
    raw = db.query_stock_ledger(
        date_from=date_from,
        date_to=date_to,
        location=location if location and location != '전체' else None,
        type_list=["SALES_OUT"],
    )
    return {
        'summary': _calc_summary(raw),
        'daily_totals': _calc_daily_totals(raw),
        'monthly_totals': _calc_monthly_totals(raw),
        'location_breakdown': _calc_location_breakdown(raw),
        'category_breakdown': _calc_category_breakdown(raw),
        'daily_location_totals': _calc_daily_location_totals(raw),
        'monthly_location_totals': _calc_monthly_location_totals(raw),
        'top_products': _calc_top_products(raw, limit=15),
    }


def _abs_qty(r):
    """stock_ledger qty는 음수(재고차감)이므로 abs 처리."""
    try:
        return abs(int(float(r.get('qty', 0) or 0)))
    except (ValueError, TypeError):
        return 0


def _calc_summary(raw):
    """총 출고수량, 품목수, 건수, 일수, 일평균 산출."""
    total_qty = sum(_abs_qty(r) for r in raw)
    count = len(raw)
    items = set(r.get('product_name', '') for r in raw if r.get('product_name'))
    dates = set(r.get('transaction_date', '') for r in raw if r.get('transaction_date'))
    days = len(dates) or 1
    return {
        'total_qty': total_qty,
        'total_items': len(items),
        'total_count': count,
        'days': days,
        'daily_avg': round(total_qty / days, 1),
    }


def _calc_daily_totals(raw):
    """일별 출고수량 리스트 반환."""
    by_date = {}
    for r in raw:
        d = r.get('transaction_date', '')
        if d:
            by_date[d] = by_date.get(d, 0) + _abs_qty(r)
    return [{'date': k, 'total': v} for k, v in sorted(by_date.items())]


def _calc_monthly_totals(raw):
    """월별 출고수량 리스트 반환."""
    by_month = {}
    for r in raw:
        d = r.get('transaction_date', '')
        if d and len(d) >= 7:
            m = d[:7]
            by_month[m] = by_month.get(m, 0) + _abs_qty(r)
    return [{'month': k, 'total': v} for k, v in sorted(by_month.items())]


def _calc_location_breakdown(raw):
    """창고별 출고 비중 리스트 반환 (내림차순)."""
    by_loc = {}
    for r in raw:
        loc = r.get('location', '') or '기타'
        by_loc[loc] = by_loc.get(loc, 0) + _abs_qty(r)
    return [{'location': k, 'total': v}
            for k, v in sorted(by_loc.items(), key=lambda x: -x[1])]


def _calc_category_breakdown(raw):
    """카테고리별 출고 비중 리스트 반환 (내림차순)."""
    by_cat = {}
    for r in raw:
        cat = r.get('category', '') or '기타'
        by_cat[cat] = by_cat.get(cat, 0) + _abs_qty(r)
    return [{'category': k, 'total': v}
            for k, v in sorted(by_cat.items(), key=lambda x: -x[1])]


def _calc_daily_location_totals(raw):
    """일별 x 창고별 출고수량 크로스탭.

    Returns:
        dict: {
            'locations': ['넥스원', '해서', ...],
            'rows': [{'date': '...', 'locations': {...}, 'total': N}, ...],
            'totals': {'넥스원': N, ...},
            'grand_total': N
        }
    """
    by_date_loc = {}
    loc_totals = {}

    for r in raw:
        d = r.get('transaction_date', '')
        loc = r.get('location', '') or '기타'
        qty = _abs_qty(r)
        if not d:
            continue
        if d not in by_date_loc:
            by_date_loc[d] = {}
        by_date_loc[d][loc] = by_date_loc[d].get(loc, 0) + qty
        loc_totals[loc] = loc_totals.get(loc, 0) + qty

    locations = [k for k, v in sorted(loc_totals.items(), key=lambda x: -x[1])]

    rows = []
    for d in sorted(by_date_loc.keys()):
        loc_data = by_date_loc[d]
        rows.append({
            'date': d,
            'locations': loc_data,
            'total': sum(loc_data.values()),
        })

    return {
        'locations': locations,
        'rows': rows,
        'totals': loc_totals,
        'grand_total': sum(loc_totals.values()),
    }


def _calc_monthly_location_totals(raw):
    """월별 x 창고별 출고수량 크로스탭.

    Returns:
        dict: {
            'locations': ['넥스원', '해서', ...],
            'rows': [{'month': '...', 'locations': {...}, 'total': N}, ...],
            'totals': {'넥스원': N, ...},
            'grand_total': N
        }
    """
    by_month_loc = {}
    loc_totals = {}

    for r in raw:
        d = r.get('transaction_date', '')
        loc = r.get('location', '') or '기타'
        qty = _abs_qty(r)
        if not d or len(d) < 7:
            continue
        m = d[:7]
        if m not in by_month_loc:
            by_month_loc[m] = {}
        by_month_loc[m][loc] = by_month_loc[m].get(loc, 0) + qty
        loc_totals[loc] = loc_totals.get(loc, 0) + qty

    locations = [k for k, v in sorted(loc_totals.items(), key=lambda x: -x[1])]

    rows = []
    for m in sorted(by_month_loc.keys()):
        loc_data = by_month_loc[m]
        rows.append({
            'month': m,
            'locations': loc_data,
            'total': sum(loc_data.values()),
        })

    return {
        'locations': locations,
        'rows': rows,
        'totals': loc_totals,
        'grand_total': sum(loc_totals.values()),
    }


def _calc_top_products(raw, limit=15):
    """출고수량 상위 품목 리스트 반환."""
    by_prod = {}
    for r in raw:
        name = r.get('product_name', '')
        if not name:
            continue
        cat = r.get('category', '') or ''
        if name not in by_prod:
            by_prod[name] = {'qty': 0, 'category': cat}
        by_prod[name]['qty'] += _abs_qty(r)
    items = [{'name': k, **v} for k, v in by_prod.items()]
    items.sort(key=lambda x: -x['qty'])
    return items[:limit]
