"""
revenue_service.py — 매출(Tab 7) 비즈니스 로직.
Tkinter 의존 제거. db 파라미터(SupabaseDB 인스턴스)를 받고 결과를 dict/list로 반환.
"""
import pandas as pd
from datetime import datetime

try:
    from excel_io import parse_revenue_payload
except ImportError:
    from services.excel_io import parse_revenue_payload


def _validate_date(date_str):
    """날짜 형식 검증. 실패 시 ValueError raise."""
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        raise ValueError(f"날짜 형식이 올바르지 않습니다: {date_str}. YYYY-MM-DD 형식으로 입력하세요.")


def process_revenue_import(db, excel_df, date_str):
    """일일매출 엑셀 데이터를 파싱하여 DB에 upsert.

    parse_revenue_payload 를 사용하여 엑셀 DataFrame을 매출 payload로 변환하고
    동일 날짜 기존 데이터를 덮어쓰기(upsert) 합니다.

    Args:
        db: SupabaseDB instance
        excel_df: pandas DataFrame (일일매출 엑셀)
            컬럼: 품목명, {카테고리}_수량, {카테고리}_단가, {카테고리}_매출
            카테고리: 일반매출, 쿠팡매출, 로켓, N배송(용인)
        date_str: 매출일자 (YYYY-MM-DD)

    Returns:
        dict: {
            "count": int,           # 업로드된 매출 건수
            "total_revenue": int    # 총 매출액
        }

    Raises:
        ValueError: 날짜 형식 오류 또는 데이터 없음
        Exception: DB 오류
    """
    payload, total_rev = parse_revenue_payload(excel_df, date_str)
    if not payload:
        return {"count": 0, "total_revenue": 0}

    db.upsert_revenue(payload)
    return {"count": len(payload), "total_revenue": total_rev}


def import_revenue(db, excel_df, date_str):
    """매출 엑셀 업로드 — process_revenue_import의 래퍼.

    날짜 유효성 검증을 포함하며, 데이터가 없을 경우 경고를 warnings에 포함.

    Args:
        db: SupabaseDB instance
        excel_df: pandas DataFrame (일일매출 엑셀, .fillna(0) 적용 필요)
        date_str: 매출일자 (YYYY-MM-DD)

    Returns:
        dict: {
            "count": int,
            "total_revenue": int,
            "warnings": list
        }

    Raises:
        ValueError: 날짜 형식 오류
        Exception: DB 오류
    """
    _validate_date(date_str)

    if not date_str.strip():
        raise ValueError("매출일자를 입력하세요.")

    warnings = []
    result = process_revenue_import(db, excel_df, date_str)

    if result["count"] == 0:
        warnings.append("업로드할 매출 데이터가 없습니다.")

    return {
        "count": result["count"],
        "total_revenue": result["total_revenue"],
        "warnings": warnings,
    }


def get_revenue_stats(db, date_from=None, date_to=None, category=None):
    """매출 통계 데이터 산출 (메인 오케스트레이터).

    Args:
        db: SupabaseDB instance
        date_from: 시작일 (YYYY-MM-DD) or None
        date_to: 종료일 (YYYY-MM-DD) or None
        category: 매출유형 필터 or None

    Returns:
        dict: summary, daily_totals, monthly_totals, category_breakdown, top_products
    """
    raw = db.query_revenue(
        date_from=date_from,
        date_to=date_to,
        category=category if category and category != '전체' else None,
    )
    return {
        'summary': _calc_summary(raw),
        'daily_totals': _calc_daily_totals(raw),
        'monthly_totals': _calc_monthly_totals(raw),
        'category_breakdown': _calc_category_breakdown(raw),
        'channel_breakdown': _calc_channel_breakdown(raw),
        'daily_channel_totals': _calc_daily_channel_totals(raw),
        'monthly_channel_totals': _calc_monthly_channel_totals(raw),
        'top_products': _calc_top_products(raw, limit=15),
    }


def _calc_summary(raw):
    """총 매출, 건수, 일수, 일평균 매출 산출."""
    total = sum(r.get('revenue', 0) for r in raw)
    settlement = sum(r.get('settlement', 0) for r in raw)
    commission = sum(r.get('commission', 0) for r in raw)
    count = len(raw)
    dates = set(r.get('revenue_date', '') for r in raw if r.get('revenue_date'))
    days = len(dates) or 1
    return {
        'total_revenue': total,
        'total_settlement': settlement,
        'total_commission': commission,
        'total_count': count,
        'days': days,
        'daily_avg': total / days,
    }


def _calc_daily_totals(raw):
    """일별 매출합계 리스트 반환."""
    by_date = {}
    for r in raw:
        d = r.get('revenue_date', '')
        if d:
            by_date[d] = by_date.get(d, 0) + r.get('revenue', 0)
    return [{'date': k, 'total': v} for k, v in sorted(by_date.items())]


def _calc_monthly_totals(raw):
    """월별 매출합계 리스트 반환."""
    by_month = {}
    for r in raw:
        d = r.get('revenue_date', '')
        if d and len(d) >= 7:
            m = d[:7]
            by_month[m] = by_month.get(m, 0) + r.get('revenue', 0)
    return [{'month': k, 'total': v} for k, v in sorted(by_month.items())]


def _calc_category_breakdown(raw):
    """카테고리별 매출 비중 리스트 반환 (내림차순)."""
    by_cat = {}
    for r in raw:
        cat = r.get('category', '기타')
        by_cat[cat] = by_cat.get(cat, 0) + r.get('revenue', 0)
    return [{'category': k, 'total': v}
            for k, v in sorted(by_cat.items(), key=lambda x: -x[1])]


def _resolve_channel(r):
    """레코드에서 채널명 추출. channel이 없으면 category로 폴백. 표시명 정규화."""
    from services.channel_config import normalize_channel_display
    raw_ch = r.get('channel', '') or ''
    if not raw_ch or raw_ch in ('None', 'none', 'null'):
        ch = r.get('category', '기타') or '기타'
    else:
        ch = raw_ch
    return normalize_channel_display(ch)


def _calc_channel_breakdown(raw):
    """채널별 매출 비중 리스트 반환 (내림차순)."""
    by_ch = {}
    for r in raw:
        ch = _resolve_channel(r)
        by_ch[ch] = by_ch.get(ch, 0) + r.get('revenue', 0)
    return [{'channel': k, 'total': v}
            for k, v in sorted(by_ch.items(), key=lambda x: -x[1])]


def _calc_daily_channel_totals(raw):
    """일별 × 채널별 매출 테이블 데이터.

    Returns:
        dict: {
            'channels': ['스마트스토어', '쿠팡', ...],  # 채널 목록 (매출 내림차순)
            'rows': [
                {'date': '2025-02-27', 'channels': {'스마트스토어': 100000, ...}, 'total': 150000},
                ...
            ],
            'totals': {'스마트스토어': 500000, ...},  # 채널별 총합
            'grand_total': 1500000
        }
    """
    by_date_ch = {}   # {date: {channel: revenue}}
    ch_totals = {}    # {channel: total_revenue}

    for r in raw:
        d = r.get('revenue_date', '')
        ch = _resolve_channel(r)
        rev = r.get('revenue', 0)
        if not d:
            continue
        if d not in by_date_ch:
            by_date_ch[d] = {}
        by_date_ch[d][ch] = by_date_ch[d].get(ch, 0) + rev
        ch_totals[ch] = ch_totals.get(ch, 0) + rev

    # 채널 목록: 총 매출 내림차순
    channels = [k for k, v in sorted(ch_totals.items(), key=lambda x: -x[1])]

    rows = []
    for d in sorted(by_date_ch.keys()):
        ch_data = by_date_ch[d]
        rows.append({
            'date': d,
            'channels': ch_data,
            'total': sum(ch_data.values()),
        })

    return {
        'channels': channels,
        'rows': rows,
        'totals': ch_totals,
        'grand_total': sum(ch_totals.values()),
    }


def _calc_monthly_channel_totals(raw):
    """월별 × 채널별 매출 테이블 데이터.

    Returns:
        dict: {
            'channels': ['스마트스토어', '쿠팡', ...],
            'rows': [
                {'month': '2025-02', 'channels': {'스마트스토어': 100000, ...}, 'total': 150000},
                ...
            ],
            'totals': {'스마트스토어': 500000, ...},
            'grand_total': 1500000
        }
    """
    by_month_ch = {}  # {month: {channel: revenue}}
    ch_totals = {}

    for r in raw:
        d = r.get('revenue_date', '')
        ch = _resolve_channel(r)
        rev = r.get('revenue', 0)
        if not d or len(d) < 7:
            continue
        m = d[:7]
        if m not in by_month_ch:
            by_month_ch[m] = {}
        by_month_ch[m][ch] = by_month_ch[m].get(ch, 0) + rev
        ch_totals[ch] = ch_totals.get(ch, 0) + rev

    channels = [k for k, v in sorted(ch_totals.items(), key=lambda x: -x[1])]

    rows = []
    for m in sorted(by_month_ch.keys()):
        ch_data = by_month_ch[m]
        rows.append({
            'month': m,
            'channels': ch_data,
            'total': sum(ch_data.values()),
        })

    return {
        'channels': channels,
        'rows': rows,
        'totals': ch_totals,
        'grand_total': sum(ch_totals.values()),
    }


def _calc_top_products(raw, limit=15):
    """매출 상위 품목 리스트 반환."""
    by_prod = {}
    for r in raw:
        name = r.get('product_name', '')
        if name not in by_prod:
            by_prod[name] = {'qty': 0, 'revenue': 0}
        by_prod[name]['qty'] += r.get('qty', 0)
        by_prod[name]['revenue'] += r.get('revenue', 0)
    items = [{'name': k, **v} for k, v in by_prod.items()]
    items.sort(key=lambda x: -x['revenue'])
    return items[:limit]


def get_revenue(db, date_from=None, date_to=None, category=None, search=None):
    """매출 데이터 조회.

    Args:
        db: SupabaseDB instance
        date_from: 시작일 (YYYY-MM-DD) or None
        date_to: 종료일 (YYYY-MM-DD) or None
        category: 매출유형 필터 ("전체"이면 전체 조회) or None
        search: 품목명 검색어 or None

    Returns:
        dict: {
            "items": list of dict,    # 매출 항목 목록
                각 dict 키: revenue_date, product_name, category, qty, unit_price, revenue
            "total_by_category": dict, # 카테고리별 합계 {카테고리명: 금액}
            "total_revenue": int       # 총 매출 합계
        }
    """
    all_data = db.query_revenue(
        date_from=date_from or None,
        date_to=date_to or None,
        category=category if category and category != "전체" else None
    )

    items = []
    total_by_cat = {}
    total_all = 0

    for r in all_data:
        # 품목명 검색 필터
        if search and search.lower() not in r.get('product_name', '').lower():
            continue

        cat = r.get('category', '')
        rev = r.get('revenue', 0)

        items.append({
            "revenue_date": r.get('revenue_date', ''),
            "product_name": r.get('product_name', ''),
            "category": cat,
            "qty": r.get('qty', 0),
            "unit_price": r.get('unit_price', 0),
            "revenue": rev,
        })

        total_by_cat[cat] = total_by_cat.get(cat, 0) + rev
        total_all += rev

    return {
        "items": items,
        "total_by_category": total_by_cat,
        "total_revenue": total_all,
    }
