"""
pnl_service.py -- 관리 손익표(P&L) 비즈니스 로직.
매출, 매출원가(COGS), 판매관리비(SGA)를 종합하여
월별/채널별 손익을 계산한다.

데이터 소스:
  - 매출: order_transactions + daily_revenue (query_revenue)
  - 매출원가: actual_cost_service (실제 투입 원가) + BOM 표준원가 추정
  - 채널비용: channel_costs (수수료율, 배송비, 포장비)
  - 간접비: expenses 테이블
"""
import logging
from collections import defaultdict
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def _month_range(year_month):
    """'2026-03' -> ('2026-03-01', '2026-03-31') 반환."""
    parts = year_month.split('-')
    year, month = int(parts[0]), int(parts[1])
    date_from = f"{year}-{month:02d}-01"
    # 해당 월의 마지막 날 계산
    if month == 12:
        next_year, next_month = year + 1, 1
    else:
        next_year, next_month = year, month + 1
    last_day = (datetime(next_year, next_month, 1) - timedelta(days=1)).day
    date_to = f"{year}-{month:02d}-{last_day}"
    return date_from, date_to


def _prev_month(year_month):
    """'2026-03' -> '2026-02' 반환."""
    parts = year_month.split('-')
    year, month = int(parts[0]), int(parts[1])
    if month == 1:
        return f"{year - 1}-12"
    return f"{year}-{month - 1:02d}"


def _safe_pct(numerator, denominator):
    """안전한 퍼센트 계산. denominator가 0이면 0.0 반환."""
    if not denominator:
        return 0.0
    return round(numerator / denominator * 100, 1)


def _change_pct(current, previous):
    """전월 대비 증감률(%). previous가 0이면 None."""
    if not previous:
        return None
    return round((current - previous) / abs(previous) * 100, 1)


# ──────────────────────────────────────────────
#  매출 집계
# ──────────────────────────────────────────────

def _calc_revenue(db, date_from, date_to):
    """매출 합계 / 채널별 / 카테고리별 분해.

    Returns:
        dict: {
            'total': int,
            'by_channel': {채널명: 금액},
            'by_category': {카테고리: 금액},
            'total_commission': int,
            'total_qty': int,
            'details': [raw rows]  -- 채널별 P&L에서 재사용
        }
    """
    from services.revenue_service import _resolve_channel

    raw = db.query_revenue(date_from=date_from, date_to=date_to)

    total = 0
    total_commission = 0
    total_qty = 0
    by_channel = defaultdict(int)
    by_category = defaultdict(int)

    for r in raw:
        rev = r.get('revenue', 0) or 0
        comm = r.get('commission', 0) or 0
        qty = r.get('qty', 0) or 0
        ch = _resolve_channel(r)
        cat = r.get('category', '기타') or '기타'

        total += rev
        total_commission += comm
        total_qty += qty
        by_channel[ch] += rev
        by_category[cat] += rev

    return {
        'total': total,
        'by_channel': dict(by_channel),
        'by_category': dict(by_category),
        'total_commission': total_commission,
        'total_qty': total_qty,
        'details': raw,
    }


# ──────────────────────────────────────────────
#  매출원가(COGS) 집계
# ──────────────────────────────────────────────

def _calc_cogs(db, date_from, date_to, revenue_qty):
    """매출원가 계산.

    1) 실제 투입 직접비 (actual_cost_service): 해당 기간 생산분 원가 합계
    2) BOM 표준원가 추정: 생산 데이터 없는 경우 판매수량 x BOM 표준원가

    Args:
        revenue_qty: 해당월 총 판매수량 (BOM 추정 폴백용)

    Returns:
        dict: {'total': N, 'actual_cost': N, 'estimated_cost': N}
    """
    from services.actual_cost_service import calculate_actual_costs
    from services.bom_cost_service import calculate_bom_costs

    actual_cost = 0
    estimated_cost = 0

    # 1. 실제 투입 원가
    try:
        actual_result = calculate_actual_costs(db, date_from, date_to)
        products = actual_result.get('products', [])
        for p in products:
            for batch in p.get('batches', []):
                actual_cost += batch.get('actual_total_cost', 0)
    except Exception as e:
        logger.warning(f"[P&L] 실제 원가 계산 실패: {e}")

    # 2. 생산 데이터가 없으면 BOM 표준원가로 추정
    if actual_cost == 0 and revenue_qty > 0:
        try:
            bom_result = calculate_bom_costs(db)
            bom_items = bom_result.get('bom_items', [])
            if bom_items:
                # 평균 BOM 원가 x 판매수량으로 대략 추정
                total_bom = sum(item.get('total_cost', 0) for item in bom_items)
                avg_bom = total_bom / len(bom_items) if bom_items else 0
                estimated_cost = round(avg_bom * revenue_qty)
        except Exception as e:
            logger.warning(f"[P&L] BOM 표준원가 추정 실패: {e}")

    total_cogs = actual_cost + estimated_cost
    return {
        'total': round(total_cogs),
        'actual_cost': round(actual_cost),
        'estimated_cost': round(estimated_cost),
    }


# ──────────────────────────────────────────────
#  판매관리비(SGA) 집계
# ──────────────────────────────────────────────

def _calc_sga(db, year_month, commission_total):
    """판매관리비 계산.

    구성:
      - 채널수수료: order_transactions.commission 합계 (매출 집계 시 이미 계산)
      - 배송비/포장비: expenses 테이블 또는 channel_costs 기반
      - 간접비: expenses 테이블 카테고리별 합계

    Returns:
        dict: {'total': N, 'commission': N, 'shipping': N, 'packaging': N,
               'labor': N, 'rent': N, 'utilities': N, 'other': N,
               'by_category': {카테고리: 금액}}
    """
    # 간접비 (expenses 테이블)
    expense_rows = db.query_expenses(month=year_month)

    by_category = defaultdict(float)
    for row in expense_rows:
        cat = row.get('category', '기타')
        amt = float(row.get('amount', 0))
        by_category[cat] += amt

    # 카테고리명 → SGA 항목 매핑
    # expenses 테이블의 카테고리를 표준 SGA 항목으로 매핑
    _CAT_MAP = {
        '인건비': 'labor',
        '급여': 'labor',
        '임차료': 'rent',
        '월세': 'rent',
        '수도광열비': 'utilities',
        '전기료': 'utilities',
        '수도료': 'utilities',
        '가스비': 'utilities',
        '배송비': 'shipping',
        '택배비': 'shipping',
        '포장비': 'packaging',
        '포장재': 'packaging',
    }

    sga = {
        'commission': round(commission_total),
        'shipping': 0,
        'packaging': 0,
        'labor': 0,
        'rent': 0,
        'utilities': 0,
        'other': 0,
    }

    for cat, amt in by_category.items():
        mapped = _CAT_MAP.get(cat, 'other')
        sga[mapped] = sga.get(mapped, 0) + round(amt)

    sga['total'] = sum(v for k, v in sga.items() if k != 'total')
    sga['by_category'] = dict(by_category)

    return sga


# ──────────────────────────────────────────────
#  월별 손익 계산 (메인)
# ──────────────────────────────────────────────

def calculate_monthly_pnl(db, year_month):
    """관리 손익표 월별 계산 (메인 오케스트레이터).

    Args:
        db: SupabaseDB instance
        year_month: '2026-03' 형식

    Returns:
        dict: 손익표 전체 데이터
    """
    date_from, date_to = _month_range(year_month)

    # 1. 매출
    revenue = _calc_revenue(db, date_from, date_to)

    # 2. 매출원가
    cogs = _calc_cogs(db, date_from, date_to, revenue['total_qty'])

    # 3. 매출총이익
    gross_profit = revenue['total'] - cogs['total']
    gross_margin = _safe_pct(gross_profit, revenue['total'])

    # 4. 판매관리비
    sga = _calc_sga(db, year_month, revenue['total_commission'])

    # 5. 영업이익
    operating_profit = gross_profit - sga['total']
    operating_margin = _safe_pct(operating_profit, revenue['total'])

    # 6. 전월 비교
    prev_ym = _prev_month(year_month)
    prev_pnl = _calc_prev_month_summary(db, prev_ym)
    prev_month_comparison = {
        'revenue_change': _change_pct(revenue['total'], prev_pnl.get('revenue', 0)),
        'profit_change': _change_pct(operating_profit, prev_pnl.get('operating_profit', 0)),
        'gross_profit_change': _change_pct(gross_profit, prev_pnl.get('gross_profit', 0)),
        'prev_revenue': prev_pnl.get('revenue', 0),
        'prev_operating_profit': prev_pnl.get('operating_profit', 0),
        'prev_gross_profit': prev_pnl.get('gross_profit', 0),
    }

    return {
        'year_month': year_month,
        'revenue': {
            'total': revenue['total'],
            'by_channel': revenue['by_channel'],
            'by_category': revenue['by_category'],
        },
        'cogs': cogs,
        'gross_profit': gross_profit,
        'gross_margin': gross_margin,
        'sga': sga,
        'operating_profit': operating_profit,
        'operating_margin': operating_margin,
        'prev_month_comparison': prev_month_comparison,
    }


def _calc_prev_month_summary(db, prev_ym):
    """전월 요약 (비교용). 간단 버전."""
    try:
        date_from, date_to = _month_range(prev_ym)
        revenue = _calc_revenue(db, date_from, date_to)
        cogs = _calc_cogs(db, date_from, date_to, revenue['total_qty'])
        gross_profit = revenue['total'] - cogs['total']
        sga = _calc_sga(db, prev_ym, revenue['total_commission'])
        operating_profit = gross_profit - sga['total']
        return {
            'revenue': revenue['total'],
            'gross_profit': gross_profit,
            'operating_profit': operating_profit,
        }
    except Exception as e:
        logger.warning(f"[P&L] 전월({prev_ym}) 요약 계산 실패: {e}")
        return {'revenue': 0, 'gross_profit': 0, 'operating_profit': 0}


# ──────────────────────────────────────────────
#  채널별 손익
# ──────────────────────────────────────────────

def calculate_channel_pnl(db, year_month):
    """채널별 손익 분석.

    각 채널별로:
      매출 - 수수료 - 배송비 - 포장비 = 채널 기여이익

    Returns:
        dict: {
            'year_month': str,
            'channels': [
                {
                    'channel': str,
                    'revenue': int,
                    'commission': int,
                    'shipping': int,
                    'packaging': int,
                    'other_cost': int,
                    'channel_profit': int,
                    'profit_margin': float,
                }
            ],
            'total': { ... 합계 ... }
        }
    """
    from services.revenue_service import _resolve_channel

    date_from, date_to = _month_range(year_month)

    # 매출 데이터
    raw = db.query_revenue(date_from=date_from, date_to=date_to)

    # 채널별 비용 설정
    channel_costs = db.query_channel_costs()

    # 채널별 집계
    ch_data = defaultdict(lambda: {
        'revenue': 0, 'commission': 0, 'qty': 0,
    })

    for r in raw:
        ch = _resolve_channel(r)
        ch_data[ch]['revenue'] += r.get('revenue', 0) or 0
        ch_data[ch]['commission'] += r.get('commission', 0) or 0
        ch_data[ch]['qty'] += r.get('qty', 0) or 0

    channels = []
    total_revenue = 0
    total_commission = 0
    total_shipping = 0
    total_packaging = 0
    total_other = 0
    total_profit = 0

    for ch_name in sorted(ch_data.keys(), key=lambda x: -ch_data[x]['revenue']):
        data = ch_data[ch_name]
        rev = data['revenue']
        comm = data['commission']
        qty = data['qty']

        # channel_costs에서 배송비/포장비 가져오기
        cost_info = channel_costs.get(ch_name, {})
        shipping_per_unit = float(cost_info.get('shipping', 0))
        packaging_per_unit = float(cost_info.get('packaging', 0))
        other_per_unit = float(cost_info.get('other_cost', 0))

        shipping = round(shipping_per_unit * qty)
        packaging = round(packaging_per_unit * qty)
        other_cost = round(other_per_unit * qty)

        channel_profit = rev - comm - shipping - packaging - other_cost
        profit_margin = _safe_pct(channel_profit, rev)

        channels.append({
            'channel': ch_name,
            'revenue': rev,
            'commission': comm,
            'shipping': shipping,
            'packaging': packaging,
            'other_cost': other_cost,
            'channel_profit': channel_profit,
            'profit_margin': profit_margin,
        })

        total_revenue += rev
        total_commission += comm
        total_shipping += shipping
        total_packaging += packaging
        total_other += other_cost
        total_profit += channel_profit

    return {
        'year_month': year_month,
        'channels': channels,
        'total': {
            'revenue': total_revenue,
            'commission': total_commission,
            'shipping': total_shipping,
            'packaging': total_packaging,
            'other_cost': total_other,
            'channel_profit': total_profit,
            'profit_margin': _safe_pct(total_profit, total_revenue),
        },
    }


# ──────────────────────────────────────────────
#  월별 추이 (최근 N개월)
# ──────────────────────────────────────────────

def calculate_pnl_trend(db, months=6):
    """최근 N개월 손익 추이.

    Args:
        db: SupabaseDB instance
        months: 조회할 개월 수 (기본 6)

    Returns:
        dict: {
            'months': ['2025-10', '2025-11', ...],
            'revenue': [N, N, ...],
            'cogs': [N, N, ...],
            'gross_profit': [N, N, ...],
            'sga': [N, N, ...],
            'operating_profit': [N, N, ...],
            'gross_margin': [%, %, ...],
            'operating_margin': [%, %, ...],
        }
    """
    from services.tz_utils import today_kst

    today = today_kst()
    current = datetime.strptime(today, '%Y-%m-%d')

    result = {
        'months': [],
        'revenue': [],
        'cogs': [],
        'gross_profit': [],
        'sga': [],
        'operating_profit': [],
        'gross_margin': [],
        'operating_margin': [],
    }

    for i in range(months - 1, -1, -1):
        # i개월 전 계산
        y = current.year
        m = current.month - i
        while m <= 0:
            m += 12
            y -= 1
        ym = f"{y}-{m:02d}"

        try:
            pnl = calculate_monthly_pnl(db, ym)
            result['months'].append(ym)
            result['revenue'].append(pnl['revenue']['total'])
            result['cogs'].append(pnl['cogs']['total'])
            result['gross_profit'].append(pnl['gross_profit'])
            result['sga'].append(pnl['sga']['total'])
            result['operating_profit'].append(pnl['operating_profit'])
            result['gross_margin'].append(pnl['gross_margin'])
            result['operating_margin'].append(pnl['operating_margin'])
        except Exception as e:
            logger.warning(f"[P&L] {ym} 추이 계산 실패: {e}")
            result['months'].append(ym)
            result['revenue'].append(0)
            result['cogs'].append(0)
            result['gross_profit'].append(0)
            result['sga'].append(0)
            result['operating_profit'].append(0)
            result['gross_margin'].append(0)
            result['operating_margin'].append(0)

    return result
