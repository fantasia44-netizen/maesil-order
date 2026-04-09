"""
pnl_service.py -- 관리 손익표(P&L) 비즈니스 로직.
매출, 매출원가(COGS), 판매관리비(SGA)를 종합하여
월별/채널별 손익을 계산한다.

데이터 소스 (v2):
  - 매출: api_settlements(온라인) + tax_invoices(거래처) → 폴백: order_transactions
  - 매출원가: tax_invoices(매입) → 폴백: actual_cost + BOM 추정
  - 판관비: api_settlements(수수료/광고비) + expenses
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
        '운반비': 'shipping',
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
#  v2: 정산서/세금계산서 기반 집계
# ──────────────────────────────────────────────

def _fetch_month_data(db, date_from, date_to, year_month):
    """한 달치 데이터 일괄 조회 — SQL RPC 1회 (Phase 1 OOM 차단).

    구 버전: 4개 테이블 Python 풀스캔 + defaultdict 4중 집계 (피크 ~6MB/월).
    신 버전: get_pnl_monthly_agg RPC 1회 호출, JSONB 파싱만 수행.
    """
    try:
        res = db.client.rpc('get_pnl_monthly_agg', {
            'p_date_from': date_from,
            'p_date_to': date_to,
            'p_year_month': year_month,
        }).execute()
        agg = res.data or {}
        if isinstance(agg, list):
            agg = agg[0] if agg else {}
    except Exception as e:
        logger.warning(f"[P&L] RPC get_pnl_monthly_agg 실패, 폴백 사용: {e}")
        agg = None

    if agg:
        return {'_rpc': agg}

    # 폴백: 기존 Python 경로
    return {
        '_rpc': None,
        'settlements': db.query_api_settlements(date_from=date_from, date_to=date_to) or [],
        'tax_sales': db.query_tax_invoices(direction='sales',
                                           date_from=date_from, date_to=date_to) or [],
        'tax_purchases': db.query_tax_invoices(direction='purchase',
                                               date_from=date_from, date_to=date_to) or [],
        'expenses': db.query_expenses(month=year_month) or [],
    }


# 정산서 파일 prefix (marketplace.py 참조)
_SETTLE_PREFIXES = (
    'nsettle_', 'wsettle_', 'rocket_', '11settle_',
    'tsettle_', 'osettle_', 'auction_', 'gmarket_',
)


def _calc_revenue_v2(db, date_from, date_to, data):
    """매출 집계 v2: RPC JSONB 우선, 폴백은 Python.

    RPC는 정산서 prefix 필터 + 플랫폼 거래처 제외까지 SQL에서 처리.
    """
    agg = data.get('_rpc')
    if agg:
        rev = agg.get('revenue') or {}
        b2b = agg.get('b2b') or {}
        online_total = int(rev.get('online_total', 0) or 0)
        online_commission = int(rev.get('online_commission', 0) or 0)
        b2b_total = int(b2b.get('b2b_total', 0) or 0)
        by_channel = {k: int(v or 0) for k, v in (rev.get('by_channel') or {}).items()}
        b2b_by_vendor = {k: int(v or 0) for k, v in (b2b.get('by_vendor') or {}).items()}

        # 데이터 없으면 기존 폴백 경로
        if online_total == 0 and b2b_total == 0:
            legacy = _calc_revenue(db, date_from, date_to)
            legacy['data_source'] = 'legacy'
            legacy['online_total'] = legacy['total']
            legacy['b2b_total'] = 0
            legacy['b2b_by_vendor'] = {}
            return legacy

        return {
            'total': online_total + b2b_total,
            'online_total': online_total,
            'b2b_total': b2b_total,
            'by_channel': by_channel,
            'b2b_by_vendor': b2b_by_vendor,
            'total_commission': online_commission,
            'total_qty': 0,
            'data_source': 'settlement_rpc',
        }

    # ── 폴백: Python 풀스캔 (RPC 실패 시에만) ──
    settlements = data.get('settlements', [])
    tax_sales = data.get('tax_sales', [])

    online_total = 0
    online_commission = 0
    online_by_channel = defaultdict(int)

    for s in settlements:
        sid = s.get('settlement_id', '')
        if not any(sid.startswith(p) for p in _SETTLE_PREFIXES):
            continue
        gross = int(s.get('gross_sales') or 0)
        comm = int(s.get('total_commission') or 0)
        ch = s.get('channel', '기타')
        online_total += gross
        online_commission += comm
        online_by_channel[ch] += gross

    _PLATFORM_BUYERS = {'쿠팡(주)', '쿠팡주식회사'}
    b2b_total = 0
    b2b_by_vendor = defaultdict(int)
    for inv in tax_sales:
        if inv.get('status') == 'cancelled':
            continue
        amt = int(inv.get('supply_cost_total') or inv.get('supply_amount') or 0)
        vendor = (inv.get('buyer_corp_name') or inv.get('vendor_name') or '기타')
        if vendor in _PLATFORM_BUYERS:
            continue
        b2b_total += amt
        b2b_by_vendor[vendor] += amt

    if online_total == 0 and b2b_total == 0:
        legacy = _calc_revenue(db, date_from, date_to)
        legacy['data_source'] = 'legacy'
        legacy['online_total'] = legacy['total']
        legacy['b2b_total'] = 0
        legacy['b2b_by_vendor'] = {}
        return legacy

    return {
        'total': online_total + b2b_total,
        'online_total': online_total,
        'b2b_total': b2b_total,
        'by_channel': dict(online_by_channel),
        'b2b_by_vendor': dict(b2b_by_vendor),
        'total_commission': online_commission,
        'total_qty': 0,
        'data_source': 'settlement',
    }


def _calc_cogs_v2(db, date_from, date_to, data, revenue_qty=0):
    """매출원가 v2: RPC JSONB 우선, 폴백 Python."""
    agg = data.get('_rpc')
    if agg:
        pur = agg.get('purchase') or {}
        purchase_total = int(pur.get('purchase_total', 0) or 0)
        by_vendor = {k: int(v or 0) for k, v in (pur.get('by_vendor') or {}).items()}

        if purchase_total == 0:
            legacy = _calc_cogs(db, date_from, date_to, revenue_qty)
            legacy['data_source'] = 'legacy'
            legacy['purchase_invoice_total'] = 0
            legacy['by_vendor'] = {}
            return legacy

        return {
            'total': purchase_total,
            'purchase_invoice_total': purchase_total,
            'by_vendor': by_vendor,
            'actual_cost': 0,
            'estimated_cost': 0,
            'data_source': 'tax_invoice_rpc',
        }

    # ── 폴백: Python 풀스캔 ──
    tax_purchases = data.get('tax_purchases', [])
    purchase_total = 0
    by_vendor = defaultdict(int)
    for inv in tax_purchases:
        if inv.get('status') == 'cancelled':
            continue
        amt = int(inv.get('supply_cost_total') or inv.get('supply_amount') or 0)
        vendor = (inv.get('supplier_corp_name') or inv.get('vendor_name') or '기타')
        purchase_total += amt
        by_vendor[vendor] += amt

    if purchase_total == 0:
        legacy = _calc_cogs(db, date_from, date_to, revenue_qty)
        legacy['data_source'] = 'legacy'
        legacy['purchase_invoice_total'] = 0
        legacy['by_vendor'] = {}
        return legacy

    return {
        'total': purchase_total,
        'purchase_invoice_total': purchase_total,
        'by_vendor': dict(by_vendor),
        'actual_cost': 0,
        'estimated_cost': 0,
        'data_source': 'tax_invoice',
    }


def _calc_sga_v2(data, commission_from_revenue):
    """판관비 v2: expenses 테이블만 사용.

    수수료/광고비는 매입 세금계산서에 포함되어 COGS에 이미 반영되므로
    판관비에서는 제외. expenses 테이블의 인건비·임차료 등만 집계.
    정산서 수수료/광고비는 참고용으로만 기록 (합산 안 함).

    카테고리 parent 분류:
      판관비: 인건비, 임차료, 세금과공과, 복리후생비, 보험료, 운반비, 지급수수료, 기타
      제조경비: 연구개발비
      영업외: 이자비용
    """
    agg = data.get('_rpc')
    if agg:
        # RPC 경로: expenses by_category + ad_cost by_channel 직접 사용
        ref_commission = round(commission_from_revenue)
        ad_info = agg.get('ad_cost') or {}
        ref_ad_cost = int(ad_info.get('total_ad_cost', 0) or 0)
        ad_by_channel = {k: int(v or 0) for k, v in (ad_info.get('by_channel') or {}).items()}

        raw_cats = (agg.get('expenses') or {}).get('by_category') or {}
    else:
        # 폴백: Python 풀스캔
        settlements = data.get('settlements', [])
        expense_rows = data.get('expenses', [])

        ref_commission = round(commission_from_revenue)
        ref_ad_cost = 0
        ad_by_channel_dd = defaultdict(int)
        for s in settlements:
            sid = s.get('settlement_id', '')
            if sid.startswith('ad_cost_'):
                cost = int(s.get('other_deductions') or 0)
                ch = s.get('channel', '기타')
                ref_ad_cost += cost
                ad_by_channel_dd[ch] += cost
        ad_by_channel = dict(ad_by_channel_dd)

        raw_cats = defaultdict(float)
        for row in expense_rows:
            cat = row.get('category', '기타')
            amt = float(row.get('amount', 0))
            raw_cats[cat] += amt

    # 영업외비용 카테고리 (판관비에서 분리)
    _NON_OPERATING = {'이자비용'}
    # 제조경비 카테고리
    _MANUFACTURING = {'연구개발비'}

    by_category = defaultdict(float)   # 판관비
    by_mfg = defaultdict(float)        # 제조경비
    by_non_op = defaultdict(float)     # 영업외비용
    for cat, amt in raw_cats.items():
        amt_f = float(amt or 0)
        if cat in _NON_OPERATING:
            by_non_op[cat] += amt_f
        elif cat in _MANUFACTURING:
            by_mfg[cat] += amt_f
        else:
            by_category[cat] += amt_f

    total_sga = sum(round(v) for v in by_category.values())
    total_mfg = sum(round(v) for v in by_mfg.values())
    total_non_op = sum(round(v) for v in by_non_op.values())

    sga = {
        'total': total_sga + total_mfg,  # 판관비+제조경비 (영업이익 계산용)
        'by_category': {k: round(v) for k, v in by_category.items()},
        'by_manufacturing': {k: round(v) for k, v in by_mfg.items()},
        'total_manufacturing': total_mfg,
        'non_operating': {k: round(v) for k, v in by_non_op.items()},
        'total_non_operating': total_non_op,
        'ad_by_channel': dict(ad_by_channel),
        # 참고용 (UI에서 매출원가 내역 표시에 활용)
        'ref_commission': ref_commission,
        'ref_ad_cost': ref_ad_cost,
    }

    return sga


# ──────────────────────────────────────────────
#  월별 손익 계산 (메인)
# ──────────────────────────────────────────────

def calculate_monthly_pnl(db, year_month):
    """관리 손익표 월별 계산 (v2: 정산서/세금계산서 우선).

    Args:
        db: SupabaseDB instance
        year_month: '2026-03' 형식

    Returns:
        dict: 손익표 전체 데이터
    """
    date_from, date_to = _month_range(year_month)

    # 0. 한 달치 데이터 일괄 조회
    data = _fetch_month_data(db, date_from, date_to, year_month)

    # 1. 매출 (v2: 정산서 + 세금계산서)
    revenue = _calc_revenue_v2(db, date_from, date_to, data)

    # 2. 매출원가 (v2: 매입 세금계산서)
    cogs = _calc_cogs_v2(db, date_from, date_to, data, revenue.get('total_qty', 0))

    # 3. 매출총이익
    gross_profit = revenue['total'] - cogs['total']
    gross_margin = _safe_pct(gross_profit, revenue['total'])

    # 4. 판매관리비 (v2: 정산서 수수료/광고비 + expenses)
    sga = _calc_sga_v2(data, revenue['total_commission'])

    # 5. 영업이익
    operating_profit = gross_profit - sga['total']
    operating_margin = _safe_pct(operating_profit, revenue['total'])

    # 6. 영업외비용 → 당기순이익
    non_operating_total = sga.get('total_non_operating', 0)
    net_profit = operating_profit - non_operating_total

    # 7. 전월 비교
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
            'online_total': revenue.get('online_total', revenue['total']),
            'b2b_total': revenue.get('b2b_total', 0),
            'by_channel': revenue.get('by_channel', {}),
            'b2b_by_vendor': revenue.get('b2b_by_vendor', {}),
            'data_source': revenue.get('data_source', 'legacy'),
        },
        'cogs': cogs,
        'gross_profit': gross_profit,
        'gross_margin': gross_margin,
        'sga': sga,
        'operating_profit': operating_profit,
        'net_profit': net_profit,
        'operating_margin': operating_margin,
        'prev_month_comparison': prev_month_comparison,
    }


def _calc_prev_month_summary(db, prev_ym):
    """전월 요약 (비교용). v2 사용."""
    try:
        date_from, date_to = _month_range(prev_ym)
        data = _fetch_month_data(db, date_from, date_to, prev_ym)
        revenue = _calc_revenue_v2(db, date_from, date_to, data)
        cogs = _calc_cogs_v2(db, date_from, date_to, data, revenue.get('total_qty', 0))
        gross_profit = revenue['total'] - cogs['total']
        sga = _calc_sga_v2(data, revenue['total_commission'])
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
    """채널별 손익 분석 (v2: api_settlements 우선).

    각 채널별로:
      매출 - 수수료 - 광고비 = 채널 기여이익
    """
    date_from, date_to = _month_range(year_month)
    data = _fetch_month_data(db, date_from, date_to, year_month)

    agg = data.get('_rpc')
    if agg:
        # RPC 경로 — 풀스캔 없이 JSONB 집계만 사용
        rev = agg.get('revenue') or {}
        ad = agg.get('ad_cost') or {}
        b2b = agg.get('b2b') or {}
        rev_by_ch = {k: int(v or 0) for k, v in (rev.get('by_channel') or {}).items()}
        comm_by_ch = {k: int(v or 0) for k, v in (rev.get('commission_by_channel') or {}).items()}
        ad_by_ch = {k: int(v or 0) for k, v in (ad.get('by_channel') or {}).items()}

        ch_data = defaultdict(lambda: {'revenue': 0, 'commission': 0, 'ad_cost': 0})
        for ch, amt in rev_by_ch.items():
            ch_data[ch]['revenue'] += amt
        for ch, amt in comm_by_ch.items():
            ch_data[ch]['commission'] += amt
        for ch, amt in ad_by_ch.items():
            ch_data[ch]['ad_cost'] += amt
        for vendor, amt in (b2b.get('by_vendor') or {}).items():
            ch_data[f'거래처({vendor})']['revenue'] += int(amt or 0)

        has_data = bool(ch_data)
    else:
        # ── 폴백: Python 풀스캔 ──
        settlements = data.get('settlements', [])
        settle_rows = [s for s in settlements
                       if any(s.get('settlement_id', '').startswith(p)
                              for p in _SETTLE_PREFIXES)]
        has_data = bool(settle_rows or [s for s in settlements
                                         if s.get('settlement_id', '').startswith('ad_cost_')])
        ch_data = defaultdict(lambda: {'revenue': 0, 'commission': 0, 'ad_cost': 0})
        if has_data:
            for s in settlements:
                sid = s.get('settlement_id', '')
                ch = s.get('channel', '기타')
                if sid.startswith('ad_cost_'):
                    ch_data[ch]['ad_cost'] += int(s.get('other_deductions') or 0)
                elif any(sid.startswith(p) for p in _SETTLE_PREFIXES):
                    ch_data[ch]['revenue'] += int(s.get('gross_sales') or 0)
                    ch_data[ch]['commission'] += int(s.get('total_commission') or 0)
            _PLATFORM_BUYERS = {'쿠팡(주)', '쿠팡주식회사'}
            for inv in data.get('tax_sales', []):
                if inv.get('status') == 'cancelled':
                    continue
                vendor = inv.get('buyer_corp_name') or inv.get('vendor_name') or '거래처'
                if vendor in _PLATFORM_BUYERS:
                    continue
                amt = int(inv.get('supply_cost_total') or inv.get('supply_amount') or 0)
                ch_data[f'거래처({vendor})']['revenue'] += amt

    if has_data:
        channels = []
        total_rev = total_comm = total_ad = total_profit = 0

        for ch_name in sorted(ch_data.keys(), key=lambda x: -ch_data[x]['revenue']):
            d = ch_data[ch_name]
            rev = d['revenue']
            comm = d['commission']
            ad = d['ad_cost']
            profit = rev - comm - ad
            margin = _safe_pct(profit, rev)

            channels.append({
                'channel': ch_name,
                'revenue': rev,
                'commission': comm,
                'shipping': 0, 'packaging': 0,
                'other_cost': ad,
                'channel_profit': profit,
                'profit_margin': margin,
            })
            total_rev += rev
            total_comm += comm
            total_ad += ad
            total_profit += profit

        return {
            'year_month': year_month,
            'channels': channels,
            'total': {
                'revenue': total_rev,
                'commission': total_comm,
                'shipping': 0, 'packaging': 0,
                'other_cost': total_ad,
                'channel_profit': total_profit,
                'profit_margin': _safe_pct(total_profit, total_rev),
            },
        }

    # 폴백: 기존 order_transactions 기반
    from services.revenue_service import _resolve_channel

    raw = db.query_revenue(date_from=date_from, date_to=date_to)
    channel_costs = db.query_channel_costs()

    ch_data = defaultdict(lambda: {'revenue': 0, 'commission': 0, 'qty': 0})
    for r in raw:
        ch = _resolve_channel(r)
        ch_data[ch]['revenue'] += r.get('revenue', 0) or 0
        ch_data[ch]['commission'] += r.get('commission', 0) or 0
        ch_data[ch]['qty'] += r.get('qty', 0) or 0

    channels = []
    total_revenue = total_commission = total_shipping = 0
    total_packaging = total_other = total_profit = 0

    for ch_name in sorted(ch_data.keys(), key=lambda x: -ch_data[x]['revenue']):
        d = ch_data[ch_name]
        rev, comm, qty = d['revenue'], d['commission'], d['qty']
        cost_info = channel_costs.get(ch_name, {})
        shipping = round(float(cost_info.get('shipping', 0)) * qty)
        packaging = round(float(cost_info.get('packaging', 0)) * qty)
        other_cost = round(float(cost_info.get('other_cost', 0)) * qty)
        profit = rev - comm - shipping - packaging - other_cost

        channels.append({
            'channel': ch_name, 'revenue': rev, 'commission': comm,
            'shipping': shipping, 'packaging': packaging,
            'other_cost': other_cost, 'channel_profit': profit,
            'profit_margin': _safe_pct(profit, rev),
        })
        total_revenue += rev; total_commission += comm
        total_shipping += shipping; total_packaging += packaging
        total_other += other_cost; total_profit += profit

    return {
        'year_month': year_month,
        'channels': channels,
        'total': {
            'revenue': total_revenue, 'commission': total_commission,
            'shipping': total_shipping, 'packaging': total_packaging,
            'other_cost': total_other, 'channel_profit': total_profit,
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
        'non_operating': [],
        'net_profit': [],
        'gross_margin': [],
        'operating_margin': [],
        'net_margin': [],
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
            rev_total = pnl['revenue']['total']
            result['revenue'].append(rev_total)
            result['cogs'].append(pnl['cogs']['total'])
            result['gross_profit'].append(pnl['gross_profit'])
            result['sga'].append(pnl['sga']['total'])
            result['operating_profit'].append(pnl['operating_profit'])
            result['non_operating'].append(pnl['sga'].get('total_non_operating', 0))
            result['net_profit'].append(pnl.get('net_profit', pnl['operating_profit']))
            result['gross_margin'].append(pnl['gross_margin'])
            result['operating_margin'].append(pnl['operating_margin'])
            result['net_margin'].append(round(pnl.get('net_profit', pnl['operating_profit']) / rev_total * 100, 1) if rev_total else 0)
        except Exception as e:
            logger.warning(f"[P&L] {ym} 추이 계산 실패: {e}")
            result['months'].append(ym)
            result['revenue'].append(0)
            result['cogs'].append(0)
            result['gross_profit'].append(0)
            result['sga'].append(0)
            result['operating_profit'].append(0)
            result['non_operating'].append(0)
            result['net_profit'].append(0)
            result['gross_margin'].append(0)
            result['operating_margin'].append(0)
            result['net_margin'].append(0)

    return result
