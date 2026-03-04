"""
services/planning_service.py — 수량 기반 생산계획 엔진 (Production Planning v1).

현재 재고 + 최근 출고 추세 → 품목별 권장 생산량 산출.
BOM 연동 미포함 (v2에서 확장 예정).

사용법:
    from services.planning_service import calculate_production_plan, get_plan_history

    plan = calculate_production_plan(db)
    # plan: {items: [...], summary: {...}, generated_at: str}
"""
from collections import defaultdict
from datetime import datetime, timedelta

from services.tz_utils import today_kst, days_ago_kst


# ═══════════════════════════════════════════════════════════════
# 상수
# ═══════════════════════════════════════════════════════════════

# 판매(수요)로 간주하는 출고 타입
DEMAND_TYPES = ('SALES_OUT', 'SET_OUT', 'ETC_OUT')

# 기본값
DEFAULT_SAFETY_STOCK = 0
DEFAULT_LEAD_TIME_DAYS = 3
DEFAULT_SALES_WINDOW = 7  # 최근 N일 평균

# 소진일수 기준 상태
STATUS_THRESHOLDS = {
    'critical': 3,   # 3일 이하: 부족
    'warning': 7,    # 7일 이하: 주의
}


# ═══════════════════════════════════════════════════════════════
# 생산 대상 품목 조회
# ═══════════════════════════════════════════════════════════════

def _get_production_targets(db):
    """생산 대상 완제품 목록 조회.

    product_costs에서:
    - cost_type='생산' 또는 material_type='완제품'
    - is_production_target=True (컬럼 있으면)

    Returns:
        dict: {product_name: {safety_stock, lead_time_days, unit, ...}}
    """
    cost_map = db.query_product_costs()
    targets = {}

    for name, info in cost_map.items():
        cost_type = info.get('cost_type', '')
        material_type = info.get('material_type', '')
        food_type = info.get('food_type', '') or ''
        is_target = info.get('is_production_target')

        # is_production_target 컬럼이 명시적으로 False면 제외
        if is_target is False:
            continue

        # 기본: cost_type='생산'인 품목만 (매입/OEM 제외)
        # is_production_target=True면 cost_type 무관하게 포함
        if is_target is not True:
            if cost_type != '생산':
                continue

        targets[name] = {
            'safety_stock': int(info.get('safety_stock', 0) or DEFAULT_SAFETY_STOCK),
            'lead_time_days': int(info.get('lead_time_days', 0) or DEFAULT_LEAD_TIME_DAYS),
            'unit': info.get('unit', '개') or '개',
            'material_type': material_type,
            'cost_type': cost_type,
            'food_type': food_type,
        }

    return targets


# ═══════════════════════════════════════════════════════════════
# 판매량(수요) 집계
# ═══════════════════════════════════════════════════════════════

def _get_sales_data(db, days=DEFAULT_SALES_WINDOW):
    """최근 N일 출고 데이터 품목별 합산.

    SALES_OUT, SET_OUT, ETC_OUT 기준 (qty는 음수로 저장됨).

    Returns:
        dict: {product_name: total_qty_sold}  (양수 반환)
    """
    date_from = days_ago_kst(days)
    date_to = today_kst()

    try:
        data = db.query_stock_ledger(
            date_from=date_from,
            date_to=date_to,
            type_list=list(DEMAND_TYPES),
        )
    except Exception:
        data = []

    sales = defaultdict(float)
    for r in data:
        name = r.get('product_name', '')
        qty = float(r.get('qty', 0) or 0)
        if name:
            # stock_ledger는 공백제거 저장, 원본 키 + 정규화 키 모두 등록
            sales[name] += abs(qty)
            norm = name.replace(' ', '')
            if norm != name:
                sales[norm] += abs(qty)

    return dict(sales)


# ═══════════════════════════════════════════════════════════════
# 현재 재고 계산 (전체 창고 합산)
# ═══════════════════════════════════════════════════════════════

def _get_current_stock(db):
    """품목별 현재 재고 (전체 창고 합산).

    Returns:
        dict: {product_name: total_qty}
    """
    date_to = today_kst()

    try:
        data = db.query_stock_ledger(date_to=date_to)
    except Exception:
        data = []

    stock = defaultdict(float)
    for r in data:
        name = r.get('product_name', '')
        qty = float(r.get('qty', 0) or 0)
        if name:
            stock[name] += qty

    return dict(stock)


# ═══════════════════════════════════════════════════════════════
# 생산계획 계산
# ═══════════════════════════════════════════════════════════════

def calculate_production_plan(db, sales_window=DEFAULT_SALES_WINDOW,
                              critical_days=None, warning_days=None,
                              save=True):
    """생산계획 계산 메인 함수.

    Args:
        db: SupabaseDB instance
        sales_window: 평균 판매량 계산 기간 (일)
        critical_days: '부족' 판정 기준일 (기본 3)
        warning_days: '주의' 판정 기준일 (기본 7)
        save: True이면 production_plan 테이블에 저장

    Returns:
        dict: {
            items: [{product_name, current_stock, avg_daily_sales, depletion_days,
                     safety_stock, lead_time_days, target_stock,
                     recommended_production, status, unit}],
            summary: {total_targets, need_production, critical, warning, stable},
            generated_at: str
        }
    """
    if critical_days is None:
        critical_days = STATUS_THRESHOLDS['critical']
    if warning_days is None:
        warning_days = STATUS_THRESHOLDS['warning']
    # warning은 critical보다 커야 함
    if warning_days <= critical_days:
        warning_days = critical_days + 4

    # 1. 데이터 수집 (DB 호출 최소화 — 3회)
    targets = _get_production_targets(db)
    sales_data = _get_sales_data(db, days=sales_window)
    stock_data = _get_current_stock(db)

    # 2. 품목별 계산
    items = []
    for name, config in targets.items():
        # stock_ledger는 공백제거 저장 → 정규화 키로도 조회
        norm_name = name.replace(' ', '')
        current_stock = stock_data.get(name, 0) or stock_data.get(norm_name, 0)
        total_sold = sales_data.get(name, 0) or sales_data.get(norm_name, 0)

        # 일평균 판매량 (0 division 방지)
        avg_daily = total_sold / sales_window if sales_window > 0 else 0

        # 소진일수
        if avg_daily > 0:
            depletion_days = current_stock / avg_daily
        else:
            depletion_days = None  # 판매 없음

        # 목표재고 = 한달치 (일평균 × 30일)
        safety = config['safety_stock']
        lead_time = config['lead_time_days']
        target_stock = avg_daily * 30

        # 권장생산량 = 한달치 - 현재재고
        if avg_daily == 0:
            recommended = 0
        else:
            recommended = target_stock - current_stock
            if recommended < 0:
                recommended = 0

        # 상태 판정 (사용자 지정 기준일)
        if depletion_days is None:
            status = '미판매'
        elif depletion_days <= critical_days:
            status = '부족'
        elif depletion_days <= warning_days:
            status = '주의'
        else:
            status = '안정'

        items.append({
            'product_name': name,
            'current_stock': round(current_stock, 1),
            'avg_daily_sales': round(avg_daily, 1),
            'total_sold_period': round(total_sold, 1),
            'depletion_days': round(depletion_days, 1) if depletion_days is not None else None,
            'safety_stock': safety,
            'lead_time_days': lead_time,
            'target_stock': round(target_stock, 1),
            'recommended_production': round(max(recommended, 0), 1),
            'status': status,
            'unit': config['unit'],
            'material_type': config.get('material_type', ''),
            'food_type': config.get('food_type', ''),
        })

    # 3. 정렬: 부족 → 주의 → 안정 → 미판매, 소진일수 오름차순
    status_order = {'부족': 0, '주의': 1, '안정': 2, '미판매': 3}
    items.sort(key=lambda x: (
        status_order.get(x['status'], 9),
        x['depletion_days'] if x['depletion_days'] is not None else 99999,
    ))

    # 4. 요약
    critical = sum(1 for i in items if i['status'] == '부족')
    warning = sum(1 for i in items if i['status'] == '주의')
    stable = sum(1 for i in items if i['status'] == '안정')
    unsold = sum(1 for i in items if i['status'] == '미판매')
    need_prod = sum(1 for i in items if i['recommended_production'] > 0)

    summary = {
        'total_targets': len(items),
        'need_production': need_prod,
        'critical': critical,
        'warning': warning,
        'stable': stable,
        'unsold': unsold,
        'sales_window': sales_window,
        'critical_days': critical_days,
        'warning_days': warning_days,
    }

    result = {
        'items': items,
        'summary': summary,
        'generated_at': datetime.now().isoformat(),
        'plan_date': today_kst(),
    }

    # 5. DB 저장
    if save:
        _save_plan(db, result)

    return result


# ═══════════════════════════════════════════════════════════════
# DB 저장 / 조회
# ═══════════════════════════════════════════════════════════════

def _save_plan(db, result):
    """생산계획 결과를 production_plan 테이블에 upsert."""
    import json
    plan_date = result['plan_date']

    payload = []
    for item in result['items']:
        payload.append({
            'plan_date': plan_date,
            'product_name': item['product_name'],
            'current_stock': item['current_stock'],
            'avg_daily_sales': item['avg_daily_sales'],
            'depletion_days': item['depletion_days'],
            'safety_stock': item['safety_stock'],
            'lead_time_days': item['lead_time_days'],
            'target_stock': item['target_stock'],
            'recommended_production': item['recommended_production'],
            'status': item['status'],
            'unit': item['unit'],
        })

    if not payload:
        return

    try:
        # 해당 날짜 기존 계획 삭제 후 삽입 (upsert 대용)
        db.client.table('production_plan').delete() \
            .eq('plan_date', plan_date).execute()
        # 배치 삽입
        for i in range(0, len(payload), 200):
            db.client.table('production_plan').insert(
                payload[i:i+200]).execute()
    except Exception as e:
        print(f"[PlanningService] 저장 실패: {e}")


def get_plan_history(db, limit=30):
    """최근 생산계획 이력 (날짜별 요약).

    Returns:
        list of dict: [{plan_date, total, critical, warning, stable}]
    """
    try:
        res = db.client.table('production_plan') \
            .select('plan_date, status') \
            .order('plan_date', desc=True) \
            .limit(limit * 50) \
            .execute()

        # 날짜별 그룹
        by_date = defaultdict(lambda: {'total': 0, 'critical': 0, 'warning': 0, 'stable': 0})
        for r in (res.data or []):
            d = r.get('plan_date', '')
            by_date[d]['total'] += 1
            st = r.get('status', '')
            if st == '부족':
                by_date[d]['critical'] += 1
            elif st == '주의':
                by_date[d]['warning'] += 1
            elif st == '안정':
                by_date[d]['stable'] += 1

        result = [{'plan_date': d, **v} for d, v in sorted(by_date.items(), reverse=True)]
        return result[:limit]
    except Exception:
        return []


def get_plan_by_date(db, plan_date):
    """특정 날짜의 생산계획 상세 조회."""
    try:
        res = db.client.table('production_plan') \
            .select('*') \
            .eq('plan_date', plan_date) \
            .order('recommended_production', desc=True) \
            .execute()
        return res.data or []
    except Exception:
        return []


def update_product_planning_config(db, product_name, safety_stock=None,
                                    lead_time_days=None, is_production_target=None):
    """품목별 생산계획 설정 수정.

    product_costs 테이블의 safety_stock, lead_time_days, is_production_target 업데이트.
    """
    update = {}
    if safety_stock is not None:
        update['safety_stock'] = int(safety_stock)
    if lead_time_days is not None:
        update['lead_time_days'] = int(lead_time_days)
    if is_production_target is not None:
        update['is_production_target'] = bool(is_production_target)

    if not update:
        return False

    try:
        db.client.table('product_costs').update(update) \
            .eq('product_name', product_name).execute()
        return True
    except Exception as e:
        print(f"[PlanningService] 설정 수정 실패: {e}")
        return False
