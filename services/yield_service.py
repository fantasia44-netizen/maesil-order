"""
yield_service.py — 수율 분석 서비스.
생산일지(PRODUCTION + PROD_OUT) 데이터 기반으로
제품별 수율, 실제원가, 일별 추이 분석.
"""
from collections import defaultdict


def calculate_yield_summary(db, date_from, date_to, location=None):
    """제품별 수율 요약.

    수율(%) = (BOM 이론원가 / 실제단위원가) × 100
    실제단위원가 = 총 투입비용 / 산출량
    투입비용 = Σ(PROD_OUT수량 × 매입단가)

    Returns:
        dict: {
            'products': [{product_name, total_output, total_input_cost,
                          actual_unit_cost, bom_unit_cost, cost_diff,
                          yield_rate, production_count, materials}],
            'summary': {total_products, avg_yield, total_output, total_cost}
        }
    """
    # 1. 생산 데이터 조회
    prod_data = db.query_stock_ledger(
        date_from=date_from, date_to=date_to,
        type_list=['PRODUCTION', 'PROD_OUT'])

    if location:
        prod_data = [r for r in prod_data if r.get('location', '') == location]

    # 2. 매입단가 로드
    cost_map_raw = db.query_product_costs()
    cost_map = {k: float(v.get('cost_price', 0)) for k, v in cost_map_raw.items()}

    # 3. BOM 이론원가 로드
    bom_cost_map = _load_bom_cost_map(db)

    # 4. 날짜+위치별로 PRODUCTION과 PROD_OUT 그룹핑
    # 같은 날짜+위치의 PRODUCTION → 그 날짜의 PROD_OUT이 해당 제품의 재료
    daily_groups = defaultdict(lambda: {'production': [], 'prod_out': []})
    for r in prod_data:
        d = r.get('transaction_date', '')
        loc = r.get('location', '')
        key = (d, loc)
        if r.get('type') == 'PRODUCTION':
            daily_groups[key]['production'].append(r)
        elif r.get('type') == 'PROD_OUT':
            daily_groups[key]['prod_out'].append(r)

    # 5. 제품별 집계
    product_stats = defaultdict(lambda: {
        'total_output': 0,
        'total_input_cost': 0,
        'production_count': 0,
        'materials': defaultdict(lambda: {'total_qty': 0, 'unit_price': 0, 'total_cost': 0}),
        'daily_data': [],
    })

    for (date, loc), group in daily_groups.items():
        productions = group['production']
        prod_outs = group['prod_out']

        if not productions:
            continue

        # 이 날짜/위치의 총 투입비용
        daily_input_cost = 0
        daily_materials = defaultdict(lambda: {'qty': 0, 'cost': 0})
        for po in prod_outs:
            mat_name = po.get('product_name', '')
            mat_qty = abs(po.get('qty', 0))
            unit_price = cost_map.get(mat_name, 0)
            mat_cost = mat_qty * unit_price
            daily_input_cost += mat_cost
            daily_materials[mat_name]['qty'] += mat_qty
            daily_materials[mat_name]['cost'] += mat_cost

        # 여러 제품이 같은 날 생산되면 산출량 비율로 비용 배분
        total_daily_output = sum(p.get('qty', 0) for p in productions)

        for prod in productions:
            pname = prod.get('product_name', '')
            pqty = prod.get('qty', 0)
            if pqty <= 0:
                continue

            # 비용 비율 배분 (같은 날 여러 제품 생산 시)
            ratio = pqty / total_daily_output if total_daily_output > 0 else 0
            allocated_cost = daily_input_cost * ratio

            stats = product_stats[pname]
            stats['total_output'] += pqty
            stats['total_input_cost'] += allocated_cost
            stats['production_count'] += 1

            # 재료 상세 (비율 배분)
            for mat_name, mat_info in daily_materials.items():
                allocated_qty = mat_info['qty'] * ratio
                allocated_mat_cost = mat_info['cost'] * ratio
                m = stats['materials'][mat_name]
                m['total_qty'] += allocated_qty
                m['unit_price'] = cost_map.get(mat_name, 0)
                m['total_cost'] += allocated_mat_cost

            # 일별 데이터 저장 (추이 차트용)
            stats['daily_data'].append({
                'date': date,
                'output': pqty,
                'input_cost': allocated_cost,
            })

    # 6. 최종 결과 구성
    products = []
    yield_sum = 0
    yield_count = 0

    for pname, stats in sorted(product_stats.items()):
        total_output = stats['total_output']
        total_input_cost = stats['total_input_cost']

        # 실제 단위원가
        actual_unit_cost = total_input_cost / total_output if total_output > 0 else 0

        # BOM 이론원가
        bom_unit_cost = bom_cost_map.get(pname, 0)

        # 원가 차이
        cost_diff = actual_unit_cost - bom_unit_cost if bom_unit_cost > 0 else 0

        # 수율 계산
        if actual_unit_cost > 0 and bom_unit_cost > 0:
            yield_rate = round(bom_unit_cost / actual_unit_cost * 100, 1)
            yield_sum += yield_rate
            yield_count += 1
        else:
            yield_rate = None

        # 재료 상세 리스트
        materials = []
        for mat_name, mat_info in sorted(stats['materials'].items()):
            materials.append({
                'name': mat_name,
                'total_qty': round(mat_info['total_qty'], 1),
                'unit_price': mat_info['unit_price'],
                'total_cost': round(mat_info['total_cost']),
            })

        products.append({
            'product_name': pname,
            'total_output': total_output,
            'total_input_cost': round(total_input_cost),
            'actual_unit_cost': round(actual_unit_cost),
            'bom_unit_cost': round(bom_unit_cost),
            'cost_diff': round(cost_diff),
            'yield_rate': yield_rate,
            'production_count': stats['production_count'],
            'materials': materials,
        })

    # 수율 낮은 순 정렬 (None은 뒤로)
    products.sort(key=lambda x: (x['yield_rate'] is None, x['yield_rate'] or 0))

    avg_yield = round(yield_sum / yield_count, 1) if yield_count > 0 else None

    return {
        'products': products,
        'summary': {
            'total_products': len(products),
            'avg_yield': avg_yield,
            'total_output': sum(p['total_output'] for p in products),
            'total_cost': sum(p['total_input_cost'] for p in products),
        },
    }


def calculate_daily_yield(db, date_from, date_to, product_name=None, location=None):
    """일별 수율 추이 데이터 (차트용).

    Returns:
        dict: {
            'dates': ['2026-02-01', ...],
            'products': {
                '제품A': {
                    'yields': [95.2, 93.1, ...],
                    'outputs': [100, 120, ...],
                    'costs': [3000, 2800, ...],
                }
            }
        }
    """
    # 1. 데이터 조회
    prod_data = db.query_stock_ledger(
        date_from=date_from, date_to=date_to,
        type_list=['PRODUCTION', 'PROD_OUT'])

    if location:
        prod_data = [r for r in prod_data if r.get('location', '') == location]

    # 2. 매입단가 + BOM원가
    cost_map_raw = db.query_product_costs()
    cost_map = {k: float(v.get('cost_price', 0)) for k, v in cost_map_raw.items()}
    bom_cost_map = _load_bom_cost_map(db)

    # 3. 날짜별 그룹핑
    daily_groups = defaultdict(lambda: {'production': [], 'prod_out': []})
    for r in prod_data:
        d = r.get('transaction_date', '')
        loc = r.get('location', '')
        key = (d, loc)
        if r.get('type') == 'PRODUCTION':
            daily_groups[key]['production'].append(r)
        elif r.get('type') == 'PROD_OUT':
            daily_groups[key]['prod_out'].append(r)

    # 4. 일별 제품별 수율 계산
    daily_product = defaultdict(lambda: defaultdict(lambda: {
        'output': 0, 'input_cost': 0
    }))
    all_dates = set()

    for (date, loc), group in daily_groups.items():
        productions = group['production']
        prod_outs = group['prod_out']

        if not productions:
            continue

        daily_input_cost = 0
        for po in prod_outs:
            mat_name = po.get('product_name', '')
            mat_qty = abs(po.get('qty', 0))
            daily_input_cost += mat_qty * cost_map.get(mat_name, 0)

        total_daily_output = sum(p.get('qty', 0) for p in productions)

        for prod in productions:
            pname = prod.get('product_name', '')
            pqty = prod.get('qty', 0)
            if pqty <= 0:
                continue

            if product_name and pname != product_name:
                continue

            ratio = pqty / total_daily_output if total_daily_output > 0 else 0
            daily_product[pname][date]['output'] += pqty
            daily_product[pname][date]['input_cost'] += daily_input_cost * ratio
            all_dates.add(date)

    # 5. 정렬된 날짜 리스트
    dates = sorted(all_dates)

    # 6. 제품별 추이 데이터 구성
    products = {}
    for pname, date_data in sorted(daily_product.items()):
        yields = []
        outputs = []
        costs = []
        bom_cost = bom_cost_map.get(pname, 0)

        for d in dates:
            info = date_data.get(d, {'output': 0, 'input_cost': 0})
            output = info['output']
            input_cost = info['input_cost']

            if output > 0:
                actual_unit = input_cost / output
                outputs.append(output)
                costs.append(round(actual_unit))
                if actual_unit > 0 and bom_cost > 0:
                    yields.append(round(bom_cost / actual_unit * 100, 1))
                else:
                    yields.append(None)
            else:
                outputs.append(0)
                costs.append(0)
                yields.append(None)

        products[pname] = {
            'yields': yields,
            'outputs': outputs,
            'costs': costs,
        }

    return {
        'dates': dates,
        'products': products,
    }


def _load_bom_cost_map(db):
    """BOM 이론원가 맵 생성. {제품명: 이론단위원가}

    1순위: BOM 세트 구성품 계산 원가 (master_bom에서 산출)
    2순위: product_costs에서 cost_type='생산' 항목의 cost_price
    3순위: product_costs에서 일반 cost_price
    """
    try:
        from services.bom_cost_service import calculate_bom_costs
        result = calculate_bom_costs(db)
        bom_map = {}

        # 1) BOM 분석 결과 (세트 + 개별 완제품)
        for item in result.get('bom_items', []):
            sn = item.get('set_name', '')
            tc = item.get('total_cost', 0)
            if sn and tc > 0:
                if sn not in bom_map:
                    bom_map[sn] = tc

        # 2) product_costs에서 '생산' 유형 항목 보충
        #    (BOM에 없지만 생산단가가 입력된 경우)
        cost_details = result.get('cost_details', {})
        for pname, detail in cost_details.items():
            if pname in bom_map:
                continue  # 이미 BOM에서 계산됨
            ct = detail.get('cost_type', '매입')
            cp = float(detail.get('cost_price', 0))
            if ct == '생산' and cp > 0:
                bom_map[pname] = cp

        return bom_map
    except Exception:
        return {}
