"""
yield_service.py — 수율 분석 서비스.
생산일지(PRODUCTION + PROD_OUT) 데이터 기반으로
제품별 수율, 실제원가, 일별 추이 분석.

배치 그룹핑 전략:
  1순위: batch_id (명시적 PRODUCTION↔PROD_OUT 연결)
  2순위: ID 순서 기반 순차 그룹핑 (레거시 — 생산처리 시 삽입 순서 활용)
         PRODUCTION → PROD_OUT → PROD_OUT → PRODUCTION → PROD_OUT ... 패턴에서
         각 PRODUCTION 이후의 PROD_OUT들을 해당 제품의 투입원료로 인식
"""
from collections import defaultdict


def _group_production_batches(prod_data):
    """PRODUCTION ↔ PROD_OUT 배치 그룹핑.

    Returns: list of {'production': [records], 'prod_out': [records]}
    각 배치 = 1개 PRODUCTION + N개 PROD_OUT (해당 제품에 투입된 원료)
    """
    batches = []

    # ── 1. batch_id가 있는 레코드 분리 ──
    batch_map = defaultdict(lambda: {'production': [], 'prod_out': []})
    unbatched = []

    for r in prod_data:
        bid = r.get('batch_id')
        if bid:
            if r.get('type') == 'PRODUCTION':
                batch_map[bid]['production'].append(r)
            elif r.get('type') == 'PROD_OUT':
                batch_map[bid]['prod_out'].append(r)
        else:
            unbatched.append(r)

    batches.extend(batch_map.values())

    # ── 2. batch_id 없는 레코드: ID 순서 기반 순차 그룹핑 ──
    # 생산처리(process_production)가 페이로드를 생성하는 순서:
    #   PRODUCTION(A) → PROD_OUT(A_mat1) → PROD_OUT(A_mat2) → PRODUCTION(B) → PROD_OUT(B_mat1) → ...
    # DB에 한번에 INSERT되므로 ID가 삽입 순서대로 부여됨.
    # (date, location) 별로 그룹 → ID 정렬 → PRODUCTION 기준 배치 분할

    if unbatched:
        dl_groups = defaultdict(list)
        for r in unbatched:
            key = (r.get('transaction_date', ''), r.get('location', ''))
            dl_groups[key].append(r)

        for (date, loc), records in dl_groups.items():
            # ID 순서로 정렬 (삽입 순서 재현)
            records.sort(key=lambda r: r.get('id', 0))

            current_batch = None
            for r in records:
                if r.get('type') == 'PRODUCTION':
                    # 이전 배치 저장
                    if current_batch and current_batch['production']:
                        batches.append(current_batch)
                    # 새 배치 시작
                    current_batch = {'production': [r], 'prod_out': []}
                elif r.get('type') == 'PROD_OUT':
                    if current_batch:
                        # 현재 PRODUCTION에 소속
                        current_batch['prod_out'].append(r)
                    else:
                        # PRODUCTION 없이 PROD_OUT만 있는 경우 (비정상)
                        # 단독 그룹으로 생성 (표시는 하되 수율 계산 불가)
                        batches.append({'production': [], 'prod_out': [r]})

            # 마지막 배치 저장
            if current_batch and current_batch['production']:
                batches.append(current_batch)

    return batches


def calculate_yield_summary(db, date_from, date_to, location=None):
    """제품별 수율 요약 (3종: 원가수율, 중량수율, 개수수율).

    원가수율(%) = (BOM이론원가 / 실제단위원가) × 100
    중량수율(%) = (산출총중량g / 투입총중량g) × 100  [주원료+반제품만]
    개수수율(%) = (실제생산개수 / 이론생산가능개수) × 100
    이론생산가능개수 = 투입총중량(g) / 완제품1개중량(g)

    Returns:
        dict: {
            'products': [{product_name, total_output, total_input_cost,
                          actual_unit_cost, bom_unit_cost, cost_diff,
                          yield_rate, weight_yield, qty_yield,
                          output_weight_g, input_weight_g,
                          production_count, materials}],
            'summary': {total_products, avg_yield, avg_weight_yield, avg_qty_yield,
                        total_output, total_cost}
        }
    """
    # 1. 생산 데이터 조회
    prod_data = db.query_stock_ledger(
        date_from=date_from, date_to=date_to,
        type_list=['PRODUCTION', 'PROD_OUT'])

    if location:
        prod_data = [r for r in prod_data if r.get('location', '') == location]

    # 2. 매입단가 + 중량/종류 맵 로드
    cost_map_raw = db.query_product_costs()
    cost_map = {k: float(v.get('cost_price', 0)) for k, v in cost_map_raw.items()}

    # 중량맵 (g 단위 통일) + 종류맵
    weight_map = {}   # {name: weight_in_grams}
    type_map = {}     # {name: material_type}
    for k, v in cost_map_raw.items():
        w = float(v.get('weight', 0) or 0)
        wu = (v.get('weight_unit', 'g') or 'g').lower()
        weight_map[k] = w * 1000 if wu == 'kg' else w
        type_map[k] = v.get('material_type', '원료') or '원료'

    # 3. BOM 이론원가 로드
    bom_cost_map = _load_bom_cost_map(db)

    # 4. 배치 그룹핑 (batch_id 우선, ID순서 폴백)
    batches = _group_production_batches(prod_data)

    # 5. 제품별 집계
    product_stats = defaultdict(lambda: {
        'total_output': 0,
        'total_input_cost': 0,
        'total_output_weight_g': 0,
        'total_input_weight_g': 0,
        'production_count': 0,
        'materials': defaultdict(lambda: {'total_qty': 0, 'unit_price': 0, 'total_cost': 0,
                                          'material_type': '원료'}),
        'daily_data': [],
    })

    for batch in batches:
        productions = batch['production']
        prod_outs = batch['prod_out']

        if not productions:
            continue

        # 이 배치의 총 투입비용 + 투입중량
        batch_input_cost = 0
        batch_input_weight = 0
        batch_materials = defaultdict(lambda: {'qty': 0, 'cost': 0, 'weight_g': 0})

        for po in prod_outs:
            mat_name = po.get('product_name', '')
            mat_qty = abs(po.get('qty', 0))
            unit_price = cost_map.get(mat_name, 0)
            mat_cost = mat_qty * unit_price
            batch_input_cost += mat_cost
            batch_materials[mat_name]['qty'] += mat_qty
            batch_materials[mat_name]['cost'] += mat_cost

            mat_type = type_map.get(mat_name, '원료')
            if mat_type in ('원료', '반제품'):
                w_g = weight_map.get(mat_name, 0) * mat_qty
                batch_input_weight += w_g
                batch_materials[mat_name]['weight_g'] += w_g

        # 배치 내 총 산출량 (보통 1개 PRODUCTION, 비율=1.0)
        total_batch_output = sum(p.get('qty', 0) for p in productions)

        for prod in productions:
            pname = prod.get('product_name', '')
            pqty = prod.get('qty', 0)
            date = prod.get('transaction_date', '')
            if pqty <= 0:
                continue

            # 비율 배분 (배치 내 1개 PRODUCTION이면 자동으로 1.0)
            ratio = pqty / total_batch_output if total_batch_output > 0 else 0

            allocated_cost = batch_input_cost * ratio
            allocated_input_weight = batch_input_weight * ratio

            product_unit_weight = weight_map.get(pname, 0)
            output_weight = pqty * product_unit_weight

            stats = product_stats[pname]
            stats['total_output'] += pqty
            stats['total_input_cost'] += allocated_cost
            stats['total_output_weight_g'] += output_weight
            stats['total_input_weight_g'] += allocated_input_weight
            stats['production_count'] += 1

            for mat_name, mat_info in batch_materials.items():
                allocated_qty = mat_info['qty'] * ratio
                allocated_mat_cost = mat_info['cost'] * ratio
                m = stats['materials'][mat_name]
                m['total_qty'] += allocated_qty
                m['unit_price'] = cost_map.get(mat_name, 0)
                m['total_cost'] += allocated_mat_cost
                m['material_type'] = type_map.get(mat_name, '원료')

            stats['daily_data'].append({
                'date': date,
                'output': pqty,
                'input_cost': allocated_cost,
                'output_weight_g': output_weight,
                'input_weight_g': allocated_input_weight,
            })

    # 6. 최종 결과 구성
    products = []
    cost_yield_sum = 0; cost_yield_count = 0
    weight_yield_sum = 0; weight_yield_count = 0
    qty_yield_sum = 0; qty_yield_count = 0

    for pname, stats in sorted(product_stats.items()):
        total_output = stats['total_output']
        total_input_cost = stats['total_input_cost']
        output_weight_g = stats['total_output_weight_g']
        input_weight_g = stats['total_input_weight_g']

        actual_unit_cost = total_input_cost / total_output if total_output > 0 else 0
        bom_unit_cost = bom_cost_map.get(pname, 0)
        cost_diff = actual_unit_cost - bom_unit_cost if bom_unit_cost > 0 else 0

        # ① 원가수율
        if actual_unit_cost > 0 and bom_unit_cost > 0:
            yield_rate = round(bom_unit_cost / actual_unit_cost * 100, 1)
            cost_yield_sum += yield_rate; cost_yield_count += 1
        else:
            yield_rate = None

        # ② 중량수율
        if input_weight_g > 0 and output_weight_g > 0:
            weight_yield = round(output_weight_g / input_weight_g * 100, 1)
            weight_yield_sum += weight_yield; weight_yield_count += 1
        else:
            weight_yield = None

        # ③ 개수수율
        product_unit_weight = weight_map.get(pname, 0)
        if product_unit_weight > 0 and input_weight_g > 0:
            theoretical_qty = input_weight_g / product_unit_weight
            qty_yield = round(total_output / theoretical_qty * 100, 1)
            qty_yield_sum += qty_yield; qty_yield_count += 1
        else:
            qty_yield = None

        materials = []
        for mat_name, mat_info in sorted(stats['materials'].items()):
            materials.append({
                'name': mat_name,
                'total_qty': round(mat_info['total_qty'], 1),
                'unit_price': mat_info['unit_price'],
                'total_cost': round(mat_info['total_cost']),
                'material_type': mat_info.get('material_type', '원료'),
            })

        products.append({
            'product_name': pname,
            'total_output': total_output,
            'total_input_cost': round(total_input_cost),
            'actual_unit_cost': round(actual_unit_cost),
            'bom_unit_cost': round(bom_unit_cost),
            'cost_diff': round(cost_diff),
            'yield_rate': yield_rate,
            'weight_yield': weight_yield,
            'qty_yield': qty_yield,
            'output_weight_g': round(output_weight_g, 1),
            'input_weight_g': round(input_weight_g, 1),
            'production_count': stats['production_count'],
            'materials': materials,
        })

    products.sort(key=lambda x: (x['yield_rate'] is None, x['yield_rate'] or 0))

    avg_yield = round(cost_yield_sum / cost_yield_count, 1) if cost_yield_count > 0 else None
    avg_weight_yield = round(weight_yield_sum / weight_yield_count, 1) if weight_yield_count > 0 else None
    avg_qty_yield = round(qty_yield_sum / qty_yield_count, 1) if qty_yield_count > 0 else None

    return {
        'products': products,
        'summary': {
            'total_products': len(products),
            'avg_yield': avg_yield,
            'avg_weight_yield': avg_weight_yield,
            'avg_qty_yield': avg_qty_yield,
            'total_output': sum(p['total_output'] for p in products),
            'total_cost': sum(p['total_input_cost'] for p in products),
        },
    }


def calculate_daily_yield(db, date_from, date_to, product_name=None, location=None):
    """일별 수율 추이 데이터 (차트용, 3종 수율 포함).

    Returns:
        dict: {
            'dates': ['2026-02-01', ...],
            'products': {
                '제품A': {
                    'cost_yields': [95.2, 93.1, ...],
                    'weight_yields': [88.0, 90.1, ...],
                    'qty_yields': [92.3, 91.0, ...],
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

    # 2. 매입단가 + BOM원가 + 중량/종류맵
    cost_map_raw = db.query_product_costs()
    cost_map = {k: float(v.get('cost_price', 0)) for k, v in cost_map_raw.items()}
    bom_cost_map = _load_bom_cost_map(db)

    weight_map = {}
    type_map = {}
    for k, v in cost_map_raw.items():
        w = float(v.get('weight', 0) or 0)
        wu = (v.get('weight_unit', 'g') or 'g').lower()
        weight_map[k] = w * 1000 if wu == 'kg' else w
        type_map[k] = v.get('material_type', '원료') or '원료'

    # 3. 배치 그룹핑 (batch_id 우선, ID순서 폴백)
    batches = _group_production_batches(prod_data)

    # 4. 일별 제품별 수율 계산
    daily_product = defaultdict(lambda: defaultdict(lambda: {
        'output': 0, 'input_cost': 0,
        'output_weight_g': 0, 'input_weight_g': 0,
    }))
    all_dates = set()

    for batch in batches:
        productions = batch['production']
        prod_outs = batch['prod_out']

        if not productions:
            continue

        batch_input_cost = 0
        batch_input_weight = 0
        for po in prod_outs:
            mat_name = po.get('product_name', '')
            mat_qty = abs(po.get('qty', 0))
            batch_input_cost += mat_qty * cost_map.get(mat_name, 0)
            mat_type = type_map.get(mat_name, '원료')
            if mat_type in ('원료', '반제품'):
                batch_input_weight += weight_map.get(mat_name, 0) * mat_qty

        total_batch_output = sum(p.get('qty', 0) for p in productions)

        for prod in productions:
            pname = prod.get('product_name', '')
            pqty = prod.get('qty', 0)
            date = prod.get('transaction_date', '')
            if pqty <= 0:
                continue

            if product_name and pname != product_name:
                continue

            ratio = pqty / total_batch_output if total_batch_output > 0 else 0

            d_info = daily_product[pname][date]
            d_info['output'] += pqty
            d_info['input_cost'] += batch_input_cost * ratio
            d_info['output_weight_g'] += pqty * weight_map.get(pname, 0)
            d_info['input_weight_g'] += batch_input_weight * ratio
            all_dates.add(date)

    # 5. 정렬된 날짜 리스트
    dates = sorted(all_dates)

    # 6. 제품별 추이 데이터 구성
    products = {}
    for pname, date_data in sorted(daily_product.items()):
        cost_yields = []
        weight_yields = []
        qty_yields = []
        outputs = []
        costs = []
        bom_cost = bom_cost_map.get(pname, 0)
        p_unit_weight = weight_map.get(pname, 0)

        for d in dates:
            info = date_data.get(d, {'output': 0, 'input_cost': 0,
                                     'output_weight_g': 0, 'input_weight_g': 0})
            output = info['output']
            input_cost = info['input_cost']
            out_wg = info['output_weight_g']
            in_wg = info['input_weight_g']

            if output > 0:
                actual_unit = input_cost / output
                outputs.append(output)
                costs.append(round(actual_unit))

                if actual_unit > 0 and bom_cost > 0:
                    cost_yields.append(round(bom_cost / actual_unit * 100, 1))
                else:
                    cost_yields.append(None)

                if in_wg > 0 and out_wg > 0:
                    weight_yields.append(round(out_wg / in_wg * 100, 1))
                else:
                    weight_yields.append(None)

                if p_unit_weight > 0 and in_wg > 0:
                    theoretical = in_wg / p_unit_weight
                    qty_yields.append(round(output / theoretical * 100, 1))
                else:
                    qty_yields.append(None)
            else:
                outputs.append(0)
                costs.append(0)
                cost_yields.append(None)
                weight_yields.append(None)
                qty_yields.append(None)

        products[pname] = {
            'cost_yields': cost_yields,
            'weight_yields': weight_yields,
            'qty_yields': qty_yields,
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
        cost_details = result.get('cost_details', {})
        for pname, detail in cost_details.items():
            if pname in bom_map:
                continue
            ct = detail.get('cost_type', '매입')
            cp = float(detail.get('cost_price', 0))
            if ct == '생산' and cp > 0:
                bom_map[pname] = cp

        return bom_map
    except Exception:
        return {}
