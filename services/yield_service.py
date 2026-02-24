"""
yield_service.py — 수율 분석 서비스.
생산일지(PRODUCTION + PROD_OUT) 데이터 기반으로
제품별 중량수율, 개수수율, 일별 추이 분석.

배치 그룹핑 전략:
  1순위: batch_id (명시적 PRODUCTION↔PROD_OUT 연결)
  2순위: ID 순서 기반 순차 그룹핑 (레거시 — 생산처리 시 삽입 순서 활용)
         PRODUCTION → PROD_OUT → PROD_OUT → PRODUCTION → PROD_OUT ... 패턴에서
         각 PRODUCTION 이후의 PROD_OUT들을 해당 제품의 투입원료로 인식
"""
from collections import defaultdict
from datetime import datetime, timedelta


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


def _aggregate_weekly(dates, product_data):
    """일별 데이터를 주간(월~일) 단위로 집계.

    Args:
        dates: ['2026-02-01', '2026-02-02', ...]
        product_data: {pname: {outputs, weight_yields, qty_yields, ...}}

    Returns:
        dict: {
            'dates': ['2026-W05', ...],
            'products': {pname: {outputs, weight_yields, qty_yields, output_weights_g, input_weights_g}}
        }
    """
    if not dates:
        return {'dates': [], 'products': {}}

    # 날짜 → 주 라벨 매핑
    week_map = {}  # date_str → week_label
    week_order = []  # 순서 보장용
    seen_weeks = set()

    for d_str in dates:
        try:
            dt = datetime.strptime(d_str, '%Y-%m-%d')
            iso = dt.isocalendar()
            week_label = f'{iso[0]}-W{iso[1]:02d}'
        except Exception:
            week_label = d_str
        week_map[d_str] = week_label
        if week_label not in seen_weeks:
            seen_weeks.add(week_label)
            week_order.append(week_label)

    # 주별 집계
    weekly_products = {}
    for pname, pdata in product_data.items():
        # 주별 누적 데이터
        w_accum = defaultdict(lambda: {
            'outputs': 0,
            'output_wg_sum': 0, 'input_wg_sum': 0,
            'weight_yield_sum': 0, 'weight_yield_cnt': 0,
            'qty_yield_sum': 0, 'qty_yield_cnt': 0,
        })

        for i, d_str in enumerate(dates):
            wk = week_map[d_str]
            acc = w_accum[wk]
            acc['outputs'] += pdata['outputs'][i] if i < len(pdata['outputs']) else 0

            owg = pdata['output_weights_g'][i] if i < len(pdata.get('output_weights_g', [])) else 0
            iwg = pdata['input_weights_g'][i] if i < len(pdata.get('input_weights_g', [])) else 0
            acc['output_wg_sum'] += owg or 0
            acc['input_wg_sum'] += iwg or 0

            wy = pdata['weight_yields'][i] if i < len(pdata.get('weight_yields', [])) else None
            if wy is not None:
                acc['weight_yield_sum'] += wy
                acc['weight_yield_cnt'] += 1

            qy = pdata['qty_yields'][i] if i < len(pdata.get('qty_yields', [])) else None
            if qy is not None:
                acc['qty_yield_sum'] += qy
                acc['qty_yield_cnt'] += 1

        # 주별 결과 구성
        w_outputs = []
        w_weight_yields = []
        w_qty_yields = []
        w_owg = []
        w_iwg = []

        for wk in week_order:
            acc = w_accum.get(wk, {})
            w_outputs.append(acc.get('outputs', 0))
            w_owg.append(round(acc.get('output_wg_sum', 0), 1))
            w_iwg.append(round(acc.get('input_wg_sum', 0), 1))

            if acc.get('weight_yield_cnt', 0) > 0:
                w_weight_yields.append(round(acc['weight_yield_sum'] / acc['weight_yield_cnt'], 1))
            else:
                w_weight_yields.append(None)

            if acc.get('qty_yield_cnt', 0) > 0:
                w_qty_yields.append(round(acc['qty_yield_sum'] / acc['qty_yield_cnt'], 1))
            else:
                w_qty_yields.append(None)

        weekly_products[pname] = {
            'outputs': w_outputs,
            'weight_yields': w_weight_yields,
            'qty_yields': w_qty_yields,
            'output_weights_g': w_owg,
            'input_weights_g': w_iwg,
        }

    return {
        'dates': week_order,
        'products': weekly_products,
    }


def calculate_yield_summary(db, date_from, date_to, location=None):
    """제품별 수율 요약 (2종: 중량수율, 개수수율).

    중량수율(%) = (산출총중량g / 투입총중량g) × 100  [주원료+반제품만]
    개수수율(%) = (실제생산개수 / 이론생산가능개수) × 100
    이론생산가능개수 = 투입총중량(g) / 완제품1개중량(g)

    Returns:
        dict: {
            'products': [{product_name, total_output,
                          weight_yield, qty_yield,
                          output_weight_g, input_weight_g,
                          production_count, materials}],
            'summary': {total_products, avg_weight_yield, avg_qty_yield,
                        total_output}
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

    # 중량맵 (g 단위 통일) + 종류맵
    weight_map = {}   # {name: weight_in_grams}
    type_map = {}     # {name: material_type}
    for k, v in cost_map_raw.items():
        w = float(v.get('weight', 0) or 0)
        wu = (v.get('weight_unit', 'g') or 'g').lower()
        weight_map[k] = w * 1000 if wu == 'kg' else w
        type_map[k] = v.get('material_type', '원료') or '원료'

    # 3. 배치 그룹핑 (batch_id 우선, ID순서 폴백)
    batches = _group_production_batches(prod_data)

    # 4. 제품별 집계
    product_stats = defaultdict(lambda: {
        'total_output': 0,
        'total_output_weight_g': 0,
        'total_input_weight_g': 0,
        'production_count': 0,
        'materials': defaultdict(lambda: {'total_qty': 0, 'weight_g': 0,
                                          'material_type': '원료'}),
        'daily_data': [],
    })

    for batch in batches:
        productions = batch['production']
        prod_outs = batch['prod_out']

        if not productions:
            continue

        # 이 배치의 투입중량 (주원료+반제품만)
        batch_input_weight = 0
        batch_materials = defaultdict(lambda: {'qty': 0, 'weight_g': 0})

        for po in prod_outs:
            mat_name = po.get('product_name', '')
            mat_qty = abs(po.get('qty', 0))
            batch_materials[mat_name]['qty'] += mat_qty

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

            allocated_input_weight = batch_input_weight * ratio

            product_unit_weight = weight_map.get(pname, 0)
            output_weight = pqty * product_unit_weight

            stats = product_stats[pname]
            stats['total_output'] += pqty
            stats['total_output_weight_g'] += output_weight
            stats['total_input_weight_g'] += allocated_input_weight
            stats['production_count'] += 1

            for mat_name, mat_info in batch_materials.items():
                allocated_qty = mat_info['qty'] * ratio
                m = stats['materials'][mat_name]
                m['total_qty'] += allocated_qty
                m['weight_g'] += mat_info['weight_g'] * ratio
                m['material_type'] = type_map.get(mat_name, '원료')

            stats['daily_data'].append({
                'date': date,
                'output': pqty,
                'output_weight_g': output_weight,
                'input_weight_g': allocated_input_weight,
            })

    # 5. 최종 결과 구성
    products = []
    weight_yield_sum = 0; weight_yield_count = 0
    qty_yield_sum = 0; qty_yield_count = 0

    for pname, stats in sorted(product_stats.items()):
        total_output = stats['total_output']
        output_weight_g = stats['total_output_weight_g']
        input_weight_g = stats['total_input_weight_g']

        # ① 중량수율
        if input_weight_g > 0 and output_weight_g > 0:
            weight_yield = round(output_weight_g / input_weight_g * 100, 1)
            weight_yield_sum += weight_yield; weight_yield_count += 1
        else:
            weight_yield = None

        # ② 개수수율
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
                'weight_g': round(mat_info['weight_g'], 1),
                'material_type': mat_info.get('material_type', '원료'),
            })

        products.append({
            'product_name': pname,
            'total_output': total_output,
            'weight_yield': weight_yield,
            'qty_yield': qty_yield,
            'output_weight_g': round(output_weight_g, 1),
            'input_weight_g': round(input_weight_g, 1),
            'production_count': stats['production_count'],
            'materials': materials,
        })

    # 정렬: 중량수율 기준 (None은 뒤로)
    products.sort(key=lambda x: (x['weight_yield'] is None, x['weight_yield'] or 0))

    avg_weight_yield = round(weight_yield_sum / weight_yield_count, 1) if weight_yield_count > 0 else None
    avg_qty_yield = round(qty_yield_sum / qty_yield_count, 1) if qty_yield_count > 0 else None

    return {
        'products': products,
        'summary': {
            'total_products': len(products),
            'avg_weight_yield': avg_weight_yield,
            'avg_qty_yield': avg_qty_yield,
            'total_output': sum(p['total_output'] for p in products),
        },
    }


def calculate_daily_yield(db, date_from, date_to, product_name=None, location=None,
                          period='day'):
    """일별/주별 수율 추이 데이터 (차트용, 중량수율+개수수율).

    Args:
        period: 'day' (일별) 또는 'week' (주별)

    Returns:
        dict: {
            'dates': ['2026-02-01', ...] or ['2026-W05', ...],
            'period': 'day' or 'week',
            'products': {
                '제품A': {
                    'weight_yields': [88.0, 90.1, ...],
                    'qty_yields': [92.3, 91.0, ...],
                    'outputs': [100, 120, ...],
                    'output_weights_g': [...],
                    'input_weights_g': [...],
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

    # 2. 중량/종류맵
    cost_map_raw = db.query_product_costs()

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
        'output': 0,
        'output_weight_g': 0, 'input_weight_g': 0,
    }))
    all_dates = set()

    for batch in batches:
        productions = batch['production']
        prod_outs = batch['prod_out']

        if not productions:
            continue

        batch_input_weight = 0
        for po in prod_outs:
            mat_name = po.get('product_name', '')
            mat_qty = abs(po.get('qty', 0))
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
            d_info['output_weight_g'] += pqty * weight_map.get(pname, 0)
            d_info['input_weight_g'] += batch_input_weight * ratio
            all_dates.add(date)

    # 5. 정렬된 날짜 리스트
    dates = sorted(all_dates)

    # 6. 제품별 추이 데이터 구성
    products = {}
    for pname, date_data in sorted(daily_product.items()):
        weight_yields = []
        qty_yields = []
        outputs = []
        output_weights_g = []
        input_weights_g = []
        p_unit_weight = weight_map.get(pname, 0)

        for d in dates:
            info = date_data.get(d, {'output': 0,
                                     'output_weight_g': 0, 'input_weight_g': 0})
            output = info['output']
            out_wg = info['output_weight_g']
            in_wg = info['input_weight_g']

            if output > 0:
                outputs.append(output)
                output_weights_g.append(round(out_wg, 1))
                input_weights_g.append(round(in_wg, 1))

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
                output_weights_g.append(0)
                input_weights_g.append(0)
                weight_yields.append(None)
                qty_yields.append(None)

        products[pname] = {
            'weight_yields': weight_yields,
            'qty_yields': qty_yields,
            'outputs': outputs,
            'output_weights_g': output_weights_g,
            'input_weights_g': input_weights_g,
        }

    # 7. 주별 집계 (period='week')
    if period == 'week':
        weekly = _aggregate_weekly(dates, products)
        return {
            'dates': weekly['dates'],
            'period': 'week',
            'products': weekly['products'],
        }

    return {
        'dates': dates,
        'period': 'day',
        'products': products,
    }
