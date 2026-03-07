"""
actual_cost_service.py -- 실제 원가 분석 서비스.
생산일지(PRODUCTION + PROD_OUT) 기반 실제 투입 원가 계산,
BOM 표준 원가 대비 차이(variance) 분석.

배치 그룹핑은 yield_service._group_production_batches()를 재사용.
"""
from collections import defaultdict

from services.yield_service import _group_production_batches
from services.bom_cost_service import calculate_bom_costs


def calculate_actual_costs(db, date_from, date_to, location=None):
    """기간별 생산 배치의 실제 투입 원가를 계산하고 BOM 표준 원가와 비교.

    Args:
        db: db_supabase 인스턴스
        date_from: 시작일 (YYYY-MM-DD)
        date_to: 종료일 (YYYY-MM-DD)
        location: 생산처 필터 (None이면 전체)

    Returns:
        dict: {
            'products': [{ product_name, batches, avg_actual_unit_cost,
                           bom_standard_cost, avg_variance_rate }],
            'summary': { total_batches, avg_variance_rate,
                         products_over_standard, products_under_standard }
        }
    """
    # 1. 생산 데이터 조회 (PRODUCTION + PROD_OUT)
    prod_data = db.query_stock_ledger(
        date_from=date_from, date_to=date_to,
        type_list=['PRODUCTION', 'PROD_OUT'])

    if location:
        prod_data = [r for r in prod_data if r.get('location', '') == location]

    # 2. 매입단가 로드 (conversion_ratio 반영: 사용단위당 단가)
    cost_map_raw = db.query_product_costs()
    cost_map = {}  # {product_name: 사용단위당 단가}
    for k, v in cost_map_raw.items():
        price = float(v.get('cost_price', 0))
        ratio = float(v.get('conversion_ratio', 1) or 1)
        unit_cost = price / ratio if ratio > 0 else price
        cost_map[k] = unit_cost
        # 공백 정규화 키도 등록
        norm = k.replace(' ', '')
        if norm != k and norm not in cost_map:
            cost_map[norm] = unit_cost

    # 3. BOM 표준 원가 로드 (제품별 BOM 기반 직접비)
    try:
        bom_result = calculate_bom_costs(db)
        bom_standard_map = {}  # {제품명: BOM 표준 원가}
        for item in bom_result.get('bom_items', []):
            sn = item.get('set_name', '')
            tc = item.get('total_cost', 0)
            if sn and tc > 0:
                bom_standard_map[sn] = tc
                norm = sn.replace(' ', '')
                if norm != sn and norm not in bom_standard_map:
                    bom_standard_map[norm] = tc
    except Exception:
        bom_standard_map = {}

    # 4. 배치 그룹핑 (batch_id 우선, ID순서 폴백)
    batches = _group_production_batches(prod_data)

    # 5. 제품별 배치 원가 계산
    product_batches = defaultdict(list)

    for batch in batches:
        productions = batch['production']
        prod_outs = batch['prod_out']

        if not productions:
            continue

        # 배치 투입 원재료별 원가 계산
        batch_inputs = []
        batch_total_cost = 0

        for po in prod_outs:
            mat_name = po.get('product_name', '')
            mat_qty = abs(po.get('qty', 0))

            # product_costs에서 사용단위당 단가 조회
            unit_cost = (cost_map.get(mat_name, 0)
                         or cost_map.get(mat_name.replace(' ', ''), 0))
            subtotal = mat_qty * unit_cost

            batch_inputs.append({
                'name': mat_name,
                'qty': round(mat_qty, 4),
                'unit_cost': round(unit_cost, 2),
                'subtotal': round(subtotal, 2),
            })
            batch_total_cost += subtotal

        # 배치 내 산출량 합계
        total_batch_output = sum(p.get('qty', 0) for p in productions)

        # 각 PRODUCTION(산출 제품)에 비율 배분
        for prod in productions:
            pname = prod.get('product_name', '')
            pqty = prod.get('qty', 0)
            pdate = prod.get('transaction_date', '')
            ploc = prod.get('location', '')
            batch_id = prod.get('batch_id', '')

            if pqty <= 0:
                continue

            ratio = pqty / total_batch_output if total_batch_output > 0 else 0
            allocated_cost = batch_total_cost * ratio
            actual_unit_cost = allocated_cost / pqty if pqty > 0 else 0

            # BOM 표준 원가 조회
            bom_std = (bom_standard_map.get(pname, 0)
                       or bom_standard_map.get(pname.replace(' ', ''), 0))

            # 차이율 = (실제 - 표준) / 표준 * 100
            if bom_std > 0:
                variance_rate = round((actual_unit_cost - bom_std) / bom_std * 100, 1)
            else:
                variance_rate = None

            # 비율 배분된 투입 내역
            allocated_inputs = []
            for inp in batch_inputs:
                allocated_inputs.append({
                    'name': inp['name'],
                    'qty': round(inp['qty'] * ratio, 4),
                    'unit_cost': inp['unit_cost'],
                    'subtotal': round(inp['subtotal'] * ratio, 2),
                })

            product_batches[pname].append({
                'batch_id': batch_id or f'BATCH_{pdate}_{prod.get("id", "")}',
                'date': pdate,
                'location': ploc,
                'output_qty': pqty,
                'inputs': allocated_inputs,
                'actual_total_cost': round(allocated_cost, 2),
                'actual_unit_cost': round(actual_unit_cost, 2),
                'bom_standard_cost': round(bom_std, 2),
                'variance_rate': variance_rate,
            })

    # 6. 제품별 집계
    products = []
    total_batches = 0
    variance_sum = 0
    variance_count = 0
    over_count = 0
    under_count = 0

    for pname in sorted(product_batches.keys()):
        batch_list = product_batches[pname]
        total_batches += len(batch_list)

        # 평균 실제 단위 원가
        total_actual_cost_sum = sum(b['actual_total_cost'] for b in batch_list)
        total_output_sum = sum(b['output_qty'] for b in batch_list)
        avg_actual = (total_actual_cost_sum / total_output_sum
                      if total_output_sum > 0 else 0)

        # BOM 표준 원가 (모든 배치에서 동일)
        bom_std = batch_list[0]['bom_standard_cost'] if batch_list else 0

        # 평균 차이율
        vr_values = [b['variance_rate'] for b in batch_list
                     if b['variance_rate'] is not None]
        avg_vr = (round(sum(vr_values) / len(vr_values), 1)
                  if vr_values else None)

        if avg_vr is not None:
            variance_sum += avg_vr
            variance_count += 1
            if avg_vr > 0:
                over_count += 1
            elif avg_vr < 0:
                under_count += 1

        products.append({
            'product_name': pname,
            'batches': batch_list,
            'avg_actual_unit_cost': round(avg_actual, 2),
            'bom_standard_cost': round(bom_std, 2),
            'avg_variance_rate': avg_vr,
        })

    # 전체 평균 차이율
    avg_variance_total = (round(variance_sum / variance_count, 1)
                          if variance_count > 0 else None)

    return {
        'products': products,
        'summary': {
            'total_batches': total_batches,
            'avg_variance_rate': avg_variance_total,
            'products_over_standard': over_count,
            'products_under_standard': under_count,
        },
    }
