"""
bom_cost_service.py — BOM 원가 분석 서비스.
BOM 구성품 × 매입단가 → 세트/제품 원가 계산, 판매가 대비 마진 분석.
채널별 수수료/배송비/포장비/기타비용 반영 순마진 산출.
"""
from collections import defaultdict


def parse_components(comp_str):
    """구성품 문자열 → [(이름, 수량)] 파싱.
    "당근3단x1,애호3단x1,쌀가루3단x5" → [("당근3단",1), ("애호3단",1), ("쌀가루3단",5)]
    """
    result = []
    if not comp_str or not comp_str.strip():
        return result
    for item in comp_str.split(','):
        item = item.strip()
        if not item:
            continue
        idx = item.rfind('x')
        if idx > 0:
            name = item[:idx].strip()
            try:
                qty = int(item[idx + 1:].strip())
            except ValueError:
                name = item
                qty = 1
        else:
            name = item
            qty = 1
        if name:
            result.append((name, qty))
    return result


def _normalize_bom_row(row):
    """BOM 행의 컬럼명을 정규화 (한국어/영어 모두 지원).
    Returns: (channel, set_name, components)
    """
    ch = row.get('channel', '') or row.get('채널', '') or '전체'
    sn = row.get('set_name', '') or row.get('세트명', '')
    comp = row.get('components', '') or row.get('구성품', '')
    return ch.strip(), sn.strip(), comp.strip()


def explode_bom_recursive(bom_lookup, set_name, channel, multiplier=1, _visited=None):
    """재귀 BOM 전개 — 최종 단품까지 펼친다.
    Returns: dict {단품명: 필요수량}
    """
    if _visited is None:
        _visited = set()
    key = (channel, set_name)
    if key in _visited:
        return {}
    _visited.add(key)

    comp_str = bom_lookup.get(key, '')
    if not comp_str:
        return {}

    components = parse_components(comp_str)
    result = defaultdict(int)

    for comp_name, comp_qty in components:
        needed = comp_qty * multiplier
        sub_key = (channel, comp_name)
        if sub_key in bom_lookup:
            sub_items = explode_bom_recursive(bom_lookup, comp_name, channel,
                                              multiplier=needed,
                                              _visited=_visited.copy())
            for item_name, item_qty in sub_items.items():
                result[item_name] += item_qty
        else:
            result[comp_name] += needed

    return dict(result)


def calculate_bom_costs(db):
    """전체 BOM 원가 분석 데이터 생성 (채널비용 포함 순마진 산출).

    Returns:
        dict: {
            'bom_items': [{
                'channel', 'set_name', 'components_str',
                'components': [{'name','qty','cost_price','subtotal','weight'}],
                'total_cost', 'total_weight',
                'prices': {'네이버': ..., '쿠팡': ..., '로켓': ...},
                'margins': {'네이버': ..., ...},
                'net_margins': {'네이버': ..., ...},
                'net_profits': {'네이버': ..., ...},
                'cost_breakdown': {'네이버': {...}, ...},
            }],
            'cost_map': {product_name: cost_price},
            'all_products': [sorted list],
            'missing_costs': [products without cost data],
            'channel_costs': {channel: {...}},
            'all_set_names': [BOM 세트명 목록],
            'all_price_products': [판매가 등록 품목명 목록],
        }
    """
    # 1. BOM 데이터 로드 (master_bom 우선, bom_master 폴백)
    bom_raw = []
    for table_name in ('master_bom', 'bom_master'):
        try:
            bom_raw = db.query_master_table(table_name)
            if bom_raw:
                break
        except Exception:
            continue

    # 2. 매입단가 로드 (conversion_ratio 반영: 사용단위당 단가)
    cost_map_raw = db.query_product_costs()
    cost_map = {}
    for k, v in cost_map_raw.items():
        price = float(v.get('cost_price', 0))
        ratio = float(v.get('conversion_ratio', 1) or 1)
        cost_map[k] = price / ratio if ratio > 0 else price

    # 2-1. 중량 맵 (weight + material_type)
    # stock_ledger의 category를 실제 종류(material_type)로 사용
    category_map = db.query_product_categories()  # {product_name: category}

    weight_map = {}
    for k, v in cost_map_raw.items():
        w = float(v.get('weight', 0) or 0)
        wu = v.get('weight_unit', 'g') or 'g'
        # stock_ledger category 우선, 없으면 product_costs.material_type 폴백
        # 공백 정규화 대응: product_costs 이름에 공백이 있을 수 있음
        mt = (category_map.get(k)
              or category_map.get(k.replace(' ', ''))
              or v.get('material_type', '원료') or '원료')
        weight_map[k] = {'weight': w, 'weight_unit': wu, 'material_type': mt}

    # 3. 판매가 로드
    price_map = db.query_price_table()

    # 4. 채널비용 로드
    channel_costs_raw = db.query_channel_costs()
    channel_costs = {}
    for ch, info in channel_costs_raw.items():
        channel_costs[ch] = {
            'fee_rate': float(info.get('fee_rate', 0) or 0),
            'shipping': float(info.get('shipping', 0) or 0),
            'packaging': float(info.get('packaging', 0) or 0),
            'other_cost': float(info.get('other_cost', 0) or 0),
            'memo': info.get('memo', ''),
        }

    # 5. BOM lookup 구축 (한국어/영어 컬럼 모두 지원)
    bom_lookup = {}
    all_set_names = set()
    for row in bom_raw:
        ch, sn, comp = _normalize_bom_row(row)
        if sn:
            bom_lookup[(ch, sn)] = comp
            all_set_names.add(sn)

    # 6. 마진 계산 헬퍼 ─────────────────────────────
    def _calc_margins(product_name, total_cost, total_weight):
        """판매가 × 채널비용 → 마진 계산. 여러 BOM 항목에서 공통 사용."""
        prices = {}
        margins = {}
        net_margins = {}
        net_profits = {}
        cost_breakdown = {}
        p = price_map.get(product_name, {})

        for label, key in [('네이버', '네이버판매가'), ('쿠팡', '쿠팡판매가'), ('로켓', '로켓판매가')]:
            sell = float(p.get(key, 0))
            prices[label] = sell

            if sell > 0 and total_cost > 0:
                margins[label] = round((sell - total_cost) / sell * 100, 1)
            else:
                margins[label] = None

            ch_cost = channel_costs.get(label, {})
            fee = sell * (ch_cost.get('fee_rate', 0) / 100)
            ship = ch_cost.get('shipping', 0)
            pack = ch_cost.get('packaging', 0)
            etc = ch_cost.get('other_cost', 0)
            total_deduct = total_cost + fee + ship + pack + etc

            cost_breakdown[label] = {
                'fee': round(fee),
                'shipping': round(ship),
                'packaging': round(pack),
                'other': round(etc),
                'total_deduct': round(total_deduct),
            }

            if sell > 0:
                net_profits[label] = round(sell - total_deduct)
                net_margins[label] = round((sell - total_deduct) / sell * 100, 1)
            else:
                net_profits[label] = None
                net_margins[label] = None

        return prices, margins, net_margins, net_profits, cost_breakdown

    # 7. 모든 BOM 항목 분석 (세트) ─────────────────
    bom_items = []
    all_component_names = set()
    processed_products = set()          # 세트로 이미 처리된 품목 추적

    for (channel, set_name), comp_str in sorted(bom_lookup.items()):
        direct_components = parse_components(comp_str)
        final_items = explode_bom_recursive(bom_lookup, set_name, channel)

        comp_details = []
        total_cost = 0
        total_weight = 0
        for comp_name, comp_qty in sorted(final_items.items()):
            unit_cost = cost_map.get(comp_name, 0)
            subtotal = unit_cost * comp_qty
            total_cost += subtotal

            w_info = weight_map.get(comp_name, {})
            comp_weight = w_info.get('weight', 0) * comp_qty
            total_weight += comp_weight

            comp_details.append({
                'name': comp_name,
                'qty': comp_qty,
                'cost_price': unit_cost,
                'subtotal': subtotal,
                'weight': w_info.get('weight', 0),
                'weight_unit': w_info.get('weight_unit', 'g'),
            })
            all_component_names.add(comp_name)

        prices, margins, net_margins, net_profits, cost_breakdown = \
            _calc_margins(set_name, total_cost, total_weight)

        bom_items.append({
            'channel': channel,
            'set_name': set_name,
            'is_set': True,
            'components_str': comp_str,
            'components': comp_details,
            'total_cost': total_cost,
            'total_weight': round(total_weight, 1),
            'prices': prices,
            'margins': margins,
            'net_margins': net_margins,
            'net_profits': net_profits,
            'cost_breakdown': cost_breakdown,
        })
        processed_products.add(set_name)

    # 8. 개별 완제품 추가 (세트가 아닌 master_prices 품목) ──
    for product_name in sorted(price_map.keys()):
        if product_name in processed_products:
            continue  # 이미 세트로 처리됨

        unit_cost = cost_map.get(product_name, 0)
        w_info = weight_map.get(product_name, {})
        item_weight = w_info.get('weight', 0)

        comp_details = []
        if unit_cost > 0 or item_weight > 0:
            comp_details.append({
                'name': product_name,
                'qty': 1,
                'cost_price': unit_cost,
                'subtotal': unit_cost,
                'weight': item_weight,
                'weight_unit': w_info.get('weight_unit', 'g'),
            })

        prices, margins, net_margins, net_profits, cost_breakdown = \
            _calc_margins(product_name, unit_cost, item_weight)

        bom_items.append({
            'channel': '전체',
            'set_name': product_name,
            'is_set': False,
            'components_str': '',
            'components': comp_details,
            'total_cost': unit_cost,
            'total_weight': round(item_weight, 1),
            'prices': prices,
            'margins': margins,
            'net_margins': net_margins,
            'net_profits': net_profits,
            'cost_breakdown': cost_breakdown,
        })

    # 원가 미입력 품목
    missing_costs = sorted([n for n in all_component_names if cost_map.get(n, 0) == 0])
    # 개별 완제품 중 원가 미입력도 추가
    for item in bom_items:
        if not item['is_set'] and item['total_cost'] == 0:
            if item['set_name'] not in missing_costs:
                missing_costs.append(item['set_name'])
    missing_costs = sorted(missing_costs)

    # 판매가 등록 품목 목록 (autocomplete용)
    all_price_products = sorted(price_map.keys())

    # 전체 품목 = BOM 구성품 + 판매가 등록품목 + 세트명 + 단가등록품목 합집합
    all_products_combined = all_component_names | set(price_map.keys()) | all_set_names | set(cost_map_raw.keys())

    return {
        'bom_items': bom_items,
        'cost_map': cost_map,
        'cost_details': cost_map_raw,
        'all_products': sorted(all_products_combined),
        'bom_components': sorted(all_component_names),   # BOM 구성품만 (필터용)
        'missing_costs': missing_costs,
        'price_map': price_map,
        'channel_costs': channel_costs,
        'all_set_names': sorted(all_set_names),
        'all_price_products': all_price_products,
    }
