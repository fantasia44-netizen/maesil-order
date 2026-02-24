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
                'components': [{'name','qty','cost_price','subtotal'}],
                'total_cost',
                'prices': {'네이버': ..., '쿠팡': ..., '로켓': ...},
                'margins': {'네이버': ..., ...},          # 원가마진
                'net_margins': {'네이버': ..., ...},      # 순마진 (비용 전체 반영)
                'net_profits': {'네이버': ..., ...},      # 순이익 (원)
                'cost_breakdown': {'네이버': {...}, ...},  # 비용 상세
            }],
            'cost_map': {product_name: cost_price},
            'all_products': [sorted list],
            'missing_costs': [products without cost data],
            'channel_costs': {channel: {fee_rate, shipping, ...}},
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

    # 2. 매입단가 로드
    cost_map_raw = db.query_product_costs()
    cost_map = {k: float(v.get('cost_price', 0)) for k, v in cost_map_raw.items()}

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

    # 5. BOM lookup 구축
    bom_lookup = {}
    for row in bom_raw:
        ch = row.get('channel', '')
        sn = row.get('set_name', '')
        comp = row.get('components', '')
        if ch and sn:
            bom_lookup[(ch, sn)] = comp

    # 6. 모든 BOM 항목 분석
    bom_items = []
    all_component_names = set()

    for (channel, set_name), comp_str in sorted(bom_lookup.items()):
        # 직접 구성품 파싱 (1단계)
        direct_components = parse_components(comp_str)
        # 재귀 전개 (최종 단품)
        final_items = explode_bom_recursive(bom_lookup, set_name, channel)

        # 구성품별 원가 계산
        comp_details = []
        total_cost = 0
        for comp_name, comp_qty in sorted(final_items.items()):
            unit_cost = cost_map.get(comp_name, 0)
            subtotal = unit_cost * comp_qty
            total_cost += subtotal
            comp_details.append({
                'name': comp_name,
                'qty': comp_qty,
                'cost_price': unit_cost,
                'subtotal': subtotal,
            })
            all_component_names.add(comp_name)

        # 판매가 + 마진 계산
        prices = {}
        margins = {}
        net_margins = {}
        net_profits = {}
        cost_breakdown = {}
        p = price_map.get(set_name, {})

        for label, key in [('네이버', '네이버판매가'), ('쿠팡', '쿠팡판매가'), ('로켓', '로켓판매가')]:
            sell = float(p.get(key, 0))
            prices[label] = sell

            # 기존 원가마진
            if sell > 0 and total_cost > 0:
                margins[label] = round((sell - total_cost) / sell * 100, 1)
            else:
                margins[label] = None

            # 채널비용 반영 순마진
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

        bom_items.append({
            'channel': channel,
            'set_name': set_name,
            'components_str': comp_str,
            'components': comp_details,
            'total_cost': total_cost,
            'prices': prices,
            'margins': margins,
            'net_margins': net_margins,
            'net_profits': net_profits,
            'cost_breakdown': cost_breakdown,
        })

    # 원가 미입력 품목
    missing_costs = sorted([n for n in all_component_names if cost_map.get(n, 0) == 0])

    return {
        'bom_items': bom_items,
        'cost_map': cost_map,
        'cost_details': cost_map_raw,
        'all_products': sorted(all_component_names),
        'missing_costs': missing_costs,
        'price_map': price_map,
        'channel_costs': channel_costs,
    }
