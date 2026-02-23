"""
set_assembly_service.py — 세트작업 비즈니스 로직.
BOM(bom_master) 기반으로 단품 FIFO 차감(SET_OUT) + 세트 산출(SET_IN) 처리.
"""
from datetime import datetime
from collections import defaultdict

try:
    from excel_io import build_stock_snapshot, snapshot_lookup
except ImportError:
    from services.excel_io import build_stock_snapshot, snapshot_lookup


def _validate_date(date_str):
    """날짜 형식 검증."""
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        raise ValueError(f"날짜 형식이 올바르지 않습니다: {date_str}. YYYY-MM-DD 형식으로 입력하세요.")


def _load_stock_snapshot(db, location):
    """특정 창고의 재고 스냅샷을 FIFO 그룹으로 반환."""
    try:
        all_data = db.query_stock_by_location(location)
        return build_stock_snapshot(all_data)
    except Exception as e:
        print(f"재고 스냅샷 조회 에러: {e}")
        return {}


def parse_components(comp_str):
    """구성품 문자열 파싱.

    "당근3단x1,애호3단x1,쌀가루3단x5" → [("당근3단", 1), ("애호3단", 1), ("쌀가루3단", 5)]
    """
    result = []
    if not comp_str or not comp_str.strip():
        return result
    for item in comp_str.split(','):
        item = item.strip()
        if not item:
            continue
        # 마지막 'x숫자' 분리
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


def explode_bom(bom_lookup, set_name, channel, multiplier=1, _visited=None):
    """재귀 BOM 전개 — 세트 안의 세트를 최종 단품까지 펼친다.

    Args:
        bom_lookup: {(channel, set_name): components_str} 딕셔너리
        set_name: 전개할 세트명
        channel: 채널 (모든채널 / 쿠팡전용)
        multiplier: 상위에서 요구하는 수량 배수
        _visited: 순환 참조 방지용 세트

    Returns:
        dict: {단품명: 필요수량} — 재귀 전개된 최종 단품 목록
    """
    if _visited is None:
        _visited = set()

    # 순환 참조 방지
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
            # 구성품이 또 다른 세트 → 재귀 전개
            sub_items = explode_bom(bom_lookup, comp_name, channel,
                                    multiplier=needed, _visited=_visited.copy())
            for item_name, item_qty in sub_items.items():
                result[item_name] += item_qty
        else:
            # 최종 단품
            result[comp_name] += needed

    return dict(result)


def process_set_assembly(db, date_str, set_name, channel, location, qty,
                         sub_materials=None):
    """세트작업 처리 메인 함수.

    Args:
        db: SupabaseDB 인스턴스
        date_str: 작업일자 (YYYY-MM-DD)
        set_name: 세트명
        channel: 판매처/채널 (모든채널 / 쿠팡전용)
        location: 창고위치
        qty: 세트 수량
        sub_materials: 부재료 목록 [{'name': str, 'qty': int}, ...]

    Returns:
        dict: {success, set_out_count, set_in_count, sub_out_count, warnings, shortage}
    """
    if sub_materials is None:
        sub_materials = []
    _validate_date(date_str)

    if qty <= 0:
        return {'success': False, 'warnings': ['수량은 1 이상이어야 합니다.'],
                'shortage': [], 'set_out_count': 0, 'set_in_count': 0}

    # 1. BOM 데이터 로드
    try:
        bom_raw = db.query_master_table('bom_master')
    except Exception as e:
        return {'success': False, 'warnings': [f'BOM 데이터 조회 실패: {e}'],
                'shortage': [], 'set_out_count': 0, 'set_in_count': 0}

    bom_lookup = {}
    for row in bom_raw:
        ch = row.get('channel', '')
        sn = row.get('set_name', '')
        comp = row.get('components', '')
        if ch and sn:
            bom_lookup[(ch, sn)] = comp

    # 2. BOM 전개 (재귀)
    final_items = explode_bom(bom_lookup, set_name, channel, multiplier=qty)

    if not final_items:
        return {'success': False,
                'warnings': [f'세트 "{set_name}" ({channel})의 BOM 데이터를 찾을 수 없습니다.'],
                'shortage': [], 'set_out_count': 0, 'set_in_count': 0}

    # 3. 재고 스냅샷 로드
    snapshot = _load_stock_snapshot(db, location)

    # 4. 부족 체크 (구성품 + 부재료)
    shortage = []
    for item_name, needed_qty in sorted(final_items.items()):
        snap_data = snapshot_lookup(snapshot, item_name)
        available = snap_data.get('total', 0)
        if available < needed_qty:
            shortage.append(
                f"{item_name}: 필요 {needed_qty}, 현재고 {available} (부족 {needed_qty - available})"
            )

    # 부재료 부족 체크 (세트 수량 곱산)
    for sm in sub_materials:
        sm_name = sm['name']
        sm_needed = sm['qty'] * qty  # 세트 1개당 부재료 수량 × 세트 수량
        snap_data = snapshot_lookup(snapshot, sm_name)
        available = snap_data.get('total', 0)
        if available < sm_needed:
            shortage.append(
                f"[부재료] {sm_name}: 필요 {sm_needed}, 현재고 {available} (부족 {sm_needed - available})"
            )

    if shortage:
        return {'success': False,
                'warnings': ['재고 부족으로 세트작업을 진행할 수 없습니다.'],
                'shortage': shortage,
                'set_out_count': 0, 'set_in_count': 0, 'sub_out_count': 0}

    # 5. FIFO 차감 (SET_OUT) + 세트 산출 (SET_IN) payload 생성
    payload = []
    set_out_count = 0
    warnings = []

    for item_name, needed_qty in sorted(final_items.items()):
        snap_data = snapshot_lookup(snapshot, item_name)
        groups = snap_data.get('groups', [])
        remain = needed_qty

        if not groups:
            # 그룹 정보 없이 전체 차감
            payload.append({
                "transaction_date": date_str,
                "type": "SET_OUT",
                "product_name": item_name,
                "qty": -remain,
                "location": location,
                "unit": snap_data.get('unit', '개'),
                "memo": f"세트작업: {set_name} ({channel})",
            })
            set_out_count += 1
        else:
            for g in groups:
                if remain <= 0:
                    break
                deduct = min(remain, g['qty'])
                if deduct <= 0:
                    continue
                payload.append({
                    "transaction_date": date_str,
                    "type": "SET_OUT",
                    "product_name": item_name,
                    "qty": -deduct,
                    "location": location,
                    "category": g.get('category', ''),
                    "expiry_date": g.get('expiry_date', ''),
                    "storage_method": g.get('storage_method', ''),
                    "unit": g.get('unit', '개'),
                    "origin": g.get('origin', ''),
                    "manufacture_date": g.get('manufacture_date', ''),
                    "memo": f"세트작업: {set_name} ({channel})",
                })
                g['qty'] -= deduct
                remain -= deduct
                set_out_count += 1

    # 6. 부재료 FIFO 차감 (SET_OUT)
    sub_out_count = 0
    for sm in sub_materials:
        sm_name = sm['name']
        sm_needed = sm['qty'] * qty  # 세트 1개당 × 세트 수량
        snap_data = snapshot_lookup(snapshot, sm_name)
        groups = snap_data.get('groups', [])
        remain = sm_needed

        if not groups:
            payload.append({
                "transaction_date": date_str,
                "type": "SET_OUT",
                "product_name": sm_name,
                "qty": -remain,
                "location": location,
                "unit": snap_data.get('unit', '개'),
                "memo": f"세트작업 부재료: {set_name} ({channel})",
            })
            sub_out_count += 1
        else:
            for g in groups:
                if remain <= 0:
                    break
                deduct = min(remain, g['qty'])
                if deduct <= 0:
                    continue
                payload.append({
                    "transaction_date": date_str,
                    "type": "SET_OUT",
                    "product_name": sm_name,
                    "qty": -deduct,
                    "location": location,
                    "category": g.get('category', ''),
                    "expiry_date": g.get('expiry_date', ''),
                    "storage_method": g.get('storage_method', ''),
                    "unit": g.get('unit', '개'),
                    "origin": g.get('origin', ''),
                    "manufacture_date": g.get('manufacture_date', ''),
                    "memo": f"세트작업 부재료: {set_name} ({channel})",
                })
                g['qty'] -= deduct
                remain -= deduct
                sub_out_count += 1

    # 7. 세트 산출 (SET_IN)
    sub_memo = ""
    if sub_materials:
        sub_memo = f", 부재료 {len(sub_materials)}종"
    payload.append({
        "transaction_date": date_str,
        "type": "SET_IN",
        "product_name": set_name,
        "qty": qty,
        "location": location,
        "category": "완제품",
        "unit": "세트",
        "memo": f"세트작업 ({channel}), 구성품 {len(final_items)}종{sub_memo}",
    })
    set_in_count = 1

    # 8. DB 삽입
    try:
        db.insert_stock_ledger(payload)
    except Exception as e:
        return {'success': False, 'warnings': [f'DB 저장 중 오류: {e}'],
                'shortage': [], 'set_out_count': 0, 'set_in_count': 0, 'sub_out_count': 0}

    return {
        'success': True,
        'set_out_count': set_out_count,
        'set_in_count': set_in_count,
        'sub_out_count': sub_out_count,
        'warnings': warnings,
        'shortage': [],
        'component_count': len(final_items),
        'total_deducted': sum(final_items.values()),
    }
