"""
etc_outbound_service.py — 기타출고 비즈니스 로직.
무상출고, 실험사용, 샘플, 폐기 등 FIFO 재고 차감 처리.
"""
from datetime import datetime

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


def _load_stock_snapshot(db, location, as_of_date=None):
    """특정 창고의 재고 스냅샷을 FIFO 그룹으로 반환.
    as_of_date가 주어지면 해당 날짜 기준 재고만 조회 (미래 데이터 제외).
    """
    try:
        if as_of_date:
            all_data = db.query_stock_ledger(
                date_to=as_of_date,
                location=location,
            )
        else:
            all_data = db.query_stock_by_location(location)
        return build_stock_snapshot(all_data)
    except Exception as e:
        print(f"재고 스냅샷 조회 에러: {e}")
        return {}


def process_etc_outbound(db, date_str, location, items):
    """기타출고 처리 메인 함수.

    Args:
        db: SupabaseDB 인스턴스
        date_str: 출고일 (YYYY-MM-DD)
        location: 창고위치
        items: 출고 품목 리스트
            [{'product_name': str, 'qty': int, 'reason': str, 'memo': str}, ...]

    Returns:
        dict: {success, out_count, warnings, shortage}
    """
    _validate_date(date_str)

    # ── 날짜 범위 검증 (미래 7일, 과거 30일 초과 차단) ──
    from datetime import timedelta
    input_date = datetime.strptime(date_str, '%Y-%m-%d')
    today = datetime.now()
    diff_days = (today - input_date).days
    if diff_days > 30:
        return {'success': False,
                'warnings': [f'출고일({date_str})이 30일 이전입니다. 날짜를 확인해주세요. (연도 오입력 주의)'],
                'shortage': [], 'out_count': 0, 'in_count': 0}
    if diff_days < -7:
        return {'success': False,
                'warnings': [f'출고일({date_str})이 7일 이후입니다. 날짜를 확인해주세요.'],
                'shortage': [], 'out_count': 0, 'in_count': 0}

    # ── 1차 실시간 검증 (Validation Engine) ──
    try:
        from core.validation_engine import _validate_date as v_date, _validate_location as v_loc
        v_date(date_str, '기타출고일자')
        v_loc(location, '출고 위치')
    except ImportError:
        pass

    if not items:
        return {'success': False, 'warnings': ['출고할 품목이 없습니다.'],
                'shortage': [], 'out_count': 0, 'in_count': 0}

    # 재고 스냅샷 로드 — 입력한 날짜 기준 재고 확인
    snapshot = _load_stock_snapshot(db, location, as_of_date=date_str)

    # 부족 체크 (차감 항목만)
    shortage = []
    for item in items:
        name = item.get('product_name', '').strip()
        qty = item.get('qty', 0)
        if not name or qty <= 0:
            continue
        snap_data = snapshot_lookup(snapshot, name)
        available = snap_data.get('total', 0)
        if available < qty:
            shortage.append(
                f"{name}: 필요 {qty}, 현재고 {available} (부족 {qty - available})"
            )

    if shortage:
        return {'success': False,
                'warnings': ['재고 부족으로 기타출고를 진행할 수 없습니다.'],
                'shortage': shortage, 'out_count': 0, 'in_count': 0}

    # payload 생성 (양수=차감 ETC_OUT, 음수=증량 ETC_IN)
    payload = []
    out_count = 0
    in_count = 0

    for item in items:
        name = item.get('product_name', '').strip()
        qty = item.get('qty', 0)
        reason = item.get('reason', '기타')
        memo = item.get('memo', '').strip()

        if not name or qty == 0:
            continue

        memo_str = f"[{reason}] {memo}" if memo else f"[{reason}]"

        snap_data = snapshot_lookup(snapshot, name)
        groups = snap_data.get('groups', [])

        # ── 음수 수량 = 증량 (ETC_IN) ──
        if qty < 0:
            add_qty = abs(qty)
            base = groups[0] if groups else {}
            payload.append({
                "transaction_date": date_str,
                "type": "ETC_IN",
                "product_name": name,
                "qty": add_qty,
                "location": location,
                "category": base.get('category', ''),
                "expiry_date": base.get('expiry_date', ''),
                "storage_method": base.get('storage_method', ''),
                "unit": base.get('unit', snap_data.get('unit', '개')),
                "origin": base.get('origin', ''),
                "manufacture_date": base.get('manufacture_date', ''),
                "memo": memo_str,
                "status": "active",
            })
            in_count += 1
            continue

        # ── 양수 수량 = 차감 (ETC_OUT, FIFO) ──
        remain = qty

        if not groups:
            payload.append({
                "transaction_date": date_str,
                "type": "ETC_OUT",
                "product_name": name,
                "qty": -remain,
                "location": location,
                "unit": snap_data.get('unit', '개'),
                "category": snap_data.get('category', ''),
                "storage_method": snap_data.get('storage_method', ''),
                "memo": memo_str,
                "status": "active",
            })
            out_count += 1
        else:
            for g in groups:
                if remain <= 0:
                    break
                deduct = min(remain, g['qty'])
                if deduct <= 0:
                    continue
                payload.append({
                    "transaction_date": date_str,
                    "type": "ETC_OUT",
                    "product_name": name,
                    "qty": -deduct,
                    "location": location,
                    "category": g.get('category', ''),
                    "expiry_date": g.get('expiry_date', ''),
                    "storage_method": g.get('storage_method', ''),
                    "unit": g.get('unit', '개'),
                    "origin": g.get('origin', ''),
                    "manufacture_date": g.get('manufacture_date', ''),
                    "memo": memo_str,
                    "status": "active",
                })
                g['qty'] -= deduct
                remain -= deduct
                out_count += 1

    # DB 삽입
    if not payload:
        return {'success': False, 'warnings': ['처리할 출고 항목이 없습니다.'],
                'shortage': [], 'out_count': 0, 'in_count': 0}

    try:
        result = db.insert_stock_ledger(payload)
        if result.get('failed', 0) > 0:
            errors = result.get('errors', [])
            err_msg = f"일부 항목 저장 실패 ({result['failed']}건): {'; '.join(errors[:3])}"
            return {'success': False, 'warnings': [err_msg],
                    'shortage': [], 'out_count': 0, 'in_count': 0}
    except Exception as e:
        return {'success': False, 'warnings': [f'DB 저장 중 오류: {e}'],
                'shortage': [], 'out_count': 0, 'in_count': 0}

    return {
        'success': True,
        'out_count': out_count,
        'in_count': in_count,
        'item_count': len(items),
        'warnings': [],
        'shortage': [],
    }
