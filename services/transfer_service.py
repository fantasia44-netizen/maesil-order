"""
transfer_service.py — 창고이동(Tab 4) 비즈니스 로직.
Tkinter 의존 제거. db 파라미터(SupabaseDB 인스턴스)를 받고 결과를 dict/list로 반환.
"""
import pandas as pd
from datetime import datetime

try:
    from excel_io import safe_int, normalize_location, build_stock_snapshot, snapshot_lookup
except ImportError:
    from services.excel_io import safe_int, normalize_location, build_stock_snapshot, snapshot_lookup


def _validate_date(date_str):
    """날짜 형식 검증. 실패 시 ValueError raise."""
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


def process_manual_transfer(db, product_name, qty, from_location, to_location,
                            date_str, mode="신규입력", lot_number=None, grade=None):
    """수동 창고 이동 (FIFO 자동 상속).

    Args:
        db: SupabaseDB instance
        product_name: 품목명
        qty: 이동 수량 (양수)
        from_location: 현재 창고 (정규화 전)
        to_location: 이동 창고 (정규화 전)
        date_str: 이동 일자 (YYYY-MM-DD)
        mode: "신규입력" or "수정입력"

    Returns:
        dict: {
            "moved_count": int,   # 이동 처리된 FIFO 그룹 수
            "warnings": list,     # 경고 메시지 목록
            "deleted_count": int  # 수정입력 시 삭제된 기존 건수
        }

    Raises:
        ValueError: 날짜 형식 오류 또는 유효성 검증 실패
        Exception: DB 오류
    """
    # ── 1차 실시간 검증 (Validation Engine) ──
    try:
        from core.validation_engine import validate, generate_transaction_id
        validate.transfer(db, date_str, product_name, qty, from_location, to_location)
        transfer_id = generate_transaction_id()
    except ImportError:
        transfer_id = None

    _validate_date(date_str)

    remain = int(qty)
    name = str(product_name).strip()
    src = normalize_location(from_location)
    dst = normalize_location(to_location)

    warnings = []
    deleted_count = 0

    # 기존 이동 기록 삭제 (신규/수정 모두 — 중복 방지, 해당 품목+창고만)
    del1 = db.delete_stock_ledger_by(date_str, "MOVE_OUT", location=src, product_names={name})
    del2 = db.delete_stock_ledger_by(date_str, "MOVE_IN", location=dst, product_names={name})
    deleted_count = del1 + del2

    # 재고 스냅샷 로드 (삭제된 후 로드해야 정확)
    stock = _load_stock_snapshot(db, src)
    _snap = snapshot_lookup(stock, name)
    groups = _snap.get('groups', [])
    total = _snap.get('total', 0)
    u = _snap.get('unit', '개')

    if remain > total:
        warnings.append(
            f"[{src}] {name}: 재고 {total}{u} / 이동요청 {remain}{u} — 재고 부족 상태에서 이동 처리됨"
        )

    # 이력번호/등급 지정 시 해당 lot만 필터링
    req_lot = str(lot_number).strip() if lot_number else ''
    req_grade = str(grade).strip() if grade else ''

    payload = []
    if not groups:
        # 재고 그룹 없음 — 빈 속성으로 이동
        payload.append({
            "transaction_date": date_str, "type": "MOVE_OUT",
            "product_name": name, "qty": -remain, "location": src,
            "manufacture_date": '',
            "lot_number": req_lot or None, "grade": req_grade or None,
            "transfer_id": transfer_id,
        })
        payload.append({
            "transaction_date": date_str, "type": "MOVE_IN",
            "product_name": name, "qty": remain, "location": dst,
            "manufacture_date": '',
            "lot_number": req_lot or None, "grade": req_grade or None,
            "transfer_id": transfer_id,
        })
    else:
        # 이력번호 지정 시 해당 lot 그룹만 대상
        if req_lot:
            groups = [g for g in groups if g.get('lot_number', '') == req_lot]
            if not groups:
                warnings.append(f"[{src}] {name}: 이력번호 '{req_lot}' 에 해당하는 재고 없음")

        # FIFO 순서로 그룹별 이동 (속성 상속)
        for g in groups:
            if remain <= 0:
                break
            deduct = min(remain, g['qty'])
            payload.append({
                "transaction_date": date_str, "type": "MOVE_OUT",
                "product_name": name,
                "qty": -deduct, "location": src,
                "category": g['category'], "expiry_date": g['expiry_date'],
                "storage_method": g['storage_method'],
                "unit": g.get('unit', '개'),
                "origin": g.get('origin', ''),
                "manufacture_date": g.get('manufacture_date', ''),
                "food_type": g.get('food_type', ''),
                "lot_number": g.get('lot_number', '') or None,
                "grade": g.get('grade', '') or None,
                "transfer_id": transfer_id,
            })
            payload.append({
                "transaction_date": date_str, "type": "MOVE_IN",
                "product_name": name,
                "qty": deduct, "location": dst,
                "category": g['category'], "expiry_date": g['expiry_date'],
                "storage_method": g['storage_method'],
                "unit": g.get('unit', '개'),
                "origin": g.get('origin', ''),
                "manufacture_date": g.get('manufacture_date', ''),
                "food_type": g.get('food_type', ''),
                "lot_number": g.get('lot_number', '') or None,
                "grade": g.get('grade', '') or None,
                "transfer_id": transfer_id,
            })
            remain -= deduct

    db.insert_stock_ledger(payload)

    return {
        "moved_count": len(payload) // 2,
        "warnings": warnings,
        "deleted_count": deleted_count,
    }


def process_transfer_excel(db, excel_df, date_str, mode="신규입력"):
    """엑셀 일괄 창고 이동 (FIFO 자동 상속).

    Args:
        db: SupabaseDB instance
        excel_df: pandas DataFrame (컬럼: 품목명, 현재창고위치, 이동창고위치, 수량입력)
        date_str: 이동 일자 (YYYY-MM-DD)
        mode: "신규입력" or "수정입력"

    Returns:
        dict: {
            "count": int,         # 이동 처리된 건수 (MOVE_OUT/MOVE_IN 쌍 수)
            "warnings": list,     # 경고 메시지 목록
            "deleted_count": int  # 수정입력 시 삭제된 기존 건수
        }

    Raises:
        ValueError: 날짜 형식 오류
        KeyError: 필수 컬럼 누락
        Exception: DB 오류
    """
    _validate_date(date_str)

    df = excel_df.fillna("")
    warnings = []
    deleted_count = 0

    # transfer_id 생성
    try:
        from core.validation_engine import generate_transaction_id
    except ImportError:
        generate_transaction_id = lambda: None

    # 기존 이동 기록 삭제 (신규/수정 모두 — 중복 방지)
    # 엑셀에 있는 품목+창고 조합만 정밀 삭제
    _del_names = set()
    _del_locs = set()
    for _, row in df.iterrows():
        _nm = str(row['품목명']).strip()
        _src = normalize_location(row['현재창고위치'])
        _dst = normalize_location(row['이동창고위치'])
        if _nm:
            _del_names.add(_nm)
        if _src:
            _del_locs.add(_src)
        if _dst:
            _del_locs.add(_dst)
    if _del_names:
        for _loc in _del_locs:
            deleted_count += db.delete_stock_ledger_by(date_str, "MOVE_OUT", location=_loc, product_names=_del_names)
            deleted_count += db.delete_stock_ledger_by(date_str, "MOVE_IN", location=_loc, product_names=_del_names)

    # 재고 스냅샷 캐시 (창고별)
    snapshots = {}

    # 1단계: 재고 부족 체크
    shortage = []
    for _, row in df.iterrows():
        name = str(row['품목명']).strip()
        src = normalize_location(row['현재창고위치'])
        move_qty = int(row['수량입력'])
        if src not in snapshots:
            snapshots[src] = _load_stock_snapshot(db, src)
        _snap = snapshot_lookup(snapshots[src], name)
        total = _snap.get('total', 0)
        u = _snap.get('unit', '개')
        if move_qty > total:
            shortage.append(f"[{src}] {name}: 요청 {move_qty}{u} / 재고 {total}{u}")

    if shortage:
        warnings.extend([f"재고 부족: {s}" for s in shortage])

    # 2단계: FIFO 이동 payload 생성
    payload = []
    for _, row in df.iterrows():
        name = str(row['품목명']).strip()
        src = normalize_location(row['현재창고위치'])
        dst = normalize_location(row['이동창고위치'])
        remain = int(row['수량입력'])
        tid = generate_transaction_id()

        groups = snapshot_lookup(snapshots[src], name).get('groups', [])

        if not groups:
            payload.append({
                "transaction_date": date_str, "type": "MOVE_OUT",
                "product_name": name, "qty": -remain, "location": src,
                "manufacture_date": '', "transfer_id": tid,
            })
            payload.append({
                "transaction_date": date_str, "type": "MOVE_IN",
                "product_name": name, "qty": remain, "location": dst,
                "manufacture_date": '', "transfer_id": tid,
            })
        else:
            for g in groups:
                if remain <= 0:
                    break
                deduct = min(remain, g['qty'])
                payload.append({
                    "transaction_date": date_str, "type": "MOVE_OUT",
                    "product_name": name,
                    "qty": -deduct, "location": src,
                    "category": g['category'], "expiry_date": g['expiry_date'],
                    "storage_method": g['storage_method'],
                    "unit": g.get('unit', '개'),
                    "origin": g.get('origin', ''),
                    "manufacture_date": g.get('manufacture_date', ''),
                    "food_type": g.get('food_type', ''),
                    "lot_number": g.get('lot_number', '') or None,
                    "grade": g.get('grade', '') or None,
                    "transfer_id": tid,
                })
                payload.append({
                    "transaction_date": date_str, "type": "MOVE_IN",
                    "product_name": name,
                    "qty": deduct, "location": dst,
                    "category": g['category'], "expiry_date": g['expiry_date'],
                    "storage_method": g['storage_method'],
                    "unit": g.get('unit', '개'),
                    "origin": g.get('origin', ''),
                    "manufacture_date": g.get('manufacture_date', ''),
                    "food_type": g.get('food_type', ''),
                    "lot_number": g.get('lot_number', '') or None,
                    "grade": g.get('grade', '') or None,
                    "transfer_id": tid,
                })
                remain -= deduct

    db.insert_stock_ledger(payload)

    return {
        "count": len(payload) // 2,
        "warnings": warnings,
        "deleted_count": deleted_count,
    }
