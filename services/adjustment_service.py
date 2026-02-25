"""
adjustment_service.py — 재고 조정 비즈니스 로직.
양수/음수 수량으로 재고 증감 조정, 사유(memo) 필수.
"""
from datetime import datetime
from services.excel_io import normalize_location, safe_qty, build_stock_snapshot, snapshot_lookup


def _validate_date(date_str):
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        raise ValueError(f"날짜 형식이 올바르지 않습니다: {date_str}. YYYY-MM-DD 형식으로 입력하세요.")


def process_adjustment_batch(db, date_str, items):
    """다건 재고 조정 처리.

    Args:
        db: SupabaseDB instance
        date_str: 조정일자 (YYYY-MM-DD)
        items: list of dicts:
            {product_name, location, qty(+/-), memo(사유)}

    Returns:
        dict: {count, increase_count, decrease_count, warnings}
    """
    _validate_date(date_str)

    warnings = []
    payload = []
    increase_count = 0
    decrease_count = 0

    # 재고 스냅샷 캐시 (차감 검증용)
    _snapshots = {}

    for item in items:
        name = str(item.get('product_name', '')).strip()
        location = normalize_location(item.get('location', ''))
        unit = str(item.get('unit', '')).strip()
        qty = safe_qty(item.get('qty', 0), unit=unit or '개')
        memo = str(item.get('memo', '')).strip()
        storage_method = str(item.get('storage_method', '')).strip()
        unit = str(item.get('unit', '')).strip()

        if not name or qty == 0 or not location or not memo:
            continue

        # ── 음수(차감) 시 재고 존재 및 충분 여부 검증 ──
        if qty < 0:
            if location not in _snapshots:
                try:
                    raw = db.query_stock_by_location(location)
                    _snapshots[location] = build_stock_snapshot(raw)
                except Exception:
                    _snapshots[location] = {}
            snap = snapshot_lookup(_snapshots[location], name)
            total = snap.get('total', 0)
            snap_unit = snap.get('unit', '')
            abs_qty = abs(qty)
            if total <= 0:
                raise ValueError(
                    f"[{location}] '{name}' 재고가 없습니다. "
                    f"품목명·위치·보관방법을 확인하세요.")
            if abs_qty > total:
                u = snap_unit or unit or '개'
                raise ValueError(
                    f"[{location}] '{name}' 재고 부족: "
                    f"차감 {abs_qty}{u} / 현재 재고 {total}{u}")

        row = {
            "transaction_date": date_str,
            "type": "ADJUST",
            "product_name": name,
            "qty": qty,
            "location": location,
            "memo": memo,
        }
        if storage_method:
            row["storage_method"] = storage_method
        if unit:
            row["unit"] = unit

        payload.append(row)

        if qty > 0:
            increase_count += 1
        else:
            decrease_count += 1

    if payload:
        db.insert_stock_ledger(payload)

    return {
        'count': len(payload),
        'increase_count': increase_count,
        'decrease_count': decrease_count,
        'warnings': warnings,
    }
