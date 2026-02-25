"""
inbound_service.py — 입고 관리 비즈니스 로직.
시스템 입력(다건 배치) 처리.
"""
from datetime import datetime
from services.excel_io import safe_date, normalize_location, safe_qty


def _validate_date(date_str):
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        raise ValueError(f"날짜 형식이 올바르지 않습니다: {date_str}. YYYY-MM-DD 형식으로 입력하세요.")


def process_inbound_batch(db, date_str, mode, items):
    """시스템 입력 다건 입고 처리.

    Args:
        db: SupabaseDB instance
        date_str: 입고일자 (YYYY-MM-DD)
        mode: '신규입력' or '수정입력'
        items: list of dicts:
            {product_name, qty, location, category, unit,
             expiry_date, storage_method, manufacture_date,
             origin?, lot_number?, grade?}

    Returns:
        dict: {count, warnings, deleted_count}
    """
    _validate_date(date_str)

    warnings = []
    deleted_count = 0

    # 수정입력: 해당 날짜 입고 기록 삭제
    if mode == '수정입력':
        deleted_count = db.delete_stock_ledger_by(date_str, "INBOUND")

    payload = []
    for item in items:
        name = str(item.get('product_name', '')).strip()
        unit = str(item.get('unit', '개')).strip() or '개'
        qty = safe_qty(item.get('qty', 0), unit=unit)
        location = normalize_location(item.get('location', ''))

        if not name or qty <= 0 or not location:
            continue

        payload.append({
            "transaction_date": date_str,
            "type": "INBOUND",
            "product_name": name,
            "qty": qty,
            "location": location,
            "category": str(item.get('category', '')).strip(),
            "expiry_date": safe_date(item.get('expiry_date', '')),
            "storage_method": str(item.get('storage_method', '')).strip(),
            "unit": str(item.get('unit', '개')).strip() or '개',
            "manufacture_date": safe_date(item.get('manufacture_date', '')),
            "food_type": str(item.get('food_type', '')).strip(),
            "origin": str(item.get('origin', '')).strip() or None,
            "lot_number": str(item.get('lot_number', '')).strip() or None,
            "grade": str(item.get('grade', '')).strip() or None,
        })

    if payload:
        try:
            db.insert_stock_ledger(payload)
        except Exception as e:
            if mode == '수정입력' and deleted_count > 0:
                warnings.append(f"주의: 기존 {deleted_count}건이 삭제되었으나 새 데이터 저장에 실패했습니다. 수정입력으로 재시도하세요.")
            raise

    return {
        'count': len(payload),
        'warnings': warnings,
        'deleted_count': deleted_count,
    }
