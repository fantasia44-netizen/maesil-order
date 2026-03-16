"""
inbound_service.py — 입고 관리 비즈니스 로직.
시스템 입력(다건 배치) 처리. 순수 누적 INSERT 방식.
"""
import time
from datetime import datetime
from services.excel_io import safe_date, normalize_location, safe_qty


def _validate_date(date_str):
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        raise ValueError(f"날짜 형식이 올바르지 않습니다: {date_str}. YYYY-MM-DD 형식으로 입력하세요.")


def process_inbound_batch(db, date_str, items, created_by=None):
    """시스템 입력 다건 입고 처리 (누적 INSERT).

    Args:
        db: SupabaseDB instance
        date_str: 입고일자 (YYYY-MM-DD)
        items: list of dicts:
            {product_name, qty, location, category, unit,
             expiry_date, storage_method, manufacture_date,
             origin?, lot_number?, grade?}
        created_by: 작업자명

    Returns:
        dict: {count, warnings}
    """
    # ── 1차 실시간 검증 (Validation Engine) ──
    try:
        from core.validation_engine import validate
        validate.inbound(db, date_str, items)
    except ImportError:
        pass  # core 미설치 시 기존 동작 유지

    _validate_date(date_str)

    warnings = []
    ts_ms = int(time.time() * 1000)

    payload = []
    for idx, item in enumerate(items):
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
            "event_uid": f"INB:{date_str}:{location}:{name}:{ts_ms}:{idx}",
            "created_by": created_by,
            "status": "active",
        })

    if payload:
        db.insert_stock_ledger(payload)

    return {
        'count': len(payload),
        'warnings': warnings,
    }
