"""
validation.py — 데이터 수정 금지. 누락/비표준/음수재고 등 Warning 목록만 생성.
"""
import pandas as pd


def detect_warnings(df):
    """데이터 품질 경고 감지 — 수정 없이 플래그만.
    df: pandas DataFrame (stock_ledger raw data)
    returns: list of dict {product_name, field, issue}
    """
    warnings = []
    seen = set()
    for _, row in df.iterrows():
        name = str(row.get('product_name', '')).strip()
        cat = str(row.get('category', '')).strip()

        def _add(field, issue):
            key = (name, field)
            if key not in seen:
                seen.add(key)
                warnings.append({"product_name": name, "field": field, "issue": issue})

        unit = str(row.get('unit', '')).strip()
        if not unit or unit in ('', 'nan', 'None'):
            _add("단위", "미입력")
        if cat in ('원료', '원재료', '부자재'):
            origin = str(row.get('origin', '')).strip()
            if not origin or origin in ('', 'nan', 'None'):
                _add("원산지", "미입력(원료/부자재)")
        mfg = str(row.get('manufacture_date', '')).strip()
        if not mfg or mfg in ('', 'nan', 'None'):
            _add("제조일", "미입력")
        exp = str(row.get('expiry_date', '')).strip()
        if not exp or exp in ('', 'nan', 'None'):
            _add("소비기한", "미입력")
        if not cat or cat in ('nan', 'None'):
            _add("종류", "미입력")
    return warnings


def detect_repack_warnings(df):
    """소분 데이터 경고 감지 — LOT/제조일/소비기한 누락."""
    warnings = []
    seen = set()
    for _, r in df.iterrows():
        name = str(r.get('product_name', '')).strip()

        def _w(field, issue):
            key = (name, field)
            if key not in seen:
                seen.add(key)
                warnings.append({"product_name": name, "field": field, "issue": issue})

        lot_val = str(r.get('lot_number', '') or r.get('source_lot', '') or r.get('result_lot', '')).strip()
        if not lot_val or lot_val in ('', 'nan', 'None'):
            _w("LOT", "미입력")
        mfg = str(r.get('manufacture_date', '')).strip()
        if not mfg or mfg in ('', 'nan', 'None'):
            _w("제조일", "미입력")
        exp = str(r.get('expiry_date', '')).strip()
        if not exp or exp in ('', 'nan', 'None'):
            _w("소비기한", "미입력")
    return warnings


def check_unit_mismatch(excel_df, db_query_fn, unit_col='단위'):
    """엑셀 품목의 단위와 DB 기존 단위를 비교.
    excel_df: pandas DataFrame
    db_query_fn: fn(product_name) -> unit string or None
    returns: (mismatch_list, no_unit_list)
        mismatch_list: ["  품목명: 엑셀=EA, DB=개", ...]
        no_unit_list:  ["  품목명: 엑셀=EA, DB=미설정(기본 '개')", ...]
    """
    excel_units = {}
    for _, row in excel_df.iterrows():
        name = str(row.get('품목명', '')).strip()
        unit = str(row.get(unit_col, '')).strip()
        if name and unit:
            excel_units[name] = unit

    if not excel_units:
        return [], []

    mismatch = []
    no_unit = []
    for name, excel_unit in excel_units.items():
        db_unit = db_query_fn(name)
        if db_unit is not None:
            if not db_unit or db_unit.strip() == '':
                no_unit.append(f"  {name}: 엑셀={excel_unit}, DB=미설정(기본 '개')")
            elif db_unit.strip() != excel_unit:
                mismatch.append(f"  {name}: 엑셀={excel_unit}, DB={db_unit.strip()}")
    return mismatch, no_unit
