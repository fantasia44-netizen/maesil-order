"""
excel_io.py — 엑셀 파싱/필드 매핑만. 데이터 수정은 하지 않는다.
"""
import pandas as pd
from datetime import datetime


def safe_int(val, default=0):
    """NaN/빈값/문자열 안전 정수 변환. Python에서 NaN은 truthy이므로 'or 0' 패턴 사용 불가."""
    try:
        n = pd.to_numeric(val, errors='coerce')
        if pd.isna(n):
            return default
        return int(n)
    except (ValueError, TypeError):
        return default


def safe_qty(val, unit='개', default=0):
    """단위 기반 수량 변환. kg → float(소수점 허용), 개 → int(정수).
    단위가 'kg'이면 소수점 유지, 그 외는 정수 변환."""
    try:
        n = pd.to_numeric(val, errors='coerce') if not isinstance(val, (int, float)) else val
        if isinstance(n, float) and pd.isna(n):
            return default
        n = float(n)
    except (ValueError, TypeError):
        return default
    if _is_decimal_unit(unit):
        # 소수점 유지, 불필요한 소수점 제거 (1.0 → 1)
        return n if n != int(n) else int(n)
    return int(n)


def _is_decimal_unit(unit):
    """소수점을 허용하는 단위인지 판별."""
    if not unit:
        return False
    u = str(unit).strip().lower()
    return u in ('kg', 'g', 'l', 'ml', 'lb')


def _snap_qty(val, unit='개'):
    """스냅샷 qty 변환: kg 등 소수점 단위면 float, 그 외는 int."""
    try:
        n = float(val)
    except (ValueError, TypeError):
        return 0
    if _is_decimal_unit(unit):
        return n if n != int(n) else int(n)
    return int(n)


def normalize_product_name(name):
    """품목명 정규화 — 공백 제거 후 비교용 키 생성.
    예: '(수)건해삼채 200g' → '(수)건해삼채200g'
    """
    if not name:
        return ''
    return str(name).replace(' ', '').strip()


def snapshot_lookup(stock, name):
    """stock snapshot에서 품목명을 정규화하여 조회.
    정확 매칭 우선, 없으면 공백 제거 후 매칭 시도.
    returns: {groups: [...], total: int, unit: str} or empty dict
    """
    # 1) 정확 매칭
    if name in stock:
        return stock[name]
    # 2) 공백 제거 후 매칭
    norm = normalize_product_name(name)
    for key, val in stock.items():
        if normalize_product_name(key) == norm:
            return val
    return {}


def safe_date(val, fmt='%Y-%m-%d'):
    """날짜 값을 안전하게 문자열로 변환. 빈값/NaT/비정상 → None 반환."""
    if val is None or val == '' or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        d = pd.to_datetime(val)
        if pd.isna(d):
            return None
        return d.strftime(fmt)
    except Exception:
        return None


def normalize_location(loc):
    """창고명 정규화: CJ용인 대소문자/공백 차이 통일."""
    if not loc:
        return loc
    s = str(loc).strip()
    low = s.replace(' ', '').lower()
    if low == 'cj용인':
        return 'CJ용인'
    return s


def flexible_column_rename(df):
    """컬럼명 유연 매핑 (창고→창고위치, 제조일자→제조일)."""
    _cm = {}
    _cols = df.columns.tolist()
    if '창고' in _cols and '창고위치' not in _cols:
        _cm['창고'] = '창고위치'
    if '제조일자' in _cols and '제조일' not in _cols:
        _cm['제조일자'] = '제조일'
    if _cm:
        df = df.rename(columns=_cm)
    return df


def detect_qty_col(df):
    """수량 컬럼명 자동 감지: 합산 > qty > 수량."""
    if '합산' in df.columns:
        return '합산'
    if 'qty' in df.columns:
        return 'qty'
    if '수량' in df.columns:
        return '수량'
    return None


def detect_material_groups(cols):
    """원재료/부재료 컬럼 그룹 자동 감지.
    반환: [{name_col, cat_col, exp_col, qty_col, origin_col}, ...]
    """
    name_cols = [c for c in cols if c in ('원재료', '부재료') or
                 (c.startswith('부재료') and c != '부재료')]
    if not name_cols:
        return []

    def suffixed(prefix):
        return sorted([c for c in cols if c.startswith(prefix + '.') or
                       (c.startswith(prefix) and c != prefix and '.' in c)],
                      key=lambda x: int(x.split('.')[-1]) if '.' in x else 0)

    cat_cols = suffixed('종류')
    exp_cols = suffixed('소비기한')
    qty_cols = sorted([c for c in cols if c == '수량' or c.startswith('수량.')],
                      key=lambda x: int(x.split('.')[-1]) if '.' in x else 0)
    origin_col = '원산지' if '원산지' in cols else None

    groups = []
    for i, nc in enumerate(name_cols):
        groups.append({
            'name_col': nc,
            'cat_col': cat_cols[i] if i < len(cat_cols) else None,
            'exp_col': exp_cols[i] if i < len(exp_cols) else None,
            'qty_col': qty_cols[i] if i < len(qty_cols) else None,
            'origin_col': origin_col if nc == '원재료' else None,
        })
    return groups


def parse_inbound_payload(df, today):
    """입고 엑셀 → payload list."""
    payload = []
    for _, row in df.iterrows():
        unit = str(row.get('단위', '개')).strip() or '개'
        qty = safe_qty(row.get('입고수량', row.get('현재재고', 0)), unit=unit)
        payload.append({
            "transaction_date": today, "type": "INBOUND",
            "product_name": str(row['품목명']).strip(),
            "qty": qty, "location": normalize_location(row['창고위치']),
            "expiry_date": safe_date(row.get('소비기한', '')),
            "category": str(row.get('종류', '')),
            "storage_method": str(row.get('보관방법', '')),
            "unit": str(row.get('단위', '개')).strip() or '개',
            "lot_number": str(row.get('이력번호', '')).strip() or None,
            "grade": str(row.get('등급', '')).strip() or None,
            "manufacture_date": safe_date(row.get('제조일', '')),
            "origin": str(row.get('원산지', '')).strip() or None,
        })
    return payload


def parse_base_data_payload(df, today):
    """기초데이터 엑셀 → payload list."""
    payload = []
    for _, row in df.iterrows():
        unit = str(row.get('단위', '개')).strip() or '개'
        qty = safe_qty(row.get('입고수량', row.get('현재재고', 0)), unit=unit)
        if qty == 0:
            continue
        payload.append({
            "transaction_date": today, "type": "INIT",
            "product_name": str(row['품목명']).strip(),
            "qty": qty, "location": normalize_location(row['창고위치']),
            "expiry_date": safe_date(row.get('소비기한', '')),
            "category": str(row.get('종류', '')),
            "storage_method": str(row.get('보관방법', '')),
            "unit": str(row.get('단위', '개')).strip() or '개',
            "lot_number": str(row.get('이력번호', '')).strip() or None,
            "grade": str(row.get('등급', '')).strip() or None,
            "manufacture_date": safe_date(row.get('제조일', '')),
            "origin": str(row.get('원산지', '')).strip() or None,
        })
    return payload


def parse_revenue_payload(df, upload_date):
    """일일매출 엑셀 → payload list. Returns (payload, total_revenue)."""
    CATS = ["일반매출", "쿠팡매출", "로켓", "N배송(용인)"]
    payload = []
    for _, row in df.iterrows():
        nm = str(row.get('품목명', '')).strip()
        if not nm or nm == '합계':
            continue
        for cat in CATS:
            q_col = f'{cat}_수량'
            p_col = f'{cat}_단가'
            r_col = f'{cat}_매출'
            qty = safe_int(row.get(q_col, 0))
            if qty == 0:
                continue
            payload.append({
                "revenue_date": upload_date,
                "product_name": nm,
                "category": cat,
                "qty": qty,
                "unit_price": safe_int(row.get(p_col, 0)),
                "revenue": safe_int(row.get(r_col, 0)),
            })
    total_rev = sum(r['revenue'] for r in payload) if payload else 0
    return payload, total_rev


def build_stock_snapshot(all_data):
    """raw stock_ledger list of dict → 품목별 FIFO 그룹 딕셔너리.
    returns: {product_name: {groups: [...], total: int, unit: str,
              category: str, storage_method: str}}
    ※ category, storage_method는 재고 0인 품목도 최근 데이터에서 가져옴 (fallback용)
    """
    if not all_data:
        return {}
    df = pd.DataFrame(all_data)
    for col in ['origin', 'manufacture_date', 'storage_method', 'category', 'unit', 'food_type']:
        if col not in df.columns:
            df[col] = ''
    df['origin'] = df['origin'].fillna('')
    df['manufacture_date'] = df['manufacture_date'].fillna('')
    df['storage_method'] = df['storage_method'].fillna('')
    df['category'] = df['category'].fillna('')
    df['unit'] = df['unit'].fillna('개')
    df['food_type'] = df['food_type'].fillna('')
    group_cols = ['product_name', 'category', 'expiry_date',
                  'storage_method', 'unit', 'origin', 'manufacture_date', 'food_type']
    summary = df.groupby(group_cols, dropna=False)['qty'].sum().reset_index()

    # ── 필터 전: 모든 품목의 메타 정보 수집 (재고 0인 품목 포함) ──
    all_meta = {}
    for _, r in summary.iterrows():
        name = r['product_name']
        if name not in all_meta:
            unit_val = r['unit'] if (pd.notna(r.get('unit')) and r['unit'] != '') else '개'
            cat_val = r['category'] if (pd.notna(r['category']) and r['category'] != '') else ''
            stg_val = r['storage_method'] if (pd.notna(r['storage_method']) and r['storage_method'] != '') else ''
            ft_val = r['food_type'] if (pd.notna(r.get('food_type')) and r['food_type'] != '') else ''
            all_meta[name] = {'unit': unit_val, 'category': cat_val, 'storage_method': stg_val, 'food_type': ft_val}

    summary = summary[summary['qty'] > 0]
    summary = summary.sort_values(['product_name', 'expiry_date'], na_position='last')

    # ── stock dict: 메타 있는 모든 품목 미리 생성 (재고 0 포함) ──
    stock = {}
    for name, meta in all_meta.items():
        stock[name] = {
            'groups': [], 'total': 0,
            'unit': meta['unit'],
            'category': meta['category'],
            'storage_method': meta['storage_method'],
            'food_type': meta.get('food_type', ''),
        }

    for _, r in summary.iterrows():
        name = r['product_name']
        # stock[name]은 위에서 이미 생성됨
        unit_val = r['unit'] if (pd.notna(r.get('unit')) and r['unit'] != '') else '개'
        stock[name]['groups'].append({
            'category': r['category'] if pd.notna(r['category']) else '',
            'expiry_date': r['expiry_date'] if pd.notna(r['expiry_date']) else None,
            'storage_method': r['storage_method'] if pd.notna(r['storage_method']) else '',
            'unit': unit_val,
            'origin': r['origin'] if pd.notna(r.get('origin')) else '',
            'manufacture_date': r['manufacture_date'] if pd.notna(r.get('manufacture_date')) else '',
            'food_type': r['food_type'] if pd.notna(r.get('food_type')) else '',
            'qty': _snap_qty(r['qty'], unit_val)
        })
        stock[name]['total'] += _snap_qty(r['qty'], unit_val)
        stock[name]['unit'] = unit_val
    return stock
