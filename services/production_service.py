"""
production_service.py -- 생산/입고 (탭2) 비즈니스 로직.
Tkinter UI 제거, 순수 데이터 반환.
"""
import uuid
import pandas as pd
from datetime import datetime

from services.excel_io import (
    safe_int, safe_date, normalize_location, flexible_column_rename,
    detect_material_groups, parse_inbound_payload, build_stock_snapshot,
    snapshot_lookup, normalize_product_name,
)
from services.validation import check_unit_mismatch


# ─── 헬퍼 ───

def _validate_date(date_str):
    """날짜 형식 검증. 유효하지 않으면 ValueError 발생."""
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        raise ValueError(f"날짜 형식이 올바르지 않습니다: {date_str}. YYYY-MM-DD 형식으로 입력하세요.")


def _load_stock_snapshot(db, location):
    """특정 창고의 재고 FIFO 스냅샷을 반환."""
    try:
        all_data = db.query_stock_by_location(location)
        return build_stock_snapshot(all_data)
    except Exception as e:
        print(f"재고 스냅샷 조회 에러: {e}")
        return {}


def _load_stock_with_meta(db, location):
    """특정 창고의 재고 FIFO 스냅샷 + 품목 메타데이터를 반환.
    Returns: (snapshot_dict, meta_dict)
    """
    raw_data = []
    try:
        raw_data = db.query_stock_by_location(location)
    except Exception as e:
        print(f"재고 데이터 조회 에러: {e}")
    snapshot = build_stock_snapshot(raw_data)
    meta = _build_product_metadata(raw_data)
    return snapshot, meta


def _build_product_metadata(raw_stock_data):
    """raw stock data에서 품목별 메타데이터 추출 (보관방법, 종류, 단위 등).
    양수 재고 레코드 우선으로 가장 대표적인 메타데이터를 반환.
    """
    meta = {}
    for r in raw_stock_data:
        name = r.get('product_name', '')
        qty = r.get('qty', 0) or 0
        if not name:
            continue
        # 이미 있는 항목보다 양수 재고가 더 큰 레코드 우선
        if name not in meta or qty > meta[name].get('_qty', 0):
            meta[name] = {
                'storage_method': r.get('storage_method', '') or '',
                'category': r.get('category', '') or '',
                'unit': r.get('unit', '개') or '개',
                'expiry_date': r.get('expiry_date', '') or '',
                'origin': r.get('origin', '') or '',
                'manufacture_date': r.get('manufacture_date', '') or '',
                '_qty': qty,
            }
    # 내부 _qty 필드 제거
    for v in meta.values():
        v.pop('_qty', None)
    return meta


def _lookup_product_meta(meta_dict, product_name):
    """메타데이터 딕셔너리에서 품목 조회 (정규화 매칭 포함)."""
    if product_name in meta_dict:
        return meta_dict[product_name]
    norm = normalize_product_name(product_name)
    for key, val in meta_dict.items():
        if normalize_product_name(key) == norm:
            return val
    return {}


def _check_unit_mismatch_result(df, db, unit_col='단위'):
    """단위 불일치 검사를 수행하여 결과를 반환.

    Returns:
        dict: {
            'has_mismatch': bool,
            'mismatch': list of str,  -- 불일치 품목 메시지
            'no_unit': list of str,   -- DB 미설정 품목 메시지
        }
    """
    mismatch, no_unit = check_unit_mismatch(df, db.query_unit_for_product, unit_col)
    return {
        'has_mismatch': bool(mismatch) or bool(no_unit),
        'mismatch': mismatch,
        'no_unit': no_unit,
    }


# ─── 입고 처리 ───

def process_inbound(db, excel_df, date_str, mode='신규입력'):
    """입고 엑셀 데이터를 DB에 등록.
    app.py의 run_inbound_ledger 로직과 동일.

    Args:
        db: SupabaseDB instance
        excel_df: pd.DataFrame -- 이미 pd.read_excel().fillna("") 된 상태
        date_str: 처리일자 (YYYY-MM-DD)
        mode: '신규입력' 또는 '수정입력'

    Returns:
        dict: {
            'success': bool,
            'count': int,              -- 등록 건수
            'warnings': list of str,   -- 경고 메시지
            'is_past_date': bool,      -- 과거 날짜 여부
            'unit_check': dict,        -- 단위 불일치 결과
            'deleted_count': int,      -- 수정입력 시 삭제 건수
            'mode': str,               -- 처리 모드
        }

    Raises:
        ValueError: 날짜 형식 오류
        Exception: DB 오류 등
    """
    _validate_date(date_str)

    df = flexible_column_rename(excel_df)

    # 단위 불일치 검사
    unit_check = _check_unit_mismatch_result(df, db)

    warnings = []
    if unit_check['mismatch']:
        warnings.append("단위 불일치 품목:\n" + "\n".join(unit_check['mismatch']))
    if unit_check['no_unit']:
        warnings.append("DB에 단위 미설정 품목:\n" + "\n".join(unit_check['no_unit']))

    is_past_date = date_str < datetime.now().strftime('%Y-%m-%d')

    payload = parse_inbound_payload(df, date_str)

    deleted_count = 0
    if mode == '수정입력':
        deleted_count = db.delete_stock_ledger_by(date_str, "INBOUND")

    db.insert_stock_ledger(payload)

    return {
        'success': True,
        'count': len(payload),
        'warnings': warnings,
        'is_past_date': is_past_date,
        'unit_check': unit_check,
        'deleted_count': deleted_count,
        'mode': mode,
    }


# ─── 생산 처리 ───

def process_production(db, excel_df, date_str, mode='신규입력'):
    """생산 엑셀 데이터를 DB에 등록. FIFO 재료 차감 포함.
    app.py의 run_production_ledger 로직과 동일.

    Args:
        db: SupabaseDB instance
        excel_df: pd.DataFrame -- 이미 pd.read_excel().fillna("") 된 상태
        date_str: 처리일자 (YYYY-MM-DD)
        mode: '신규입력' 또는 '수정입력'

    Returns:
        dict: {
            'success': bool,
            'count': int,              -- 전체 payload 건수
            'produced': int,           -- 생산품(PRODUCTION) 건수
            'materials_used': int,     -- 재료 차감(PROD_OUT) 건수
            'warnings': list of str,   -- 경고 메시지
            'shortage': list of str,   -- 재료 부족 메시지 (경고)
            'is_past_date': bool,
            'unit_check': dict,
            'deleted_count': int,      -- 수정입력 시 삭제 건수
            'mode': str,
        }

    Raises:
        ValueError: 날짜 형식 오류
        Exception: DB 오류 등
    """
    _validate_date(date_str)

    df = flexible_column_rename(excel_df)

    # 단위 불일치 검사
    unit_check = _check_unit_mismatch_result(df, db)

    warnings = []
    if unit_check['mismatch']:
        warnings.append("단위 불일치 품목:\n" + "\n".join(unit_check['mismatch']))
    if unit_check['no_unit']:
        warnings.append("DB에 단위 미설정 품목:\n" + "\n".join(unit_check['no_unit']))

    is_past_date = date_str < datetime.now().strftime('%Y-%m-%d')

    # ── 수정입력: 기존 데이터 먼저 삭제 (snapshot 로드 전에 실행) ──
    deleted_count = 0
    if mode == '수정입력':
        del1 = db.delete_stock_ledger_by(date_str, "PRODUCTION")
        del2 = db.delete_stock_ledger_by(date_str, "PROD_OUT")
        deleted_count = del1 + del2

    cols = list(df.columns)
    material_groups = detect_material_groups(cols)

    # ── 재료 부족 사전 확인 ──
    shortage = []
    snapshots = {}
    product_metas = {}
    for _, row in df.iterrows():
        loc = normalize_location(row['창고위치'])
        for mg in material_groups:
            mat_name = str(row.get(mg['name_col'], '')).strip()
            if not mat_name:
                continue
            mat_qty = safe_int(row.get(mg['qty_col'], 0))
            if mat_qty <= 0:
                continue
            if loc not in snapshots:
                snapshots[loc], product_metas[loc] = _load_stock_with_meta(db, loc)
            _snap = snapshot_lookup(snapshots[loc], mat_name)
            total = _snap.get('total', 0)
            u = _snap.get('unit', '개')
            if mat_qty > total:
                shortage.append(f"  [{loc}] {mat_name}: 필요 {mat_qty}{u} / 재고 {total}{u}")

    if shortage:
        warnings.append("재료 재고 부족:\n" + "\n".join(shortage))

    # ── 페이로드 생성 ──
    payload = []
    prod_count = 0
    raw_count = 0

    for _, row in df.iterrows():
        name = str(row['품목명']).strip()
        prod_qty = safe_int(row.get('생산수량', 0))
        loc = normalize_location(row['창고위치'])
        exp_date = row.get('소비기한', '')

        # 이 행(생산제품)에 대한 고유 batch_id — PRODUCTION+PROD_OUT 연결
        batch_id = f"PROD_{date_str}_{uuid.uuid4().hex[:8]}"

        if prod_qty > 0:
            payload.append({
                "transaction_date": date_str,
                "type": "PRODUCTION",
                "product_name": name,
                "qty": prod_qty,
                "location": loc,
                "expiry_date": safe_date(exp_date),
                "category": str(row.get('종류', '')),
                "storage_method": str(row.get('보관방법', '')),
                "unit": str(row.get('단위', '개')).strip() or '개',
                "lot_number": str(row.get('이력번호', '')).strip() or None,
                "grade": str(row.get('등급', '')).strip() or None,
                "manufacture_date": safe_date(row.get('제조일', '')),
                "batch_id": batch_id,
            })
            prod_count += 1

        for mg in material_groups:
            mat_name = str(row.get(mg['name_col'], '')).strip()
            mat_qty = safe_int(row.get(mg['qty_col'], 0))
            if not mat_name or mat_qty <= 0:
                continue
            mat_origin = str(row.get(mg['origin_col'], '')).strip() if mg.get('origin_col') else ''

            if loc not in snapshots:
                snapshots[loc], product_metas[loc] = _load_stock_with_meta(db, loc)
            groups = snapshot_lookup(snapshots[loc], mat_name).get('groups', [])

            remain = mat_qty
            if not groups:
                meta = _lookup_product_meta(product_metas.get(loc, {}), mat_name)
                payload.append({
                    "transaction_date": date_str,
                    "type": "PROD_OUT",
                    "product_name": mat_name,
                    "qty": -remain,
                    "location": loc,
                    "storage_method": meta.get('storage_method', ''),
                    "category": meta.get('category', ''),
                    "unit": meta.get('unit', '개'),
                    "expiry_date": meta.get('expiry_date', ''),
                    "origin": meta.get('origin', '') or mat_origin,
                    "manufacture_date": meta.get('manufacture_date', ''),
                    "batch_id": batch_id,
                })
                raw_count += 1
            else:
                for g in groups:
                    if remain <= 0:
                        break
                    deduct = min(remain, g['qty'])
                    if deduct <= 0:
                        continue
                    payload.append({
                        "transaction_date": date_str,
                        "type": "PROD_OUT",
                        "product_name": mat_name,
                        "qty": -deduct,
                        "location": loc,
                        "category": g['category'],
                        "expiry_date": g['expiry_date'],
                        "storage_method": g['storage_method'],
                        "unit": g.get('unit', '개'),
                        "origin": g.get('origin', '') or mat_origin,
                        "manufacture_date": g.get('manufacture_date', ''),
                        "batch_id": batch_id,
                    })
                    g['qty'] -= deduct
                    remain -= deduct
                    raw_count += 1

    if payload:
        db.insert_stock_ledger(payload)

    return {
        'success': True,
        'count': len(payload),
        'produced': prod_count,
        'materials_used': raw_count,
        'warnings': warnings,
        'shortage': shortage,
        'is_past_date': is_past_date,
        'unit_check': unit_check,
        'deleted_count': deleted_count,
        'mode': mode,
    }


# ─── 시스템 입력 생산 배치 처리 ───

def process_production_batch(db, date_str, mode, location, items):
    """시스템 입력 다건 생산 처리 (FIFO 재료 차감 포함).

    Args:
        db: SupabaseDB instance
        date_str: 생산일자 (YYYY-MM-DD)
        mode: '신규입력' or '수정입력'
        location: 생산 위치
        items: list of dicts:
            {product_name, qty, expiry_date, category, storage_method,
             unit, manufacture_date,
             materials: [{product_name, qty}]}

    Returns:
        dict: {produced, materials_used, warnings, shortage, deleted_count}
    """
    from services.excel_io import normalize_location as norm_loc

    _validate_date(date_str)
    loc = norm_loc(location) if location else ''

    warnings = []
    shortage = []
    deleted_count = 0

    # 수정입력: 기존 데이터 삭제
    if mode == '수정입력':
        del1 = db.delete_stock_ledger_by(date_str, "PRODUCTION")
        del2 = db.delete_stock_ledger_by(date_str, "PROD_OUT")
        deleted_count = del1 + del2

    # 재고 스냅샷 + 메타데이터 로드 (재료 차감용)
    if loc:
        stock, product_meta = _load_stock_with_meta(db, loc)
    else:
        stock, product_meta = {}, {}

    # 재료 부족 사전 확인
    for item in items:
        for mat in item.get('materials', []):
            mat_name = str(mat.get('product_name', '')).strip()
            mat_qty = safe_int(mat.get('qty', 0))
            if not mat_name or mat_qty <= 0:
                continue
            _snap = snapshot_lookup(stock, mat_name)
            total = _snap.get('total', 0)
            u = _snap.get('unit', '개')
            if mat_qty > total:
                shortage.append(f"[{loc}] {mat_name}: 필요 {mat_qty}{u} / 재고 {total}{u}")

    if shortage:
        warnings.append("재료 재고 부족:\n" + "\n".join(shortage))

    # 페이로드 생성
    payload = []
    prod_count = 0
    raw_count = 0

    for item in items:
        name = str(item.get('product_name', '')).strip()
        prod_qty = safe_int(item.get('qty', 0))
        if not name or prod_qty <= 0:
            continue

        # 이 항목(생산제품)에 대한 고유 batch_id — PRODUCTION+PROD_OUT 연결
        batch_id = f"PROD_{date_str}_{uuid.uuid4().hex[:8]}"

        # PRODUCTION 산출
        payload.append({
            "transaction_date": date_str,
            "type": "PRODUCTION",
            "product_name": name,
            "qty": prod_qty,
            "location": loc,
            "expiry_date": safe_date(item.get('expiry_date', '')),
            "category": str(item.get('category', '')).strip(),
            "storage_method": str(item.get('storage_method', '')).strip(),
            "unit": str(item.get('unit', '개')).strip() or '개',
            "manufacture_date": safe_date(item.get('manufacture_date', '')),
            "batch_id": batch_id,
        })
        prod_count += 1

        # PROD_OUT 재료 차감 (FIFO)
        for mat in item.get('materials', []):
            mat_name = str(mat.get('product_name', '')).strip()
            mat_qty = safe_int(mat.get('qty', 0))
            if not mat_name or mat_qty <= 0:
                continue

            groups = snapshot_lookup(stock, mat_name).get('groups', [])
            remain = mat_qty

            if not groups:
                meta = _lookup_product_meta(product_meta, mat_name)
                payload.append({
                    "transaction_date": date_str,
                    "type": "PROD_OUT",
                    "product_name": mat_name,
                    "qty": -remain,
                    "location": loc,
                    "storage_method": meta.get('storage_method', ''),
                    "category": meta.get('category', ''),
                    "unit": meta.get('unit', '개'),
                    "expiry_date": meta.get('expiry_date', ''),
                    "origin": meta.get('origin', ''),
                    "manufacture_date": meta.get('manufacture_date', ''),
                    "batch_id": batch_id,
                })
                raw_count += 1
            else:
                for g in groups:
                    if remain <= 0:
                        break
                    deduct = min(remain, g['qty'])
                    if deduct <= 0:
                        continue
                    payload.append({
                        "transaction_date": date_str,
                        "type": "PROD_OUT",
                        "product_name": mat_name,
                        "qty": -deduct,
                        "location": loc,
                        "category": g['category'],
                        "expiry_date": g['expiry_date'],
                        "storage_method": g['storage_method'],
                        "unit": g.get('unit', '개'),
                        "manufacture_date": g.get('manufacture_date', ''),
                        "batch_id": batch_id,
                    })
                    g['qty'] -= deduct
                    remain -= deduct
                    raw_count += 1

    if payload:
        try:
            db.insert_stock_ledger(payload)
        except Exception as e:
            if mode == '수정입력' and deleted_count > 0:
                warnings.append(f"주의: 기존 {deleted_count}건이 삭제되었으나 새 데이터 저장에 실패했습니다. 수정입력으로 재시도하세요.")
            raise

    return {
        'produced': prod_count,
        'materials_used': raw_count,
        'warnings': warnings,
        'shortage': shortage,
        'deleted_count': deleted_count,
    }
