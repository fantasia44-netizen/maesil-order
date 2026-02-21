"""
production_service.py -- 생산/입고 (탭2) 비즈니스 로직.
Tkinter UI 제거, 순수 데이터 반환.
"""
import pandas as pd
from datetime import datetime

from services.excel_io import (
    safe_int, safe_date, normalize_location, flexible_column_rename,
    detect_material_groups, parse_inbound_payload, build_stock_snapshot,
    snapshot_lookup,
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
                snapshots[loc] = _load_stock_snapshot(db, loc)
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
            })
            prod_count += 1

        for mg in material_groups:
            mat_name = str(row.get(mg['name_col'], '')).strip()
            mat_qty = safe_int(row.get(mg['qty_col'], 0))
            if not mat_name or mat_qty <= 0:
                continue
            mat_origin = str(row.get(mg['origin_col'], '')).strip() if mg.get('origin_col') else ''

            if loc not in snapshots:
                snapshots[loc] = _load_stock_snapshot(db, loc)
            groups = snapshot_lookup(snapshots[loc], mat_name).get('groups', [])

            remain = mat_qty
            if not groups:
                payload.append({
                    "transaction_date": date_str,
                    "type": "PROD_OUT",
                    "product_name": mat_name,
                    "qty": -remain,
                    "location": loc,
                    "origin": mat_origin,
                    "manufacture_date": '',
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
