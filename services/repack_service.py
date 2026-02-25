"""
repack_service.py — 소분/리패킹(Tab 10) 비즈니스 로직.
Tkinter 의존 제거. db 파라미터(SupabaseDB 인스턴스)를 받고 결과를 dict/list로 반환.
"""
import pandas as pd
from datetime import datetime

try:
    from excel_io import (
        safe_int, safe_qty, safe_date, normalize_location, detect_material_groups,
        build_stock_snapshot, snapshot_lookup
    )
except ImportError:
    from services.excel_io import (
        safe_int, safe_qty, safe_date, normalize_location, detect_material_groups,
        build_stock_snapshot, snapshot_lookup
    )
from models import INV_TYPE_LABELS


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


def generate_repack_doc_no(db, date_str):
    """소분 문서번호 자동 생성.

    형식: RP-{YYYYMMDD}-{seq:03d}
    해당 날짜의 기존 소분 문서번호 수를 세어 다음 순번을 부여합니다.

    Args:
        db: SupabaseDB instance
        date_str: 작업일자 (YYYY-MM-DD)

    Returns:
        str: 생성된 문서번호 (예: "RP-20260221-001")
    """
    date_part = date_str.replace('-', '')
    try:
        res = db.client.table("stock_ledger").select("repack_doc_no") \
            .eq("transaction_date", date_str) \
            .in_("type", ["REPACK_OUT", "REPACK_IN"]) \
            .execute()
        existing = set()
        if res.data:
            for r in res.data:
                dn = r.get('repack_doc_no', '')
                if dn:
                    existing.add(dn)
        seq = len(existing) + 1
    except Exception:
        seq = 1
    return f"RP-{date_part}-{seq:03d}"


def process_repack(db, excel_df, date_str, mode="신규입력"):
    """소분(리패킹) 엑셀 업로드 처리.

    벌크/대포장 투입품을 FIFO로 차감(REPACK_OUT)하고 산출품을 입고(REPACK_IN)합니다.
    생산 형식 엑셀을 자동 감지하여 소분 형식으로 변환하는 로직도 포함합니다.

    Args:
        db: SupabaseDB instance
        excel_df: pandas DataFrame (소분 엑셀 또는 생산 형식 엑셀)
            소분 형식 필수 컬럼: 소분번호, 산출품목명, 산출수량, 산출창고,
                              투입품목명, 투입수량, 투입창고
            생산 형식 필수 컬럼: 품목명, 생산수량, 창고위치, 원재료/부재료, 수량
        date_str: 작업일자 (YYYY-MM-DD)
        mode: "신규입력" or "수정입력"

    Returns:
        dict: {
            "count": int,              # 총 거래 건수
            "doc_nos": list,           # 소분번호 목록
            "repack_in_count": int,    # REPACK_IN 건수
            "repack_out_count": int,   # REPACK_OUT 건수
            "sub_out_count": int,      # 부자재 차감 건수
            "warnings": list,          # 경고 메시지 목록
            "deleted_count": int,      # 수정입력 시 삭제된 기존 건수
            "auto_converted": bool,    # 생산 형식에서 자동 변환 여부
            "converted_count": int     # 자동 변환된 건수
        }

    Raises:
        ValueError: 날짜 형식 오류, 필수 컬럼 누락, 재고 부족
        Exception: DB 오류
    """
    _validate_date(date_str)

    df = excel_df.fillna("")
    cols = df.columns.tolist()
    warnings = []
    auto_converted = False
    converted_count = 0

    # ── 생산 형식 엑셀 자동 감지 & 소분 형식 변환 ──
    production_style = ('품목명' in cols and '생산수량' in cols and '창고위치' in cols
                        and '산출품목명' not in cols)

    if production_style:
        material_groups = detect_material_groups(cols)
        if not material_groups:
            raise ValueError("생산 형식 엑셀이지만 원재료 컬럼을 찾을 수 없습니다.")

        # 단위 컬럼 그룹 감지
        unit_cols = sorted([c for c in cols if c == '단위' or c.startswith('단위.')],
                           key=lambda x: int(x.split('.')[-1]) if '.' in x else 0)

        # 생산 형식 -> 소분 형식 변환
        rows_new = []
        seq = 1
        date_part = date_str.replace('-', '')

        for _, row in df.iterrows():
            out_name = str(row.get('품목명', '')).strip()
            out_unit = str(row.get('단위', '개')).strip() or '개'
            out_qty = safe_qty(row.get('생산수량', 0), unit=out_unit)
            out_loc = normalize_location(str(row.get('창고위치', '')))
            if not out_name or out_qty <= 0:
                continue

            doc_no = f"RP-{date_part}-{seq:03d}"

            for gi, mg in enumerate(material_groups):
                mat_name = str(row.get(mg['name_col'], '')).strip()
                # 단위: 원재료 단위는 unit_cols[1](단위.1) 이후부터 매핑
                _mat_unit_col = unit_cols[gi + 1] if (gi + 1) < len(unit_cols) else None
                _mat_unit_tmp = str(row.get(_mat_unit_col, '개')).strip() if _mat_unit_col else '개'
                if not _mat_unit_tmp or _mat_unit_tmp == 'nan':
                    _mat_unit_tmp = '개'
                mat_qty = safe_qty(row.get(mg['qty_col'], 0), unit=_mat_unit_tmp)
                if not mat_name or mat_qty <= 0:
                    continue

                mat_unit = _mat_unit_tmp

                rows_new.append({
                    '소분번호': doc_no,
                    '산출품목명': out_name,
                    '산출수량': out_qty,
                    '산출단위': str(row.get('단위', '개')).strip() or '개',
                    '산출창고': out_loc,
                    '산출종류': str(row.get('종류', '')).strip(),
                    '산출보관방법': str(row.get('보관방법', '')).strip(),
                    '산출소비기한': row.get('소비기한', ''),
                    '산출이력번호': str(row.get('이력번호', '')).strip(),
                    '산출등급': str(row.get('등급', '')).strip(),
                    '산출제조일': row.get('제조일자', row.get('제조일', '')),
                    '투입품목명': mat_name,
                    '투입수량': mat_qty,
                    '투입단위': mat_unit,
                    '투입창고': out_loc,
                    '투입종류': str(row.get(mg.get('cat_col') or '', '')).strip(),
                    '투입소비기한': row.get(mg.get('exp_col') or '', ''),
                    '투입원산지': str(row.get(mg.get('origin_col') or '', '')).strip(),
                    '투입제조일': '',
                    '비고': '',
                })
            seq += 1

        if not rows_new:
            raise ValueError("변환 가능한 소분 데이터가 없습니다.")

        df = pd.DataFrame(rows_new).fillna("")
        cols = df.columns.tolist()
        auto_converted = True
        converted_count = len(rows_new)

    # ── 필수 컬럼 검증 ──
    required = ['소분번호', '산출품목명', '산출수량', '산출창고',
                '투입품목명', '투입수량', '투입창고']
    missing = [c for c in required if c not in cols]
    if missing:
        raise ValueError(f"필수 컬럼 누락: {', '.join(missing)}")

    if df.empty:
        raise ValueError("엑셀에 데이터가 없습니다.")

    if date_str < datetime.now().strftime('%Y-%m-%d'):
        warnings.append(f"작업일자가 과거입니다: {date_str}")

    # ── 수정입력: 기존 소분 데이터를 먼저 삭제 (재고 복원 후 부족 체크) ──
    deleted_count = 0
    if mode == "수정입력":
        del1 = db.delete_stock_ledger_by(date_str, "REPACK_OUT")
        del2 = db.delete_stock_ledger_by(date_str, "REPACK_IN")
        deleted_count = del1 + del2

    # ── 투입품 재고 부족 체크 ──
    input_demand = {}
    for _, row in df.iterrows():
        inp_name = str(row['투입품목명']).strip()
        inp_unit = str(row.get('투입단위', '개')).strip() or '개'
        inp_qty = safe_qty(row['투입수량'], unit=inp_unit)
        inp_loc = normalize_location(str(row['투입창고']).strip())
        if inp_name and inp_qty > 0:
            key = (inp_name, inp_loc)
            input_demand[key] = input_demand.get(key, 0) + inp_qty

    # 부자재 수요도 포함
    if '부자재명' in cols and '부자재수량' in cols:
        for _, row in df.iterrows():
            sub_name = str(row.get('부자재명', '')).strip()
            sub_unit = str(row.get('부자재단위', '개')).strip() or '개'
            sub_qty = safe_qty(row.get('부자재수량', 0), unit=sub_unit)
            sub_loc = normalize_location(
                str(row.get('부자재창고', row.get('투입창고', ''))).strip()
            )
            if sub_name and sub_qty > 0:
                key = (sub_name, sub_loc)
                input_demand[key] = input_demand.get(key, 0) + sub_qty

    shortage = []
    snapshots = {}
    for (item_name, loc), need_qty in input_demand.items():
        if loc not in snapshots:
            snapshots[loc] = _load_stock_snapshot(db, loc)
        _snap = snapshot_lookup(snapshots[loc], item_name)
        total = _snap.get('total', 0)
        u = _snap.get('unit', '개')
        if need_qty > total:
            shortage.append(f"[{loc}] {item_name}: 필요 {need_qty}{u} / 재고 {total}{u}")

    if shortage:
        raise ValueError("투입품 재고 부족:\n" + "\n".join(shortage))

    # ── payload 생성 ──
    payload = []
    doc_nos = set()
    repack_in_count = 0
    repack_out_count = 0
    sub_out_count = 0

    for _, row in df.iterrows():
        doc_no = str(row['소분번호']).strip()
        if not doc_no:
            doc_no = generate_repack_doc_no(db, date_str)
        doc_nos.add(doc_no)
        memo = str(row.get('비고', row.get('memo', ''))).strip()

        # ─ 투입품 차감 (REPACK_OUT, FIFO) ─
        inp_name = str(row['투입품목명']).strip()
        inp_unit = str(row.get('투입단위', '개')).strip() or '개'
        inp_qty = safe_qty(row['투입수량'], unit=inp_unit)
        inp_loc = normalize_location(str(row['투입창고']).strip())

        if inp_name and inp_qty > 0:
            if inp_loc not in snapshots:
                snapshots[inp_loc] = _load_stock_snapshot(db, inp_loc)
            groups = snapshot_lookup(snapshots[inp_loc], inp_name).get('groups', [])
            remain = inp_qty

            if not groups:
                payload.append({
                    "transaction_date": date_str, "type": "REPACK_OUT",
                    "product_name": inp_name, "qty": -remain, "location": inp_loc,
                    "repack_doc_no": doc_no, "memo": memo,
                    "source_lot": str(row.get('투입이력번호', '')).strip() or None,
                    "category": str(row.get('투입종류', '')).strip() or None,
                    "unit": str(row.get('투입단위', '개')).strip() or '개',
                    "expiry_date": safe_date(row.get('투입소비기한', '')),
                    "manufacture_date": safe_date(row.get('투입제조일', '')),
                })
                repack_out_count += 1
            else:
                for g in groups:
                    if remain <= 0:
                        break
                    deduct = min(remain, g['qty'])
                    if deduct <= 0:
                        continue
                    payload.append({
                        "transaction_date": date_str, "type": "REPACK_OUT",
                        "product_name": inp_name, "qty": -deduct, "location": inp_loc,
                        "category": g['category'], "expiry_date": g['expiry_date'],
                        "storage_method": g['storage_method'],
                        "unit": g.get('unit', '개'),
                        "origin": g.get('origin', ''),
                        "manufacture_date": g.get('manufacture_date', ''),
                        "repack_doc_no": doc_no, "memo": memo,
                        "source_lot": str(row.get('투입이력번호', '')).strip() or None,
                    })
                    g['qty'] -= deduct
                    remain -= deduct
                    repack_out_count += 1

        # ─ 산출품 입고 (REPACK_IN) ─
        out_name = str(row['산출품목명']).strip()
        out_unit = str(row.get('산출단위', '개')).strip() or '개'
        out_qty = safe_qty(row['산출수량'], unit=out_unit)
        out_loc = normalize_location(str(row['산출창고']).strip())

        if out_name and out_qty > 0:
            payload.append({
                "transaction_date": date_str, "type": "REPACK_IN",
                "product_name": out_name, "qty": out_qty, "location": out_loc,
                "category": str(row.get('산출종류', '')).strip() or None,
                "storage_method": str(row.get('산출보관방법', '')).strip() or None,
                "unit": str(row.get('산출단위', '개')).strip() or '개',
                "lot_number": str(row.get('산출이력번호', '')).strip() or None,
                "grade": str(row.get('산출등급', '')).strip() or None,
                "expiry_date": safe_date(row.get('산출소비기한', '')),
                "manufacture_date": safe_date(row.get('산출제조일', '')),
                "repack_doc_no": doc_no, "memo": memo,
                "result_lot": str(row.get('산출이력번호', '')).strip() or None,
            })
            repack_in_count += 1

        # ─ 부자재 차감 (REPACK_OUT, FIFO) ─
        if '부자재명' in cols:
            sub_name = str(row.get('부자재명', '')).strip()
            sub_unit = str(row.get('부자재단위', '개')).strip() or '개'
            sub_qty = safe_qty(row.get('부자재수량', 0), unit=sub_unit)
            sub_loc = normalize_location(
                str(row.get('부자재창고', inp_loc)).strip()
            )
            if sub_name and sub_qty > 0:
                if sub_loc not in snapshots:
                    snapshots[sub_loc] = _load_stock_snapshot(db, sub_loc)
                sub_groups = snapshot_lookup(snapshots[sub_loc], sub_name).get('groups', [])
                sub_remain = sub_qty

                if not sub_groups:
                    payload.append({
                        "transaction_date": date_str, "type": "REPACK_OUT",
                        "product_name": sub_name, "qty": -sub_remain,
                        "location": sub_loc,
                        "repack_doc_no": doc_no,
                        "memo": f"부자재({memo})" if memo else "부자재",
                        "unit": str(row.get('부자재단위', '개')).strip() or '개',
                    })
                    sub_out_count += 1
                else:
                    for g in sub_groups:
                        if sub_remain <= 0:
                            break
                        deduct = min(sub_remain, g['qty'])
                        if deduct <= 0:
                            continue
                        payload.append({
                            "transaction_date": date_str, "type": "REPACK_OUT",
                            "product_name": sub_name, "qty": -deduct,
                            "location": sub_loc,
                            "category": g['category'],
                            "expiry_date": g['expiry_date'],
                            "storage_method": g['storage_method'],
                            "unit": g.get('unit', '개'),
                            "repack_doc_no": doc_no,
                            "memo": f"부자재({memo})" if memo else "부자재",
                        })
                        g['qty'] -= deduct
                        sub_remain -= deduct
                        sub_out_count += 1

    if not payload:
        raise ValueError("생성할 소분 거래가 없습니다.")

    # ── DB 저장 ──
    db.insert_stock_ledger(payload)

    return {
        "count": len(payload),
        "doc_nos": sorted(list(doc_nos)),
        "repack_in_count": repack_in_count,
        "repack_out_count": repack_out_count,
        "sub_out_count": sub_out_count,
        "warnings": warnings,
        "deleted_count": deleted_count,
        "auto_converted": auto_converted,
        "converted_count": converted_count,
    }


def process_repack_batch(db, date_str, mode, location, items):
    """시스템 입력 다건 소분 처리.

    Args:
        db: SupabaseDB instance
        date_str: 작업일자 (YYYY-MM-DD)
        mode: "신규입력" or "수정입력"
        location: 작업 위치/창고
        items: [{
            product_name: str (산출품),
            qty: int (산출수량),
            category, unit, storage_method, expiry_date, manufacture_date, lot_number,
            materials: [{product_name, qty}]  (투입품)
        }]

    Returns:
        dict: {repack_in_count, repack_out_count, warnings, deleted_count, doc_nos}
    """
    _validate_date(date_str)

    warnings = []
    if date_str < datetime.now().strftime('%Y-%m-%d'):
        warnings.append(f"작업일자가 과거입니다: {date_str}")

    loc = normalize_location(location)

    # ── 사전 유효성 검증 (DB 변경 전) ──
    # 재고 스냅샷 먼저 로드 (단위 조회용)
    snapshot = _load_stock_snapshot(db, loc) if loc else {}
    input_demand = {}
    for item in items:
        for mat in item.get('materials', []):
            mat_name = str(mat.get('product_name', '')).strip()
            _ms = snapshot_lookup(snapshot, mat_name)
            mat_unit = _ms.get('unit', '개')
            mat_qty = safe_qty(mat.get('qty', 0), unit=mat_unit)
            if mat_name and mat_qty > 0:
                key = (mat_name, loc)
                input_demand[key] = input_demand.get(key, 0) + mat_qty
    shortage = []
    for (name, _loc), need in input_demand.items():
        snap = snapshot_lookup(snapshot, name)
        total = snap.get('total', 0)
        unit = snap.get('unit', '개')
        if need > total:
            shortage.append(f"[{_loc}] {name}: 필요 {need}{unit} / 재고 {total}{unit}")

    if shortage:
        raise ValueError("투입품 재고 부족:\n" + "\n".join(shortage))

    # ── 수정입력: 기존 삭제 (검증 통과 후에만 실행) ──
    deleted_count = 0
    if mode == "수정입력":
        del1 = db.delete_stock_ledger_by(date_str, "REPACK_OUT")
        del2 = db.delete_stock_ledger_by(date_str, "REPACK_IN")
        deleted_count = del1 + del2
        # 수정입력 후 스냅샷 재로드
        snapshot = _load_stock_snapshot(db, loc) if loc else {}

    # ── 문서번호 생성 ──
    doc_no = generate_repack_doc_no(db, date_str)

    # ── payload 생성 ──
    payload = []
    repack_in_count = 0
    repack_out_count = 0

    for item in items:
        out_name = str(item.get('product_name', '')).strip()
        out_unit = str(item.get('unit', '개')).strip() or '개'
        out_qty = safe_qty(item.get('qty', 0), unit=out_unit)

        if not out_name or out_qty <= 0:
            continue

        # 산출품 (REPACK_IN)
        payload.append({
            "transaction_date": date_str,
            "type": "REPACK_IN",
            "product_name": out_name,
            "qty": out_qty,
            "location": loc,
            "category": str(item.get('category', '')).strip() or None,
            "storage_method": str(item.get('storage_method', '')).strip() or None,
            "unit": out_unit,
            "expiry_date": safe_date(item.get('expiry_date', '')),
            "manufacture_date": safe_date(item.get('manufacture_date', '')),
            "food_type": str(item.get('food_type', '')).strip(),
            "lot_number": str(item.get('lot_number', '')).strip() or None,
            "repack_doc_no": doc_no,
        })
        repack_in_count += 1

        # 투입품 (REPACK_OUT, FIFO)
        for mat in item.get('materials', []):
            mat_name = str(mat.get('product_name', '')).strip()
            if not mat_name:
                continue
            _ms = snapshot_lookup(snapshot, mat_name)
            mat_unit = _ms.get('unit', '개')
            mat_qty = safe_qty(mat.get('qty', 0), unit=mat_unit)

            if mat_qty <= 0:
                continue

            groups = _ms.get('groups', [])
            remain = mat_qty

            if not groups:
                payload.append({
                    "transaction_date": date_str,
                    "type": "REPACK_OUT",
                    "product_name": mat_name,
                    "qty": -remain,
                    "location": loc,
                    "repack_doc_no": doc_no,
                    "unit": '개',
                })
                repack_out_count += 1
            else:
                for g in groups:
                    if remain <= 0:
                        break
                    deduct = min(remain, g['qty'])
                    if deduct <= 0:
                        continue
                    payload.append({
                        "transaction_date": date_str,
                        "type": "REPACK_OUT",
                        "product_name": mat_name,
                        "qty": -deduct,
                        "location": loc,
                        "category": g.get('category', ''),
                        "expiry_date": g.get('expiry_date', ''),
                        "storage_method": g.get('storage_method', ''),
                        "unit": g.get('unit', '개'),
                        "repack_doc_no": doc_no,
                    })
                    g['qty'] -= deduct
                    remain -= deduct
                    repack_out_count += 1

    if not payload:
        raise ValueError("생성할 소분 거래가 없습니다.")

    # ── DB 저장 ──
    try:
        db.insert_stock_ledger(payload)
    except Exception as e:
        if mode == "수정입력" and deleted_count > 0:
            warnings.append(f"주의: 기존 {deleted_count}건이 삭제되었으나 새 데이터 저장에 실패했습니다. 수정입력으로 재시도하세요.")
        raise

    return {
        "repack_in_count": repack_in_count,
        "repack_out_count": repack_out_count,
        "warnings": warnings,
        "deleted_count": deleted_count,
        "doc_nos": [doc_no],
    }


def get_repack_history(db, date_from=None, date_to=None):
    """소분 이력 조회.

    Args:
        db: SupabaseDB instance
        date_from: 시작일 (YYYY-MM-DD) or None
        date_to: 종료일 (YYYY-MM-DD), 필수

    Returns:
        dict: {
            "items": list of dict,     # 소분 이력 항목 목록
            "doc_no_count": int,       # 소분번호 수
            "total_in_qty": int,       # 총 산출 수량
            "total_out_qty": int,      # 총 투입 수량
            "total_count": int         # 총 거래 건수
        }

    Raises:
        ValueError: 종료일 미입력
    """
    if not date_to:
        raise ValueError("종료일을 입력하세요.")

    all_data = db.query_stock_ledger(
        date_to,
        date_from=date_from or None,
        type_list=["REPACK_OUT", "REPACK_IN"]
    )

    # 날짜 오름차순 정렬
    all_data.sort(key=lambda x: x.get('transaction_date', ''))

    if not all_data:
        return {
            "items": [],
            "doc_no_count": 0,
            "total_in_qty": 0,
            "total_out_qty": 0,
            "total_count": 0,
        }

    items = []
    doc_nos = set()
    total_in_qty = 0
    total_out_qty = 0

    for r in all_data:
        rtype = r.get('type', '')
        type_label = INV_TYPE_LABELS.get(rtype, rtype)
        qty = r.get('qty', 0)
        doc_no = r.get('repack_doc_no', '') or ''
        if doc_no:
            doc_nos.add(doc_no)

        # 태그 분류 (UI 렌더링에 활용 가능)
        if rtype == 'REPACK_IN':
            total_in_qty += qty
            tag = "repack_in"
        elif rtype == 'REPACK_OUT' and '부자재' in str(r.get('memo', '')):
            tag = "repack_sub"
            total_out_qty += abs(qty)
        else:
            tag = "repack_out"
            total_out_qty += abs(qty)

        items.append({
            "transaction_date": r.get('transaction_date', ''),
            "repack_doc_no": doc_no,
            "type_label": type_label,
            "type": rtype,
            "product_name": r.get('product_name', ''),
            "qty_display": abs(qty) if rtype == 'REPACK_OUT' else qty,
            "qty_raw": qty,
            "unit": r.get('unit', '개') or '개',
            "location": r.get('location', ''),
            "category": r.get('category', '') or '',
            "lot_number": (r.get('lot_number', '') or
                           r.get('source_lot', '') or
                           r.get('result_lot', '') or ''),
            "expiry_date": r.get('expiry_date', '') or '',
            "manufacture_date": r.get('manufacture_date', '') or '',
            "memo": r.get('memo', '') or '',
            "tag": tag,
        })

    return {
        "items": items,
        "doc_no_count": len(doc_nos),
        "total_in_qty": total_in_qty,
        "total_out_qty": total_out_qty,
        "total_count": len(all_data),
    }
