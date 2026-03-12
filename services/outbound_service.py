"""
outbound_service.py -- 통합출고 (탭3) 비즈니스 로직.
Tkinter UI 제거, 순수 데이터 반환.
"""
import os
import logging
import pandas as pd
from datetime import datetime

from services.excel_io import (
    safe_int, safe_qty, normalize_location, detect_qty_col,
    build_stock_snapshot, snapshot_lookup, parse_revenue_payload,
)

logger = logging.getLogger(__name__)


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


# ─── FIFO 출고 배치 처리 (코어) ───

def process_outbound_batch(db, df, location, qty_col, date_str,
                           force_shortage=False):
    """단일 창고에 대한 FIFO 출고 처리.
    app.py의 _process_outbound_batch 로직과 동일.

    Args:
        db: SupabaseDB instance
        df: pd.DataFrame -- 출고 데이터 (품목명, qty_col 컬럼 포함)
        location: 출고 창고명
        qty_col: 수량 컬럼명
        date_str: 처리일자 (YYYY-MM-DD)
        force_shortage: True이면 재고 부족 시에도 처리 계속 (기본 False)

    Returns:
        dict: {
            'count': int,              -- 등록 건수
            'shortage': list of str,   -- 재고 부족 메시지
            'location': str,           -- 처리 창고
        }
    """
    # ── 1차 실시간 검증 (Validation Engine) ──
    try:
        from core.validation_engine import _validate_date as v_date, _validate_location as v_loc
        v_date(date_str, '출고일자')
        v_loc(location, '출고 위치')
    except ImportError:
        pass  # core 미설치 시 기존 동작 유지

    stock = _load_stock_snapshot(db, location)
    shortage = []

    for _, row in df.iterrows():
        name = str(row['품목명']).strip()
        req_qty = abs(safe_int(row[qty_col]))
        _snap = snapshot_lookup(stock, name)
        total = _snap.get('total', 0)
        u = _snap.get('unit', '개')
        if req_qty > total:
            shortage.append(f"  {name}: 요청 {req_qty}{u} / 재고 {total}{u}")

    if shortage and not force_shortage:
        return {
            'count': 0,
            'shortage': shortage,
            'location': location,
            'aborted': True,
        }

    payload = []
    for _, row in df.iterrows():
        name = str(row['품목명']).strip()
        remain = abs(safe_int(row[qty_col]))
        snap_data = snapshot_lookup(stock, name)
        groups = snap_data.get('groups', [])
        if not groups:
            payload.append({
                "transaction_date": date_str,
                "type": "SALES_OUT",
                "product_name": name,
                "qty": -remain,
                "location": location,
                "unit": snap_data.get('unit', '개'),
                "category": snap_data.get('category', ''),
                "storage_method": snap_data.get('storage_method', ''),
                "manufacture_date": '',
            })
            continue
        for g in groups:
            if remain <= 0:
                break
            deduct = min(remain, g['qty'])
            if deduct <= 0:
                continue
            payload.append({
                "transaction_date": date_str,
                "type": "SALES_OUT",
                "product_name": name,
                "qty": -deduct,
                "location": location,
                "category": g['category'],
                "expiry_date": g['expiry_date'],
                "storage_method": g['storage_method'],
                "unit": g.get('unit', '개'),
                "manufacture_date": g.get('manufacture_date', ''),
            })
            remain -= deduct

    if payload:
        logger.info(f"[재고차감] {date_str} | {location} | SALES_OUT {len(payload)}건 insert 시도")
        for p in payload:
            logger.info(f"[재고차감] {date_str} | {p['product_name']} | {p['qty']} | {location} | {p['type']}")
        try:
            insert_result = db.insert_stock_ledger(payload)
            if isinstance(insert_result, dict):
                actual_count = insert_result.get('inserted', len(payload))
            else:
                actual_count = len(payload)
            logger.info(f"[재고차감완료] {date_str} | {location} | {actual_count}건 성공")
        except Exception as e:
            logger.error(f"[재고차감실패] {date_str} | {location} | {len(payload)}건 | {str(e)}")
            raise
    else:
        actual_count = 0

    return {
        'count': actual_count,
        'shortage': shortage,
        'location': location,
        'aborted': False,
    }


# ─── 단건 출고 (폼 기반) ───

def process_single_outbound(db, date_str, location, items):
    """폼 기반 단건 출고 처리 (FIFO).

    Args:
        db: SupabaseDB instance
        date_str: 처리일자 (YYYY-MM-DD)
        location: 출고 창고명
        items: list of dict -- [{product_name, qty, unit_price, unit}, ...]

    Returns:
        dict: {success, count, shortage, warnings}
    """
    _validate_date(date_str)
    stock = _load_stock_snapshot(db, location)
    shortage = []
    warnings = []

    # 재고 검증
    for item in items:
        name = str(item['product_name']).strip()
        _snap = snapshot_lookup(stock, name)
        u = _snap.get('unit', item.get('unit', '개'))
        req_qty = abs(safe_qty(item['qty'], unit=u))
        total = _snap.get('total', 0)
        if req_qty > total:
            shortage.append(f"{name}: 요청 {req_qty}{u} / 재고 {total}{u}")

    if shortage:
        return {
            'success': False,
            'count': 0,
            'shortage': shortage,
            'warnings': ['재고 부족으로 출고가 중단되었습니다.'],
        }

    # FIFO 차감
    payload = []
    for item in items:
        name = str(item['product_name']).strip()
        _snap = snapshot_lookup(stock, name)
        u = _snap.get('unit', item.get('unit', '개'))
        remain = abs(safe_qty(item['qty'], unit=u))
        snap_data = snapshot_lookup(stock, name)
        groups = snap_data.get('groups', [])
        if not groups:
            payload.append({
                "transaction_date": date_str,
                "type": "SALES_OUT",
                "product_name": name,
                "qty": -remain,
                "location": location,
                "unit": snap_data.get('unit', item.get('unit', '개')),
                "category": snap_data.get('category', ''),
                "storage_method": snap_data.get('storage_method', ''),
                "manufacture_date": '',
            })
            continue
        for g in groups:
            if remain <= 0:
                break
            deduct = min(remain, g['qty'])
            if deduct <= 0:
                continue
            payload.append({
                "transaction_date": date_str,
                "type": "SALES_OUT",
                "product_name": name,
                "qty": -deduct,
                "location": location,
                "category": g['category'],
                "expiry_date": g['expiry_date'],
                "storage_method": g['storage_method'],
                "unit": g.get('unit', '개'),
                "manufacture_date": g.get('manufacture_date', ''),
            })
            remain -= deduct

    insert_result = {'inserted': 0, 'failed': 0, 'errors': []}
    if payload:
        logger.info(f"[단건재고차감] {date_str} | {location} | SALES_OUT {len(payload)}건 insert 시도")
        for p in payload:
            logger.info(f"[재고차감] {date_str} | {p['product_name']} | {p['qty']} | {location} | {p['type']}")
        try:
            insert_result = db.insert_stock_ledger(payload)
            # insert_stock_ledger가 dict를 반환하지 않는 경우 (호환)
            if not isinstance(insert_result, dict):
                insert_result = {'inserted': len(payload), 'failed': 0, 'errors': []}
            if insert_result.get('failed', 0) > 0:
                for err in insert_result.get('errors', []):
                    warnings.append(f'재고차감 일부 실패: {err}')
                logger.error(f"[단건재고차감실패] {date_str} | {location} | 실패 {insert_result['failed']}건 | {insert_result.get('errors', [])}")
            else:
                logger.info(f"[단건재고차감완료] {date_str} | {location} | {insert_result.get('inserted', len(payload))}건 성공")
        except Exception as e:
            logger.error(f"[단건재고차감실패] {date_str} | {location} | {len(payload)}건 | {str(e)}")
            raise

    return {
        'success': True,
        'count': insert_result.get('inserted', len(payload)),
        'shortage': [],
        'warnings': warnings,
        'insert_detail': insert_result,
    }


# ─── 개별 출고 (run_outbound_ledger) ───

def process_outbound(db, excel_df, date_str, location='넥스원',
                     filename='', mode='신규입력', force_shortage=False):
    """개별 출고 엑셀 처리.
    app.py의 run_outbound_ledger 로직과 동일.

    Args:
        db: SupabaseDB instance
        excel_df: pd.DataFrame -- 이미 pd.read_excel().fillna(0) 된 상태
        date_str: 처리일자 (YYYY-MM-DD)
        location: 기본 출고 창고 (파일명에서 자동감지 안될 때 사용)
        filename: 원본 파일명 (창고 자동감지용)
        mode: '신규입력' 또는 '수정입력'
        force_shortage: True이면 재고 부족 시에도 처리 계속

    Returns:
        dict: {
            'success': bool,
            'total_count': int,
            'results': list of dict,    -- 창고별 처리 결과
            'warnings': list of str,
            'deleted_count': int,
            'mode': str,
        }

    Raises:
        ValueError: 날짜 형식/수량 컬럼 오류
    """
    _validate_date(date_str)

    df = excel_df
    qty_col = detect_qty_col(df)
    if not qty_col:
        raise ValueError("엑셀에 '합산', 'qty', 또는 '수량' 컬럼이 없습니다.")

    df = df[pd.to_numeric(df[qty_col], errors='coerce').fillna(0).astype(int) != 0]
    if df.empty:
        return {
            'success': True,
            'total_count': 0,
            'results': [],
            'warnings': ['출고할 품목이 없습니다 (수량이 모두 0).'],
            'deleted_count': 0,
            'mode': mode,
        }

    fname = filename or ''
    fname_low = fname.lower()
    wh_col = 'warehouse' if 'warehouse' in df.columns else ('출고지' if '출고지' in df.columns else None)

    # 수정입력: 기존 데이터 삭제
    deleted_count = 0
    if mode == '수정입력':
        if wh_col:
            for wh_name in df[wh_col].unique():
                deleted_count += db.delete_stock_ledger_by(
                    date_str, "SALES_OUT", normalize_location(wh_name))
        elif "넥스원" in fname:
            deleted_count = db.delete_stock_ledger_by(date_str, "SALES_OUT", "넥스원")
        elif "해서" in fname:
            deleted_count = db.delete_stock_ledger_by(date_str, "SALES_OUT", "해서")
        elif "cj용인" in fname_low:
            deleted_count = db.delete_stock_ledger_by(date_str, "SALES_OUT", "CJ용인")
        else:
            deleted_count = db.delete_stock_ledger_by(date_str, "SALES_OUT", location)

    results = []
    total_cnt = 0
    warnings = []

    if wh_col:
        # warehouse 컬럼이 있으면 창고별로 분리 처리
        for wh_name in df[wh_col].unique():
            wh_name_str = str(wh_name).strip()
            if not wh_name_str:
                continue
            wh_df = df[df[wh_col] == wh_name]
            result = process_outbound_batch(
                db, wh_df, wh_name_str, qty_col, date_str,
                force_shortage=force_shortage)
            results.append(result)
            total_cnt += result.get('count', 0)
            if result.get('shortage'):
                warnings.extend([f"[{wh_name_str}] {s}" for s in result['shortage']])
    elif "넥스원" in fname:
        result = process_outbound_batch(
            db, df, "넥스원", qty_col, date_str,
            force_shortage=force_shortage)
        results.append(result)
        total_cnt = result.get('count', 0)
        if result.get('shortage'):
            warnings.extend(result['shortage'])
    elif "해서" in fname:
        result = process_outbound_batch(
            db, df, "해서", qty_col, date_str,
            force_shortage=force_shortage)
        results.append(result)
        total_cnt = result.get('count', 0)
        if result.get('shortage'):
            warnings.extend(result['shortage'])
    elif "cj용인" in fname_low:
        result = process_outbound_batch(
            db, df, "CJ용인", qty_col, date_str,
            force_shortage=force_shortage)
        results.append(result)
        total_cnt = result.get('count', 0)
        if result.get('shortage'):
            warnings.extend(result['shortage'])
    else:
        t_loc = location
        result = process_outbound_batch(
            db, df, t_loc, qty_col, date_str,
            force_shortage=force_shortage)
        results.append(result)
        total_cnt = result.get('count', 0)
        if result.get('shortage'):
            warnings.extend(result['shortage'])

    return {
        'success': True,
        'total_count': total_cnt,
        'results': results,
        'warnings': warnings,
        'deleted_count': deleted_count,
        'mode': mode,
    }


# ─── 매출 임포트 (내부 헬퍼) ───

def _process_revenue_import(db, df, upload_date):
    """매출 데이터 임포트.

    Returns:
        tuple: (count, total_revenue)
    """
    payload, total_rev = parse_revenue_payload(df, upload_date)
    if not payload:
        return 0, 0
    db.upsert_revenue(payload)
    return len(payload), total_rev


# ─── 일괄 출고+매출 업로드 (run_batch_upload) ───

def process_batch_outbound(db, file_paths, date_str, mode='신규입력',
                           force_shortage=False):
    """일괄 출고+매출 파일 처리.
    app.py의 run_batch_upload 로직과 동일.

    Args:
        db: SupabaseDB instance
        file_paths: list of str -- 파일 경로 리스트
        date_str: 처리일자 (YYYY-MM-DD)
        mode: '신규입력' 또는 '수정입력'
        force_shortage: True이면 재고 부족 시에도 처리 계속

    Returns:
        dict: {
            'success': bool,
            'results': list of str,          -- 처리 결과 메시지
            'errors': list of str,           -- 에러 메시지
            'total_count': int,              -- 총 출고 건수
            'summary': list of str,          -- 미리보기 요약
            'outbound_files': list of tuple, -- (path, warehouse)
            'integrated_files': list of str,
            'revenue_files': list of str,
            'unknown_files': list of str,
            'deleted_count': int,
            'mode': str,
        }

    Raises:
        ValueError: 날짜 형식 오류, 인식 가능한 파일 없음
    """
    _validate_date(date_str)

    outbound_files = []
    integrated_files = []
    revenue_files = []
    unknown_files = []

    for p in file_paths:
        fname = os.path.basename(p)
        if fname.startswith("통합출고_"):
            parts = fname.replace(".xlsx", "").split("_")
            if len(parts) >= 3:
                outbound_files.append((p, normalize_location(parts[1])))
            else:
                unknown_files.append(fname)
        elif fname.startswith("통합집계표"):
            integrated_files.append(p)
        elif fname.startswith("일일매출"):
            revenue_files.append(p)
        else:
            unknown_files.append(fname)

    if not outbound_files and not integrated_files and not revenue_files:
        raise ValueError(
            "인식 가능한 파일이 없습니다.\n\n"
            "파일명 형식:\n  통합출고_넥스원_*.xlsx\n  통합출고_해서_*.xlsx\n"
            "  통합출고_CJ용인_*.xlsx\n  통합집계표_*.xlsx\n  일일매출_*.xlsx"
        )

    # ── 미리보기 요약 생성 ──
    summary = []
    for p, wh in outbound_files:
        try:
            tdf = pd.read_excel(p).fillna(0)
            qc = detect_qty_col(tdf)
            if qc:
                tdf = tdf[pd.to_numeric(tdf[qc], errors='coerce').fillna(0).astype(int) != 0]
            summary.append(f"  [{wh}] 출고: {len(tdf)}종 -> stock_ledger FIFO")
        except Exception:
            summary.append(f"  [{wh}] 출고: 읽기 오류")

    for p in integrated_files:
        try:
            tdf = pd.read_excel(p).fillna(0)
            if 'warehouse' in tdf.columns:
                for wh_name, cnt in tdf['warehouse'].value_counts().items():
                    summary.append(f"  [{wh_name}] 통합집계: {cnt}종 -> stock_ledger FIFO")
            else:
                summary.append("  통합집계표: warehouse 컬럼 없음")
        except Exception:
            summary.append("  통합집계표: 읽기 오류")

    for p in revenue_files:
        try:
            tdf = pd.read_excel(p).fillna(0)
            cnt = len(tdf[tdf['품목명'].astype(str).str.strip().isin(['', '합계']) == False])
            summary.append(f"  일일매출: {cnt}종 -> daily_revenue")
        except Exception:
            summary.append("  일일매출: 읽기 오류")

    if unknown_files:
        summary.append(f"\n  인식 불가 (건너뜀): {', '.join(unknown_files)}")

    # ── 중복 경고 확인 ──
    duplicate_warning = None
    if outbound_files and integrated_files:
        dup_whs = set(wh for _, wh in outbound_files)
        duplicate_warning = (
            f"통합출고 파일({', '.join(dup_whs)})과 통합집계표가 동시에 선택되었습니다.\n"
            "같은 창고의 출고가 중복 처리될 수 있습니다."
        )

    # ── 기존 데이터 삭제 (신규/수정 모두 — 중복 방지) ──
    deleted_count = 0
    for _, wh in outbound_files:
        deleted_count += db.delete_stock_ledger_by(date_str, "SALES_OUT", wh)
    for p in integrated_files:
        try:
            tdf = pd.read_excel(p).fillna(0)
            if 'warehouse' in tdf.columns:
                for wh_name in tdf['warehouse'].unique():
                    deleted_count += db.delete_stock_ledger_by(
                        date_str, "SALES_OUT", str(wh_name).strip())
        except Exception:
            pass

    # ── 실제 처리 ──
    results = []
    errors = []
    total_count = 0

    # 1) 통합출고 파일 처리
    for p, wh_name in outbound_files:
        try:
            tdf = pd.read_excel(p).fillna(0)
            qc = detect_qty_col(tdf)
            if not qc:
                errors.append(f"[{wh_name}] 수량 컬럼 없음")
                continue
            tdf = tdf[pd.to_numeric(tdf[qc], errors='coerce').fillna(0).astype(int) != 0]
            if tdf.empty:
                results.append(f"[{wh_name}] 출고 0건 (수량 모두 0)")
                continue
            batch_result = process_outbound_batch(
                db, tdf, wh_name, qc, date_str,
                force_shortage=force_shortage)
            cnt = batch_result.get('count', 0)
            results.append(f"[{wh_name}] FIFO 출고: {cnt}건")
            total_count += cnt
        except Exception as e:
            errors.append(f"[{wh_name}] 출고 오류: {e}")

    # 2) 통합집계표 파일 처리
    for p in integrated_files:
        try:
            tdf = pd.read_excel(p).fillna(0)
            qc = detect_qty_col(tdf)
            if not qc:
                errors.append("통합집계표: 수량 컬럼 없음")
                continue
            tdf = tdf[pd.to_numeric(tdf[qc], errors='coerce').fillna(0).astype(int) != 0]
            if tdf.empty:
                results.append("통합집계표: 출고 0건")
                continue
            wh_col = 'warehouse' if 'warehouse' in tdf.columns else None
            if wh_col:
                for wh_name in tdf[wh_col].unique():
                    wh_str = str(wh_name).strip()
                    if not wh_str:
                        continue
                    wh_df = tdf[tdf[wh_col] == wh_name]
                    batch_result = process_outbound_batch(
                        db, wh_df, wh_str, qc, date_str,
                        force_shortage=force_shortage)
                    cnt = batch_result.get('count', 0)
                    results.append(f"[{wh_str}] 통합집계 FIFO: {cnt}건")
                    total_count += cnt
            else:
                errors.append("통합집계표에 warehouse 컬럼 없음")
        except Exception as e:
            errors.append(f"통합집계표 오류: {e}")

    # 3) 매출 파일 처리
    for p in revenue_files:
        try:
            tdf = pd.read_excel(p).fillna(0)
            cnt, total_rev = _process_revenue_import(db, tdf, date_str)
            if cnt > 0:
                results.append(f"매출 업로드: {cnt}건, 총매출 {total_rev:,}원")
            else:
                results.append("매출: 데이터 없음")
        except Exception as e:
            errors.append(f"매출 오류: {e}")

    return {
        'success': True,
        'results': results,
        'errors': errors,
        'total_count': total_count,
        'summary': summary,
        'outbound_files': [(os.path.basename(p), wh) for p, wh in outbound_files],
        'integrated_files': [os.path.basename(p) for p in integrated_files],
        'revenue_files': [os.path.basename(p) for p in revenue_files],
        'unknown_files': unknown_files,
        'duplicate_warning': duplicate_warning,
        'deleted_count': deleted_count,
        'mode': mode,
    }
