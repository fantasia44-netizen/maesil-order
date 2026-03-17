"""
master.py — 마스터 데이터 관리 Blueprint.
품목/단가/BOM 마스터 엑셀 동기화, 품목명 공백 정리, 이력 검색.
"""
import os

import pandas as pd
from flask import (
    Blueprint, render_template, request, current_app,
    flash, redirect, url_for, jsonify,
)
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename

from auth import role_required, _log_action
from services.storage_helper import backup_to_storage
from db_utils import get_db

master_bp = Blueprint('master', __name__, url_prefix='/master')

ALLOWED_EXT = {'xlsx', 'xls'}

MASTER_TABLES = {
    'product': 'master_products',
    'price': 'master_prices',
    'bom': 'master_bom',
    'option': 'option_master',
}


def _allowed(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXT


@master_bp.route('/')
@role_required('admin')
def index():
    """마스터 데이터 현황"""
    db = get_db()
    counts = {}
    for key, table_name in MASTER_TABLES.items():
        try:
            if key == 'option':
                counts[key] = db.count_option_master()
            else:
                counts[key] = db.count_master_table(table_name)
        except Exception:
            counts[key] = -1

    return render_template('master/index.html', counts=counts)


@master_bp.route('/sync-product', methods=['POST'])
@role_required('admin')
def sync_product():
    """품목 마스터 동기화"""
    return _sync_master('product', 'master_products', '품목 마스터')


@master_bp.route('/sync-price', methods=['POST'])
@role_required('admin')
def sync_price():
    """가격표 동기화 (옵션 엑셀의 '가격표' 시트에서 읽기)"""
    import unicodedata
    def _n(t): return unicodedata.normalize('NFC', str(t).strip())

    file = request.files.get('file')
    if not file or not _allowed(file.filename):
        flash('엑셀 파일(.xlsx/.xls)을 선택하세요.', 'danger')
        return redirect(url_for('master.index'))

    upload_dir = current_app.config['UPLOAD_FOLDER']
    os.makedirs(upload_dir, exist_ok=True)
    filename = secure_filename(file.filename)
    filepath = os.path.join(upload_dir, filename)
    file.save(filepath)
    backup_to_storage(get_db(), filepath, 'upload', 'master')

    try:
        xls = pd.ExcelFile(filepath)
        sheet_map = {_n(sn): sn for sn in xls.sheet_names}
        price_sheet = sheet_map.get(_n('가격표'))

        if not price_sheet:
            flash(f'가격표 시트를 찾을 수 없습니다. 시트 목록: {xls.sheet_names}', 'danger')
            return redirect(url_for('master.index'))

        import numpy as np
        df = pd.read_excel(xls, sheet_name=price_sheet)
        df.columns = [_n(c) for c in df.columns]

        # 빈 행 제거 (품목명이 없는 행)
        name_col = next((c for c in df.columns if '품목' in c or '상품' in c), df.columns[0])
        df = df[df[name_col].notna() & (df[name_col].astype(str).str.strip() != '')]

        if df.empty:
            flash('가격표 시트에 데이터가 없습니다.', 'warning')
            return redirect(url_for('master.index'))

        # NaN/inf/numpy 타입 → JSON 안전 값으로 완전 치환
        def _clean(v):
            if v is None:
                return 0
            try:
                if pd.isna(v):
                    return 0
            except (TypeError, ValueError):
                pass
            if isinstance(v, (np.integer,)):
                return int(v)
            if isinstance(v, (np.floating,)):
                if np.isnan(v) or np.isinf(v):
                    return 0
                return float(v)
            if isinstance(v, float):
                if v != v or v == float('inf') or v == float('-inf'):
                    return 0
            return v

        payload = []
        for row in df.to_dict('records'):
            payload.append({k: _clean(v) for k, v in row.items()})

        get_db().sync_master_table('master_prices', payload)
        _log_action('sync_master', target='master_prices',
                     detail=f'{len(payload)}건 동기화')
        flash(f'가격표 동기화 완료: {len(payload)}건', 'success')
    except Exception as e:
        flash(f'가격표 동기화 중 오류: {e}', 'danger')
    finally:
        if os.path.exists(filepath):
            os.remove(filepath)

    return redirect(url_for('master.index'))


@master_bp.route('/sync-bom', methods=['POST'])
@role_required('admin')
def sync_bom():
    """BOM 마스터 동기화"""
    return _sync_master('bom', 'master_bom', 'BOM 마스터')


@master_bp.route('/sync-option', methods=['POST'])
@role_required('admin')
def sync_option():
    """옵션마스터 동기화 (옵션리스트 엑셀 → option_master 테이블)"""
    file = request.files.get('file')
    if not file or not _allowed(file.filename):
        flash('엑셀 파일(.xlsx/.xls)을 선택하세요.', 'danger')
        return redirect(url_for('master.index'))

    upload_dir = current_app.config['UPLOAD_FOLDER']
    os.makedirs(upload_dir, exist_ok=True)
    filename = secure_filename(file.filename)
    filepath = os.path.join(upload_dir, filename)
    file.save(filepath)
    backup_to_storage(get_db(), filepath, 'upload', 'master')

    try:
        df = pd.read_excel(filepath, header=None, dtype=str).fillna('')
        if len(df.columns) < 6:
            flash('옵션리스트는 최소 F열(바코드)까지 필요합니다.', 'danger')
            return redirect(url_for('master.index'))

        payload = []
        # 헤더행으로 의심되는 값 (엑셀 첫 행이 컬럼명일 때 걸러냄)
        _header_keywords = {'original_name', 'product_name', 'standard_name',
                            '원문명', '품목명', 'line_code', 'sort_order', 'barcode',
                            '라인코드', '출력순서', '바코드'}
        for _, row in df.iterrows():
            orig = str(row.iloc[0]).strip()
            if not orig or orig == 'nan':
                continue
            # 헤더행 스킵
            if orig.replace(' ', '').lower() in _header_keywords:
                continue
            try:
                sv = float(str(row.iloc[4]).strip() or '999')
                sort_order = 999 if (sv != sv) else int(sv)  # NaN != NaN
            except (ValueError, TypeError):
                sort_order = 999
            bc = str(row.iloc[5]).strip() if len(row) > 5 else ''
            if bc == 'nan':
                bc = ''
            pn = str(row.iloc[1]).strip()
            if pn == 'nan':
                pn = ''
            lc = str(row.iloc[2]).strip()
            if lc == 'nan':
                lc = '0'
            payload.append({
                'original_name': orig,
                'product_name': pn,
                'line_code': lc,
                'sort_order': sort_order,
                'barcode': bc,
            })

        get_db().sync_option_master(payload)
        _log_action('sync_option_master', detail=f'{len(payload)}건 동기화')
        flash(f'옵션마스터 동기화 완료: {len(payload)}건', 'success')
    except Exception as e:
        flash(f'옵션마스터 동기화 중 오류: {e}', 'danger')
    finally:
        if os.path.exists(filepath):
            os.remove(filepath)

    return redirect(url_for('master.index'))


def _sync_master(key, table_name, label):
    """공통 마스터 동기화 로직"""
    file = request.files.get('file')
    if not file or not _allowed(file.filename):
        flash('엑셀 파일(.xlsx/.xls)을 선택하세요.', 'danger')
        return redirect(url_for('master.index'))

    upload_dir = current_app.config['UPLOAD_FOLDER']
    os.makedirs(upload_dir, exist_ok=True)
    filename = secure_filename(file.filename)
    filepath = os.path.join(upload_dir, filename)
    file.save(filepath)
    backup_to_storage(get_db(), filepath, 'upload', 'master')

    try:
        df = pd.read_excel(filepath)
        if df.empty:
            flash(f'{label}: 엑셀에 데이터가 없습니다.', 'warning')
            return redirect(url_for('master.index'))

        # NaN → None 변환 후 dict list 생성
        df = df.where(pd.notna(df), None)
        # 컬럼명 공백 제거
        df.columns = [str(c).strip() for c in df.columns]
        payload = df.to_dict('records')

        get_db().sync_master_table(table_name, payload)
        _log_action('sync_master', target=table_name,
                     detail=f'{len(payload)}건 동기화')
        flash(f'{label} 동기화 완료: {len(payload)}건', 'success')
    except Exception as e:
        flash(f'{label} 동기화 중 오류: {e}', 'danger')
    finally:
        if os.path.exists(filepath):
            os.remove(filepath)

    return redirect(url_for('master.index'))


@master_bp.route('/fix-spaces', methods=['POST'])
@role_required('admin')
def fix_spaces():
    """품목명 공백 정리 (stock_ledger + daily_revenue)"""
    try:
        fixed_count, dupe_groups = get_db().fix_product_name_spaces()
        _log_action('fix_product_spaces', detail=f'{fixed_count}건 정리')

        if dupe_groups:
            for norm, variants in dupe_groups.items():
                flash(f'통합: {", ".join(variants)} → {norm}', 'info')

        flash(f'품목명 공백 정리 완료: {fixed_count}건 수정', 'success')
    except Exception as e:
        flash(f'공백 정리 중 오류: {e}', 'danger')

    return redirect(url_for('master.index'))


@master_bp.route('/stale-options')
@role_required('admin')
def stale_options():
    """미사용 옵션 조회 (30일 이상 매칭 안 된 옵션)"""
    days = int(request.args.get('days', 30))
    try:
        stale = get_db().query_stale_options(days)
    except Exception as e:
        flash(f'미사용 옵션 조회 오류: {e}', 'danger')
        stale = []

    counts = {}
    for key, tbl in MASTER_TABLES.items():
        try:
            if key == 'option':
                counts[key] = get_db().count_option_master()
            else:
                counts[key] = get_db().count_master_table(tbl)
        except Exception:
            counts[key] = -1

    return render_template('master/index.html',
                           counts=counts,
                           stale_options=stale,
                           stale_days=days)


@master_bp.route('/cleanup-options', methods=['POST'])
@role_required('admin')
def cleanup_options():
    """미사용 옵션 일괄 삭제"""
    days = int(request.form.get('days', 30))
    try:
        deleted = get_db().delete_stale_options(days)
        _log_action('cleanup_stale_options', detail=f'{days}일 미사용 {deleted}건 삭제')
        flash(f'{days}일 이상 미사용 옵션 {deleted}건 삭제 완료', 'success')
    except Exception as e:
        flash(f'미사용 옵션 삭제 오류: {e}', 'danger')
    return redirect(url_for('master.index'))


@master_bp.route('/search', methods=['GET', 'POST'])
@role_required('admin')
def search():
    """마스터 데이터 검색"""
    table_key = request.args.get('table', 'product')
    search_term = request.args.get('q', '').strip()

    if table_key not in MASTER_TABLES:
        flash('잘못된 마스터 테이블입니다.', 'danger')
        return redirect(url_for('master.index'))

    table_name = MASTER_TABLES[table_key]
    data = []

    try:
        if table_key == 'option':
            raw = get_db().query_option_master()
        else:
            raw = get_db().query_master_table(table_name)

        if search_term:
            term_lower = search_term.replace(' ', '').lower()
            data = [r for r in raw
                    if any(term_lower in str(v).replace(' ', '').lower()
                           for v in r.values())]
        else:
            data = raw[:200]  # 검색어 없으면 최대 200건
    except Exception as e:
        flash(f'마스터 검색 중 오류: {e}', 'danger')

    # counts 데이터도 함께 전달 (index.html의 마스터 카드 영역에 필요)
    counts = {}
    for key, tbl in MASTER_TABLES.items():
        try:
            if key == 'option':
                counts[key] = get_db().count_option_master()
            else:
                counts[key] = get_db().count_master_table(tbl)
        except Exception:
            counts[key] = -1

    # 검색 결과 컬럼 추출
    search_columns = list(data[0].keys()) if data else []

    return render_template('master/index.html',
                           counts=counts,
                           search_results=data,
                           search_columns=search_columns,
                           search_query=search_term,
                           master_type=table_key,
                           table_key=table_key,
                           tables=MASTER_TABLES)


# ================================================================
# 옵션마스터 CRUD API (AJAX)
# ================================================================

@master_bp.route('/api/options')
@role_required('admin')
def api_options():
    """옵션마스터 목록 API (검색 + 페이지네이션)"""
    keyword = request.args.get('q', '').strip()
    page = max(int(request.args.get('page', 1)), 1)
    per_page = min(int(request.args.get('per_page', 50)), 200)

    db = get_db()
    if keyword:
        all_data = db.search_option_master(keyword)
    else:
        all_data = db.query_option_master()

    total = len(all_data)
    start = (page - 1) * per_page
    page_data = all_data[start:start + per_page]

    return jsonify({
        'data': page_data,
        'total': total,
        'page': page,
        'per_page': per_page,
        'total_pages': max((total + per_page - 1) // per_page, 1),
    })


@master_bp.route('/api/options', methods=['POST'])
@role_required('admin')
def api_option_create():
    """옵션마스터 등록 API"""
    data = request.get_json()
    if not data:
        return jsonify({'error': '데이터가 없습니다.'}), 400

    original_name = (data.get('original_name') or '').strip()
    product_name = (data.get('product_name') or '').strip()
    if not original_name or not product_name:
        return jsonify({'error': '원문명과 품목명은 필수입니다.'}), 400

    try:
        sort_val = int(data.get('sort_order', 999))
    except (ValueError, TypeError):
        sort_val = 999

    payload = {
        'original_name': original_name,
        'product_name': product_name,
        'line_code': (data.get('line_code') or '0').strip(),
        'sort_order': sort_val,
        'barcode': (data.get('barcode') or '').strip(),
    }

    try:
        get_db().insert_option_master(payload)
        _log_action('insert_option_master', target=original_name,
                     detail=f'{original_name} → {product_name}')
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@master_bp.route('/api/options/<int:option_id>', methods=['PUT'])
@role_required('admin')
def api_option_update(option_id):
    """옵션마스터 수정 API"""
    data = request.get_json()
    if not data:
        return jsonify({'error': '데이터가 없습니다.'}), 400

    allowed_fields = {'original_name', 'product_name', 'line_code', 'sort_order', 'barcode'}
    update_data = {k: v for k, v in data.items() if k in allowed_fields}

    if 'sort_order' in update_data:
        try:
            update_data['sort_order'] = int(update_data['sort_order'])
        except (ValueError, TypeError):
            update_data['sort_order'] = 999

    if not update_data:
        return jsonify({'error': '수정할 필드가 없습니다.'}), 400

    try:
        get_db().update_option_master(option_id, update_data)
        _log_action('update_option_master', target=str(option_id),
                     detail=str(update_data))
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@master_bp.route('/api/options/<int:option_id>', methods=['DELETE'])
@role_required('admin')
def api_option_delete(option_id):
    """옵션마스터 삭제 API"""
    try:
        get_db().delete_option_master(option_id)
        _log_action('delete_option_master', target=str(option_id))
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ================================================================
# 품목보기 (product_costs) API
# ================================================================

@master_bp.route('/api/products')
@role_required('admin', 'manager')
def api_products():
    """품목(product_costs) 목록 API — 검색 + 미기입 필터 + 페이지네이션"""
    keyword = request.args.get('q', '').strip()
    filter_mode = request.args.get('filter', '')  # 'missing_food_type', 'missing_category', ''
    page = max(int(request.args.get('page', 1)), 1)
    per_page = min(int(request.args.get('per_page', 50)), 200)

    db = get_db()
    cost_map = db.query_product_costs()  # {product_name: {...}}

    # dict → list
    all_data = []
    for pname, info in cost_map.items():
        row = dict(info)
        row['product_name'] = pname
        all_data.append(row)

    # 정렬: 품목명 기준
    all_data.sort(key=lambda r: r.get('product_name', ''))

    # 키워드 필터
    if keyword:
        kw = keyword.replace(' ', '').lower()
        all_data = [r for r in all_data
                    if kw in r.get('product_name', '').replace(' ', '').lower()]

    # 미기입 필터
    if filter_mode == 'missing_food_type':
        all_data = [r for r in all_data if not r.get('food_type')]
    elif filter_mode == 'missing_unit':
        all_data = [r for r in all_data if not r.get('unit')]
    elif filter_mode == 'missing_weight':
        all_data = [r for r in all_data if not r.get('weight') or float(r.get('weight', 0)) == 0]

    total = len(all_data)
    start = (page - 1) * per_page
    page_data = all_data[start:start + per_page]

    return jsonify({
        'data': page_data,
        'total': total,
        'page': page,
        'per_page': per_page,
        'total_pages': max((total + per_page - 1) // per_page, 1),
    })


@master_bp.route('/api/products/<path:product_name>', methods=['PUT'])
@role_required('admin', 'manager')
def api_product_update(product_name):
    """품목(product_costs) 개별 필드 수정 API"""
    data = request.get_json()
    if not data:
        return jsonify({'error': '데이터가 없습니다.'}), 400

    db = get_db()
    cost_map = db.query_product_costs()
    existing = cost_map.get(product_name) or cost_map.get(product_name.replace(' ', ''))
    if not existing:
        return jsonify({'error': f'품목 "{product_name}"을 찾을 수 없습니다.'}), 404

    # 수정 가능 필드
    allowed = {'cost_price', 'unit', 'memo', 'weight', 'weight_unit',
               'cost_type', 'material_type', 'food_type',
               'purchase_unit', 'standard_unit', 'conversion_ratio'}
    updates = {k: v for k, v in data.items() if k in allowed}

    if not updates:
        return jsonify({'error': '수정할 필드가 없습니다.'}), 400

    try:
        # 기존 값과 병합하여 upsert
        merged = {
            'product_name': product_name,
            'cost_price': updates.get('cost_price', existing.get('cost_price', 0)),
            'unit': updates.get('unit', existing.get('unit', '')),
            'memo': updates.get('memo', existing.get('memo', '')),
            'weight': updates.get('weight', existing.get('weight', 0)),
            'weight_unit': updates.get('weight_unit', existing.get('weight_unit', 'g')),
            'cost_type': updates.get('cost_type', existing.get('cost_type', '매입')),
            'material_type': updates.get('material_type', existing.get('material_type', '원료')),
            'food_type': updates.get('food_type', existing.get('food_type', '')),
            'purchase_unit': updates.get('purchase_unit', existing.get('purchase_unit', '')),
            'standard_unit': updates.get('standard_unit', existing.get('standard_unit', '')),
            'conversion_ratio': updates.get('conversion_ratio', existing.get('conversion_ratio', 1)),
        }

        db.upsert_product_cost(**merged)
        _log_action('update_product_cost', target=product_name,
                     detail=str(updates))
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
