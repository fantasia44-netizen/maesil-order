"""
master.py — 마스터 데이터 관리 Blueprint.
품목/단가/BOM 마스터 엑셀 동기화, 품목명 공백 정리, 이력 검색.
"""
import os

import pandas as pd
from flask import (
    Blueprint, render_template, request, current_app,
    flash, redirect, url_for,
)
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename

from auth import role_required, _log_action

master_bp = Blueprint('master', __name__, url_prefix='/master')

ALLOWED_EXT = {'xlsx', 'xls'}

MASTER_TABLES = {
    'product': 'master_products',
    'price': 'master_prices',
    'bom': 'master_bom',
}


def _allowed(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXT


@master_bp.route('/')
@role_required('admin')
def index():
    """마스터 데이터 현황"""
    db = current_app.db
    counts = {}
    for key, table_name in MASTER_TABLES.items():
        try:
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
    """단가 마스터 동기화"""
    return _sync_master('price', 'master_prices', '단가 마스터')


@master_bp.route('/sync-bom', methods=['POST'])
@role_required('admin')
def sync_bom():
    """BOM 마스터 동기화"""
    return _sync_master('bom', 'master_bom', 'BOM 마스터')


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

        current_app.db.sync_master_table(table_name, payload)
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
        fixed_count, dupe_groups = current_app.db.fix_product_name_spaces()
        _log_action('fix_product_spaces', detail=f'{fixed_count}건 정리')

        if dupe_groups:
            for norm, variants in dupe_groups.items():
                flash(f'통합: {", ".join(variants)} → {norm}', 'info')

        flash(f'품목명 공백 정리 완료: {fixed_count}건 수정', 'success')
    except Exception as e:
        flash(f'공백 정리 중 오류: {e}', 'danger')

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
        raw = current_app.db.query_master_table(table_name)

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
            counts[key] = current_app.db.count_master_table(tbl)
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
