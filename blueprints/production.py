"""
production.py — 생산 관리 Blueprint.
시스템 입력(다건 배치) + 엑셀 업로드 + 생산일지 PDF + 생산 이력 조회.
"""
import os
import io
import tempfile
from datetime import datetime

import pandas as pd
from flask import (
    Blueprint, render_template, request, current_app,
    flash, redirect, url_for, send_file, jsonify,
)
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename

from auth import role_required

production_bp = Blueprint('production', __name__, url_prefix='/production')

ALLOWED_EXT = {'xlsx', 'xls'}


def _allowed(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXT


# ── 생산 폼 ──

@production_bp.route('/')
@role_required('admin', 'manager', 'logistics', 'production')
def index():
    """생산 관리 페이지"""
    db = current_app.db
    locations = []
    try:
        locations, _ = db.query_filter_options()
    except Exception:
        pass
    return render_template('production/index.html', locations=locations)


# ── API: 품목 자동완성 ──

@production_bp.route('/api/products')
@role_required('admin', 'manager', 'logistics', 'production')
def api_products():
    """전체 고유 품목명 목록 JSON (생산품 자동완성)"""
    try:
        names = current_app.db.query_unique_product_names()
        return jsonify([{'name': n} for n in names])
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@production_bp.route('/api/stock')
@role_required('admin', 'manager', 'logistics', 'production')
def api_stock():
    """창고별 재고 품목 목록 JSON (원료/반제품 자동완성)"""
    location = request.args.get('location', '')
    if not location:
        return jsonify([])
    try:
        from services.excel_io import build_stock_snapshot
        all_data = current_app.db.query_stock_by_location(location)
        snapshot = build_stock_snapshot(all_data)
        products = []
        for name, info in snapshot.items():
            if info['total'] > 0:
                products.append({
                    'name': name,
                    'qty': info['total'],
                    'unit': info.get('unit', '개'),
                })
        products.sort(key=lambda x: x['name'])
        return jsonify(products)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── API: 생산 이력 조회 ──

@production_bp.route('/api/history')
@role_required('admin', 'manager', 'logistics', 'production')
def api_history():
    """생산 이력 조회 JSON"""
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    if not date_from or not date_to:
        return jsonify([])
    try:
        data = current_app.db.query_stock_ledger(
            date_from=date_from, date_to=date_to,
            type_list=['PRODUCTION', 'PROD_OUT'])
        rows = []
        for r in data:
            rows.append({
                'id': r.get('id'),
                'date': r.get('transaction_date', ''),
                'type': r.get('type', ''),
                'product_name': r.get('product_name', ''),
                'qty': r.get('qty', 0),
                'location': r.get('location', ''),
                'category': r.get('category', ''),
                'unit': r.get('unit', '개'),
            })
        rows.sort(key=lambda x: (x['date'], x['type'], x['product_name']))
        return jsonify(rows)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── API: 개별 삭제 (admin 전용) ──

@production_bp.route('/api/delete/<int:record_id>', methods=['POST'])
@role_required('admin')
def api_delete(record_id):
    """개별 생산 이력 삭제 (admin 전용)"""
    try:
        current_app.db.delete_stock_ledger_by_id(record_id)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── 시스템 입력 배치 생산 ──

@production_bp.route('/batch', methods=['POST'])
@role_required('admin', 'manager', 'logistics', 'production')
def batch():
    """다건 일괄 생산 처리 (JSON, 중첩 materials 포함)"""
    data = request.get_json(silent=True)
    if not data:
        return jsonify({'error': '요청 데이터가 없습니다.'}), 400

    items = data.get('items', [])
    date_str = data.get('date', datetime.now().strftime('%Y-%m-%d'))
    mode = data.get('mode', '신규입력')
    location = data.get('location', '')

    if not items:
        return jsonify({'error': '생산 항목이 없습니다.'}), 400

    if not location:
        return jsonify({'error': '생산 위치를 선택하세요.'}), 400

    # 유효성 검증
    for i, item in enumerate(items):
        name = str(item.get('product_name', '')).strip()
        qty = item.get('qty', 0)
        if not name:
            return jsonify({'error': f'{i+1}번째 항목: 품목명을 입력하세요.'}), 400
        try:
            if int(float(qty)) <= 0:
                raise ValueError
        except (ValueError, TypeError):
            return jsonify({'error': f'{i+1}번째 항목 ({name}): 생산수량이 올바르지 않습니다.'}), 400

        for j, mat in enumerate(item.get('materials', [])):
            mat_name = str(mat.get('product_name', '')).strip()
            mat_qty = mat.get('qty', 0)
            if not mat_name:
                return jsonify({'error': f'{i+1}번째 항목 재료{j+1}: 재료명을 입력하세요.'}), 400
            try:
                if int(float(mat_qty)) <= 0:
                    raise ValueError
            except (ValueError, TypeError):
                return jsonify({'error': f'{i+1}번째 항목 재료 ({mat_name}): 수량이 올바르지 않습니다.'}), 400

    try:
        from services.production_service import process_production_batch
        result = process_production_batch(
            current_app.db, date_str, mode, location, items)
        return jsonify({
            'success': True,
            'produced': result.get('produced', 0),
            'materials_used': result.get('materials_used', 0),
            'warnings': result.get('warnings', []),
            'deleted_count': result.get('deleted_count', 0),
        })
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        return jsonify({'error': f'생산 처리 중 오류: {e}'}), 500


# ── 엑셀 생산 업로드 ──

@production_bp.route('/excel', methods=['POST'])
@role_required('admin', 'manager', 'logistics', 'production')
def excel_upload():
    """생산 엑셀 업로드 → DB 반영 (생산 산출 + 원재료 차감)"""
    file = request.files.get('file')
    if not file or not _allowed(file.filename):
        flash('엑셀 파일(.xlsx/.xls)을 선택하세요.', 'danger')
        return redirect(url_for('production.index'))

    date_str = request.form.get('date', datetime.now().strftime('%Y-%m-%d'))
    mode = request.form.get('mode', '신규입력')

    upload_dir = current_app.config['UPLOAD_FOLDER']
    os.makedirs(upload_dir, exist_ok=True)
    ext = file.filename.rsplit('.', 1)[1].lower() if '.' in file.filename else 'xlsx'
    fname = f"prod_{datetime.now().strftime('%H%M%S%f')}.{ext}"
    filepath = os.path.join(upload_dir, fname)
    file.save(filepath)

    try:
        from services.production_service import process_production
        df = pd.read_excel(filepath).fillna("")
        result = process_production(current_app.db, df, date_str, mode)

        if result.get('warnings'):
            for w in result['warnings']:
                flash(w, 'warning')

        flash(f"생산 처리 완료: 산출 {result.get('produced', 0)}건, "
              f"원재료 차감 {result.get('materials_used', 0)}건"
              + (f", 기존 {result.get('deleted_count', 0)}건 삭제" if mode == '수정입력' else ''),
              'success')
    except Exception as e:
        flash(f'생산 처리 중 오류: {e}', 'danger')
    finally:
        if os.path.exists(filepath):
            os.remove(filepath)

    return redirect(url_for('production.index'))


# ── 생산일지 PDF ──

@production_bp.route('/log_pdf')
@role_required('admin', 'manager', 'logistics', 'production')
def log_pdf():
    """생산일지 PDF 다운로드"""
    date_str = request.args.get('date', '')
    location = request.args.get('location', '')

    if not date_str:
        flash('생산일자를 입력하세요.', 'warning')
        return redirect(url_for('production.index'))

    db = current_app.db

    try:
        from services.stock_service import query_all_stock_data
        from models import APPROVAL_LABELS
        from reports.production_daily import generate_production_log_pdf

        df = query_all_stock_data(db, date_str)
        if df.empty:
            flash('해당 일자의 생산 데이터가 없습니다.', 'warning')
            return redirect(url_for('production.index'))

        df = df[df['transaction_date'] == date_str]
        if location:
            df = df[df['location'] == location]

        df_prod = df[df['type'] == 'PRODUCTION'].copy()
        df_out = df[df['type'] == 'PROD_OUT'].copy()

        if df_prod.empty and df_out.empty:
            flash('해당 일자의 생산 데이터가 없습니다.', 'warning')
            return redirect(url_for('production.index'))

        config = {
            'target_date': date_str,
            'approvals': {label: '' for label in APPROVAL_LABELS},
            'title': '생산일지',
            'include_warnings': False,
        }

        tmp = tempfile.NamedTemporaryFile(suffix='.pdf', delete=False)
        tmp_path = tmp.name
        tmp.close()

        try:
            generate_production_log_pdf(tmp_path, config, df_prod, df_out)
            with open(tmp_path, 'rb') as f:
                pdf_bytes = io.BytesIO(f.read())
            fname = f"생산일지_{date_str}.pdf"
            return send_file(pdf_bytes, mimetype='application/pdf',
                             as_attachment=True, download_name=fname)
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    except Exception as e:
        flash(f'생산일지 PDF 생성 중 오류: {e}', 'danger')
        return redirect(url_for('production.index'))
