"""
inbound.py — 입고 관리 Blueprint.
시스템 입력(다건 배치) + 엑셀 업로드 + 입고 이력 조회.
"""
import os
from datetime import datetime

import pandas as pd
from flask import (
    Blueprint, render_template, request, current_app,
    flash, redirect, url_for, jsonify,
)
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename

from auth import role_required

inbound_bp = Blueprint('inbound', __name__, url_prefix='/inbound')

ALLOWED_EXT = {'xlsx', 'xls'}


def _allowed(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXT


@inbound_bp.route('/')
@role_required('admin', 'manager', 'logistics', 'production')
def index():
    """입고 관리 페이지"""
    db = current_app.db
    locations = []
    try:
        locations, _ = db.query_filter_options()
    except Exception:
        pass
    return render_template('inbound/index.html', locations=locations)


@inbound_bp.route('/api/products')
@role_required('admin', 'manager', 'logistics', 'production')
def api_products():
    """전체 고유 품목명 목록 JSON (자동완성용)"""
    try:
        names = current_app.db.query_unique_product_names()
        return jsonify([{'name': n} for n in names])
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@inbound_bp.route('/api/history')
@role_required('admin', 'manager', 'logistics', 'production')
def api_history():
    """입고 이력 조회 JSON"""
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    if not date_from or not date_to:
        return jsonify([])
    try:
        data = current_app.db.query_stock_ledger(
            date_from=date_from, date_to=date_to, type_list=['INBOUND'])
        rows = []
        for r in data:
            rows.append({
                'id': r.get('id'),
                'date': r.get('transaction_date', ''),
                'product_name': r.get('product_name', ''),
                'qty': r.get('qty', 0),
                'location': r.get('location', ''),
                'category': r.get('category', ''),
                'unit': r.get('unit', '개'),
                'expiry_date': r.get('expiry_date', ''),
                'storage_method': r.get('storage_method', ''),
                'manufacture_date': r.get('manufacture_date', ''),
            })
        rows.sort(key=lambda x: (x['date'], x['product_name']))
        return jsonify(rows)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── API: 개별 삭제 (admin 전용) ──

@inbound_bp.route('/api/delete/<int:record_id>', methods=['POST'])
@role_required('admin')
def api_delete(record_id):
    """개별 입고 이력 삭제 (admin 전용)"""
    try:
        current_app.db.delete_stock_ledger_by_id(record_id)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@inbound_bp.route('/batch', methods=['POST'])
@role_required('admin', 'manager', 'logistics', 'production')
def batch():
    """다건 일괄 입고 (JSON)"""
    data = request.get_json(silent=True)
    if not data:
        return jsonify({'error': '요청 데이터가 없습니다.'}), 400

    items = data.get('items', [])
    date_str = data.get('date', datetime.now().strftime('%Y-%m-%d'))
    mode = data.get('mode', '신규입력')

    if not items:
        return jsonify({'error': '입고 항목이 없습니다.'}), 400

    # 유효성 검증
    for i, item in enumerate(items):
        name = str(item.get('product_name', '')).strip()
        qty = item.get('qty', 0)
        location = str(item.get('location', '')).strip()
        if not name:
            return jsonify({'error': f'{i+1}번째 항목: 품목명을 입력하세요.'}), 400
        if not location:
            return jsonify({'error': f'{i+1}번째 항목: 창고위치를 선택하세요.'}), 400
        try:
            if int(float(qty)) <= 0:
                raise ValueError
        except (ValueError, TypeError):
            return jsonify({'error': f'{i+1}번째 항목 ({name}): 수량이 올바르지 않습니다.'}), 400

    try:
        from services.inbound_service import process_inbound_batch
        result = process_inbound_batch(current_app.db, date_str, mode, items)
        return jsonify({
            'success': True,
            'count': result.get('count', 0),
            'warnings': result.get('warnings', []),
            'deleted_count': result.get('deleted_count', 0),
        })
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        return jsonify({'error': f'입고 처리 중 오류: {e}'}), 500


@inbound_bp.route('/excel', methods=['POST'])
@role_required('admin', 'manager', 'logistics', 'production')
def excel():
    """입고 엑셀 업로드 (기존 production.inbound 이관)"""
    file = request.files.get('file')
    if not file or not _allowed(file.filename):
        flash('엑셀 파일(.xlsx/.xls)을 선택하세요.', 'danger')
        return redirect(url_for('inbound.index'))

    date_str = request.form.get('date', datetime.now().strftime('%Y-%m-%d'))
    mode = request.form.get('mode', '신규입력')

    upload_dir = current_app.config['UPLOAD_FOLDER']
    os.makedirs(upload_dir, exist_ok=True)
    ext = file.filename.rsplit('.', 1)[1].lower() if '.' in file.filename else 'xlsx'
    fname = f"inbound_{datetime.now().strftime('%H%M%S%f')}.{ext}"
    filepath = os.path.join(upload_dir, fname)
    file.save(filepath)

    try:
        from services.production_service import process_inbound
        df = pd.read_excel(filepath).fillna("")
        result = process_inbound(current_app.db, df, date_str, mode)

        if result.get('warnings'):
            for w in result['warnings']:
                flash(w, 'warning')

        flash(f"입고 처리 완료: {result.get('count', 0)}건 등록"
              + (f", 기존 {result.get('deleted_count', 0)}건 삭제" if mode == '수정입력' else ''),
              'success')
    except Exception as e:
        flash(f'입고 처리 중 오류: {e}', 'danger')
    finally:
        if os.path.exists(filepath):
            os.remove(filepath)

    return redirect(url_for('inbound.index'))
