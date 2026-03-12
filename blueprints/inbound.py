"""
inbound.py — 입고 관리 Blueprint.
시스템 입력(다건 배치) + 엑셀 업로드 + 입고 이력 조회.
"""
import os
import io
import tempfile
from datetime import datetime
from services.tz_utils import today_kst, now_kst

import pandas as pd
from flask import (
    Blueprint, render_template, request, current_app,
    flash, redirect, url_for, jsonify, send_file,
)
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename

from auth import role_required, _log_action
from services.storage_helper import backup_to_storage, backup_bytes_to_storage

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
        products = current_app.db.query_unique_product_names()
        return jsonify(products)
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
        old_record = current_app.db.query_stock_ledger_by_id(record_id)
        current_app.db.delete_stock_ledger_by_id(record_id)
        _log_action('delete_inbound', target=str(record_id),
                     old_value=old_record)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── API: 개별 수정 (admin 전용) ──

@inbound_bp.route('/api/update/<int:record_id>', methods=['POST'])
@role_required('admin')
def api_update(record_id):
    """개별 입고 이력 수정 (admin 전용)"""
    data = request.get_json(silent=True)
    if not data:
        return jsonify({'error': '수정 데이터가 없습니다.'}), 400
    allowed = {'product_name', 'qty', 'location', 'category', 'unit',
               'expiry_date', 'storage_method', 'manufacture_date', 'origin', 'lot_number', 'grade'}
    update_data = {k: v for k, v in data.items() if k in allowed}
    if 'qty' in update_data:
        try:
            update_data['qty'] = float(update_data['qty'])
            if update_data['qty'] <= 0:
                raise ValueError
            if update_data['qty'] == int(update_data['qty']):
                update_data['qty'] = int(update_data['qty'])
        except (ValueError, TypeError):
            return jsonify({'error': '수량이 올바르지 않습니다.'}), 400
    # 빈 문자열 → None 변환 (PostgreSQL DATE/TEXT 컬럼 호환)
    for key in ('expiry_date', 'manufacture_date', 'storage_method'):
        if key in update_data and update_data[key] == '':
            update_data[key] = None
    if not update_data:
        return jsonify({'error': '수정할 항목이 없습니다.'}), 400
    try:
        old_record = current_app.db.query_stock_ledger_by_id(record_id)
        current_app.db.update_stock_ledger(record_id, update_data)
        _log_action('update_inbound', target=str(record_id),
                     old_value=old_record, new_value=update_data)
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
    date_str = data.get('date', today_kst())
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
            if float(qty) <= 0:
                raise ValueError
        except (ValueError, TypeError):
            return jsonify({'error': f'{i+1}번째 항목 ({name}): 수량이 올바르지 않습니다.'}), 400

    try:
        from services.inbound_service import process_inbound_batch
        result = process_inbound_batch(current_app.db, date_str, mode, items)
        _log_action('batch_inbound',
                     detail=f'{date_str} 일괄입고 {result.get("count", 0)}건 등록 '
                            f'(모드: {mode}, 항목 {len(items)}건)')
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

    date_str = request.form.get('date', today_kst())
    mode = request.form.get('mode', '신규입력')

    upload_dir = current_app.config['UPLOAD_FOLDER']
    os.makedirs(upload_dir, exist_ok=True)
    ext = file.filename.rsplit('.', 1)[1].lower() if '.' in file.filename else 'xlsx'
    fname = f"inbound_{now_kst().strftime('%H%M%S%f')}.{ext}"
    filepath = os.path.join(upload_dir, fname)
    file.save(filepath)
    backup_to_storage(current_app.db, filepath, 'upload', 'inbound')

    try:
        from services.production_service import process_inbound
        df = pd.read_excel(filepath).fillna("")
        result = process_inbound(current_app.db, df, date_str, mode)

        if result.get('warnings'):
            for w in result['warnings']:
                flash(w, 'warning')

        _log_action('excel_inbound',
                     detail=f'{file.filename}: {date_str} 입고 {result.get("count", 0)}건 등록 '
                            f'(모드: {mode})')
        flash(f"입고 처리 완료: {result.get('count', 0)}건 등록"
              + (f", 기존 {result.get('deleted_count', 0)}건 삭제" if mode == '수정입력' else ''),
              'success')
    except Exception as e:
        flash(f'입고 처리 중 오류: {e}', 'danger')
    finally:
        if os.path.exists(filepath):
            os.remove(filepath)

    return redirect(url_for('inbound.index'))


# ── 입고일지 PDF ──

@inbound_bp.route('/log_pdf')
@role_required('admin', 'manager', 'logistics', 'production')
def log_pdf():
    """입고일지 PDF 다운로드"""
    date_str = request.args.get('date', '')

    if not date_str:
        flash('입고일자를 입력하세요.', 'warning')
        return redirect(url_for('inbound.index'))

    db = current_app.db

    try:
        from services.stock_service import query_all_stock_data
        from models import APPROVAL_LABELS
        from reports.inbound_daily import generate_inbound_log_pdf

        df = query_all_stock_data(db, date_str)
        if df.empty:
            flash('해당 일자의 입고 데이터가 없습니다.', 'warning')
            return redirect(url_for('inbound.index'))

        df = df[df['transaction_date'] == date_str]
        df_inbound = df[df['type'] == 'INBOUND'].copy()

        if df_inbound.empty:
            flash('해당 일자의 입고 데이터가 없습니다.', 'warning')
            return redirect(url_for('inbound.index'))

        config = {
            'target_date': date_str,
            'approvals': {label: '' for label in APPROVAL_LABELS},
            'title': '입고일지',
            'include_warnings': False,
        }

        tmp = tempfile.NamedTemporaryFile(suffix='.pdf', delete=False)
        tmp_path = tmp.name
        tmp.close()

        try:
            generate_inbound_log_pdf(tmp_path, config, df_inbound)
            with open(tmp_path, 'rb') as f:
                pdf_bytes = io.BytesIO(f.read())
            fname = f"입고일지_{date_str}.pdf"
            backup_bytes_to_storage(db, pdf_bytes.getvalue(), fname, 'report', 'inbound')
            return send_file(pdf_bytes, mimetype='application/pdf',
                             as_attachment=True, download_name=fname)
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    except Exception as e:
        flash(f'입고일지 PDF 생성 중 오류: {e}', 'danger')
        return redirect(url_for('inbound.index'))
