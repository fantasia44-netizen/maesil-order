"""
repack.py — 소분(리패킹) 관리 Blueprint.
시스템 입력(다건 배치) + 엑셀 업로드 + 소분 이력 조회 + PDF/엑셀 다운로드.
"""
import os
import io
import tempfile
from datetime import datetime
from services.tz_utils import today_kst

import pandas as pd
from flask import (
    Blueprint, render_template, request, current_app,
    flash, redirect, url_for, send_file, jsonify,
)
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename

from auth import role_required, _log_action
from services.storage_helper import backup_to_storage, backup_bytes_to_storage
from models import INV_TYPE_LABELS

repack_bp = Blueprint('repack', __name__, url_prefix='/repack')

ALLOWED_EXT = {'xlsx', 'xls'}


def _allowed(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXT


# ── 소분 관리 페이지 ──

@repack_bp.route('/')
@role_required('admin', 'manager', 'production')
def index():
    """소분 관리 페이지"""
    db = current_app.db
    locations = []
    try:
        locations, _ = db.query_filter_options()
    except Exception:
        pass
    return render_template('repack/index.html', locations=locations)


# ── API: 품목 자동완성 ──

@repack_bp.route('/api/products')
@role_required('admin', 'manager', 'production')
def api_products():
    """전체 고유 품목명 목록 JSON (산출품 자동완성)"""
    try:
        products = current_app.db.query_unique_product_names()
        return jsonify(products)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@repack_bp.route('/api/stock')
@role_required('admin', 'manager', 'production')
def api_stock():
    """창고별 재고 품목 목록 JSON (투입품 자동완성)"""
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
                    'unit': info.get('unit') or '개',
                })
        products.sort(key=lambda x: x['name'])
        return jsonify(products)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── API: 소분 이력 조회 ──

@repack_bp.route('/api/history')
@role_required('admin', 'manager', 'production')
def api_history():
    """소분 이력 조회 JSON"""
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    if not date_from or not date_to:
        return jsonify([])
    try:
        data = current_app.db.query_stock_ledger(
            date_from=date_from, date_to=date_to,
            type_list=['REPACK_OUT', 'REPACK_IN'])
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
                'repack_doc_no': r.get('repack_doc_no', ''),
            })
        rows.sort(key=lambda x: (x['date'], x['repack_doc_no'], x['type'], x['product_name']))
        return jsonify(rows)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── API: 개별 삭제 (admin 전용) ──

@repack_bp.route('/api/delete/<int:record_id>', methods=['POST'])
@role_required('admin')
def api_delete(record_id):
    """개별 소분 이력 블라인드 처리 (admin 전용)"""
    try:
        old_record = current_app.db.query_stock_ledger_by_id(record_id)
        current_app.db.blind_stock_ledger(record_id, blinded_by=current_user.username)
        _log_action('blind_repack', target=str(record_id),
                     old_value=old_record)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── 시스템 입력 배치 소분 ──

# ── API: 개별 수정 (admin 전용) ──

@repack_bp.route('/api/update/<int:record_id>', methods=['POST'])
@role_required('admin')
def api_update(record_id):
    """개별 소분 이력 수정 (admin 전용)"""
    data = request.get_json(silent=True)
    if not data:
        return jsonify({'error': '수정 데이터가 없습니다.'}), 400
    allowed = {'product_name', 'qty', 'location', 'category', 'unit'}
    update_data = {k: v for k, v in data.items() if k in allowed}
    if 'qty' in update_data:
        try:
            update_data['qty'] = float(update_data['qty'])
            if update_data['qty'] == int(update_data['qty']):
                update_data['qty'] = int(update_data['qty'])
        except (ValueError, TypeError):
            return jsonify({'error': '수량이 올바르지 않습니다.'}), 400
    if not update_data:
        return jsonify({'error': '수정할 항목이 없습니다.'}), 400
    try:
        result = current_app.db.replace_stock_ledger(
            record_id, update_data, replaced_by_user=current_user.username)
        _log_action('replace_repack', target=str(record_id),
                     old_value=result.get('old_record'), new_value=update_data)
        return jsonify({'success': True, 'new_id': result.get('new_id')})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── 시스템 입력 배치 소분 ──

@repack_bp.route('/batch', methods=['POST'])
@role_required('admin', 'manager', 'production')
def batch():
    """다건 일괄 소분 처리 (JSON, 중첩 materials 포함)"""
    data = request.get_json(silent=True)
    if not data:
        return jsonify({'error': '요청 데이터가 없습니다.'}), 400

    items = data.get('items', [])
    date_str = data.get('date', today_kst())
    location = data.get('location', '')

    if not items:
        return jsonify({'error': '소분 항목이 없습니다.'}), 400

    if not location:
        return jsonify({'error': '작업 위치를 선택하세요.'}), 400

    # 유효성 검증
    for i, item in enumerate(items):
        name = str(item.get('product_name', '')).strip()
        qty = item.get('qty', 0)
        if not name:
            return jsonify({'error': f'{i+1}번째 항목: 산출품명을 입력하세요.'}), 400
        try:
            if float(qty) <= 0:
                raise ValueError
        except (ValueError, TypeError):
            return jsonify({'error': f'{i+1}번째 항목 ({name}): 산출수량이 올바르지 않습니다.'}), 400

        for j, mat in enumerate(item.get('materials', [])):
            mat_name = str(mat.get('product_name', '')).strip()
            mat_qty = mat.get('qty', 0)
            if not mat_name:
                return jsonify({'error': f'{i+1}번째 항목 투입품{j+1}: 투입품명을 입력하세요.'}), 400
            try:
                if float(mat_qty) <= 0:
                    raise ValueError
            except (ValueError, TypeError):
                return jsonify({'error': f'{i+1}번째 항목 투입품 ({mat_name}): 수량이 올바르지 않습니다.'}), 400

    try:
        from services.repack_service import process_repack_batch
        result = process_repack_batch(
            current_app.db, date_str, location, items,
            created_by=current_user.username)
        _log_action('batch_repack',
                     detail=f'{date_str} {location} 소분 — '
                            f'투입 {result.get("repack_out_count", 0)}건, '
                            f'산출 {result.get("repack_in_count", 0)}건 '
                            f'(항목 {len(items)}건)')
        return jsonify({
            'success': True,
            'repack_in_count': result.get('repack_in_count', 0),
            'repack_out_count': result.get('repack_out_count', 0),
            'warnings': result.get('warnings', []),
            'doc_nos': result.get('doc_nos', []),
        })
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        return jsonify({'error': f'소분 처리 중 오류: {e}'}), 500


# ── 엑셀 소분 업로드 ──

@repack_bp.route('/process', methods=['POST'])
@role_required('admin', 'manager', 'production')
def process():
    """소분 엑셀 업로드 — 비활성화 (추후 재구현)"""
    return jsonify({'error': '엑셀 업로드 기능은 비활성화되었습니다. 추후 재구현 예정입니다.'}), 410


# ── 소분 이력 엑셀 다운로드 ──

@repack_bp.route('/export')
@role_required('admin', 'manager', 'production')
def export():
    """소분 이력 엑셀 다운로드"""
    db = current_app.db

    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')

    try:
        raw = db.query_stock_ledger(
            date_to=date_to or '9999-12-31',
            date_from=date_from or None,
            type_list=['REPACK_OUT', 'REPACK_IN'],
            order_desc=True,
        )

        if not raw:
            flash('다운로드할 소분 이력이 없습니다.', 'warning')
            return redirect(url_for('repack.index'))

        df = pd.DataFrame(raw)

        col_map = {
            'transaction_date': '일자',
            'type': '유형',
            'product_name': '품목명',
            'qty': '수량',
            'location': '창고',
            'category': '종류',
            'unit': '단위',
            'expiry_date': '소비기한',
            'lot_number': '이력번호',
            'manufacture_date': '제조일',
        }
        export_cols = [c for c in col_map.keys() if c in df.columns]
        df = df[export_cols].rename(columns=col_map)

        if '유형' in df.columns:
            df['유형'] = df['유형'].map(lambda x: INV_TYPE_LABELS.get(x, x))

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='소분이력')
        output.seek(0)

        fname = f"소분이력_{date_from or 'all'}_{date_to or 'all'}.xlsx"
        backup_bytes_to_storage(db, output.getvalue(), fname, 'output', 'repack')
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=fname,
        )
    except Exception as e:
        flash(f'소분 이력 다운로드 중 오류: {e}', 'danger')
        return redirect(url_for('repack.index'))


# ── 소분작업일지 PDF ──

@repack_bp.route('/pdf')
@role_required('admin', 'manager', 'production')
def pdf():
    """소분작업일지 PDF 다운로드"""
    date_str = request.args.get('date', '')

    if not date_str:
        flash('작업일자를 입력하세요.', 'warning')
        return redirect(url_for('repack.index'))

    db = current_app.db

    try:
        from services.stock_service import query_all_stock_data
        from models import APPROVAL_LABELS
        from reports.repack_daily import generate_repack_log_pdf

        df = query_all_stock_data(db, date_str)
        if df.empty:
            flash('해당 일자의 소분 데이터가 없습니다.', 'warning')
            return redirect(url_for('repack.index'))

        df = df[df['transaction_date'] == date_str]
        df = df[df['type'].isin(['REPACK_OUT', 'REPACK_IN'])].copy()

        if df.empty:
            flash('해당 일자의 소분 데이터가 없습니다.', 'warning')
            return redirect(url_for('repack.index'))

        config = {
            'target_date': date_str,
            'approvals': {label: '' for label in APPROVAL_LABELS},
            'title': '소분작업일지',
            'include_warnings': False,
        }

        tmp = tempfile.NamedTemporaryFile(suffix='.pdf', delete=False)
        tmp_path = tmp.name
        tmp.close()

        try:
            generate_repack_log_pdf(tmp_path, config, df)
            with open(tmp_path, 'rb') as f:
                pdf_bytes = io.BytesIO(f.read())
            fname = f"소분작업일지_{date_str}.pdf"
            backup_bytes_to_storage(db, pdf_bytes.getvalue(), fname, 'report', 'repack')
            return send_file(pdf_bytes, mimetype='application/pdf',
                             as_attachment=True, download_name=fname)
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    except Exception as e:
        flash(f'소분작업일지 PDF 생성 중 오류: {e}', 'danger')
        return redirect(url_for('repack.index'))
