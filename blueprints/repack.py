"""
repack.py — 소분(리패킹) 관리 Blueprint.
소분 엑셀 업로드, 이력 조회, 엑셀 다운로드.
"""
import os
import io
import tempfile
from datetime import datetime

import pandas as pd
from flask import (
    Blueprint, render_template, request, current_app,
    flash, redirect, url_for, send_file,
)
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename

from auth import role_required
from models import INV_TYPE_LABELS

repack_bp = Blueprint('repack', __name__, url_prefix='/repack')

ALLOWED_EXT = {'xlsx', 'xls'}


def _allowed(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXT


@repack_bp.route('/')
@role_required('admin', 'manager', 'production')
def index():
    """소분 관리 폼 + 이력"""
    db = current_app.db

    # 위치 목록 로드
    locations = []
    try:
        locations, _ = db.query_filter_options()
    except Exception:
        pass

    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')

    history = []
    if date_from or date_to:
        try:
            raw = db.query_stock_ledger(
                date_to=date_to or '9999-12-31',
                date_from=date_from or None,
                type_list=['REPACK_OUT', 'REPACK_IN'],
                order_desc=True,
            )
            history = raw
        except Exception as e:
            flash(f'소분 이력 조회 중 오류: {e}', 'danger')

    return render_template('repack/index.html',
                           history=history, locations=locations,
                           date_from=date_from, date_to=date_to,
                           type_labels=INV_TYPE_LABELS)


@repack_bp.route('/process', methods=['POST'])
@role_required('admin', 'manager', 'production')
def process():
    """소분 엑셀 업로드 → DB 반영"""
    file = request.files.get('file')
    if not file or not _allowed(file.filename):
        flash('엑셀 파일(.xlsx/.xls)을 선택하세요.', 'danger')
        return redirect(url_for('repack.index'))

    date_str = request.form.get('date', datetime.now().strftime('%Y-%m-%d'))
    mode = request.form.get('mode', '신규입력')
    location = request.form.get('location', '')

    upload_dir = current_app.config['UPLOAD_FOLDER']
    os.makedirs(upload_dir, exist_ok=True)
    filename = secure_filename(file.filename)
    filepath = os.path.join(upload_dir, filename)
    file.save(filepath)

    try:
        from services.repack_service import process_repack
        df = pd.read_excel(filepath).fillna("")
        result = process_repack(current_app.db, df, date_str, mode)

        if result.get('warnings'):
            for w in result['warnings']:
                flash(w, 'warning')

        flash(f"소분 처리 완료: 투입 {result.get('repack_out_count', 0)}건, "
              f"산출 {result.get('repack_in_count', 0)}건"
              + (f", 기존 {result.get('deleted_count', 0)}건 삭제" if mode == '수정입력' else ''),
              'success')
    except Exception as e:
        flash(f'소분 처리 중 오류: {e}', 'danger')
    finally:
        if os.path.exists(filepath):
            os.remove(filepath)

    return redirect(url_for('repack.index'))


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
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=fname,
        )
    except Exception as e:
        flash(f'소분 이력 다운로드 중 오류: {e}', 'danger')
        return redirect(url_for('repack.index'))


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
            return send_file(pdf_bytes, mimetype='application/pdf',
                             as_attachment=True, download_name=fname)
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    except Exception as e:
        flash(f'소분작업일지 PDF 생성 중 오류: {e}', 'danger')
        return redirect(url_for('repack.index'))
