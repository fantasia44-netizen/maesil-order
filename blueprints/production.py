"""
production.py — 생산/입고 관리 Blueprint.
입고 엑셀 업로드, 생산 엑셀 업로드 처리.
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

production_bp = Blueprint('production', __name__, url_prefix='/production')

ALLOWED_EXT = {'xlsx', 'xls'}


def _allowed(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXT


# ── 입고/생산 폼 ──

@production_bp.route('/')
@role_required('admin', 'manager', 'logistics', 'production')
def index():
    """생산/입고 업로드 폼"""
    return render_template('production/index.html')


# ── 입고 처리 ──

@production_bp.route('/inbound', methods=['POST'])
@role_required('admin', 'manager', 'logistics', 'production')
def inbound():
    """입고 엑셀 업로드 → DB 반영"""
    file = request.files.get('file')
    if not file or not _allowed(file.filename):
        flash('엑셀 파일(.xlsx/.xls)을 선택하세요.', 'danger')
        return redirect(url_for('production.index'))

    date_str = request.form.get('date', datetime.now().strftime('%Y-%m-%d'))
    mode = request.form.get('mode', '신규입력')

    upload_dir = current_app.config['UPLOAD_FOLDER']
    os.makedirs(upload_dir, exist_ok=True)
    filename = secure_filename(file.filename)
    filepath = os.path.join(upload_dir, filename)
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

    return redirect(url_for('production.index'))


# ── 생산 처리 ──

@production_bp.route('/production', methods=['POST'])
@role_required('admin', 'manager', 'logistics', 'production')
def production():
    """생산 엑셀 업로드 → DB 반영 (생산 산출 + 원재료 차감)"""
    file = request.files.get('file')
    if not file or not _allowed(file.filename):
        flash('엑셀 파일(.xlsx/.xls)을 선택하세요.', 'danger')
        return redirect(url_for('production.index'))

    date_str = request.form.get('date', datetime.now().strftime('%Y-%m-%d'))
    mode = request.form.get('mode', '신규입력')
    location = request.form.get('location', '')

    upload_dir = current_app.config['UPLOAD_FOLDER']
    os.makedirs(upload_dir, exist_ok=True)
    filename = secure_filename(file.filename)
    filepath = os.path.join(upload_dir, filename)
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
