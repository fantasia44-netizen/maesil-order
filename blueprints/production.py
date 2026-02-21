"""
production.py — 생산/입고 관리 Blueprint.
입고 엑셀 업로드, 생산 엑셀 업로드 처리.
"""
import os
from datetime import datetime

import pandas as pd
from flask import (
    Blueprint, render_template, request, current_app,
    flash, redirect, url_for,
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
@role_required('admin', 'manager', 'production')
def index():
    """생산/입고 업로드 폼"""
    return render_template('production/index.html')


# ── 입고 처리 ──

@production_bp.route('/inbound', methods=['POST'])
@role_required('admin', 'manager', 'production')
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
        result = process_inbound(current_app.db, filepath, date_str, mode)

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
@role_required('admin', 'manager', 'production')
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
        result = process_production(current_app.db, filepath, date_str, mode, location)

        if result.get('warnings'):
            for w in result['warnings']:
                flash(w, 'warning')

        flash(f"생산 처리 완료: 산출 {result.get('prod_count', 0)}건, "
              f"원재료 차감 {result.get('mat_count', 0)}건"
              + (f", 기존 {result.get('deleted_count', 0)}건 삭제" if mode == '수정입력' else ''),
              'success')
    except Exception as e:
        flash(f'생산 처리 중 오류: {e}', 'danger')
    finally:
        if os.path.exists(filepath):
            os.remove(filepath)

    return redirect(url_for('production.index'))
