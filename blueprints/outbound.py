"""
outbound.py — 출고 관리 Blueprint.
단건 출고 엑셀 업로드, 일괄(batch) 출고 처리.
"""
import os
from datetime import datetime

from flask import (
    Blueprint, render_template, request, current_app,
    flash, redirect, url_for,
)
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename

from auth import role_required

outbound_bp = Blueprint('outbound', __name__, url_prefix='/outbound')

ALLOWED_EXT = {'xlsx', 'xls'}


def _allowed(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXT


@outbound_bp.route('/')
@role_required('admin', 'manager', 'logistics')
def index():
    """출고 업로드 폼"""
    return render_template('outbound/index.html')


@outbound_bp.route('/process', methods=['POST'])
@role_required('admin', 'manager', 'logistics')
def process():
    """단건 출고 엑셀 처리"""
    file = request.files.get('file')
    if not file or not _allowed(file.filename):
        flash('엑셀 파일(.xlsx/.xls)을 선택하세요.', 'danger')
        return redirect(url_for('outbound.index'))

    date_str = request.form.get('date', datetime.now().strftime('%Y-%m-%d'))
    mode = request.form.get('mode', '신규입력')
    location = request.form.get('location', '')
    category = request.form.get('category', '')

    upload_dir = current_app.config['UPLOAD_FOLDER']
    os.makedirs(upload_dir, exist_ok=True)
    filename = secure_filename(file.filename)
    filepath = os.path.join(upload_dir, filename)
    file.save(filepath)

    try:
        from services.outbound_service import process_outbound
        result = process_outbound(
            current_app.db, filepath, date_str, mode,
            location=location, category=category,
        )

        if result.get('warnings'):
            for w in result['warnings']:
                flash(w, 'warning')

        flash(f"출고 처리 완료: {result.get('count', 0)}건"
              + (f", 기존 {result.get('deleted_count', 0)}건 삭제" if mode == '수정입력' else ''),
              'success')
    except Exception as e:
        flash(f'출고 처리 중 오류: {e}', 'danger')
    finally:
        if os.path.exists(filepath):
            os.remove(filepath)

    return redirect(url_for('outbound.index'))


@outbound_bp.route('/batch', methods=['POST'])
@role_required('admin', 'manager', 'logistics')
def batch():
    """일괄 출고 — 여러 엑셀 파일 동시 업로드"""
    files = request.files.getlist('files')
    if not files or all(f.filename == '' for f in files):
        flash('엑셀 파일을 하나 이상 선택하세요.', 'danger')
        return redirect(url_for('outbound.index'))

    date_str = request.form.get('date', datetime.now().strftime('%Y-%m-%d'))
    mode = request.form.get('mode', '신규입력')

    upload_dir = current_app.config['UPLOAD_FOLDER']
    os.makedirs(upload_dir, exist_ok=True)

    total_count = 0
    total_warnings = []
    errors = []

    for file in files:
        if not file or file.filename == '' or not _allowed(file.filename):
            continue

        filename = secure_filename(file.filename)
        filepath = os.path.join(upload_dir, filename)
        file.save(filepath)

        try:
            from services.outbound_service import process_outbound
            result = process_outbound(
                current_app.db, filepath, date_str, mode,
            )
            total_count += result.get('count', 0)
            total_warnings.extend(result.get('warnings', []))
        except Exception as e:
            errors.append(f'{filename}: {e}')
        finally:
            if os.path.exists(filepath):
                os.remove(filepath)

    if total_warnings:
        for w in total_warnings:
            flash(w, 'warning')
    if errors:
        for e in errors:
            flash(e, 'danger')

    flash(f"일괄 출고 완료: 총 {total_count}건 처리", 'success')
    return redirect(url_for('outbound.index'))
