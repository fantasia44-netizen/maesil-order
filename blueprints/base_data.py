"""
base_data.py — 기초 데이터 초기화 Blueprint.
위험한 전체 삭제/초기화 작업 — 관리자 전용.
"""
import os
from datetime import datetime
from services.tz_utils import today_kst

import pandas as pd
from flask import (
    Blueprint, render_template, request, current_app,
    flash, redirect, url_for,
)
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename

from auth import role_required, _log_action

base_data_bp = Blueprint('base_data', __name__, url_prefix='/base-data')

ALLOWED_EXT = {'xlsx', 'xls'}


def _allowed(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXT


@base_data_bp.route('/')
@role_required('admin', 'manager')
def index():
    """기초 데이터 관리 폼"""
    db = current_app.db
    stats = {}
    try:
        # 현재 데이터 건수 요약
        all_data = db.query_stock_ledger(date_to='9999-12-31')
        stats['stock_count'] = len(all_data) if all_data else 0
        # order_transactions 건수
        order_res = db.client.table("order_transactions").select("id", count="exact").execute()
        stats['order_count'] = order_res.count if order_res.count is not None else len(order_res.data or [])
    except Exception:
        stats = {'stock_count': '조회 실패', 'order_count': '조회 실패'}

    return render_template('base_data/index.html', stats=stats)


@base_data_bp.route('/reset-base', methods=['POST'])
@role_required('admin')
def reset_base():
    """기초재고 초기화 — 전체 삭제 후 엑셀 업로드로 재설정"""
    confirm = request.form.get('confirm', '')
    if confirm != 'RESET':
        flash('확인 문구를 정확히 입력하세요. (RESET)', 'danger')
        return redirect(url_for('base_data.index'))

    file = request.files.get('file')
    db = current_app.db

    try:
        # 전체 재고 삭제
        deleted = db.delete_stock_ledger_all()
        _log_action('reset_stock_ledger', detail=f'전체 삭제: {deleted}건')
        flash(f'기존 재고 데이터 {deleted}건 삭제 완료', 'info')

        # 엑셀 파일이 있으면 기초재고로 등록
        if file and file.filename and _allowed(file.filename):
            upload_dir = current_app.config['UPLOAD_FOLDER']
            os.makedirs(upload_dir, exist_ok=True)
            filename = secure_filename(file.filename)
            filepath = os.path.join(upload_dir, filename)
            file.save(filepath)

            try:
                from services.excel_io import parse_base_data_payload, flexible_column_rename
                df = pd.read_excel(filepath)
                df = flexible_column_rename(df)
                today = request.form.get('date', today_kst())
                payload = parse_base_data_payload(df, today)

                if payload:
                    db.insert_stock_ledger(payload)
                    _log_action('init_stock_ledger', detail=f'기초재고 {len(payload)}건 등록')
                    flash(f'기초재고 {len(payload)}건 등록 완료', 'success')
                else:
                    flash('엑셀에서 유효한 데이터가 없습니다.', 'warning')
            finally:
                if os.path.exists(filepath):
                    os.remove(filepath)
        else:
            flash('기초재고 엑셀 없이 전체 삭제만 수행되었습니다.', 'warning')

    except Exception as e:
        flash(f'기초 데이터 초기화 중 오류: {e}', 'danger')

    return redirect(url_for('base_data.index'))


@base_data_bp.route('/reset-revenue', methods=['POST'])
@role_required('admin')
def reset_revenue():
    """매출 데이터 초기화 — 전체 또는 기간 삭제"""
    confirm = request.form.get('confirm', '')
    if confirm != 'RESET':
        flash('확인 문구를 정확히 입력하세요. (RESET)', 'danger')
        return redirect(url_for('base_data.index'))

    db = current_app.db
    date_from = request.form.get('date_from', '').strip()
    date_to = request.form.get('date_to', '').strip()

    try:
        if date_from or date_to:
            deleted = db.delete_revenue_by_date(
                date_from=date_from or None,
                date_to=date_to or None,
            )
            period = f"{date_from or '처음'}~{date_to or '끝'}"
            _log_action('reset_revenue', detail=f'기간 삭제({period}): {deleted}건')
            flash(f'매출 데이터 {deleted}건 삭제 완료 ({period})', 'success')
        else:
            deleted = db.delete_revenue_all()
            _log_action('reset_revenue', detail=f'전체 삭제: {deleted}건')
            flash(f'매출 데이터 전체 {deleted}건 삭제 완료', 'success')
    except Exception as e:
        flash(f'매출 초기화 중 오류: {e}', 'danger')

    return redirect(url_for('base_data.index'))
