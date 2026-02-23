"""
revenue.py — 매출 관리 Blueprint.
일일매출 엑셀 업로드, 조회, 엑셀 다운로드.
"""
import os
import io
from datetime import datetime

import pandas as pd
from flask import (
    Blueprint, render_template, request, current_app,
    flash, redirect, url_for, send_file,
)
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename

from auth import role_required
from models import REVENUE_CATEGORIES

revenue_bp = Blueprint('revenue', __name__, url_prefix='/revenue')

ALLOWED_EXT = {'xlsx', 'xls'}


def _allowed(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXT


@revenue_bp.route('/')
@role_required('admin', 'manager', 'sales', 'general')
def index():
    """매출 조회"""
    db = current_app.db

    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    category = request.args.get('category', '전체')

    data = []
    total_revenue = 0

    try:
        data = db.query_revenue(
            date_from=date_from or None,
            date_to=date_to or None,
            category=category if category != '전체' else None,
        )
        total_revenue = sum(r.get('revenue', 0) for r in data)
    except Exception as e:
        flash(f'매출 조회 중 오류: {e}', 'danger')

    return render_template('revenue/index.html',
                           revenues=data, total_revenue=total_revenue,
                           date_from=date_from, date_to=date_to,
                           category=category,
                           categories=REVENUE_CATEGORIES)


@revenue_bp.route('/import', methods=['POST'])
@role_required('admin', 'manager', 'sales', 'general')
def import_revenue():
    """매출 엑셀 업로드"""
    file = request.files.get('file')
    if not file or not _allowed(file.filename):
        flash('엑셀 파일(.xlsx/.xls)을 선택하세요.', 'danger')
        return redirect(url_for('revenue.index'))

    upload_date = request.form.get('date', datetime.now().strftime('%Y-%m-%d'))

    upload_dir = current_app.config['UPLOAD_FOLDER']
    os.makedirs(upload_dir, exist_ok=True)
    filename = secure_filename(file.filename)
    filepath = os.path.join(upload_dir, filename)
    file.save(filepath)

    try:
        from services.excel_io import parse_revenue_payload
        df = pd.read_excel(filepath)
        payload, total_rev = parse_revenue_payload(df, upload_date)

        if not payload:
            flash('엑셀에서 유효한 매출 데이터가 없습니다.', 'warning')
            return redirect(url_for('revenue.index'))

        current_app.db.upsert_revenue(payload)
        flash(f'매출 {len(payload)}건 등록 완료 (합계: {total_rev:,}원)', 'success')
    except Exception as e:
        flash(f'매출 업로드 중 오류: {e}', 'danger')
    finally:
        if os.path.exists(filepath):
            os.remove(filepath)

    return redirect(url_for('revenue.index'))


@revenue_bp.route('/export')
@role_required('admin', 'manager', 'sales', 'general')
def export():
    """매출 데이터 엑셀 다운로드"""
    db = current_app.db

    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    category = request.args.get('category', '전체')

    try:
        data = db.query_revenue(
            date_from=date_from or None,
            date_to=date_to or None,
            category=category if category != '전체' else None,
        )

        if not data:
            flash('다운로드할 데이터가 없습니다.', 'warning')
            return redirect(url_for('revenue.index'))

        df = pd.DataFrame(data)

        # 컬럼 정리
        col_map = {
            'revenue_date': '매출일자',
            'product_name': '품목명',
            'category': '매출구분',
            'qty': '수량',
            'unit_price': '단가',
            'revenue': '매출액',
        }
        export_cols = [c for c in col_map.keys() if c in df.columns]
        df = df[export_cols].rename(columns=col_map)

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='매출')
        output.seek(0)

        fname = f"매출_{date_from or 'all'}_{date_to or 'all'}.xlsx"
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=fname,
        )
    except Exception as e:
        flash(f'매출 다운로드 중 오류: {e}', 'danger')
        return redirect(url_for('revenue.index'))
