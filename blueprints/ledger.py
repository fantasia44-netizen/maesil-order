"""
ledger.py — 수불장(재고원장) 조회/내보내기 Blueprint.
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

from auth import role_required
from models import INV_TYPE_LABELS, LEDGER_CATEGORY_MAP

ledger_bp = Blueprint('ledger', __name__, url_prefix='/ledger')


@ledger_bp.route('/')
@role_required('admin', 'manager')
def index():
    """수불장 조회"""
    db = current_app.db

    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    location = request.args.get('location', '전체')
    ledger_type = request.args.get('ledger_type', '')

    locations, categories = [], []
    try:
        locations, categories = db.query_filter_options()
    except Exception:
        pass

    data = []
    if date_from or date_to:
        try:
            # 수불장 유형에 따라 종류 필터
            category_filter = None
            if ledger_type and ledger_type in LEDGER_CATEGORY_MAP:
                category_filter = LEDGER_CATEGORY_MAP[ledger_type]

            raw = db.query_stock_ledger(
                date_to=date_to or '9999-12-31',
                date_from=date_from or None,
                location=location if location != '전체' else None,
                order_desc=False,
            )

            if category_filter:
                raw = [r for r in raw if r.get('category', '') in category_filter]

            data = raw
        except Exception as e:
            flash(f'수불장 조회 중 오류: {e}', 'danger')

    return render_template('ledger/index.html',
                           data=data,
                           date_from=date_from, date_to=date_to,
                           location=location, ledger_type=ledger_type,
                           locations=locations,
                           ledger_types=list(LEDGER_CATEGORY_MAP.keys()),
                           type_labels=INV_TYPE_LABELS)


@ledger_bp.route('/export')
@role_required('admin', 'manager')
def export():
    """수불장 엑셀 다운로드"""
    db = current_app.db

    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    location = request.args.get('location', '전체')
    ledger_type = request.args.get('ledger_type', '')

    try:
        raw = db.query_stock_ledger(
            date_to=date_to or '9999-12-31',
            date_from=date_from or None,
            location=location if location != '전체' else None,
            order_desc=False,
        )

        if ledger_type and ledger_type in LEDGER_CATEGORY_MAP:
            category_filter = LEDGER_CATEGORY_MAP[ledger_type]
            raw = [r for r in raw if r.get('category', '') in category_filter]

        if not raw:
            flash('다운로드할 데이터가 없습니다.', 'warning')
            return redirect(url_for('ledger.index'))

        df = pd.DataFrame(raw)

        # 컬럼 한글화
        col_map = {
            'transaction_date': '일자',
            'type': '유형',
            'product_name': '품목명',
            'qty': '수량',
            'location': '창고',
            'category': '종류',
            'unit': '단위',
            'expiry_date': '소비기한',
            'storage_method': '보관방법',
            'lot_number': '이력번호',
            'grade': '등급',
            'manufacture_date': '제조일',
            'origin': '원산지',
        }
        export_cols = [c for c in col_map.keys() if c in df.columns]
        df = df[export_cols].rename(columns=col_map)

        # 유형 라벨 변환
        if '유형' in df.columns:
            df['유형'] = df['유형'].map(lambda x: INV_TYPE_LABELS.get(x, x))

        output = io.BytesIO()
        sheet_name = ledger_type or '수불장'
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name=sheet_name)
        output.seek(0)

        fname = f"{sheet_name}_{date_from or 'all'}_{date_to or 'all'}.xlsx"
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=fname,
        )
    except Exception as e:
        flash(f'수불장 다운로드 중 오류: {e}', 'danger')
        return redirect(url_for('ledger.index'))
