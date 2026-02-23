"""
ledger.py — 수불장(재고원장) 조회/내보내기 Blueprint.
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

from auth import role_required
from models import INV_TYPE_LABELS, LEDGER_CATEGORY_MAP

ledger_bp = Blueprint('ledger', __name__, url_prefix='/ledger')


@ledger_bp.route('/')
@role_required('admin', 'manager', 'logistics', 'production', 'general')
def index():
    """수불장 조회"""
    db = current_app.db

    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    location = request.args.get('location', '전체')
    ledger_type = request.args.get('ledger_type', '')
    view_mode = request.args.get('view_mode', 'default')  # default / manufacture / expiry

    locations, categories = [], []
    try:
        locations, categories = db.query_filter_options()
    except Exception:
        pass

    ledger_rows = []
    if date_from or date_to:
        try:
            from services.stock_service import query_ledger_data

            loc = location if location != '전체' else None
            result = query_ledger_data(
                db, date_from, date_to or '9999-12-31',
                location=loc,
                split_manufacture=(view_mode == 'manufacture'),
                split_expiry=(view_mode == 'expiry'),
            )

            # 수불장 유형 필터 적용
            sorted_keys = result['sorted_keys']
            if ledger_type and ledger_type in LEDGER_CATEGORY_MAP:
                cat_filter = set(LEDGER_CATEGORY_MAP[ledger_type])
                sorted_keys = [k for k in sorted_keys if k[2] in cat_filter]

            prev_dict = result['prev_dict']
            period_groups = result['period_groups']

            # 각 그룹키별로 집계 요약 생성
            for key in sorted_keys:
                opening = prev_dict.get(key, 0)
                txns = period_groups.get(key, [])

                inbound = 0       # 입고 (INBOUND)
                production = 0    # 생산/소분산출 (PRODUCTION, REPACK_IN)
                outbound = 0      # 출고 (SALES_OUT, PROD_OUT, REPACK_OUT)
                transfer = 0      # 이동 (MOVE_IN, MOVE_OUT)

                for tx in txns:
                    t = tx.get('type', '')
                    q = tx.get('qty', 0)
                    if t == 'INBOUND':
                        inbound += q
                    elif t in ('PRODUCTION', 'REPACK_IN'):
                        production += q
                    elif t in ('SALES_OUT', 'PROD_OUT', 'REPACK_OUT'):
                        outbound += abs(q)
                    elif t in ('MOVE_IN', 'MOVE_OUT'):
                        transfer += q
                    elif t == 'INIT':
                        inbound += q

                period_total = sum(tx.get('qty', 0) for tx in txns)
                closing = opening + period_total

                # key = (product_name, location, category, unit, [manufacture_date/expiry_date])
                row_data = {
                    'product_name': key[0],
                    'location': key[1],
                    'category': key[2],
                    'unit': key[3] if len(key) > 3 else '',
                    'opening': opening,
                    'inbound': inbound,
                    'production': production,
                    'outbound': outbound,
                    'transfer': transfer,
                    'closing': closing,
                }
                if view_mode == 'manufacture' and len(key) > 4:
                    row_data['manufacture_date'] = key[4] or '-'
                elif view_mode == 'expiry' and len(key) > 4:
                    row_data['expiry_date'] = key[4] or '-'
                ledger_rows.append(row_data)
        except Exception as e:
            flash(f'수불장 조회 중 오류: {e}', 'danger')

    return render_template('ledger/index.html',
                           ledger=ledger_rows,
                           date_from=date_from, date_to=date_to,
                           location=location, ledger_type=ledger_type,
                           view_mode=view_mode,
                           locations=locations,
                           ledger_types=list(LEDGER_CATEGORY_MAP.keys()),
                           type_labels=INV_TYPE_LABELS)


@ledger_bp.route('/export')
@role_required('admin', 'manager', 'logistics', 'production', 'general')
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


@ledger_bp.route('/pdf')
@role_required('admin', 'manager', 'logistics', 'production', 'general')
def pdf():
    """수불장 PDF 다운로드"""
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    location = request.args.get('location', '전체')
    ledger_type = request.args.get('ledger_type', '')
    view_mode = request.args.get('view_mode', 'default')
    fit_one_page = request.args.get('fit_one_page', '') == '1'

    if not date_to:
        flash('종료일을 입력하세요.', 'warning')
        return redirect(url_for('ledger.index'))

    db = current_app.db

    try:
        from services.stock_service import query_ledger_data
        from models import APPROVAL_LABELS
        from reports.ledger_report import generate_ledger_pdf

        loc = location if location != '전체' else None

        result = query_ledger_data(
            db, date_from, date_to, location=loc,
            split_manufacture=(view_mode == 'manufacture'),
            split_expiry=(view_mode == 'expiry'),
        )

        # 수불장 유형 필터 적용
        if ledger_type and ledger_type in LEDGER_CATEGORY_MAP:
            cat_filter = set(LEDGER_CATEGORY_MAP[ledger_type])
            filtered_keys = [k for k in result['sorted_keys']
                             if k[2] in cat_filter]  # k[2] = category
            filtered_set = set(filtered_keys)
            result['sorted_keys'] = filtered_keys
            result['prev_dict'] = {k: v for k, v in result['prev_dict'].items()
                                    if k in filtered_set}
            result['period_groups'] = {k: v for k, v in result['period_groups'].items()
                                        if k in filtered_set}

        if not result['sorted_keys']:
            flash('PDF로 출력할 수불장 데이터가 없습니다.', 'warning')
            return redirect(url_for('ledger.index', date_from=date_from,
                                     date_to=date_to, location=location,
                                     ledger_type=ledger_type))

        title = ledger_type or '수불장'
        config = {
            'date_from': date_from,
            'date_to': date_to,
            'approvals': {label: '' for label in APPROVAL_LABELS},
            'title': title,
            'include_warnings': False,
            'fit_one_page': fit_one_page,
        }

        tmp = tempfile.NamedTemporaryFile(suffix='.pdf', delete=False)
        tmp_path = tmp.name
        tmp.close()

        try:
            generate_ledger_pdf(
                tmp_path, config,
                result['prev_dict'],
                result['period_groups'],
                result['sorted_keys'],
                result['group_keys'],
            )
            with open(tmp_path, 'rb') as f:
                pdf_bytes = io.BytesIO(f.read())
            fname = f"{title}_{date_from or 'all'}_{date_to}.pdf"
            return send_file(pdf_bytes, mimetype='application/pdf',
                             as_attachment=True, download_name=fname)
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    except Exception as e:
        flash(f'수불장 PDF 생성 중 오류: {e}', 'danger')
        return redirect(url_for('ledger.index', date_from=date_from,
                                 date_to=date_to, location=location,
                                 ledger_type=ledger_type))
