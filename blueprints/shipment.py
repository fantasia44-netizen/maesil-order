"""
shipment.py — 출고관리 Blueprint.
stock_ledger SALES_OUT 데이터 조회, 엑셀 다운로드.
"""
import io
from flask import (
    Blueprint, render_template, request, current_app,
    flash, send_file,
)
from flask_login import login_required

from auth import role_required

shipment_bp = Blueprint('shipment', __name__, url_prefix='/shipment')


@shipment_bp.route('/')
@role_required('admin', 'ceo', 'manager', 'sales', 'logistics', 'general')
def index():
    """출고 내역 조회 (SALES_OUT 기반)"""
    db = current_app.db

    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    location = request.args.get('location', '전체')
    product_filter = request.args.get('product', '').strip()

    locations = []
    try:
        locs, _ = db.query_filter_options()
        locations = locs
    except Exception:
        pass

    rows = []
    stats = {'total_items': 0, 'total_qty': 0, 'total_count': 0}

    if date_from or date_to:
        try:
            effective_to = date_to or date_from
            effective_from = date_from or date_to

            data = db.query_stock_ledger(
                date_to=effective_to,
                date_from=effective_from,
                location=location if location != '전체' else None,
                type_list=["SALES_OUT"],
                order_desc=True,
            )

            for r in data:
                qty = r.get('qty', 0)
                rows.append({
                    'id': r.get('id'),
                    'transaction_date': r.get('transaction_date', ''),
                    'product_name': r.get('product_name', ''),
                    'qty': abs(qty),
                    'unit': r.get('unit', '개') or '개',
                    'location': r.get('location', ''),
                    'category': r.get('category', '') or '',
                    'channel': r.get('channel', '') or '',
                    'memo': r.get('memo', '') or '',
                    'lot_number': r.get('lot_number', '') or '',
                    'expiry_date': r.get('expiry_date', '') or '',
                })

            # 품목명 필터
            if product_filter:
                pf = product_filter.lower()
                rows = [r for r in rows if pf in r.get('product_name', '').lower()]

            stats = {
                'total_items': len(set(r['product_name'] for r in rows)),
                'total_qty': sum(r['qty'] for r in rows),
                'total_count': len(rows),
            }
        except Exception as e:
            flash(f'출고 조회 중 오류: {e}', 'danger')

    return render_template('shipment/index.html',
                           date_from=date_from, date_to=date_to,
                           location=location,
                           product_filter=product_filter,
                           locations=locations,
                           rows=rows, stats=stats)


@shipment_bp.route('/export')
@role_required('admin', 'ceo', 'manager', 'sales', 'logistics', 'general')
def export():
    """출고 데이터 엑셀 다운로드"""
    import pandas as pd

    db = current_app.db
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    location = request.args.get('location', '전체')

    try:
        effective_to = date_to or date_from
        effective_from = date_from or date_to

        if not effective_from:
            flash('기간을 선택하세요.', 'warning')
            from flask import redirect, url_for
            return redirect(url_for('shipment.index'))

        data = db.query_stock_ledger(
            date_to=effective_to,
            date_from=effective_from,
            location=location if location != '전체' else None,
            type_list=["SALES_OUT"],
            order_desc=True,
        )

        if not data:
            flash('다운로드할 데이터가 없습니다.', 'warning')
            from flask import redirect, url_for
            return redirect(url_for('shipment.index'))

        export_rows = []
        for r in data:
            export_rows.append({
                '출고일자': r.get('transaction_date', ''),
                '품목명': r.get('product_name', ''),
                '수량': abs(r.get('qty', 0)),
                '단위': r.get('unit', '개') or '개',
                '창고': r.get('location', ''),
                '종류': r.get('category', '') or '',
                '채널': r.get('channel', '') or '',
                '비고': r.get('memo', '') or '',
            })

        df = pd.DataFrame(export_rows)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='출고내역')
        output.seek(0)

        fname = f"출고내역_{date_from or 'all'}_{date_to or 'all'}.xlsx"
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=fname,
        )
    except Exception as e:
        flash(f'출고 다운로드 중 오류: {e}', 'danger')
        from flask import redirect, url_for
        return redirect(url_for('shipment.index'))
