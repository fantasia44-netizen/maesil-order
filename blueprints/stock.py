"""
stock.py — 재고 현황 조회 Blueprint.
"""
import os
import io
import tempfile

from flask import (
    Blueprint, render_template, request, current_app,
    flash, redirect, url_for, send_file,
)
from flask_login import login_required

from auth import role_required
from services.storage_helper import backup_bytes_to_storage

stock_bp = Blueprint('stock', __name__, url_prefix='/stock')


@stock_bp.route('/')
@role_required('admin', 'ceo', 'manager', 'sales', 'logistics', 'production', 'general')
def index():
    """재고 현황 조회"""
    date_str = request.args.get('date', '')
    location = request.args.get('location', '전체')
    category = request.args.get('category', '전체')
    food_type_filter = request.args.get('food_type', '전체')
    view_mode = request.args.get('view_mode', '기본')
    product_filter = request.args.get('product', '').strip()

    db = current_app.db
    locations, categories = [], []
    try:
        locations, categories = db.query_filter_options()
    except Exception:
        pass

    rows = []
    stats = {'total_items': 0, 'total_qty': 0}

    if date_str:
        try:
            from services.stock_service import query_stock_snapshot
            split_manufacture = (view_mode == '제조일분리')
            split_expiry = (view_mode == '소비기한분리')
            split_lot_number = (view_mode == '이력번호분리')

            rows = query_stock_snapshot(
                db, date_str,
                location=location,
                category=category,
                food_type=food_type_filter,
                split_manufacture=split_manufacture,
                split_expiry=split_expiry,
                split_lot_number=split_lot_number,
            )

            # 품목명 필터
            if product_filter:
                pf = product_filter.lower()
                rows = [r for r in rows if pf in r.get('product_name', '').lower()]

            stats = {
                'total_items': len(set(r['product_name'] for r in rows)),
                'total_qty': sum(r['qty'] for r in rows),
            }
        except Exception as e:
            flash(f'재고 조회 중 오류: {e}', 'danger')

    return render_template('stock/index.html',
                           date_str=date_str, location=location,
                           category=category, view_mode=view_mode,
                           food_type_filter=food_type_filter,
                           product_filter=product_filter,
                           locations=locations, categories=categories,
                           rows=rows, stats=stats)


@stock_bp.route('/pdf')
@role_required('admin', 'ceo', 'manager', 'sales', 'logistics', 'production', 'general')
def pdf():
    """재고현황 PDF 다운로드"""
    import pandas as pd

    date_str = request.args.get('date', '')
    location = request.args.get('location', '전체')
    category = request.args.get('category', '전체')
    food_type_filter = request.args.get('food_type', '전체')
    view_mode = request.args.get('view_mode', '기본')
    product_filter = request.args.get('product', '').strip()

    if not date_str:
        flash('기준일을 입력하세요.', 'warning')
        return redirect(url_for('stock.index'))

    db = current_app.db

    try:
        from services.stock_service import query_stock_snapshot
        from models import APPROVAL_LABELS
        from reports.snapshot_report import generate_stock_snapshot_pdf

        split_manufacture = (view_mode == '제조일분리')
        split_expiry = (view_mode == '소비기한분리')

        rows = query_stock_snapshot(
            db, date_str,
            location=location,
            category=category,
            food_type=food_type_filter,
            split_manufacture=split_manufacture,
            split_expiry=split_expiry,
        )

        # 품목명 필터
        if product_filter:
            pf = product_filter.lower()
            rows = [r for r in rows if pf in r.get('product_name', '').lower()]

        if not rows:
            flash('PDF로 출력할 데이터가 없습니다.', 'warning')
            return redirect(url_for('stock.index', date=date_str,
                                     location=location, category=category,
                                     view_mode=view_mode))

        df = pd.DataFrame(rows)
        # PDF에 필요한 컬럼 보완
        for col in ['origin', 'manufacture_date', 'expiry_date', 'storage_method']:
            if col not in df.columns:
                df[col] = ''

        config = {
            'target_date': date_str,
            'approvals': {label: '' for label in APPROVAL_LABELS},
            'title': '재고현황',
            'include_warnings': False,
        }

        tmp = tempfile.NamedTemporaryFile(suffix='.pdf', delete=False)
        tmp_path = tmp.name
        tmp.close()

        try:
            generate_stock_snapshot_pdf(tmp_path, config, df)
            with open(tmp_path, 'rb') as f:
                pdf_bytes = io.BytesIO(f.read())
            fname = f"재고현황_{date_str}.pdf"
            backup_bytes_to_storage(current_app.db, pdf_bytes.getvalue(), fname, 'report', 'stock')
            return send_file(pdf_bytes, mimetype='application/pdf',
                             as_attachment=True, download_name=fname)
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    except Exception as e:
        flash(f'PDF 생성 중 오류: {e}', 'danger')
        return redirect(url_for('stock.index', date=date_str,
                                 location=location, category=category,
                                 view_mode=view_mode))


@stock_bp.route('/excel')
@role_required('admin', 'ceo', 'manager', 'sales', 'logistics', 'production', 'general')
def excel():
    """재고현황 엑셀 다운로드"""
    import pandas as pd
    from openpyxl.utils import get_column_letter

    date_str = request.args.get('date', '')
    location = request.args.get('location', '전체')
    category = request.args.get('category', '전체')
    food_type_filter = request.args.get('food_type', '전체')
    view_mode = request.args.get('view_mode', '기본')
    product_filter = request.args.get('product', '').strip()

    if not date_str:
        flash('기준일을 입력하세요.', 'warning')
        return redirect(url_for('stock.index'))

    db = current_app.db

    try:
        from services.stock_service import query_stock_snapshot

        split_manufacture = (view_mode == '제조일분리')
        split_expiry = (view_mode == '소비기한분리')

        rows = query_stock_snapshot(
            db, date_str,
            location=location,
            category=category,
            food_type=food_type_filter,
            split_manufacture=split_manufacture,
            split_expiry=split_expiry,
        )

        # 품목명 필터
        if product_filter:
            pf = product_filter.lower()
            rows = [r for r in rows if pf in r.get('product_name', '').lower()]

        if not rows:
            flash('엑셀로 출력할 데이터가 없습니다.', 'warning')
            return redirect(url_for('stock.index', date=date_str,
                                     location=location, category=category,
                                     view_mode=view_mode))

        df = pd.DataFrame(rows)

        # 컬럼 한글 매핑
        col_map = {
            'product_name': '품목명',
            'category': '카테고리',
            'food_type': '식품유형',
            'qty': '수량',
            'unit': '단위',
            'location': '위치',
            'storage_method': '보관방법',
        }
        if split_expiry:
            col_map['expiry_date'] = '소비기한'
        if split_manufacture:
            col_map['manufacture_date'] = '제조일'

        # 필요한 컬럼만 선택 + 한글 이름
        use_cols = [c for c in col_map if c in df.columns]
        df_out = df[use_cols].rename(columns=col_map)

        # 엑셀 생성
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine='openpyxl') as writer:
            df_out.to_excel(writer, index=False, sheet_name='재고현황')

            # 열 너비 자동 조정
            ws = writer.sheets['재고현황']
            for i, col_name in enumerate(df_out.columns, 1):
                max_len = max(
                    len(str(col_name)) * 2,  # 한글은 2배
                    df_out.iloc[:, i - 1].astype(str).str.len().max() if len(df_out) > 0 else 0
                )
                ws.column_dimensions[get_column_letter(i)].width = min(max_len + 4, 40)

        buf.seek(0)
        fname = f"재고현황_{date_str}.xlsx"
        backup_bytes_to_storage(current_app.db, buf.getvalue(), fname, 'output', 'stock')
        return send_file(buf, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                         as_attachment=True, download_name=fname)

    except Exception as e:
        flash(f'엑셀 생성 중 오류: {e}', 'danger')
        return redirect(url_for('stock.index', date=date_str,
                                 location=location, category=category,
                                 view_mode=view_mode))
