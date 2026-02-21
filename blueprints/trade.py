"""
trade.py — 거래처/거래 관리 Blueprint.
거래처 CRUD, 수동 거래 등록, 거래명세서 PDF.
"""
import os
import io
from datetime import datetime

from flask import (
    Blueprint, render_template, request, current_app,
    flash, redirect, url_for, send_file, abort,
)
from flask_login import login_required, current_user

from auth import role_required, _log_action

trade_bp = Blueprint('trade', __name__, url_prefix='/trade')


# ── 거래처 관리 ──

@trade_bp.route('/')
@role_required('admin', 'manager', 'sales')
def index():
    """거래처 목록"""
    db = current_app.db
    partners = []
    try:
        partners = db.query_partners()
    except Exception as e:
        flash(f'거래처 조회 중 오류: {e}', 'danger')

    return render_template('trade/index.html', partners=partners)


@trade_bp.route('/add', methods=['POST'])
@role_required('admin', 'manager', 'sales')
def add_partner():
    """거래처 등록"""
    partner_name = request.form.get('partner_name', '').strip()
    if not partner_name:
        flash('거래처명을 입력하세요.', 'danger')
        return redirect(url_for('trade.index'))

    payload = {
        'partner_name': partner_name,
        'business_number': request.form.get('business_number', '').strip() or None,
        'representative': request.form.get('representative', '').strip() or None,
        'address': request.form.get('address', '').strip() or None,
        'business_type': request.form.get('business_type', '').strip() or None,
        'business_item': request.form.get('business_item', '').strip() or None,
        'phone': request.form.get('phone', '').strip() or None,
        'fax': request.form.get('fax', '').strip() or None,
        'email': request.form.get('email', '').strip() or None,
        'notes': request.form.get('notes', '').strip() or None,
    }

    try:
        current_app.db.insert_partner(payload)
        _log_action('add_partner', target=partner_name)
        flash(f'거래처 "{partner_name}" 등록 완료', 'success')
    except Exception as e:
        flash(f'거래처 등록 중 오류: {e}', 'danger')

    return redirect(url_for('trade.index'))


@trade_bp.route('/delete/<int:partner_id>', methods=['POST'])
@role_required('admin', 'manager', 'sales')
def delete_partner(partner_id):
    """거래처 삭제"""
    try:
        current_app.db.delete_partner(partner_id)
        _log_action('delete_partner', target=str(partner_id))
        flash('거래처 삭제 완료', 'success')
    except Exception as e:
        flash(f'거래처 삭제 중 오류: {e}', 'danger')

    return redirect(url_for('trade.index'))


# ── 거래 관리 ──

@trade_bp.route('/trades')
@role_required('admin', 'manager', 'sales')
def trades():
    """거래 목록"""
    db = current_app.db

    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    partner_name = request.args.get('partner_name', '전체')

    trade_list = []
    partners = []
    try:
        partners = db.query_partners()
        trade_list = db.query_manual_trades(
            date_from=date_from or None,
            date_to=date_to or None,
            partner_name=partner_name if partner_name != '전체' else None,
        )
    except Exception as e:
        flash(f'거래 조회 중 오류: {e}', 'danger')

    return render_template('trade/trades.html',
                           trades=trade_list, partners=partners,
                           date_from=date_from, date_to=date_to,
                           partner_name=partner_name)


@trade_bp.route('/trades/add', methods=['POST'])
@role_required('admin', 'manager', 'sales')
def add_trade():
    """수동 거래 등록"""
    partner_name = request.form.get('partner_name', '').strip()
    product_name = request.form.get('product_name', '').strip()
    trade_date = request.form.get('trade_date', datetime.now().strftime('%Y-%m-%d'))

    if not partner_name or not product_name:
        flash('거래처명과 품목명을 입력하세요.', 'danger')
        return redirect(url_for('trade.trades'))

    try:
        qty = int(request.form.get('qty', 0))
        unit_price = int(request.form.get('unit_price', 0))
    except (ValueError, TypeError):
        flash('수량과 단가는 숫자로 입력하세요.', 'danger')
        return redirect(url_for('trade.trades'))

    payload = {
        'partner_name': partner_name,
        'product_name': product_name,
        'trade_date': trade_date,
        'trade_type': request.form.get('trade_type', '판매'),
        'qty': qty,
        'unit': request.form.get('unit', '개').strip(),
        'unit_price': unit_price,
        'amount': qty * unit_price,
        'notes': request.form.get('notes', '').strip() or None,
        'registered_by': current_user.username,
    }

    try:
        current_app.db.insert_manual_trade(payload)
        _log_action('add_trade', target=f'{partner_name}/{product_name}')
        flash(f'거래 등록 완료: {partner_name} — {product_name}', 'success')
    except Exception as e:
        flash(f'거래 등록 중 오류: {e}', 'danger')

    return redirect(url_for('trade.trades'))


# ── 거래명세서 PDF ──

@trade_bp.route('/invoice/<int:trade_id>')
@role_required('admin', 'manager', 'sales')
def invoice(trade_id):
    """거래명세서 PDF 생성/다운로드"""
    db = current_app.db

    try:
        # 거래 정보 조회
        trades = db.query_manual_trades()
        trade = next((t for t in trades if t.get('id') == trade_id), None)
        if not trade:
            abort(404)

        # 거래처 정보 조회
        partners = db.query_partners()
        partner = next(
            (p for p in partners if p.get('partner_name') == trade.get('partner_name')),
            None
        )

        # 내 사업장 정보
        my_biz_list = db.query_my_business()
        my_biz = my_biz_list[0] if my_biz_list else {}

        # PDF 생성
        from services.trade_service import generate_invoice_pdf
        output_dir = current_app.config['OUTPUT_FOLDER']
        os.makedirs(output_dir, exist_ok=True)

        fname = f"거래명세서_{trade.get('partner_name', '')}_{trade.get('trade_date', '')}.pdf"
        pdf_path = os.path.join(output_dir, fname)

        generate_invoice_pdf(pdf_path, trade, partner or {}, my_biz)

        return send_file(
            pdf_path,
            mimetype='application/pdf',
            as_attachment=True,
            download_name=fname,
        )
    except Exception as e:
        flash(f'거래명세서 생성 중 오류: {e}', 'danger')
        return redirect(url_for('trade.trades'))
