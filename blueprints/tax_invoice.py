"""tax_invoice.py -- 세금계산서 관리 Blueprint."""
import json
from flask import Blueprint, render_template, request, current_app, flash, redirect, url_for, jsonify
from flask_login import login_required, current_user
from auth import role_required, _log_action
from services.tz_utils import today_kst, days_ago_kst

tax_invoice_bp = Blueprint('tax_invoice', __name__, url_prefix='/tax-invoice')


@tax_invoice_bp.route('/')
@role_required('admin', 'ceo', 'manager', 'general')
def index():
    """세금계산서 목록"""
    db = current_app.db
    direction = request.args.get('direction', '전체')
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')

    invoices = db.query_tax_invoices(
        direction={'매출': 'sales', '매입': 'purchase'}.get(direction),
        date_from=date_from or None,
        date_to=date_to or None,
    )

    sales_total = sum(i.get('total_amount', 0) for i in invoices if i.get('direction') == 'sales')
    purchase_total = sum(i.get('total_amount', 0) for i in invoices if i.get('direction') == 'purchase')

    return render_template('tax_invoice/index.html',
                           invoices=invoices,
                           sales_total=sales_total,
                           purchase_total=purchase_total,
                           direction=direction,
                           date_from=date_from, date_to=date_to)


@tax_invoice_bp.route('/issue', methods=['GET', 'POST'])
@role_required('admin', 'manager', 'general')
def issue():
    """세금계산서 발행"""
    db = current_app.db

    if request.method == 'GET':
        partners = db.query_partners()
        my_biz = db.query_default_business()
        return render_template('tax_invoice/issue.html',
                               partners=partners, my_biz=my_biz)

    # POST: 발행 처리
    try:
        from services.tax_invoice_service import build_invoice_from_trade, save_invoice_to_db

        partner_id = int(request.form.get('partner_id', 0))
        trade_date = request.form.get('write_date', today_kst())
        items_json = request.form.get('items', '[]')
        items = json.loads(items_json)

        if not items:
            flash('품목을 추가하세요.', 'danger')
            return redirect(url_for('tax_invoice.issue'))

        invoice_data = build_invoice_from_trade(db, partner_id, trade_date, items)

        # 팝빌 발행 시도
        popbill_result = None
        if current_app.popbill.is_ready:
            try:
                popbill_result = current_app.popbill.issue_sales_invoice(invoice_data)
            except Exception as e:
                flash(f'팝빌 발행 오류 (DB에는 저장합니다): {e}', 'warning')

        # DB 저장
        invoice_id = save_invoice_to_db(
            db, invoice_data, 'sales', popbill_result,
            registered_by=current_user.username,
        )

        _log_action('issue_tax_invoice',
                    detail=f'ID={invoice_id}, 금액={invoice_data.get("total_amount", 0):,}')

        nts = popbill_result.get('nts_confirm_num', '') if popbill_result else ''
        if nts:
            flash(f'세금계산서 발행 완료 (승인번호: {nts})', 'success')
        else:
            flash(f'세금계산서 저장 완료 (팝빌 미연동 — 키 발급 후 자동 발행됩니다)', 'info')

    except Exception as e:
        flash(f'세금계산서 발행 오류: {e}', 'danger')

    return redirect(url_for('tax_invoice.index'))


@tax_invoice_bp.route('/detail/<int:invoice_id>')
@role_required('admin', 'ceo', 'manager', 'general')
def detail(invoice_id):
    """세금계산서 상세"""
    invoice = current_app.db.query_tax_invoice_by_id(invoice_id)
    if not invoice:
        flash('세금계산서를 찾을 수 없습니다.', 'danger')
        return redirect(url_for('tax_invoice.index'))
    return render_template('tax_invoice/detail.html', invoice=invoice)


@tax_invoice_bp.route('/cancel/<int:invoice_id>', methods=['POST'])
@role_required('admin', 'manager')
def cancel(invoice_id):
    """세금계산서 발행 취소"""
    db = current_app.db
    invoice = db.query_tax_invoice_by_id(invoice_id)
    if not invoice:
        flash('세금계산서를 찾을 수 없습니다.', 'danger')
        return redirect(url_for('tax_invoice.index'))

    try:
        # 팝빌 취소
        if current_app.popbill.is_ready and invoice.get('mgt_key'):
            current_app.popbill.cancel_issue(invoice['mgt_key'])

        db.update_tax_invoice(invoice_id, {'status': 'cancelled'})
        _log_action('cancel_tax_invoice',
                    detail=f'ID={invoice_id}')
        flash('세금계산서가 취소되었습니다.', 'success')
    except Exception as e:
        flash(f'취소 오류: {e}', 'danger')

    return redirect(url_for('tax_invoice.index'))


@tax_invoice_bp.route('/api/partners')
@login_required
def api_partners():
    """거래처 목록 JSON (자동완성용)"""
    partners = current_app.db.query_partners()
    return jsonify([{
        'id': p['id'],
        'name': p.get('partner_name', ''),
        'business_number': p.get('business_number', ''),
        'representative': p.get('representative', ''),
    } for p in partners])
