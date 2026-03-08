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
        # 팝빌 취소 시도 (실패해도 DB 취소는 진행)
        popbill_ok = True
        if current_app.popbill.is_ready and invoice.get('mgt_key'):
            try:
                current_app.popbill.cancel_issue(invoice['mgt_key'])
            except Exception as pe:
                popbill_ok = False
                flash(f'팝빌 취소 실패 (DB에서는 취소 처리합니다): {pe}', 'warning')

        db.update_tax_invoice(invoice_id, {'status': 'cancelled'})
        _log_action('cancel_tax_invoice',
                    detail=f'ID={invoice_id}')
        if popbill_ok:
            flash('세금계산서가 취소되었습니다.', 'success')
        else:
            flash('세금계산서가 DB에서 취소되었습니다.', 'info')
    except Exception as e:
        flash(f'취소 오류: {e}', 'danger')

    return redirect(url_for('tax_invoice.index'))


@tax_invoice_bp.route('/sync', methods=['POST'])
@role_required('admin', 'manager')
def sync():
    """팝빌에서 세금계산서 가져오기 (매출+매입)."""
    db = current_app.db
    popbill = current_app.popbill

    if not popbill.is_ready:
        flash('팝빌 연동이 설정되지 않았습니다. 키를 확인하세요.', 'danger')
        return redirect(url_for('tax_invoice.index'))

    date_from = request.form.get('date_from', '')
    date_to = request.form.get('date_to', '')

    if not date_from or not date_to:
        flash('동기화 기간을 선택하세요.', 'warning')
        return redirect(url_for('tax_invoice.index'))

    # YYYYMMDD 형식으로 변환
    s_date = date_from.replace('-', '')
    e_date = date_to.replace('-', '')

    total_synced = 0

    for direction, dir_label in [('SELL', 'sales'), ('BUY', 'purchase')]:
        try:
            result = popbill.search_invoices(
                direction=direction,
                start_date=s_date,
                end_date=e_date,
                page=1, per_page=500,
            )
            items = getattr(result, 'list', []) if result else []

            for item in items:
                inv_num = getattr(item, 'invoiceNum', '') or ''
                mgt_key = getattr(item, 'invoicerMgtKey', '') if direction == 'SELL' \
                    else getattr(item, 'invoiceeMgtKey', '')
                mgt_key = mgt_key or ''

                # 중복 체크 (승인번호 또는 관리번호)
                existing = db.query_tax_invoices(direction=dir_label)
                is_dup = any(
                    (inv_num and e.get('invoice_number') == inv_num) or
                    (mgt_key and e.get('mgt_key') == mgt_key)
                    for e in existing
                )
                if is_dup:
                    continue

                write_date = getattr(item, 'writeDate', '')
                if write_date and len(write_date) == 8:
                    write_date = f"{write_date[:4]}-{write_date[4:6]}-{write_date[6:8]}"

                issue_date = getattr(item, 'issueDate', '')
                if issue_date and len(issue_date) == 8:
                    issue_date = f"{issue_date[:4]}-{issue_date[4:6]}-{issue_date[6:8]}"

                supply = int(getattr(item, 'supplyCostTotal', '0') or 0)
                tax = int(getattr(item, 'taxTotal', '0') or 0)
                total = int(getattr(item, 'totalAmount', '0') or 0)

                payload = {
                    'direction': dir_label,
                    'invoice_number': inv_num,
                    'mgt_key': mgt_key,
                    'write_date': write_date or today_kst(),
                    'issue_date': issue_date or None,
                    'tax_type': getattr(item, 'taxType', '과세') or '과세',
                    'supplier_corp_num': getattr(item, 'invoicerCorpNum', '') or '',
                    'supplier_corp_name': getattr(item, 'invoicerCorpName', '') or '',
                    'supplier_ceo_name': getattr(item, 'invoicerCEOName', '') or '',
                    'buyer_corp_num': getattr(item, 'invoiceeCorpNum', '') or '',
                    'buyer_corp_name': getattr(item, 'invoiceeCorpName', '') or '',
                    'buyer_ceo_name': getattr(item, 'invoiceeCEOName', '') or '',
                    'supply_cost_total': supply,
                    'tax_total': tax,
                    'total_amount': total,
                    'status': 'issued',
                    'memo': '팝빌 동기화',
                    'registered_by': current_user.username,
                }
                db.insert_tax_invoice(payload)
                total_synced += 1

        except Exception as e:
            flash(f'팝빌 {direction} 동기화 오류: {e}', 'danger')

    if total_synced > 0:
        _log_action('sync_tax_invoices', detail=f'{total_synced}건 동기화')
        flash(f'팝빌 동기화 완료: {total_synced}건 가져옴', 'success')
    else:
        flash('새로 가져올 세금계산서가 없습니다.', 'info')

    return redirect(url_for('tax_invoice.index',
                            direction=request.args.get('direction', '전체'),
                            date_from=date_from, date_to=date_to))


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
