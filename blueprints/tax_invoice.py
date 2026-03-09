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

    # 취소건 제외한 합계
    sales_total = sum(i.get('total_amount', 0) for i in invoices
                      if i.get('direction') == 'sales' and i.get('status') != 'cancelled')
    purchase_total = sum(i.get('total_amount', 0) for i in invoices
                         if i.get('direction') == 'purchase' and i.get('status') != 'cancelled')

    # 팝빌 상태는 AJAX로 비동기 로드 (블로킹 방지)
    show_popbill_status = current_user.role in ('admin', 'manager')

    return render_template('tax_invoice/index.html',
                           invoices=invoices,
                           sales_total=sales_total,
                           purchase_total=purchase_total,
                           direction=direction,
                           date_from=date_from, date_to=date_to,
                           show_popbill_status=show_popbill_status)


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
        tax_type = request.form.get('tax_type', '과세')
        items_json = request.form.get('items', '[]')
        items = json.loads(items_json)

        if not items:
            flash('품목을 추가하세요.', 'danger')
            return redirect(url_for('tax_invoice.issue'))

        invoice_data = build_invoice_from_trade(db, partner_id, trade_date, items, tax_type=tax_type)

        # 팝빌 발행 시도
        popbill_result = None
        cert_error = False
        if current_app.popbill.is_ready:
            try:
                popbill_result = current_app.popbill.issue_sales_invoice(invoice_data)
            except Exception as e:
                if current_app.popbill.is_cert_error(e):
                    cert_error = True
                    flash('팝빌 인증서가 등록되지 않았습니다. '
                          '팝빌 사이트에서 공동인증서를 등록한 후 다시 시도하세요. '
                          '(DB에는 임시 저장됩니다)', 'warning')
                else:
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
        elif cert_error:
            pass  # 위에서 이미 안내함
        elif not current_app.popbill.is_ready:
            flash('세금계산서 저장 완료 (팝빌 미연동 — SecretKey 설정 필요)', 'info')
        else:
            flash('세금계산서 DB 저장 완료 (팝빌 발행 실패 — 수동 발행 필요)', 'info')

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
    """팝빌에서 세금계산서 동기화 (매출+매입)"""
    try:
        from services.tax_invoice_service import sync_all_tax_invoices

        # 동기화 기간 (기본: 3개월)
        months = int(request.form.get('months', 3))
        end_date = today_kst().replace('-', '')
        start_date = days_ago_kst(months * 30).replace('-', '')

        results = sync_all_tax_invoices(
            current_app.db, current_app.popbill,
            start_date=start_date, end_date=end_date,
        )

        sell = results.get('sell', {})
        buy = results.get('buy', {})

        if sell.get('error') or buy.get('error'):
            errors = []
            if sell.get('error'):
                errors.append(f"매출: {sell['error']}")
            if buy.get('error'):
                errors.append(f"매입: {buy['error']}")
            flash(f'동기화 오류: {"; ".join(errors)}', 'danger')
        else:
            sell_new = sell.get('new_count', 0)
            buy_new = buy.get('new_count', 0)
            sell_total = sell.get('total_fetched', 0)
            buy_total = buy.get('total_fetched', 0)
            flash(
                f'세금계산서 동기화 완료: '
                f'매출 {sell_total}건 중 신규 {sell_new}건, '
                f'매입 {buy_total}건 중 신규 {buy_new}건',
                'success'
            )

        _log_action('sync_tax_invoices',
                    detail=f'매출 신규 {sell.get("new_count", 0)}건, '
                           f'매입 신규 {buy.get("new_count", 0)}건')

    except Exception as e:
        flash(f'세금계산서 동기화 오류: {e}', 'danger')

    return redirect(url_for('tax_invoice.index'))


@tax_invoice_bp.route('/sync-sell', methods=['POST'])
@role_required('admin', 'manager')
def sync_sell():
    """팝빌에서 매출 세금계산서만 동기화"""
    try:
        from services.tax_invoice_service import sync_tax_invoices

        months = int(request.form.get('months', 3))
        end_date = today_kst().replace('-', '')
        start_date = days_ago_kst(months * 30).replace('-', '')

        result = sync_tax_invoices(
            current_app.db, current_app.popbill,
            start_date, end_date, direction='SELL',
        )

        flash(
            f'매출 세금계산서 동기화 완료: '
            f'{result["total_fetched"]}건 중 신규 {result["new_count"]}건',
            'success'
        )
        _log_action('sync_sell_invoices', detail=f'신규 {result["new_count"]}건')

    except Exception as e:
        flash(f'매출 동기화 오류: {e}', 'danger')

    return redirect(url_for('tax_invoice.index'))


@tax_invoice_bp.route('/sync-buy', methods=['POST'])
@role_required('admin', 'manager')
def sync_buy():
    """팝빌에서 매입 세금계산서만 동기화"""
    try:
        from services.tax_invoice_service import sync_tax_invoices

        months = int(request.form.get('months', 3))
        end_date = today_kst().replace('-', '')
        start_date = days_ago_kst(months * 30).replace('-', '')

        result = sync_tax_invoices(
            current_app.db, current_app.popbill,
            start_date, end_date, direction='BUY',
        )

        flash(
            f'매입 세금계산서 동기화 완료: '
            f'{result["total_fetched"]}건 중 신규 {result["new_count"]}건',
            'success'
        )
        _log_action('sync_buy_invoices', detail=f'신규 {result["new_count"]}건')

    except Exception as e:
        flash(f'매입 동기화 오류: {e}', 'danger')

    return redirect(url_for('tax_invoice.index'))


@tax_invoice_bp.route('/popbill-join', methods=['POST'])
@role_required('admin')
def popbill_join():
    """팝빌 연동회원 가입 (API) — 폼에서 사업장 정보 입력받아 가입."""
    popbill = current_app.popbill

    if not popbill.is_ready:
        flash('팝빌 SDK가 초기화되지 않았습니다.', 'danger')
        return redirect(url_for('tax_invoice.index'))

    corp_num = popbill.corp_num
    result = popbill.join_member(
        corp_num=corp_num,
        corp_name=request.form.get('corp_name', '').strip() or '사업자',
        ceo_name=request.form.get('ceo_name', '').strip() or '대표자',
        addr=request.form.get('addr', '').strip() or '-',
        biz_type=request.form.get('biz_type', '').strip() or '도소매',
        biz_class=request.form.get('biz_class', '').strip() or '식품',
        contact_name=request.form.get('contact_name', '').strip() or '',
        contact_tel=request.form.get('contact_tel', '').strip() or '',
        contact_email=request.form.get('contact_email', '').strip() or '',
    )

    if result['success']:
        flash('팝빌 연동회원 가입 완료! 이제 인증서를 등록하세요.', 'success')
        _log_action('popbill_join', detail=f'사업자 {corp_num} 팝빌 연동회원 가입')
    else:
        flash(f'팝빌 회원가입 결과: {result["message"]}', 'warning')

    return redirect(url_for('tax_invoice.index'))


@tax_invoice_bp.route('/api/popbill-status')
@role_required('admin', 'manager')
def api_popbill_status():
    """팝빌 연동 상태 JSON"""
    status = current_app.popbill.get_status_summary()
    return jsonify(status)


@tax_invoice_bp.route('/api/popbill-cert-url')
@role_required('admin', 'manager')
def api_popbill_cert_url():
    """팝빌 인증서 등록 팝업 URL 발급 (getTaxCertURL)."""
    popbill = current_app.popbill
    if not popbill.is_ready:
        return jsonify({'error': 'Popbill SDK 미초기화'}), 400

    url = popbill.get_tax_cert_url()
    if url:
        return jsonify({'url': url})

    # getTaxCertURL 실패 시 getPopbillURL(CERT) 폴백
    url = popbill.get_cert_url()
    if url:
        return jsonify({'url': url})

    return jsonify({'error': '인증서 등록 URL 발급 실패. 팝빌 사이트에서 직접 등록하세요.'}), 500


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
