"""accounting.py -- 회계 대시보드 / 매출-입금 매칭 / 리포트 Blueprint."""
from flask import Blueprint, render_template, request, current_app, flash, redirect, url_for, jsonify, send_file
from flask_login import login_required, current_user
from auth import role_required, _log_action
from services.tz_utils import today_kst, days_ago_kst

accounting_bp = Blueprint('accounting', __name__, url_prefix='/accounting')


@accounting_bp.route('/')
@role_required('admin', 'ceo', 'manager', 'general')
def dashboard():
    """회계 대시보드"""
    db = current_app.db
    from services.bank_service import get_transaction_summary
    from services.matching_service import get_receivables, get_matching_summary, get_payables_summary
    from services.settlement_service import get_settlement_summary

    today = today_kst()
    month_start = today[:7] + '-01'

    # 이번 달 입출금 요약
    summary = get_transaction_summary(db, date_from=month_start, date_to=today)

    # 미수금 현황
    receivables = get_receivables(db)
    total_receivable = sum(r['total_amount'] for r in receivables)

    # 미지급금 현황
    payables_summary = get_payables_summary(db, date_from=month_start, date_to=today)

    # 매칭 현황
    match_summary = get_matching_summary(db, date_from=month_start, date_to=today)

    # 이번 달 매출/매입 세금계산서 합계
    invoices = db.query_tax_invoices(date_from=month_start, date_to=today)
    sales_total = sum(i.get('total_amount', 0) for i in invoices if i.get('direction') == 'sales')
    purchase_total = sum(i.get('total_amount', 0) for i in invoices if i.get('direction') == 'purchase')

    # 플랫폼 정산 현황
    settlement_summary = get_settlement_summary(db, date_from=month_start, date_to=today)
    settlement_total = settlement_summary.get('total_net', 0)
    platform_fee_total = settlement_summary.get('total_fee', 0)

    return render_template('accounting/dashboard.html',
                           summary=summary,
                           receivables=receivables,
                           total_receivable=total_receivable,
                           payables_summary=payables_summary,
                           match_summary=match_summary,
                           sales_total=sales_total,
                           purchase_total=purchase_total,
                           settlement_total=settlement_total,
                           platform_fee_total=platform_fee_total,
                           settlement_summary=settlement_summary,
                           month_start=month_start,
                           today=today)


@accounting_bp.route('/matching')
@role_required('admin', 'manager', 'general')
def matching():
    """매출-입금 매칭"""
    db = current_app.db
    date_from = request.args.get('date_from', days_ago_kst(30))
    date_to = request.args.get('date_to', today_kst())

    unmatched_invoices = db.query_tax_invoices(
        direction='sales', unmatched_only=True,
        date_from=date_from, date_to=date_to,
    )
    unmatched_deposits = db.query_bank_transactions(
        transaction_type='입금', unmatched_only=True,
        date_from=date_from, date_to=date_to,
    )
    matches = db.query_payment_matches(date_from=date_from, date_to=date_to)

    return render_template('accounting/matching.html',
                           invoices=unmatched_invoices,
                           deposits=unmatched_deposits,
                           matches=matches,
                           date_from=date_from, date_to=date_to)


@accounting_bp.route('/matching/auto', methods=['POST'])
@role_required('admin', 'manager')
def auto_match():
    """자동 매칭 실행"""
    from services.matching_service import auto_match_invoices, confirm_match
    date_from = request.form.get('date_from', days_ago_kst(30))
    date_to = request.form.get('date_to', today_kst())

    try:
        result = auto_match_invoices(current_app.db, date_from, date_to)
        for c in result['candidates']:
            confirm_match(current_app.db, c['invoice_id'], c['transaction_id'],
                          matched_by=current_user.username)
        _log_action('auto_match',
                    detail=f'{result["matched_count"]}건 매칭')
        flash(f'자동 매칭 {result["matched_count"]}건 완료', 'success')
    except Exception as e:
        flash(f'자동 매칭 오류: {e}', 'danger')

    return redirect(url_for('accounting.matching',
                            date_from=date_from, date_to=date_to))


@accounting_bp.route('/matching/manual', methods=['POST'])
@role_required('admin', 'manager', 'general')
def manual_match_action():
    """수동 매칭"""
    from services.matching_service import manual_match
    invoice_id = request.form.get('invoice_id', type=int)
    transaction_id = request.form.get('transaction_id', type=int)

    if not invoice_id or not transaction_id:
        flash('세금계산서와 입금 거래를 선택하세요.', 'danger')
        return redirect(url_for('accounting.matching'))

    try:
        manual_match(current_app.db, invoice_id, transaction_id,
                     matched_by=current_user.username)
        _log_action('manual_match',
                    detail=f'세금계산서 {invoice_id} ↔ 거래 {transaction_id}')
        flash('수동 매칭 완료', 'success')
    except Exception as e:
        flash(f'매칭 오류: {e}', 'danger')

    return redirect(url_for('accounting.matching'))


@accounting_bp.route('/matching/unmatch/<int:match_id>', methods=['POST'])
@role_required('admin', 'manager')
def unmatch_action(match_id):
    """매칭 해제"""
    from services.matching_service import unmatch
    try:
        unmatch(current_app.db, match_id)
        _log_action('unmatch',
                    detail=f'매칭 {match_id} 해제')
        flash('매칭이 해제되었습니다.', 'success')
    except Exception as e:
        flash(f'매칭 해제 오류: {e}', 'danger')
    return redirect(url_for('accounting.matching'))


@accounting_bp.route('/receivables')
@role_required('admin', 'ceo', 'manager', 'general')
def receivables():
    """미수금 관리"""
    from services.matching_service import get_receivables
    items = get_receivables(current_app.db)
    total = sum(r['total_amount'] for r in items)
    return render_template('accounting/receivables.html',
                           receivables=items, total=total)


@accounting_bp.route('/payables')
@role_required('admin', 'ceo', 'manager', 'general')
def payables():
    """미지급금 관리"""
    from services.matching_service import get_payables
    date_from = request.args.get('date_from', days_ago_kst(90))
    date_to = request.args.get('date_to', today_kst())

    items = get_payables(current_app.db, date_from=date_from, date_to=date_to)
    total_unpaid = sum(p['unpaid_amount'] for p in items)
    total_paid = sum(p['paid_amount'] for p in items)

    return render_template('accounting/payables.html',
                           payables=items,
                           total_unpaid=total_unpaid,
                           total_paid=total_paid,
                           date_from=date_from,
                           date_to=date_to)


@accounting_bp.route('/payables/auto-match', methods=['POST'])
@role_required('admin', 'manager')
def auto_match_payables():
    """매입-출금 자동 매칭 실행"""
    from services.matching_service import auto_match_payables as _auto_match, confirm_payable_match
    date_from = request.form.get('date_from', days_ago_kst(90))
    date_to = request.form.get('date_to', today_kst())

    try:
        result = _auto_match(current_app.db, date_from, date_to)
        for c in result['candidates']:
            confirm_payable_match(current_app.db, c['invoice_id'], c['transaction_id'],
                                  matched_by=current_user.username)
        _log_action('auto_match_payables',
                    detail=f'{result["matched_count"]}건 지급 매칭')
        flash(f'자동 지급 매칭 {result["matched_count"]}건 완료', 'success')
    except Exception as e:
        flash(f'자동 매칭 오류: {e}', 'danger')

    return redirect(url_for('accounting.payables',
                            date_from=date_from, date_to=date_to))


@accounting_bp.route('/settlements')
@role_required('admin', 'ceo', 'manager', 'general')
def settlements():
    """플랫폼 정산 관리"""
    from services.settlement_service import get_settlement_summary, CHANNEL_DISPLAY
    date_from = request.args.get('date_from', days_ago_kst(30))
    date_to = request.args.get('date_to', today_kst())
    selected_channel = request.args.get('channel', '')

    db = current_app.db
    settlement_list = db.query_platform_settlements(
        channel=selected_channel or None,
        date_from=date_from, date_to=date_to,
    )
    summary = get_settlement_summary(db, date_from=date_from, date_to=date_to)

    return render_template('accounting/settlements.html',
                           settlements=settlement_list,
                           summary=summary,
                           channels=CHANNEL_DISPLAY,
                           selected_channel=selected_channel,
                           date_from=date_from,
                           date_to=date_to)


@accounting_bp.route('/settlements/sync', methods=['POST'])
@role_required('admin', 'manager')
def sync_settlements():
    """플랫폼 정산 동기화 (order_transactions → platform_settlements)"""
    from services.settlement_service import sync_all_channels
    date_from = request.form.get('date_from', days_ago_kst(30))
    date_to = request.form.get('date_to', today_kst())

    try:
        results = sync_all_channels(current_app.db, date_from, date_to)
        total = sum(r.get('created_count', 0) for r in results if 'error' not in r)
        errors = [r for r in results if 'error' in r]

        _log_action('sync_settlements',
                    detail=f'{total}건 동기화, 오류 {len(errors)}건')

        if errors:
            error_channels = ', '.join(r['channel'] for r in errors)
            flash(f'정산 동기화 완료 ({total}건). 일부 채널 오류: {error_channels}', 'warning')
        else:
            flash(f'정산 동기화 {total}건 완료', 'success')
    except Exception as e:
        flash(f'정산 동기화 오류: {e}', 'danger')

    return redirect(url_for('accounting.settlements',
                            date_from=date_from, date_to=date_to))


@accounting_bp.route('/settlements/auto-match', methods=['POST'])
@role_required('admin', 'manager')
def auto_match_settlements_action():
    """정산금-입금 자동 매칭 실행"""
    from services.matching_service import auto_match_settlements, confirm_settlement_match
    date_from = request.form.get('date_from', days_ago_kst(30))
    date_to = request.form.get('date_to', today_kst())

    try:
        result = auto_match_settlements(current_app.db, date_from, date_to)
        for c in result['candidates']:
            confirm_settlement_match(current_app.db, c['settlement_id'], c['transaction_id'],
                                     matched_by=current_user.username)
        _log_action('auto_match_settlements',
                    detail=f'{result["matched_count"]}건 정산 매칭')
        flash(f'자동 정산 매칭 {result["matched_count"]}건 완료', 'success')
    except Exception as e:
        flash(f'자동 매칭 오류: {e}', 'danger')

    return redirect(url_for('accounting.settlements',
                            date_from=date_from, date_to=date_to))


@accounting_bp.route('/api/dashboard-data')
@role_required('admin', 'ceo', 'manager', 'general')
def api_dashboard_data():
    """대시보드 데이터 JSON (차트 갱신용)"""
    db = current_app.db
    from services.bank_service import get_transaction_summary
    from services.matching_service import get_receivables

    today = today_kst()
    month_start = today[:7] + '-01'

    summary = get_transaction_summary(db, date_from=month_start, date_to=today)
    receivables = get_receivables(db)

    return jsonify({
        'summary': summary,
        'total_receivable': sum(r['total_amount'] for r in receivables),
        'receivable_count': len(receivables),
    })


# ═══════════════════════════════════════════════════════════════
#  세무사 전달용 리포트
# ═══════════════════════════════════════════════════════════════

@accounting_bp.route('/reports')
@role_required('admin', 'ceo', 'manager', 'general')
def reports():
    """리포트 페이지"""
    from services.report_service import generate_monthly_summary

    # 기본값: 이번 달
    month = request.args.get('month', today_kst()[:7])

    try:
        summary = generate_monthly_summary(current_app.db, month)
    except Exception as e:
        flash(f'리포트 생성 오류: {e}', 'danger')
        summary = None

    return render_template('accounting/reports.html',
                           month=month, summary=summary)


@accounting_bp.route('/api/reports/monthly-summary')
@role_required('admin', 'ceo', 'manager', 'general')
def api_monthly_summary():
    """월간 요약 JSON"""
    from services.report_service import generate_monthly_summary

    month = request.args.get('month', today_kst()[:7])
    try:
        summary = generate_monthly_summary(current_app.db, month)
        return jsonify(summary)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@accounting_bp.route('/api/reports/tax-invoices')
@role_required('admin', 'ceo', 'manager', 'general')
def api_export_tax_invoices():
    """세금계산서 엑셀 다운로드"""
    from services.report_service import export_tax_invoices_excel

    month = request.args.get('month', today_kst()[:7])
    direction = request.args.get('direction', 'sales')

    if direction not in ('sales', 'purchase'):
        return jsonify({'error': 'direction은 sales 또는 purchase'}), 400

    try:
        output = export_tax_invoices_excel(current_app.db, month, direction)
        direction_label = '매출' if direction == 'sales' else '매입'
        filename = f'{month}_{direction_label}_세금계산서.xlsx'

        _log_action('export_report',
                    detail=f'{month} {direction_label} 세금계산서 엑셀 다운로드')

        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename,
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@accounting_bp.route('/api/reports/bank-summary')
@role_required('admin', 'ceo', 'manager', 'general')
def api_export_bank_summary():
    """은행 거래내역 월간 요약 엑셀 다운로드"""
    from services.report_service import export_bank_summary_excel

    month = request.args.get('month', today_kst()[:7])

    try:
        output = export_bank_summary_excel(current_app.db, month)
        filename = f'{month}_은행거래내역.xlsx'

        _log_action('export_report',
                    detail=f'{month} 은행 거래내역 엑셀 다운로드')

        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename,
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500
