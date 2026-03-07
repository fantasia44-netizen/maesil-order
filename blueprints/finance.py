"""
finance.py -- 재무 관리 Blueprint.
비용 등록/수정/삭제, 반복 비용 자동 생성, 카테고리별 합계.
관리 손익표(P&L): 월별/채널별 손익 분석.
CEO 재무현황 대시보드, 세무사 전달용 리포트.
관리자/총괄책임자/CEO 전용.
"""
from flask import (
    Blueprint, render_template, request, current_app,
    jsonify, send_file,
)
from flask_login import login_required, current_user

from auth import role_required, _log_action

finance_bp = Blueprint('finance', __name__, url_prefix='/finance')


@finance_bp.route('/expenses')
@role_required('admin', 'manager')
def expenses():
    """비용 관리 메인 페이지"""
    return render_template('finance/expenses.html')


@finance_bp.route('/api/expenses')
@role_required('admin', 'manager')
def api_expenses():
    """비용 목록 JSON API (월별/카테고리 필터)"""
    db = current_app.db
    month = request.args.get('month', '')
    category = request.args.get('category', '')

    try:
        rows = db.query_expenses(
            month=month or None,
            category=category or None,
        )
        categories = db.query_expense_categories()

        # 카테고리별 합계
        cat_totals = {}
        grand_total = 0
        for r in rows:
            cat = r.get('category', '기타')
            amt = float(r.get('amount', 0))
            cat_totals[cat] = cat_totals.get(cat, 0) + amt
            grand_total += amt

        # 전월 데이터 조회 (증감 비교용)
        prev_total = 0
        prev_cat_totals = {}
        if month:
            parts = month.split('-')
            year, mon = int(parts[0]), int(parts[1])
            if mon == 1:
                prev_month = f"{year - 1}-12"
            else:
                prev_month = f"{year}-{mon - 1:02d}"
            prev_rows = db.query_expenses(month=prev_month)
            for r in prev_rows:
                cat = r.get('category', '기타')
                amt = float(r.get('amount', 0))
                prev_cat_totals[cat] = prev_cat_totals.get(cat, 0) + amt
                prev_total += amt

        return jsonify({
            'success': True,
            'expenses': rows,
            'categories': categories,
            'cat_totals': cat_totals,
            'grand_total': grand_total,
            'prev_total': prev_total,
            'prev_cat_totals': prev_cat_totals,
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@finance_bp.route('/api/expenses', methods=['POST'])
@role_required('admin', 'manager')
def api_create_expense():
    """비용 1건 등록"""
    db = current_app.db
    data = request.get_json()
    if not data:
        return jsonify({'error': '데이터가 없습니다.'}), 400

    expense_date = (data.get('expense_date') or '').strip()
    category = (data.get('category') or '').strip()
    amount = data.get('amount', 0)

    if not expense_date or not category:
        return jsonify({'error': '날짜와 카테고리는 필수입니다.'}), 400

    try:
        amount = float(amount)
    except (ValueError, TypeError):
        return jsonify({'error': '금액이 올바르지 않습니다.'}), 400

    # expense_month 자동 생성 (YYYY-MM)
    expense_month = expense_date[:7]

    payload = {
        'expense_date': expense_date,
        'expense_month': expense_month,
        'category': category,
        'subcategory': (data.get('subcategory') or '').strip(),
        'amount': amount,
        'is_recurring': bool(data.get('is_recurring', False)),
        'tax_invoice_id': (data.get('tax_invoice_id') or '').strip() or None,
        'memo': (data.get('memo') or '').strip(),
        'registered_by': current_user.username,
    }

    try:
        result = db.insert_expense(payload)
        _log_action('create_expense', target=category,
                     detail=f'날짜={expense_date}, 금액={amount:,.0f}',
                     new_value=payload)
        return jsonify({'success': True, 'expense': result})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@finance_bp.route('/api/expenses/<int:expense_id>', methods=['PUT'])
@role_required('admin', 'manager')
def api_update_expense(expense_id):
    """비용 1건 수정"""
    db = current_app.db
    data = request.get_json()
    if not data:
        return jsonify({'error': '데이터가 없습니다.'}), 400

    expense_date = (data.get('expense_date') or '').strip()
    category = (data.get('category') or '').strip()
    amount = data.get('amount', 0)

    if not expense_date or not category:
        return jsonify({'error': '날짜와 카테고리는 필수입니다.'}), 400

    try:
        amount = float(amount)
    except (ValueError, TypeError):
        return jsonify({'error': '금액이 올바르지 않습니다.'}), 400

    expense_month = expense_date[:7]

    payload = {
        'expense_date': expense_date,
        'expense_month': expense_month,
        'category': category,
        'subcategory': (data.get('subcategory') or '').strip(),
        'amount': amount,
        'is_recurring': bool(data.get('is_recurring', False)),
        'tax_invoice_id': (data.get('tax_invoice_id') or '').strip() or None,
        'memo': (data.get('memo') or '').strip(),
    }

    try:
        result = db.update_expense(expense_id, payload)
        _log_action('update_expense', target=f'{category} (id={expense_id})',
                     detail=f'날짜={expense_date}, 금액={amount:,.0f}',
                     new_value=payload)
        return jsonify({'success': True, 'expense': result})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@finance_bp.route('/api/expenses/<int:expense_id>', methods=['DELETE'])
@role_required('admin')
def api_delete_expense(expense_id):
    """비용 1건 삭제 -- 관리자만"""
    db = current_app.db
    try:
        # 삭제 전 데이터 조회
        rows = db.query_expenses()
        old_data = next((r for r in rows if r.get('id') == expense_id), None)

        db.delete_expense(expense_id)
        _log_action('delete_expense', target=f'id={expense_id}',
                     old_value=old_data)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@finance_bp.route('/api/expenses/generate-recurring', methods=['POST'])
@role_required('admin', 'manager')
def api_generate_recurring():
    """반복 비용 자동 생성"""
    db = current_app.db
    data = request.get_json() or {}
    target_month = (data.get('target_month') or '').strip()

    if not target_month:
        return jsonify({'error': '대상 월을 지정해주세요.'}), 400

    try:
        count = db.generate_recurring_expenses(target_month)
        _log_action('generate_recurring_expenses',
                     detail=f'대상월={target_month}, 생성={count}건')
        return jsonify({'success': True, 'count': count})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ══════════════════════════════════════════════
#  관리 손익표 (P&L)
# ══════════════════════════════════════════════

@finance_bp.route('/pnl')
@role_required('admin', 'ceo', 'manager')
def pnl():
    """관리 손익표 메인 페이지"""
    return render_template('finance/pnl.html')


@finance_bp.route('/api/pnl')
@role_required('admin', 'ceo', 'manager')
def api_pnl():
    """월별 손익표 JSON API.
    Query params: month=2026-03 (기본: 현재월)
    """
    from services.pnl_service import calculate_monthly_pnl
    from services.tz_utils import today_kst

    db = current_app.db
    month = request.args.get('month', '')

    if not month:
        today = today_kst()
        month = today[:7]  # YYYY-MM

    try:
        result = calculate_monthly_pnl(db, month)
        return jsonify({'success': True, 'pnl': result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@finance_bp.route('/api/pnl/trend')
@role_required('admin', 'ceo', 'manager')
def api_pnl_trend():
    """손익 추이 JSON API (최근 N개월).
    Query params: months=6
    """
    from services.pnl_service import calculate_pnl_trend

    db = current_app.db
    months = request.args.get('months', 6, type=int)
    months = max(2, min(months, 12))  # 2~12 범위

    try:
        result = calculate_pnl_trend(db, months=months)
        return jsonify({'success': True, 'trend': result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@finance_bp.route('/api/pnl/by-channel')
@role_required('admin', 'ceo', 'manager')
def api_pnl_by_channel():
    """채널별 손익 JSON API.
    Query params: month=2026-03
    """
    from services.pnl_service import calculate_channel_pnl
    from services.tz_utils import today_kst

    db = current_app.db
    month = request.args.get('month', '')

    if not month:
        today = today_kst()
        month = today[:7]

    try:
        result = calculate_channel_pnl(db, month)
        return jsonify({'success': True, 'channel_pnl': result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ══════════════════════════════════════════════
#  CEO 재무현황 대시보드
# ══════════════════════════════════════════════

@finance_bp.route('/dashboard')
@role_required('admin', 'ceo', 'manager')
def dashboard():
    """CEO 재무현황 대시보드 메인 페이지"""
    return render_template('finance/dashboard.html')


@finance_bp.route('/api/ceo-summary')
@role_required('admin', 'ceo', 'manager')
def api_ceo_summary():
    """CEO 재무현황 요약 JSON API.
    Query params: month=2026-03 (기본: 현재월)
    """
    from services.financial_report_service import get_ceo_financial_summary
    from services.tz_utils import today_kst

    db = current_app.db
    month = request.args.get('month', '')

    if not month:
        today = today_kst()
        month = today[:7]

    try:
        result = get_ceo_financial_summary(db, month)
        return jsonify({'success': True, 'summary': result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@finance_bp.route('/api/tax-report/download')
@role_required('admin', 'ceo', 'manager')
def api_tax_report_download():
    """세무사 전달용 월간 엑셀 리포트 다운로드.
    Query params: month=2026-03
    """
    from services.financial_report_service import generate_tax_report
    from services.tz_utils import today_kst

    db = current_app.db
    month = request.args.get('month', '')

    if not month:
        today = today_kst()
        month = today[:7]

    try:
        buf = generate_tax_report(db, month)
        fname = f"세무리포트_{month}.xlsx"
        _log_action('download_tax_report',
                     detail=f'월={month}')
        return send_file(
            buf,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=fname,
        )
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
