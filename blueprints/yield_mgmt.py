"""
yield_mgmt.py — 수율 관리 Blueprint.
생산 수율 분석 + 실제원가 비교 + 일별 추이 차트.
관리자/총괄책임자/생산담당 전용.
"""
from flask import (
    Blueprint, render_template, request, current_app, jsonify,
)
from flask_login import login_required, current_user

from auth import role_required

yield_bp = Blueprint('yield_mgmt', __name__, url_prefix='/yield')


@yield_bp.route('/')
@role_required('admin', 'manager', 'production')
def index():
    """수율 관리 메인 페이지"""
    locations = []
    try:
        locations, _ = current_app.db.query_filter_options()
    except Exception:
        pass
    return render_template('yield/index.html', locations=locations)


@yield_bp.route('/api/summary')
@role_required('admin', 'manager', 'production')
def api_summary():
    """제품별 수율 요약 JSON"""
    date_from = request.args.get('from', '')
    date_to = request.args.get('to', '')
    location = request.args.get('location', '')

    if not date_from or not date_to:
        return jsonify({'error': '기간을 선택하세요.'}), 400

    try:
        from services.yield_service import calculate_yield_summary
        result = calculate_yield_summary(
            current_app.db, date_from, date_to,
            location=location or None)
        return jsonify({'success': True, **result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@yield_bp.route('/api/daily')
@role_required('admin', 'manager', 'production')
def api_daily():
    """일별 수율 추이 JSON (차트용)"""
    date_from = request.args.get('from', '')
    date_to = request.args.get('to', '')
    product = request.args.get('product', '')
    location = request.args.get('location', '')

    if not date_from or not date_to:
        return jsonify({'error': '기간을 선택하세요.'}), 400

    period = request.args.get('period', 'day')

    try:
        from services.yield_service import calculate_daily_yield
        result = calculate_daily_yield(
            current_app.db, date_from, date_to,
            product_name=product or None,
            location=location or None,
            period=period)
        return jsonify({'success': True, **result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
