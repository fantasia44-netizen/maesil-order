"""
dashboard.py — 대시보드 Blueprint.
KPI, 매출 차트, 채널 분포, 재고 현황, 최근 활동 표시.
AJAX lazy load: 페이지 골격 즉시 표시, 데이터는 /api/dashboard 로 비동기 로드.
"""
from flask import Blueprint, render_template, jsonify, request, current_app
from flask_login import login_required, current_user
from datetime import datetime

from services.dashboard_service import (
    get_dashboard_data, get_revenue_chart_data, get_channel_chart_data,
)
from db_utils import get_db

dashboard_bp = Blueprint('main', __name__)


@dashboard_bp.route('/')
@login_required
def dashboard():
    """대시보드 메인 페이지 — 데이터 없이 즉시 렌더링."""
    # 승인 대기 (캐시된 count 사용)
    pending_users = 0
    if current_user.is_admin():
        try:
            pending_users = get_db().count_pending_users()
        except Exception:
            pass

    return render_template('dashboard.html', pending_users=pending_users)


@dashboard_bp.route('/api/dashboard')
@login_required
def api_dashboard():
    """AJAX 대시보드 데이터 (캐시 5분)."""
    try:
        date = request.args.get('date')
        force = request.args.get('refresh') == '1'
        data = get_dashboard_data(get_db(), date=date, force_refresh=force)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@dashboard_bp.route('/api/dashboard/revenue-chart')
@login_required
def api_revenue_chart():
    """매출 차트 데이터."""
    days = request.args.get('days', 30, type=int)
    try:
        data = get_revenue_chart_data(get_db(), days=days)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@dashboard_bp.route('/api/dashboard/channel-chart')
@login_required
def api_channel_chart():
    """채널 분포 데이터."""
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')
    try:
        data = get_channel_chart_data(get_db(),
                                       date_from=date_from, date_to=date_to)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
