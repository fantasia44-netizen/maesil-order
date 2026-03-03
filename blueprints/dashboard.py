"""
dashboard.py — 대시보드 Blueprint.
KPI, 매출 차트, 채널 분포, 재고 현황, 최근 활동 표시.
"""
from flask import Blueprint, render_template, jsonify, request, current_app
from flask_login import login_required, current_user
from datetime import datetime

from services.dashboard_service import (
    get_dashboard_data, get_revenue_chart_data, get_channel_chart_data,
)

dashboard_bp = Blueprint('main', __name__)


@dashboard_bp.route('/')
@login_required
def dashboard():
    """대시보드 메인 페이지."""
    try:
        data = get_dashboard_data(current_app.db)
    except Exception as e:
        print(f"[Dashboard] data load error: {e}")
        data = {"kpi": {}, "revenue_trend": [], "channel_breakdown": [],
                "warehouse_stock": [], "top_products": [], "recent_activity": []}

    # 승인 대기 사용자 (관리자용)
    pending_users = 0
    if current_user.is_admin():
        try:
            users = current_app.db.query_all_users()
            pending_users = sum(1 for u in users if not u.get('is_approved'))
        except Exception:
            pass

    return render_template('dashboard.html', data=data, pending_users=pending_users)


@dashboard_bp.route('/api/dashboard')
@login_required
def api_dashboard():
    """AJAX 새로고침용 JSON API."""
    try:
        date = request.args.get('date')
        data = get_dashboard_data(current_app.db, date=date)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@dashboard_bp.route('/api/dashboard/revenue-chart')
@login_required
def api_revenue_chart():
    """매출 차트 데이터."""
    days = request.args.get('days', 30, type=int)
    try:
        data = get_revenue_chart_data(current_app.db, days=days)
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
        data = get_channel_chart_data(current_app.db,
                                       date_from=date_from, date_to=date_to)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
