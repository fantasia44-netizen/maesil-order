"""
stock.py — 재고 현황 조회 Blueprint.
"""
import os
from flask import Blueprint, render_template, request, current_app, jsonify
from flask_login import login_required
from auth import role_required

stock_bp = Blueprint('stock', __name__, url_prefix='/stock')


@stock_bp.route('/')
@role_required('admin', 'manager', 'logistics')
def index():
    """재고 현황 조회"""
    date_str = request.args.get('date', '')
    location = request.args.get('location', '전체')

    db = current_app.db
    locations, categories = [], []
    try:
        locations, categories = db.query_filter_options()
    except Exception:
        pass

    snapshot = {}
    stats = {'total_items': 0, 'total_qty': 0}

    if date_str:
        try:
            from services.stock_service import get_stock_snapshot, get_stats
            snapshot = get_stock_snapshot(db, date_str, location)
            stats = get_stats(db, date_str, location)
        except Exception as e:
            from flask import flash
            flash(f'재고 조회 중 오류: {e}', 'danger')

    return render_template('stock/index.html',
                           date_str=date_str, location=location,
                           locations=locations, categories=categories,
                           snapshot=snapshot, stats=stats)
