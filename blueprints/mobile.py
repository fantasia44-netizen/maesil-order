"""
mobile.py — 모바일 전용 조회 Blueprint.
재고현황, 매출현황, 거래처목록, 이력조회 (읽기 전용).
"""
from flask import (
    Blueprint, render_template, request, current_app, flash,
)
from flask_login import login_required, current_user

from models import INV_TYPE_LABELS, REVENUE_CATEGORIES

mobile_bp = Blueprint('mobile', __name__, url_prefix='/m')


@mobile_bp.route('/')
@login_required
def home():
    """모바일 홈"""
    return render_template('mobile/home.html')


@mobile_bp.route('/stock')
@login_required
def stock():
    """모바일 재고현황"""
    date_str = request.args.get('date', '')
    location = request.args.get('location', '전체')

    db = current_app.db
    locations = []
    try:
        locations, _ = db.query_filter_options()
    except Exception:
        pass

    rows = []
    stats = {'total_items': 0, 'total_qty': 0}

    if date_str:
        try:
            from services.stock_service import query_stock_snapshot
            rows = query_stock_snapshot(
                db, date_str,
                location=location,
            )
            stats = {
                'total_items': len(set(r['product_name'] for r in rows)),
                'total_qty': sum(r['qty'] for r in rows),
            }
        except Exception as e:
            flash(f'재고 조회 중 오류: {e}', 'danger')

    return render_template('mobile/stock.html',
                           date_str=date_str, location=location,
                           locations=locations, rows=rows, stats=stats)


@mobile_bp.route('/revenue')
@login_required
def revenue():
    """모바일 매출현황"""
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    category = request.args.get('category', '전체')

    db = current_app.db
    revenues = []
    total_revenue = 0

    if date_from or date_to or category != '전체':
        try:
            revenues = db.query_revenue(
                date_from=date_from or None,
                date_to=date_to or None,
                category=category if category != '전체' else None,
            )
            total_revenue = sum(r.get('revenue', 0) for r in revenues)
        except Exception as e:
            flash(f'매출 조회 중 오류: {e}', 'danger')

    return render_template('mobile/revenue.html',
                           revenues=revenues, total_revenue=total_revenue,
                           date_from=date_from, date_to=date_to,
                           category=category, categories=REVENUE_CATEGORIES)


@mobile_bp.route('/partners')
@login_required
def partners():
    """모바일 거래처목록"""
    db = current_app.db
    q = request.args.get('q', '').strip()

    partners_list = []
    try:
        partners_list = db.query_partners()
        if q:
            search_term = q.replace(' ', '').lower()
            partners_list = [
                p for p in partners_list
                if search_term in str(p.get('partner_name', '')).replace(' ', '').lower()
            ]
    except Exception as e:
        flash(f'거래처 조회 중 오류: {e}', 'danger')

    return render_template('mobile/partners.html',
                           partners=partners_list, q=q)


@mobile_bp.route('/history')
@login_required
def history():
    """모바일 이력조회"""
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    location = request.args.get('location', '전체')
    record_type = request.args.get('type', '')
    product_name = request.args.get('product_name', '')

    db = current_app.db
    locations = []
    try:
        locations, _ = db.query_filter_options()
    except Exception:
        pass

    results = []
    searched = False

    if date_from or date_to or product_name:
        searched = True
        try:
            type_list = [record_type] if record_type else None
            raw = db.query_stock_ledger(
                date_to=date_to or '9999-12-31',
                date_from=date_from or None,
                location=location if location != '전체' else None,
                type_list=type_list,
                order_desc=True,
            )

            # 품목명 필터
            if product_name:
                search_term = product_name.replace(' ', '').lower()
                raw = [r for r in raw
                       if search_term in str(r.get('product_name', '')).replace(' ', '').lower()]

            results = raw[:300]  # 모바일은 300건 제한
        except Exception as e:
            flash(f'이력 조회 중 오류: {e}', 'danger')

    return render_template('mobile/history.html',
                           date_from=date_from, date_to=date_to,
                           location=location, record_type=record_type,
                           product_name=product_name,
                           locations=locations,
                           type_labels=INV_TYPE_LABELS,
                           records=results, searched=searched)
