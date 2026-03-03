"""
mobile.py — 모바일 전용 조회 Blueprint.
재고현황, 매출현황, 거래처목록, 이력조회 (읽기 전용).
"""
from flask import (
    Blueprint, render_template, request, current_app, flash, redirect, url_for,
)
from flask_login import login_required, current_user

from models import INV_TYPE_LABELS, REVENUE_CATEGORIES
from auth import role_required

mobile_bp = Blueprint('mobile', __name__, url_prefix='/m')


@mobile_bp.route('/')
@login_required
def home():
    """모바일 홈 — CEO는 대시보드로 자동 이동"""
    if current_user.role == 'ceo':
        return redirect(url_for('mobile.ceo_dashboard'))
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


@mobile_bp.route('/ceo')
@role_required('admin', 'ceo')
def ceo_dashboard():
    """CEO 모바일 대시보드 — 매출 그래프 + KPI"""
    from db_supabase import today_kst, days_ago_kst

    db = current_app.db
    today = today_kst()

    # 오늘 매출 KPI
    today_rev = db.sum_revenue_by_date(today)

    # 이번 달 시작일
    month_start = today[:8] + '01'

    # 월 매출 (order_transactions 합산)
    month_channel = db.query_orders_by_channel(date_from=month_start, date_to=today)
    month_total = sum(c.get('amount', 0) for c in month_channel)
    month_count = sum(c.get('count', 0) for c in month_channel)

    # 30일 매출 추이
    revenue_trend = db.query_revenue_trend(days=30)

    # TOP 10 상품
    top_products = db.query_top_products_by_revenue(days=30, limit=10)

    # 재고 요약
    stock_summary = db.query_stock_summary_by_location()
    total_stock_items = sum(s.get('product_count', 0) for s in stock_summary)

    return render_template('mobile/ceo_dashboard.html',
                           today=today,
                           today_rev=today_rev,
                           month_total=month_total,
                           month_count=month_count,
                           month_channel=month_channel,
                           revenue_trend=revenue_trend,
                           top_products=top_products,
                           stock_summary=stock_summary,
                           total_stock_items=total_stock_items)
