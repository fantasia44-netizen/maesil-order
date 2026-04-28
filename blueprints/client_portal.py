"""
client_portal.py — 플로워 화주사 포털 Blueprint
역할: client
"""
from datetime import datetime
from flask import Blueprint, render_template, request, jsonify, send_file
from flask_login import login_required, current_user
from functools import wraps

from db_utils import get_db

client_bp = Blueprint('client_portal', __name__, url_prefix='/client')


def client_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated:
            from flask import redirect, url_for
            return redirect(url_for('auth.login'))
        if current_user.role not in ('client', 'operator', 'admin', 'ceo'):
            from flask import redirect, url_for
            return redirect(url_for('auth.login'))
        return f(*args, **kwargs)
    return decorated


def get_client_id():
    if current_user.role == 'client':
        return getattr(current_user, 'client_id', None)
    return None


# ── 대시보드 ──────────────────────────────────────────────────
@client_bp.route('/')
@client_bp.route('/dashboard')
@login_required
@client_required
def dashboard():
    db = get_db()
    client_id = get_client_id()
    try:
        filters = {'client_id': client_id} if client_id else {}
        total_skus  = db.count('skus',   filters=filters) or 0
        pending_ord = db.count('orders', filters={**filters, 'status': 'pending'}) or 0
        skus        = db.select('skus',  filters=filters, order='total_qty', limit=5) or []
        orders      = db.select('orders', filters=filters, order='created_at desc', limit=10) or []
        month       = datetime.now().strftime('%Y-%m')
        billing     = db.select('billing', filters={**filters, 'year_month': month}, single=True) or {}
        total_qty   = sum(s.get('total_qty', 0) or 0 for s in skus)
        client_name = ''
        if client_id:
            c = db.select('clients', filters={'id': client_id}, single=True)
            client_name = (c or {}).get('company_name', '')
    except Exception:
        total_skus = pending_ord = total_qty = 0
        skus = orders = []
        billing = {}
        client_name = ''

    return render_template('client/dashboard.html',
        client_name=client_name,
        total_skus=total_skus,
        pending_orders=pending_ord,
        total_qty=total_qty,
        top_skus=skus,
        recent_orders=orders,
        this_month_billing=billing,
        current_month=datetime.now().strftime('%Y-%m'),
    )


# ── 내 재고 ───────────────────────────────────────────────────
@client_bp.route('/inventory')
@login_required
@client_required
def inventory():
    db = get_db()
    client_id = get_client_id()
    search  = request.args.get('search', '')
    storage = request.args.get('storage', '')
    try:
        filters = {'client_id': client_id} if client_id else {}
        if storage: filters['storage_temp'] = storage
        rows     = db.select('skus', filters=filters, order='name') or []
        sku_cnt  = len(rows)
        total    = sum(r.get('total_qty', 0) or 0 for r in rows)
        c = (db.select('clients', filters={'id': client_id}, single=True) or {}) if client_id else {}
        client_name = c.get('company_name', '')
    except Exception:
        rows = []
        sku_cnt = total = 0
        client_name = ''

    return render_template('client/inventory.html',
        skus=rows,
        sku_count=sku_cnt, total_qty=total,
        client_name=client_name,
        filter_search=search, filter_storage=storage,
    )


@client_bp.route('/inventory/export')
@login_required
@client_required
def inventory_export():
    return jsonify({'ok': False, 'error': '준비 중'})


# ── 주문 현황 ─────────────────────────────────────────────────
@client_bp.route('/orders')
@login_required
@client_required
def orders():
    db = get_db()
    client_id = get_client_id()
    status    = request.args.get('status', '')
    search    = request.args.get('search', '')
    date_from = request.args.get('date_from', '')
    date_to   = request.args.get('date_to', '')
    page      = request.args.get('page', 1, type=int)
    try:
        base_filters = {'client_id': client_id} if client_id else {}
        filters = {**base_filters}
        if status: filters['status'] = status
        rows  = db.select('orders', filters=filters, order='created_at desc', limit=50, offset=(page-1)*50) or []
        total = db.count('orders', filters=filters) or 0
        counts = {'total': total}
    except Exception:
        rows = []
        total = 0
        counts = {}

    return render_template('client/orders.html',
        orders=rows,
        total_count=total, counts=counts,
        filter_status=status, filter_search=search,
        filter_date_from=date_from, filter_date_to=date_to,
    )


# ── 과금 내역 ─────────────────────────────────────────────────
@client_bp.route('/billing')
@login_required
@client_required
def billing():
    db = get_db()
    client_id = get_client_id()
    month = request.args.get('month', datetime.now().strftime('%Y-%m'))
    try:
        filters  = {'client_id': client_id} if client_id else {}
        bill     = db.select('billing', filters={**filters, 'year_month': month}, single=True) or {}
        history  = db.select('billing', filters=filters, order='year_month desc', limit=24) or []
        details  = db.select('billing_details', filters={**filters, 'year_month': month}, order='date') or []
        c = (db.select('clients', filters={'id': client_id}, single=True) or {}) if client_id else {}
        client_name = c.get('company_name', '')
    except Exception:
        bill = {}
        history = details = []
        client_name = ''

    return render_template('client/billing.html',
        billing=bill,
        monthly_history=history,
        billing_details=details,
        client_name=client_name,
        filter_month=month,
        current_month=datetime.now().strftime('%Y-%m'),
    )


@client_bp.route('/billing/export')
@login_required
@client_required
def billing_export():
    return jsonify({'ok': False, 'error': '준비 중'})
