"""
operator.py — 플로워 운영자 포털 Blueprint
역할: operator (3PL운영자), admin
"""
from datetime import datetime, date
from flask import Blueprint, render_template, request, jsonify, redirect, url_for
from flask_login import login_required, current_user
from functools import wraps

from db_utils import get_db

operator_bp = Blueprint('operator', __name__, url_prefix='/operator')


def operator_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated:
            return redirect(url_for('auth.login'))
        if current_user.role not in ('operator', 'admin', 'ceo', 'manager'):
            return redirect(url_for('auth.login'))
        return f(*args, **kwargs)
    return decorated


# ── 대시보드 ──────────────────────────────────────────────────
@operator_bp.route('/dashboard')
@login_required
@operator_required
def dashboard():
    db = get_db()
    try:
        pending_orders = db.count('orders', filters={'status': 'pending'}) or 0
        today_shipped  = db.count('orders', filters={'status': 'shipped'}) or 0
        total_orders   = db.count('orders') or 0
        total_skus     = db.count('skus') or 0
        client_count   = db.count('clients') or 0
        skus           = db.select('skus', order='total_qty', limit=100) or []
        low_stock      = [s for s in skus if 0 < (s.get('total_qty') or 0) <= 10]
        low_stock_count = len(low_stock) + len([s for s in skus if (s.get('total_qty') or 0) == 0])
        expiring_count = 0
        order_counts   = {'confirmed': 0, 'packing': 0}
        recent_orders  = db.select('orders', order='created_at desc', limit=10) or []
        billing_rows   = db.select('billing', filters={'year_month': datetime.now().strftime('%Y-%m')}) or []
        client_revenue = {r.get('client_name', ''): r.get('total', 0) for r in billing_rows if r.get('total')}
        monthly_total  = sum(r.get('total', 0) for r in billing_rows)
        recent_orders  = db.select('orders', order='created_at desc', limit=10) or []
    except Exception:
        pending_orders = today_shipped = total_orders = total_skus = client_count = 0
        low_stock_count = expiring_count = monthly_total = 0
        low_stock = recent_orders = []
        client_revenue = {}
        order_counts = {'confirmed': 0, 'packing': 0}

    return render_template('operator/dashboard.html',
        now=datetime.now(),
        pending_orders=pending_orders,
        today_shipped=today_shipped,
        total_orders=total_orders,
        total_skus=total_skus,
        client_count=client_count,
        monthly_total=monthly_total,
        low_stock=low_stock,
        low_stock_count=low_stock_count,
        expiring_count=expiring_count,
        order_counts=order_counts,
        recent_orders=recent_orders,
        client_revenue=client_revenue,
        inout_labels=[],
        inout_in=[],
        inout_out=[],
    )


# ── 화주사 관리 ───────────────────────────────────────────────
@operator_bp.route('/clients')
@login_required
@operator_required
def clients():
    db = get_db()
    try:
        rows = db.select('clients', order='company_name') or []
    except Exception:
        rows = []
    return render_template('operator/clients.html', clients=rows)


@operator_bp.route('/clients/create', methods=['POST'])
@login_required
@operator_required
def clients_create():
    db = get_db()
    data = {
        'company_name': request.form.get('company_name', '').strip(),
        'contact_name': request.form.get('contact_name', '').strip(),
        'contact_phone': request.form.get('contact_phone', '').strip(),
        'contact_email': request.form.get('contact_email', '').strip(),
        'address': request.form.get('address', '').strip(),
        'is_active': True,
    }
    try:
        db.insert('clients', data)
    except Exception as e:
        pass
    return redirect(url_for('operator.clients'))


# ── SKU 관리 ──────────────────────────────────────────────────
@operator_bp.route('/skus')
@login_required
@operator_required
def skus():
    db = get_db()
    page   = request.args.get('page', 1, type=int)
    search = request.args.get('search', '')
    storage = request.args.get('storage', '')
    alert  = request.args.get('stock_alert', '')
    client_id = request.args.get('client_id', '')

    try:
        filters = {}
        if storage:  filters['storage_temp'] = storage
        if client_id: filters['client_id'] = client_id
        rows = db.select('skus', filters=filters, order='name', limit=50, offset=(page-1)*50) or []
        clients = db.select('clients', order='company_name') or []
        total = db.count('skus', filters=filters) or 0
    except Exception:
        rows = clients = []
        total = 0

    return render_template('operator/skus.html',
        skus=rows, clients=clients,
        total_count=total,
        current_page=page, total_pages=max(1, -(-total//50)),
        filter_search=search, filter_storage=storage,
        filter_stock_alert=alert, filter_client=client_id,
    )


@operator_bp.route('/skus/create', methods=['POST'])
@login_required
@operator_required
def skus_create():
    db = get_db()
    data = {
        'client_id': request.form.get('client_id'),
        'sku_code': request.form.get('sku_code', '').strip(),
        'barcode': request.form.get('barcode', '').strip() or None,
        'name': request.form.get('name', '').strip(),
        'category': request.form.get('category', '').strip() or None,
        'storage_temp': request.form.get('storage_temp', 'room'),
        'unit': request.form.get('unit', '').strip() or None,
        'units_per_box': request.form.get('units_per_box', 1, type=int),
        'weight_g': request.form.get('weight_g', type=int),
        'shelf_life_days': request.form.get('shelf_life_days', type=int),
    }
    try:
        db.insert('skus', data)
    except Exception:
        pass
    return redirect(url_for('operator.skus'))


@operator_bp.route('/skus/<int:sku_id>/delete', methods=['POST'])
@login_required
@operator_required
def skus_delete(sku_id):
    db = get_db()
    try:
        qty = db.select('skus', filters={'id': sku_id}, single=True) or {}
        if (qty.get('total_qty') or 0) > 0:
            return jsonify({'ok': False, 'error': '재고가 있는 SKU는 삭제할 수 없습니다'})
        db.delete('skus', filters={'id': sku_id})
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})


# ── 재고 현황 ─────────────────────────────────────────────────
@operator_bp.route('/inventory')
@login_required
@operator_required
def inventory():
    db = get_db()
    search    = request.args.get('search', '')
    storage   = request.args.get('storage', '')
    client_id = request.args.get('client_id', '')
    alert     = request.args.get('alert', '')
    try:
        filters = {}
        if storage:   filters['storage_temp'] = storage
        if client_id: filters['client_id'] = client_id
        rows    = db.select('skus', filters=filters, order='name') or []
        clients = db.select('clients', order='company_name') or []
        total   = sum(r.get('total_qty', 0) or 0 for r in rows)
        low     = [r for r in rows if 0 < (r.get('total_qty') or 0) <= 10]
        out     = [r for r in rows if (r.get('total_qty') or 0) == 0]
    except Exception:
        rows = clients = low = out = []
        total = 0

    return render_template('operator/inventory.html',
        skus=rows, clients=clients,
        total_qty=total, sku_count=len(rows),
        low_stock_count=len(low), expiry_soon_count=0,
        filter_search=search, filter_storage=storage,
        filter_client=client_id, filter_alert=alert,
    )


# ── 입고 관리 ─────────────────────────────────────────────────
@operator_bp.route('/inbound')
@login_required
@operator_required
def inbound():
    db = get_db()
    status    = request.args.get('status', '')
    client_id = request.args.get('client_id', '')
    date_from = request.args.get('date_from', '')
    date_to   = request.args.get('date_to', '')
    page      = request.args.get('page', 1, type=int)
    try:
        filters = {}
        if status:    filters['status'] = status
        if client_id: filters['client_id'] = client_id
        rows    = db.select('inbound_orders', filters=filters, order='created_at desc', limit=50, offset=(page-1)*50) or []
        clients = db.select('clients', order='company_name') or []
        total   = db.count('inbound_orders', filters=filters) or 0
        counts  = {'total': total}
    except Exception:
        rows = clients = []
        total = 0
        counts = {}

    return render_template('operator/inbound.html',
        inbounds=rows, clients=clients,
        total_count=total, counts=counts,
        current_page=page, total_pages=max(1, -(-total//50)),
        filter_status=status, filter_client=client_id,
        filter_date_from=date_from, filter_date_to=date_to,
        skus_by_client={},
    )


@operator_bp.route('/inbound/create', methods=['POST'])
@login_required
@operator_required
def inbound_create():
    db = get_db()
    data = {
        'client_id': request.form.get('client_id'),
        'expected_date': request.form.get('expected_date'),
        'supplier': request.form.get('supplier', '').strip() or None,
        'note': request.form.get('note', '').strip() or None,
        'status': 'pending',
        'created_by': current_user.username,
    }
    try:
        db.insert('inbound_orders', data)
    except Exception:
        pass
    return redirect(url_for('operator.inbound'))


# ── 출고 관리 ─────────────────────────────────────────────────
@operator_bp.route('/shipments')
@login_required
@operator_required
def shipments():
    db = get_db()
    status    = request.args.get('status', '')
    search    = request.args.get('search', '')
    client_id = request.args.get('client_id', '')
    date_from = request.args.get('date_from', '')
    date_to   = request.args.get('date_to', '')
    page      = request.args.get('page', 1, type=int)
    try:
        filters = {}
        if status:    filters['status'] = status
        if client_id: filters['client_id'] = client_id
        rows    = db.select('orders', filters=filters, order='created_at desc', limit=50, offset=(page-1)*50) or []
        clients = db.select('clients', order='company_name') or []
        total   = db.count('orders', filters=filters) or 0
        counts  = {'total': total}
    except Exception:
        rows = clients = []
        total = 0
        counts = {}

    return render_template('operator/shipments.html',
        orders=rows, clients=clients,
        total_count=total, counts=counts,
        current_page=page, total_pages=max(1, -(-total//50)),
        filter_status=status, filter_search=search,
        filter_client=client_id, filter_date_from=date_from, filter_date_to=date_to,
    )


@operator_bp.route('/shipments/bulk-ship', methods=['POST'])
@login_required
@operator_required
def shipments_bulk_ship():
    db = get_db()
    data = request.get_json() or {}
    order_ids = data.get('order_ids', [])
    try:
        for oid in order_ids:
            db.update('orders', {'status': 'shipped', 'shipped_date': date.today().isoformat()},
                      filters={'id': oid})
        return jsonify({'ok': True, 'count': len(order_ids)})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})


# ── 반품 관리 ─────────────────────────────────────────────────
@operator_bp.route('/returns')
@login_required
@operator_required
def returns():
    db = get_db()
    status    = request.args.get('status', '')
    client_id = request.args.get('client_id', '')
    date_from = request.args.get('date_from', '')
    date_to   = request.args.get('date_to', '')
    page      = request.args.get('page', 1, type=int)
    try:
        filters = {}
        if status:    filters['status'] = status
        if client_id: filters['client_id'] = client_id
        rows    = db.select('return_orders', filters=filters, order='created_at desc', limit=50, offset=(page-1)*50) or []
        clients = db.select('clients', order='company_name') or []
        total   = db.count('return_orders', filters=filters) or 0
        counts  = {'total': total}
    except Exception:
        rows = clients = []
        total = 0
        counts = {}

    return render_template('operator/returns.html',
        returns=rows, clients=clients,
        total_count=total, counts=counts,
        current_page=page, total_pages=max(1, -(-total//50)),
        filter_status=status, filter_client=client_id,
        filter_date_from=date_from, filter_date_to=date_to,
        filter_search='',
    )


@operator_bp.route('/returns/<int:return_id>/process', methods=['POST'])
@login_required
@operator_required
def returns_process(return_id):
    db = get_db()
    data = request.get_json() or {}
    try:
        db.update('return_orders',
                  {'status': data.get('action'), 'note': data.get('note'), 'processed_by': current_user.username},
                  filters={'id': return_id})
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})


# ── 주문 현황 ─────────────────────────────────────────────────
@operator_bp.route('/orders')
@login_required
@operator_required
def orders():
    return redirect(url_for('operator.shipments'))


# ── 과금 / 정산 ───────────────────────────────────────────────
@operator_bp.route('/billing')
@login_required
@operator_required
def billing():
    db = get_db()
    month     = request.args.get('month', datetime.now().strftime('%Y-%m'))
    client_id = request.args.get('client_id', '')
    try:
        clients = db.select('clients', order='company_name') or []
        rows    = db.select('billing', filters={'year_month': month}, order='client_id') or []
    except Exception:
        clients = rows = []

    return render_template('operator/billing.html',
        billing_rows=rows, clients=clients,
        filter_month=month, filter_client=client_id,
        current_month=datetime.now().strftime('%Y-%m'),
        summary={},
    )


@operator_bp.route('/billing/calculate', methods=['POST'])
@login_required
@operator_required
def billing_calculate():
    db = get_db()
    data = request.get_json() or {}
    month     = data.get('month') or request.form.get('month')
    client_id = data.get('client_id') or request.form.get('client_id')
    try:
        return jsonify({'ok': True, 'month': month, 'client_id': client_id})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})


# ── 정산/재무 ─────────────────────────────────────────────────
@operator_bp.route('/finance')
@login_required
@operator_required
def finance():
    month = request.args.get('month', datetime.now().strftime('%Y-%m'))
    return render_template('operator/finance.html',
        filter_month=month,
        current_month=datetime.now().strftime('%Y-%m'),
        summary={}, billing_rows=[], overdue_rows=[],
        monthly_data=[], client_pie=[],
    )


@operator_bp.route('/finance/billing/<int:billing_id>/mark-paid', methods=['POST'])
@login_required
@operator_required
def finance_mark_paid(billing_id):
    db = get_db()
    try:
        db.update('billing', {'status': 'paid', 'paid_at': datetime.now().isoformat()},
                  filters={'id': billing_id})
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})


@operator_bp.route('/finance/calculate', methods=['POST'])
@login_required
@operator_required
def finance_calculate():
    data = request.get_json() or {}
    return jsonify({'ok': True, 'month': data.get('month')})


# ── KPI ───────────────────────────────────────────────────────
@operator_bp.route('/kpi')
@login_required
@operator_required
def kpi():
    month = request.args.get('month', datetime.now().strftime('%Y-%m'))
    return render_template('operator/kpi.html',
        filter_month=month,
        kpi={}, monthly_data=[], inout_data=[],
        client_pie=[], storage_data={}, client_kpi=[], low_stock=[],
    )


# ── 창고 설정 ─────────────────────────────────────────────────
@operator_bp.route('/warehouses')
@login_required
@operator_required
def warehouses():
    db = get_db()
    try:
        whs = db.select('warehouses', order='name') or []
        for wh in whs:
            wh['locations'] = db.select('locations', filters={'warehouse_id': wh['id']}, order='location_code') or []
    except Exception:
        whs = []
    return render_template('operator/warehouses.html', warehouses=whs)


@operator_bp.route('/warehouses/create', methods=['POST'])
@login_required
@operator_required
def warehouses_create():
    db = get_db()
    data = {
        'name': request.form.get('name', '').strip(),
        'storage_type': request.form.get('storage_type', 'mixed'),
        'area_sqm': request.form.get('area_sqm', type=float),
        'address': request.form.get('address', '').strip() or None,
    }
    try:
        db.insert('warehouses', data)
    except Exception:
        pass
    return redirect(url_for('operator.warehouses'))


@operator_bp.route('/warehouses/<int:wh_id>/locations/create', methods=['POST'])
@login_required
@operator_required
def locations_create(wh_id):
    db = get_db()
    data = {
        'warehouse_id': wh_id,
        'location_code': request.form.get('location_code', '').strip().upper(),
        'zone': request.form.get('zone', '').strip() or None,
        'capacity': request.form.get('capacity', type=int),
        'is_active': True,
    }
    try:
        db.insert('locations', data)
    except Exception:
        pass
    return redirect(url_for('operator.warehouses'))


# ── 사용자 관리 ───────────────────────────────────────────────
@operator_bp.route('/users')
@login_required
@operator_required
def users():
    db = get_db()
    role   = request.args.get('role', '')
    try:
        filters = {'role': role} if role else {}
        rows    = db.select('app_users', filters=filters, order='name') or []
        clients = db.select('clients', order='company_name') or []
    except Exception:
        rows = clients = []
    return render_template('operator/users.html',
        users=rows, clients=clients, filter_role=role)


@operator_bp.route('/users/<int:user_id>/toggle-active', methods=['POST'])
@login_required
@operator_required
def users_toggle(user_id):
    db = get_db()
    try:
        u = db.select('app_users', filters={'id': user_id}, single=True)
        if not u:
            return jsonify({'ok': False, 'error': '사용자 없음'})
        db.update('app_users', {'is_active': not u.get('is_active', True)}, filters={'id': user_id})
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})


@operator_bp.route('/users/<int:user_id>/reset-password', methods=['POST'])
@login_required
@operator_required
def users_reset_password(user_id):
    return jsonify({'ok': True})
