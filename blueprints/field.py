"""
field.py — 플로워 현장 직원 모드 Blueprint
역할: packing, operator, admin
"""
from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required, current_user
from functools import wraps
from datetime import datetime

from db_utils import get_db

field_bp = Blueprint('field', __name__, url_prefix='/field')


def field_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated:
            from flask import redirect, url_for
            return redirect(url_for('auth.login'))
        if current_user.role not in ('packing', 'operator', 'admin', 'ceo', 'manager'):
            from flask import redirect, url_for
            return redirect(url_for('auth.login'))
        return f(*args, **kwargs)
    return decorated


# ── 현장 홈 ───────────────────────────────────────────────────
@field_bp.route('/')
@login_required
@field_required
def dashboard():
    db = get_db()
    today = datetime.now().strftime('%Y-%m-%d')
    try:
        today_inbound  = db.count('inbound_movements',  filters={'date': today}) or 0
        today_outbound = db.count('outbound_movements', filters={'date': today}) or 0
        today_transfer = db.count('transfer_movements', filters={'date': today}) or 0
        pending_picks  = db.count('pick_lists',  filters={'status': 'pending'}) or 0
        recent_moves   = db.select('movements', order='created_at desc', limit=10) or []
    except Exception:
        today_inbound = today_outbound = today_transfer = pending_picks = 0
        recent_moves = []

    return render_template('field/dashboard.html',
        today_inbound=today_inbound,
        today_outbound=today_outbound,
        today_transfer=today_transfer,
        pending_picks=pending_picks,
        recent_moves=recent_moves,
    )


# ── 입고 스캔 ─────────────────────────────────────────────────
@field_bp.route('/inbound')
@login_required
@field_required
def inbound():
    return render_template('field/inbound.html')


# ── 출고 / 상차 스캔 ──────────────────────────────────────────
@field_bp.route('/shipping')
@login_required
@field_required
def shipping():
    return render_template('field/shipping.html')


# ── 피킹 ──────────────────────────────────────────────────────
@field_bp.route('/picking')
@login_required
@field_required
def picking():
    db = get_db()
    try:
        pending_lists = db.select('pick_lists', filters={'status': 'pending'}, order='created_at') or []
    except Exception:
        pending_lists = []
    return render_template('field/picking.html',
        active_list=None,
        pending_lists=pending_lists,
        done_count=0, total_count=0,
    )


@field_bp.route('/picking/<int:list_id>')
@login_required
@field_required
def picking_detail(list_id):
    db = get_db()
    try:
        pl    = db.select('pick_lists', filters={'id': list_id}, single=True)
        items = db.select('pick_list_items', filters={'pick_list_id': list_id}, order='location_code') or []
        if pl:
            pl['items'] = items
        pending_lists = db.select('pick_lists', filters={'status': 'pending'}, order='created_at') or []
        done  = sum(1 for i in items if i.get('is_done'))
        total = len(items)
    except Exception:
        pl = None
        pending_lists = []
        done = total = 0

    return render_template('field/picking.html',
        active_list=pl,
        pending_lists=pending_lists,
        done_count=done, total_count=total,
    )


# ── 바코드 조회 ───────────────────────────────────────────────
@field_bp.route('/scan')
@login_required
@field_required
def scan():
    return render_template('field/scan.html')


# ── 재고 실사 ─────────────────────────────────────────────────
@field_bp.route('/stockcheck')
@login_required
@field_required
def stockcheck():
    return render_template('field/stockcheck.html')


# ── 창고 이동 ─────────────────────────────────────────────────
@field_bp.route('/transfer')
@login_required
@field_required
def transfer():
    return render_template('field/transfer.html')


# ── API: 바코드 스캔 → SKU 조회 ──────────────────────────────
@field_bp.route('/api/sku-lookup', methods=['POST'])
@login_required
@field_required
def api_sku_lookup():
    db = get_db()
    data    = request.get_json() or {}
    barcode = data.get('barcode', '').strip()
    if not barcode:
        return jsonify({'ok': False, 'error': '바코드 없음'})
    try:
        sku = db.select('skus', filters={'barcode': barcode}, single=True)
        if not sku:
            sku = db.select('skus', filters={'sku_code': barcode}, single=True)
        if not sku:
            return jsonify({'ok': False, 'error': f'상품을 찾을 수 없습니다: {barcode}'})
        locs = db.select('locations', filters={'sku_id': sku['id']}) or []
        sku['locations'] = [l['location_code'] for l in locs]
        return jsonify({'ok': True, 'sku': sku})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})


# ── API: 입고 스캔 등록 ───────────────────────────────────────
@field_bp.route('/api/inbound-scan', methods=['POST'])
@login_required
@field_required
def api_inbound_scan():
    db = get_db()
    data    = request.get_json() or {}
    barcode  = data.get('barcode', '').strip()
    location = data.get('location', '').strip().upper()
    qty      = int(data.get('qty') or 1)
    lot      = data.get('lot', '').strip() or None

    if not barcode:
        return jsonify({'ok': False, 'error': '바코드 없음'})
    try:
        sku = db.select('skus', filters={'barcode': barcode}, single=True)
        if not sku:
            sku = db.select('skus', filters={'sku_code': barcode}, single=True)
        if not sku:
            return jsonify({'ok': False, 'error': f'상품 없음: {barcode}'})

        movement = {
            'sku_id': sku['id'], 'movement_type': 'inbound',
            'location': location, 'qty': qty, 'lot': lot,
            'created_by': current_user.username,
            'created_at': datetime.now().isoformat(),
        }
        db.insert('movements', movement)
        db.rpc('increment_sku_qty', {'p_sku_id': sku['id'], 'p_qty': qty})

        return jsonify({'ok': True, 'sku_name': sku['name'], 'qty': qty})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})


# ── API: 송장 스캔 → 출고 처리 ───────────────────────────────
@field_bp.route('/api/ship-scan', methods=['POST'])
@login_required
@field_required
def api_ship_scan():
    db = get_db()
    data       = request.get_json() or {}
    invoice_no = data.get('invoice_no', '').strip()
    if not invoice_no:
        return jsonify({'ok': False, 'error': '송장번호 없음'})
    try:
        order = db.select('orders', filters={'invoice_no': invoice_no}, single=True)
        if not order:
            return jsonify({'ok': False, 'error': f'송장번호를 찾을 수 없습니다: {invoice_no}'})
        if order.get('status') == 'shipped':
            return jsonify({'ok': False, 'error': '이미 처리된 송장입니다'})
        db.update('orders', {'status': 'shipped', 'shipped_at': datetime.now().isoformat()},
                  filters={'invoice_no': invoice_no})
        return jsonify({
            'ok': True,
            'order_no': order.get('order_no'),
            'recipient_name': order.get('recipient_name'),
            'invoice_no': invoice_no,
            'item_summary': order.get('item_summary', ''),
        })
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})


# ── API: 피킹 스캔 ────────────────────────────────────────────
@field_bp.route('/api/pick-scan', methods=['POST'])
@login_required
@field_required
def api_pick_scan():
    db = get_db()
    data    = request.get_json() or {}
    barcode = data.get('barcode', '').strip()
    list_id = data.get('list_id')
    if not barcode or not list_id:
        return jsonify({'ok': False, 'error': '파라미터 오류'})
    try:
        item = db.select('pick_list_items',
                         filters={'pick_list_id': list_id, 'barcode': barcode, 'is_done': False},
                         single=True)
        if not item:
            return jsonify({'ok': False, 'error': '해당 바코드를 찾을 수 없거나 이미 완료된 항목입니다'})

        new_picked = (item.get('picked_qty') or 0) + 1
        is_done    = new_picked >= item.get('required_qty', 1)
        db.update('pick_list_items', {'picked_qty': new_picked, 'is_done': is_done},
                  filters={'id': item['id']})

        remaining = db.count('pick_list_items', filters={'pick_list_id': list_id, 'is_done': False}) or 0
        all_done  = remaining == 0
        if all_done:
            db.update('pick_lists', {'status': 'done', 'completed_at': datetime.now().isoformat()},
                      filters={'id': list_id})

        return jsonify({'ok': True, 'item_id': item['id'], 'is_done': is_done, 'all_done': all_done})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})


# ── API: 피킹 완료 ────────────────────────────────────────────
@field_bp.route('/api/pick-complete', methods=['POST'])
@login_required
@field_required
def api_pick_complete():
    db = get_db()
    data    = request.get_json() or {}
    list_id = data.get('list_id')
    try:
        db.update('pick_lists',
                  {'status': 'done', 'completed_at': datetime.now().isoformat(), 'completed_by': current_user.username},
                  filters={'id': list_id})
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})


# ── API: 재고 실사 ────────────────────────────────────────────
@field_bp.route('/api/stock-check', methods=['POST'])
@login_required
@field_required
def api_stock_check():
    db = get_db()
    data    = request.get_json() or {}
    barcode  = data.get('barcode', '').strip()
    location = data.get('location', '').strip().upper()
    count_qty = int(data.get('count_qty') or 0)
    try:
        sku = db.select('skus', filters={'barcode': barcode}, single=True)
        if not sku:
            sku = db.select('skus', filters={'sku_code': barcode}, single=True)
        if not sku:
            return jsonify({'ok': False, 'error': f'상품 없음: {barcode}'})

        system_qty = sku.get('total_qty') or 0
        entry = {
            'sku_id': sku['id'], 'location': location,
            'count_qty': count_qty, 'system_qty': system_qty,
            'diff': count_qty - system_qty,
            'counted_by': current_user.username,
            'counted_at': datetime.now().isoformat(),
            'status': 'pending',
        }
        result = db.insert('stock_checks', entry)
        entry_id = (result or {}).get('id') or 0
        return jsonify({'ok': True, 'id': entry_id, 'sku_name': sku['name'],
                        'sku_code': sku['sku_code'], 'system_qty': system_qty})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})


@field_bp.route('/api/stock-check-submit', methods=['POST'])
@login_required
@field_required
def api_stock_check_submit():
    db = get_db()
    data    = request.get_json() or {}
    entries = data.get('entries', [])
    try:
        for e in entries:
            if e.get('id'):
                db.update('stock_checks', {'status': 'submitted'}, filters={'id': e['id']})
        return jsonify({'ok': True, 'count': len(entries)})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})


# ── API: 창고 이동 ────────────────────────────────────────────
@field_bp.route('/api/transfer', methods=['POST'])
@login_required
@field_required
def api_transfer():
    db = get_db()
    data = request.get_json() or {}
    sku_id       = data.get('sku_id')
    from_location = data.get('from_location', '').upper()
    to_location   = data.get('to_location', '').upper()
    qty           = int(data.get('qty') or 1)
    try:
        movement = {
            'sku_id': sku_id, 'movement_type': 'transfer',
            'from_location': from_location, 'to_location': to_location,
            'qty': qty, 'created_by': current_user.username,
            'created_at': datetime.now().isoformat(),
        }
        db.insert('movements', movement)
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})
