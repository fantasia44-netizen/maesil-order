"""
promotions.py — 행사등록/쿠폰등록 관리 Blueprint.
품목+채널+기간 기반 판매가 조정(행사) 및 할인(쿠폰).
"""
from flask import (
    Blueprint, render_template, request, current_app, jsonify,
)
from flask_login import login_required, current_user

from auth import role_required, _log_action
from models import REVENUE_CATEGORIES
from db_utils import get_db

promotions_bp = Blueprint('promotions', __name__, url_prefix='/promotions')


@promotions_bp.route('/')
@role_required('admin', 'manager', 'sales', 'general')
def index():
    """행사/쿠폰 메인 페이지."""
    return render_template(
        'promotions/index.html',
        categories=REVENUE_CATEGORIES,
    )


# ══════════════════════════════════════════════════════════════
# 행사 API
# ══════════════════════════════════════════════════════════════

@promotions_bp.route('/api/promotions', methods=['GET'])
@role_required('admin', 'manager', 'sales', 'general')
def api_promotions_list():
    """행사 목록 조회."""
    db = get_db()
    product = request.args.get('product', '').strip()
    category = request.args.get('category', '').strip()
    date_from = request.args.get('date_from', '').strip()
    date_to = request.args.get('date_to', '').strip()
    active_only = request.args.get('active_only', '').strip() == '1'

    rows = db.query_promotions(
        product_name=product or None,
        category=category or None,
        date_from=date_from or None,
        date_to=date_to or None,
        active_only=active_only,
    )
    return jsonify({'data': rows})


@promotions_bp.route('/api/promotions', methods=['POST'])
@role_required('admin', 'manager', 'sales')
def api_promotions_create():
    """행사 등록."""
    db = get_db()
    data = request.get_json(force=True)

    required = ['product_name', 'category', 'start_date', 'end_date', 'promo_price']
    for key in required:
        if not data.get(key):
            return jsonify({'error': f'{key} 필수 입력입니다.'}), 400

    try:
        promo_price = int(data['promo_price'])
    except (ValueError, TypeError):
        return jsonify({'error': '행사가는 숫자여야 합니다.'}), 400

    payload = {
        'name': data.get('name', '').strip(),
        'product_name': data['product_name'].strip(),
        'category': data['category'].strip(),
        'start_date': data['start_date'],
        'end_date': data['end_date'],
        'promo_price': promo_price,
        'memo': data.get('memo', '').strip(),
        'is_active': data.get('is_active', True),
        'created_by': current_user.name,
    }

    try:
        row = db.insert_promotion(payload)
        _log_action('create_promotion',
                     target=payload['product_name'],
                     detail=f'{payload["category"]} {payload["start_date"]}~{payload["end_date"]} 행사가:{promo_price}')
        return jsonify({'success': True, 'data': row})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@promotions_bp.route('/api/promotions/<int:promo_id>', methods=['PUT'])
@role_required('admin', 'manager', 'sales')
def api_promotions_update(promo_id):
    """행사 수정."""
    db = get_db()
    data = request.get_json(force=True)

    updates = {}
    for key in ['name', 'product_name', 'category', 'start_date', 'end_date',
                'promo_price', 'memo', 'is_active']:
        if key in data:
            updates[key] = data[key]

    if 'promo_price' in updates:
        try:
            updates['promo_price'] = int(updates['promo_price'])
        except (ValueError, TypeError):
            return jsonify({'error': '행사가는 숫자여야 합니다.'}), 400

    try:
        row = db.update_promotion(promo_id, updates)
        _log_action('update_promotion', target=str(promo_id),
                     detail=f'행사 수정: {updates}')
        return jsonify({'success': True, 'data': row})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@promotions_bp.route('/api/promotions/<int:promo_id>', methods=['DELETE'])
@role_required('admin', 'manager', 'sales')
def api_promotions_delete(promo_id):
    """행사 삭제."""
    db = get_db()
    try:
        db.delete_promotion(promo_id)
        _log_action('delete_promotion', target=str(promo_id))
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ══════════════════════════════════════════════════════════════
# 쿠폰 API
# ══════════════════════════════════════════════════════════════

@promotions_bp.route('/api/coupons', methods=['GET'])
@role_required('admin', 'manager', 'sales', 'general')
def api_coupons_list():
    """쿠폰 목록 조회."""
    db = get_db()
    product = request.args.get('product', '').strip()
    category = request.args.get('category', '').strip()
    date_from = request.args.get('date_from', '').strip()
    date_to = request.args.get('date_to', '').strip()
    active_only = request.args.get('active_only', '').strip() == '1'

    rows = db.query_coupons(
        product_name=product or None,
        category=category or None,
        date_from=date_from or None,
        date_to=date_to or None,
        active_only=active_only,
    )
    return jsonify({'data': rows})


@promotions_bp.route('/api/coupons', methods=['POST'])
@role_required('admin', 'manager', 'sales')
def api_coupons_create():
    """쿠폰 등록."""
    db = get_db()
    data = request.get_json(force=True)

    required = ['product_name', 'category', 'start_date', 'end_date',
                'discount_type', 'discount_value']
    for key in required:
        if not data.get(key) and data.get(key) != 0:
            return jsonify({'error': f'{key} 필수 입력입니다.'}), 400

    discount_type = data['discount_type'].strip()
    if discount_type not in ('금액', '%'):
        return jsonify({'error': '할인유형은 "금액" 또는 "%" 이어야 합니다.'}), 400

    try:
        discount_value = float(data['discount_value'])
    except (ValueError, TypeError):
        return jsonify({'error': '할인값은 숫자여야 합니다.'}), 400

    payload = {
        'name': data.get('name', '').strip(),
        'product_name': data['product_name'].strip(),
        'category': data['category'].strip(),
        'start_date': data['start_date'],
        'end_date': data['end_date'],
        'discount_type': discount_type,
        'discount_value': discount_value,
        'memo': data.get('memo', '').strip(),
        'is_active': data.get('is_active', True),
        'created_by': current_user.name,
    }

    try:
        row = db.insert_coupon(payload)
        _log_action('create_coupon',
                     target=payload['product_name'],
                     detail=f'{payload["category"]} {payload["start_date"]}~{payload["end_date"]} {discount_type}{discount_value}')
        return jsonify({'success': True, 'data': row})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@promotions_bp.route('/api/coupons/<int:coupon_id>', methods=['PUT'])
@role_required('admin', 'manager', 'sales')
def api_coupons_update(coupon_id):
    """쿠폰 수정."""
    db = get_db()
    data = request.get_json(force=True)

    updates = {}
    for key in ['name', 'product_name', 'category', 'start_date', 'end_date',
                'discount_type', 'discount_value', 'memo', 'is_active']:
        if key in data:
            updates[key] = data[key]

    if 'discount_type' in updates and updates['discount_type'] not in ('금액', '%'):
        return jsonify({'error': '할인유형은 "금액" 또는 "%" 이어야 합니다.'}), 400

    if 'discount_value' in updates:
        try:
            updates['discount_value'] = float(updates['discount_value'])
        except (ValueError, TypeError):
            return jsonify({'error': '할인값은 숫자여야 합니다.'}), 400

    try:
        row = db.update_coupon(coupon_id, updates)
        _log_action('update_coupon', target=str(coupon_id),
                     detail=f'쿠폰 수정: {updates}')
        return jsonify({'success': True, 'data': row})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@promotions_bp.route('/api/coupons/<int:coupon_id>', methods=['DELETE'])
@role_required('admin', 'manager', 'sales')
def api_coupons_delete(coupon_id):
    """쿠폰 삭제."""
    db = get_db()
    try:
        db.delete_coupon(coupon_id)
        _log_action('delete_coupon', target=str(coupon_id))
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
