"""
bom_cost.py — BOM 원가 관리 Blueprint.
BOM 구성품 매입단가 관리 + 원가 분석 + 마진 분석.
관리자/총괄책임자 전용.
"""
from flask import (
    Blueprint, render_template, request, current_app,
    flash, redirect, url_for, jsonify,
)
from flask_login import login_required, current_user

from auth import role_required, _log_action

bom_cost_bp = Blueprint('bom_cost', __name__, url_prefix='/bom-cost')


@bom_cost_bp.route('/')
@role_required('admin', 'manager')
def index():
    """BOM 원가 관리 메인 페이지"""
    return render_template('bom_cost/index.html')


@bom_cost_bp.route('/api/data')
@role_required('admin', 'manager')
def api_data():
    """BOM 원가 분석 전체 데이터 API"""
    db = current_app.db

    try:
        from services.bom_cost_service import calculate_bom_costs
        result = calculate_bom_costs(db)

        # cost_details → 직렬화 가능하게 변환
        cost_list = []
        for name, detail in result.get('cost_details', {}).items():
            cost_list.append({
                'product_name': name,
                'cost_price': float(detail.get('cost_price', 0)),
                'unit': detail.get('unit', ''),
                'memo': detail.get('memo', ''),
            })

        return jsonify({
            'success': True,
            'bom_items': result['bom_items'],
            'cost_list': sorted(cost_list, key=lambda x: x['product_name']),
            'all_products': result['all_products'],
            'missing_costs': result['missing_costs'],
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@bom_cost_bp.route('/api/cost', methods=['POST'])
@role_required('admin', 'manager')
def api_save_cost():
    """단가 1건 저장"""
    db = current_app.db
    data = request.get_json()
    if not data:
        return jsonify({'error': '데이터가 없습니다.'}), 400

    product_name = (data.get('product_name') or '').strip()
    if not product_name:
        return jsonify({'error': '품목명은 필수입니다.'}), 400

    cost_price = float(data.get('cost_price', 0))
    unit = (data.get('unit') or '').strip()
    memo = (data.get('memo') or '').strip()

    try:
        db.upsert_product_cost(product_name, cost_price, unit, memo)
        _log_action('update_product_cost', target=product_name,
                     detail=f'단가={cost_price}')
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bom_cost_bp.route('/api/cost/batch', methods=['POST'])
@role_required('admin', 'manager')
def api_save_cost_batch():
    """단가 일괄 저장"""
    db = current_app.db
    data = request.get_json()
    if not data or not data.get('items'):
        return jsonify({'error': '데이터가 없습니다.'}), 400

    items = data['items']
    valid_items = []
    for item in items:
        pn = (item.get('product_name') or '').strip()
        if pn:
            valid_items.append({
                'product_name': pn,
                'cost_price': float(item.get('cost_price', 0)),
                'unit': (item.get('unit') or '').strip(),
                'memo': (item.get('memo') or '').strip(),
            })

    if not valid_items:
        return jsonify({'error': '유효한 항목이 없습니다.'}), 400

    try:
        db.upsert_product_costs_batch(valid_items)
        _log_action('batch_update_product_cost',
                     detail=f'{len(valid_items)}건 일괄 저장')
        return jsonify({'success': True, 'count': len(valid_items)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bom_cost_bp.route('/api/cost/<path:product_name>', methods=['DELETE'])
@role_required('admin', 'manager')
def api_delete_cost(product_name):
    """단가 1건 삭제"""
    db = current_app.db
    try:
        db.delete_product_cost(product_name)
        _log_action('delete_product_cost', target=product_name)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
