"""
bom_cost.py — BOM 원가 관리 Blueprint.
BOM 구성품 매입단가 관리 + 원가 분석 + 마진 분석 + 채널비용 관리.
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
                'weight': float(detail.get('weight', 0) or 0),
                'weight_unit': detail.get('weight_unit', 'g') or 'g',
                'cost_type': detail.get('cost_type', '매입') or '매입',
                'material_type': detail.get('material_type', '원료') or '원료',
            })

        return jsonify({
            'success': True,
            'bom_items': result['bom_items'],
            'cost_list': sorted(cost_list, key=lambda x: x['product_name']),
            'all_products': result['all_products'],
            'bom_components': result.get('bom_components', []),
            'missing_costs': result['missing_costs'],
            'channel_costs': result.get('channel_costs', {}),
            'all_set_names': result.get('all_set_names', []),
            'all_price_products': result.get('all_price_products', []),
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
    weight = float(data.get('weight', 0) or 0)
    weight_unit = (data.get('weight_unit') or 'g').strip()
    cost_type = (data.get('cost_type') or '매입').strip()

    try:
        # 수정 전 데이터 조회 (롤백용 + material_type 보존)
        cost_map_raw = db.query_product_costs()
        old_data = cost_map_raw.get(product_name)
        old_value = None
        # material_type: 프론트에서 안 보내면 기존값 유지 (자동 관리)
        material_type = old_data.get('material_type', '원료') if old_data else '원료'
        if old_data:
            old_value = {
                'cost_price': float(old_data.get('cost_price', 0)),
                'unit': old_data.get('unit', ''),
                'memo': old_data.get('memo', ''),
                'weight': float(old_data.get('weight', 0) or 0),
                'weight_unit': old_data.get('weight_unit', 'g'),
                'cost_type': old_data.get('cost_type', '매입'),
            }

        new_value = {
            'cost_price': cost_price, 'unit': unit, 'memo': memo,
            'weight': weight, 'weight_unit': weight_unit,
            'cost_type': cost_type,
        }

        db.upsert_product_cost(product_name, cost_price, unit, memo,
                               weight=weight, weight_unit=weight_unit,
                               cost_type=cost_type, material_type=material_type)
        _log_action('update_product_cost', target=product_name,
                     detail=f'유형={cost_type}, 단가={cost_price}, 중량={weight}{weight_unit}',
                     old_value=old_value, new_value=new_value)
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
    # 기존 material_type 보존을 위해 현재 DB 조회
    cost_map_raw = db.query_product_costs()
    valid_items = []
    for item in items:
        pn = (item.get('product_name') or '').strip()
        if pn:
            # material_type: 기존 DB 값 유지 (UI에서 관리 안 함)
            existing = cost_map_raw.get(pn)
            mt = existing.get('material_type', '원료') if existing else '원료'
            valid_items.append({
                'product_name': pn,
                'cost_price': float(item.get('cost_price', 0)),
                'unit': (item.get('unit') or '').strip(),
                'memo': (item.get('memo') or '').strip(),
                'weight': float(item.get('weight', 0) or 0),
                'weight_unit': (item.get('weight_unit') or 'g').strip(),
                'cost_type': (item.get('cost_type') or '매입').strip(),
                'material_type': mt,
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
@role_required('admin')
def api_delete_cost(product_name):
    """단가 1건 삭제 — 관리자만 (삭제 전 데이터 보존)"""
    db = current_app.db
    try:
        # 삭제 전 데이터 조회 (롤백용)
        cost_map_raw = db.query_product_costs()
        old_data = cost_map_raw.get(product_name)
        old_value = None
        if old_data:
            old_value = {
                'cost_price': float(old_data.get('cost_price', 0)),
                'unit': old_data.get('unit', ''),
                'memo': old_data.get('memo', ''),
                'weight': float(old_data.get('weight', 0) or 0),
                'weight_unit': old_data.get('weight_unit', 'g'),
                'cost_type': old_data.get('cost_type', '매입'),
                'material_type': old_data.get('material_type', '원료'),
            }

        db.delete_product_cost(product_name)
        _log_action('delete_product_cost', target=product_name,
                     old_value=old_value)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── 채널비용 관리 API ──

@bom_cost_bp.route('/api/channels')
@role_required('admin', 'manager')
def api_channels():
    """채널비용 목록 JSON"""
    db = current_app.db
    try:
        costs = db.query_channel_costs()
        result = []
        for ch, info in costs.items():
            result.append({
                'channel': ch,
                'fee_rate': float(info.get('fee_rate', 0) or 0),
                'shipping': float(info.get('shipping', 0) or 0),
                'packaging': float(info.get('packaging', 0) or 0),
                'other_cost': float(info.get('other_cost', 0) or 0),
                'memo': info.get('memo', ''),
            })
        return jsonify({'success': True, 'channels': result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@bom_cost_bp.route('/api/channel', methods=['POST'])
@role_required('admin', 'manager')
def api_save_channel():
    """채널비용 저장"""
    db = current_app.db
    data = request.get_json()
    if not data:
        return jsonify({'error': '데이터가 없습니다.'}), 400

    channel = (data.get('channel') or '').strip()
    if not channel:
        return jsonify({'error': '채널명은 필수입니다.'}), 400

    try:
        db.upsert_channel_cost(
            channel=channel,
            fee_rate=float(data.get('fee_rate', 0)),
            shipping=float(data.get('shipping', 0)),
            packaging=float(data.get('packaging', 0)),
            other_cost=float(data.get('other_cost', 0)),
            memo=(data.get('memo') or '').strip(),
        )
        _log_action('update_channel_cost', target=channel,
                     detail=f"수수료={data.get('fee_rate',0)}%")
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bom_cost_bp.route('/api/channel/<path:channel>', methods=['DELETE'])
@role_required('admin')
def api_delete_channel(channel):
    """채널비용 삭제 — 관리자만 (삭제 전 데이터 보존)"""
    db = current_app.db
    try:
        # 삭제 전 데이터 조회
        ch_costs = db.query_channel_costs()
        old_data = ch_costs.get(channel)
        old_value = None
        if old_data:
            old_value = {
                'fee_rate': float(old_data.get('fee_rate', 0) or 0),
                'shipping': float(old_data.get('shipping', 0) or 0),
                'packaging': float(old_data.get('packaging', 0) or 0),
                'other_cost': float(old_data.get('other_cost', 0) or 0),
                'memo': old_data.get('memo', ''),
            }

        db.delete_channel_cost(channel)
        _log_action('delete_channel_cost', target=channel,
                     old_value=old_value)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
