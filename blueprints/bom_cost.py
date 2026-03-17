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
from db_utils import get_db

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
    db = get_db()

    try:
        from services.bom_cost_service import calculate_bom_costs
        result = calculate_bom_costs(db)

        # stock_ledger.category → 정규 종류(material_type) 매핑
        # 이 값이 product_costs.material_type보다 정확함
        category_map = db.query_product_categories()

        # cost_details → 직렬화 가능하게 변환
        cost_list = []
        for name, detail in result.get('cost_details', {}).items():
            # stock_ledger.category만 사용. 없으면 빈값 (기본값 '원료' 폴백 없음)
            mt = (category_map.get(name)
                  or category_map.get(name.replace(' ', ''))
                  or '')
            ratio = float(detail.get('conversion_ratio', 1) or 1)
            cost_price = float(detail.get('cost_price', 0))
            cost_list.append({
                'product_name': name,
                'cost_price': cost_price,
                'unit': detail.get('unit', ''),
                'memo': detail.get('memo', ''),
                'weight': float(detail.get('weight', 0) or 0),
                'weight_unit': detail.get('weight_unit', 'g') or 'g',
                'cost_type': detail.get('cost_type', '매입') or '매입',
                'material_type': mt,
                'purchase_unit': detail.get('purchase_unit', '') or '',
                'standard_unit': detail.get('standard_unit', '') or '',
                'conversion_ratio': ratio,
                'unit_cost': round(cost_price / ratio, 2) if ratio > 0 else cost_price,
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
            'category_map': category_map,  # 프론트 종류필터용
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@bom_cost_bp.route('/api/cost', methods=['POST'])
@role_required('admin', 'manager')
def api_save_cost():
    """단가 1건 저장"""
    db = get_db()
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
    purchase_unit = (data.get('purchase_unit') or '').strip()
    standard_unit = (data.get('standard_unit') or '').strip()
    conversion_ratio = float(data.get('conversion_ratio', 1) or 1)
    food_type = (data.get('food_type') or '').strip()

    try:
        # 수정 전 데이터 조회 (롤백용 + material_type 보존)
        cost_map_raw = db.query_product_costs()
        old_data = cost_map_raw.get(product_name)
        old_value = None
        # material_type: stock_ledger.category만 사용. 없으면 빈값
        category_map = db.query_product_categories()
        material_type = (category_map.get(product_name)
                         or category_map.get(product_name.replace(' ', ''))
                         or '')
        if old_data:
            old_value = {
                'cost_price': float(old_data.get('cost_price', 0)),
                'unit': old_data.get('unit', ''),
                'memo': old_data.get('memo', ''),
                'weight': float(old_data.get('weight', 0) or 0),
                'weight_unit': old_data.get('weight_unit', 'g'),
                'cost_type': old_data.get('cost_type', '매입'),
                'purchase_unit': old_data.get('purchase_unit', ''),
                'standard_unit': old_data.get('standard_unit', ''),
                'conversion_ratio': float(old_data.get('conversion_ratio', 1) or 1),
                'material_type': old_data.get('material_type', '원료'),
                'food_type': old_data.get('food_type', ''),
            }

        new_value = {
            'cost_price': cost_price, 'unit': unit, 'memo': memo,
            'weight': weight, 'weight_unit': weight_unit,
            'cost_type': cost_type,
            'purchase_unit': purchase_unit, 'standard_unit': standard_unit,
            'conversion_ratio': conversion_ratio,
            'material_type': material_type,
            'food_type': food_type,
        }

        # 단가 또는 변환비율이 변경되었으면 이력 자동 저장
        if old_data:
            old_cp = float(old_data.get('cost_price', 0))
            old_cr = float(old_data.get('conversion_ratio', 1) or 1)
            if old_cp != cost_price or old_cr != conversion_ratio:
                try:
                    changed_by = (current_user.username
                                  if hasattr(current_user, 'username')
                                  else str(getattr(current_user, 'id', '')))
                    db.insert_cost_history(
                        product_name=product_name,
                        old_cost_price=old_cp,
                        new_cost_price=cost_price,
                        old_conversion_ratio=old_cr,
                        new_conversion_ratio=conversion_ratio,
                        changed_by=changed_by,
                    )
                except Exception:
                    pass  # 이력 저장 실패해도 단가 저장은 진행

        db.upsert_product_cost(product_name, cost_price, unit, memo,
                               weight=weight, weight_unit=weight_unit,
                               cost_type=cost_type, material_type=material_type,
                               purchase_unit=purchase_unit,
                               standard_unit=standard_unit,
                               conversion_ratio=conversion_ratio,
                               food_type=food_type)
        _log_action('update_product_cost', target=product_name,
                     detail=f'유형={cost_type}, 종류={material_type}, 단가={cost_price}, 변환={conversion_ratio}',
                     old_value=old_value, new_value=new_value)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bom_cost_bp.route('/api/cost/batch', methods=['POST'])
@role_required('admin', 'manager')
def api_save_cost_batch():
    """단가 일괄 저장"""
    db = get_db()
    data = request.get_json()
    if not data or not data.get('items'):
        return jsonify({'error': '데이터가 없습니다.'}), 400

    items = data['items']
    # material_type: stock_ledger.category만 사용. 없으면 빈값
    category_map = db.query_product_categories()
    valid_items = []
    for item in items:
        pn = (item.get('product_name') or '').strip()
        if pn:
            mt = (category_map.get(pn)
                  or category_map.get(pn.replace(' ', ''))
                  or '')
            valid_items.append({
                'product_name': pn,
                'cost_price': float(item.get('cost_price', 0)),
                'unit': (item.get('unit') or '').strip(),
                'memo': (item.get('memo') or '').strip(),
                'weight': float(item.get('weight', 0) or 0),
                'weight_unit': (item.get('weight_unit') or 'g').strip(),
                'cost_type': (item.get('cost_type') or '매입').strip(),
                'material_type': mt,
                'purchase_unit': (item.get('purchase_unit') or '').strip(),
                'standard_unit': (item.get('standard_unit') or '').strip(),
                'conversion_ratio': float(item.get('conversion_ratio', 1) or 1),
                'food_type': (item.get('food_type') or '').strip(),
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
    db = get_db()
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
                'purchase_unit': old_data.get('purchase_unit', ''),
                'standard_unit': old_data.get('standard_unit', ''),
                'conversion_ratio': float(old_data.get('conversion_ratio', 1) or 1),
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
    db = get_db()
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
    db = get_db()
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
    db = get_db()
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


# ── 실제 원가 분석 API ──

@bom_cost_bp.route('/api/actual-cost')
@role_required('admin', 'manager')
def api_actual_cost():
    """기간별 실제 생산 원가 분석 API.
    GET /bom-cost/api/actual-cost?from=2026-03-01&to=2026-03-07&location=본사
    """
    db = get_db()
    date_from = request.args.get('from', '')
    date_to = request.args.get('to', '')
    location = request.args.get('location', '') or None

    if not date_from or not date_to:
        return jsonify({'error': '기간(from, to)은 필수입니다.'}), 400

    try:
        from services.actual_cost_service import calculate_actual_costs
        result = calculate_actual_costs(db, date_from, date_to, location)
        return jsonify({'success': True, **result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ── 매입단가 이력 조회 API ──

@bom_cost_bp.route('/api/cost-history')
@role_required('admin', 'manager')
def api_cost_history():
    """매입단가 변경 이력 조회 API.
    GET /bom-cost/api/cost-history?product_name=당근
    product_name 없으면 최신 전체 이력 반환.
    """
    db = get_db()
    product_name = request.args.get('product_name', '') or None

    try:
        history = db.query_cost_history(product_name=product_name)
        return jsonify({'success': True, 'history': history})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
