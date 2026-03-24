"""
price_mgmt.py — 판매관리 (가격표) Blueprint.
제품별 판매처 단가 조회/수정. 옵션 가격표 로딩.
관리자/총괄/영업/총무 전용.
"""
from flask import (
    Blueprint, render_template, request, current_app, jsonify,
)
from flask_login import login_required, current_user

from auth import role_required, _log_action
from db_utils import get_db

price_mgmt_bp = Blueprint('price_mgmt', __name__, url_prefix='/price')


@price_mgmt_bp.route('/')
@role_required('admin', 'manager', 'sales', 'general')
def index():
    """판매관리 메인 페이지"""
    return render_template('price_mgmt/index.html')


@price_mgmt_bp.route('/api/data')
@role_required('admin', 'manager', 'sales', 'general')
def api_data():
    """가격표 전체 데이터 API"""
    db = get_db()
    try:
        # master_prices raw 데이터
        raw = db.query_master_table('master_prices')

        items = []
        for r in raw:
            name = r.get('품목명', '') or r.get('product_name', '')
            if not name or not str(name).strip():
                continue
            items.append({
                'id': r.get('id', 0),
                'product_name': str(name).strip(),
                'sku': str(r.get('SKU', r.get('sku', ''))),
                'naver_price': float(r.get('네이버판매가', r.get('naver_price', 0)) or 0),
                'coupang_price': float(r.get('쿠팡판매가', r.get('coupang_price', 0)) or 0),
                'rocket_price': float(r.get('로켓판매가', r.get('rocket_price', 0)) or 0),
            })

        def sku_sort_key(x):
            try:
                return (0, int(x['sku']), x['product_name'])
            except (ValueError, TypeError):
                return (1, 0, x['product_name'])
        items.sort(key=sku_sort_key)

        # 통계
        total = len(items)
        naver_count = sum(1 for i in items if i['naver_price'] > 0)
        coupang_count = sum(1 for i in items if i['coupang_price'] > 0)
        rocket_count = sum(1 for i in items if i['rocket_price'] > 0)

        return jsonify({
            'success': True,
            'items': items,
            'summary': {
                'total': total,
                'naver_count': naver_count,
                'coupang_count': coupang_count,
                'rocket_count': rocket_count,
            },
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@price_mgmt_bp.route('/api/save', methods=['POST'])
@role_required('admin', 'manager', 'sales')
def api_save():
    """가격 1건 수정"""
    db = get_db()
    data = request.get_json()
    if not data:
        return jsonify({'error': '데이터가 없습니다.'}), 400

    row_id = data.get('id')
    product_name = (data.get('product_name') or '').strip()
    if not row_id and not product_name:
        return jsonify({'error': '품목 정보가 없습니다.'}), 400

    try:
        update_data = {}
        if 'naver_price' in data:
            update_data['네이버판매가'] = float(data['naver_price'] or 0)
        if 'coupang_price' in data:
            update_data['쿠팡판매가'] = float(data['coupang_price'] or 0)
        if 'rocket_price' in data:
            update_data['로켓판매가'] = float(data['rocket_price'] or 0)
        if 'sku' in data:
            update_data['SKU'] = str(data['sku'])

        if not update_data:
            return jsonify({'error': '수정할 데이터가 없습니다.'}), 400

        if row_id:
            db.client.table('master_prices').update(
                update_data
            ).eq('id', row_id).execute()
        else:
            db.client.table('master_prices').update(
                update_data
            ).eq('품목명', product_name).execute()

        _log_action('update_price', target=product_name,
                     detail=str(update_data))
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@price_mgmt_bp.route('/api/save/batch', methods=['POST'])
@role_required('admin', 'manager', 'sales')
def api_save_batch():
    """가격 일괄 수정"""
    db = get_db()
    data = request.get_json()
    if not data or not data.get('items'):
        return jsonify({'error': '데이터가 없습니다.'}), 400

    items = data['items']
    saved = 0

    try:
        for item in items:
            row_id = item.get('id')
            if not row_id:
                continue
            update_data = {
                '네이버판매가': float(item.get('naver_price', 0) or 0),
                '쿠팡판매가': float(item.get('coupang_price', 0) or 0),
                '로켓판매가': float(item.get('rocket_price', 0) or 0),
            }
            db.client.table('master_prices').update(
                update_data
            ).eq('id', row_id).execute()
            saved += 1

        _log_action('batch_update_price', detail=f'{saved}건 일괄 수정')
        return jsonify({'success': True, 'count': saved})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
