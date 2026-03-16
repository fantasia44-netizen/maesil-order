"""
adjustment.py — 재고 조정 Blueprint.
양수/음수 수량으로 재고 증감 조정, 사유(memo) 필수.
"""
from datetime import datetime
from services.tz_utils import today_kst

from flask import (
    Blueprint, render_template, request, current_app,
    jsonify,
)
from flask_login import login_required, current_user

from auth import role_required, _log_action

adjustment_bp = Blueprint('adjustment', __name__, url_prefix='/adjustment')


@adjustment_bp.route('/')
@role_required('admin', 'manager', 'production', 'logistics', 'general')
def index():
    """재고 조정 페이지"""
    db = current_app.db
    locations = []
    try:
        locations, _ = db.query_filter_options()
    except Exception:
        pass
    return render_template('adjustment/index.html', locations=locations)


@adjustment_bp.route('/api/products')
@role_required('admin', 'manager', 'production', 'logistics', 'general')
def api_products():
    """창고별 재고 품목 목록 JSON (자동완성용)"""
    location = request.args.get('location', '')
    if not location:
        return jsonify([])
    try:
        from services.excel_io import build_stock_snapshot
        all_data = current_app.db.query_stock_by_location(location)
        snapshot = build_stock_snapshot(all_data)
        products = []
        for name, info in snapshot.items():
            if info['total'] > 0:
                products.append({
                    'name': name,
                    'qty': info['total'],
                    'unit': info.get('unit', '개'),
                    'category': info.get('category', ''),
                    'storage_method': info.get('storage_method', ''),
                })
        products.sort(key=lambda x: x['name'])
        return jsonify(products)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@adjustment_bp.route('/api/history')
@role_required('admin', 'manager', 'production', 'logistics', 'general')
def api_history():
    """재고 조정 이력 조회 JSON"""
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    if not date_from or not date_to:
        return jsonify([])
    try:
        data = current_app.db.query_stock_ledger(
            date_from=date_from, date_to=date_to, type_list=['ADJUST'])
        rows = []
        for r in data:
            rows.append({
                'id': r.get('id'),
                'date': r.get('transaction_date', ''),
                'product_name': r.get('product_name', ''),
                'qty': r.get('qty', 0),
                'location': r.get('location', ''),
                'storage_method': r.get('storage_method', ''),
                'unit': r.get('unit', ''),
                'memo': r.get('memo', ''),
                'category': r.get('category', ''),
            })
        rows.sort(key=lambda x: (x['date'], x['product_name']))
        return jsonify(rows)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── API: 개별 삭제 (admin 전용) ──

@adjustment_bp.route('/api/delete/<int:record_id>', methods=['POST'])
@role_required('admin')
def api_delete(record_id):
    """개별 조정 이력 블라인드 처리 (admin 전용)"""
    try:
        old_record = current_app.db.query_stock_ledger_by_id(record_id)
        current_app.db.blind_stock_ledger(record_id, blinded_by=current_user.username)
        _log_action('blind_adjustment', target=str(record_id),
                     old_value=old_record)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── API: 개별 수정 (admin 전용) ──

@adjustment_bp.route('/api/update/<int:record_id>', methods=['POST'])
@role_required('admin')
def api_update(record_id):
    """개별 조정 이력 수정 (admin 전용)"""
    data = request.get_json(silent=True)
    if not data:
        return jsonify({'error': '수정 데이터가 없습니다.'}), 400
    allowed = {'product_name', 'qty', 'location', 'memo', 'storage_method', 'unit', 'category'}
    update_data = {k: v for k, v in data.items() if k in allowed}
    if 'qty' in update_data:
        try:
            update_data['qty'] = float(update_data['qty'])
            if update_data['qty'] == 0:
                raise ValueError
        except (ValueError, TypeError):
            return jsonify({'error': '수량은 0이 아니어야 합니다.'}), 400
    if 'memo' in update_data and not update_data['memo'].strip():
        return jsonify({'error': '사유를 입력하세요.'}), 400
    # 빈 문자열 → None 변환 (PostgreSQL TEXT 컬럼 호환)
    for key in ('storage_method', 'category'):
        if key in update_data and update_data[key] == '':
            update_data[key] = None
    if not update_data:
        return jsonify({'error': '수정할 항목이 없습니다.'}), 400
    try:
        result = current_app.db.replace_stock_ledger(
            record_id, update_data, replaced_by_user=current_user.username)
        _log_action('replace_adjustment', target=str(record_id),
                     old_value=result.get('old_record'), new_value=update_data)
        return jsonify({'success': True, 'new_id': result.get('new_id')})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@adjustment_bp.route('/batch', methods=['POST'])
@role_required('admin', 'manager', 'production', 'logistics', 'general')
def batch():
    """다건 일괄 재고 조정 (JSON)"""
    data = request.get_json(silent=True)
    if not data:
        return jsonify({'error': '요청 데이터가 없습니다.'}), 400

    items = data.get('items', [])
    date_str = data.get('date', today_kst())

    if not items:
        return jsonify({'error': '조정 항목이 없습니다.'}), 400

    # 유효성 검증
    for i, item in enumerate(items):
        name = str(item.get('product_name', '')).strip()
        location = str(item.get('location', '')).strip()
        qty = item.get('qty', 0)
        memo = str(item.get('memo', '')).strip()
        if not name:
            return jsonify({'error': f'{i+1}번째 항목: 품목명을 입력하세요.'}), 400
        if not location:
            return jsonify({'error': f'{i+1}번째 항목: 창고위치를 선택하세요.'}), 400
        try:
            if float(qty) == 0:
                raise ValueError
        except (ValueError, TypeError):
            return jsonify({'error': f'{i+1}번째 항목 ({name}): 수량은 0이 아니어야 합니다.'}), 400
        if not memo:
            return jsonify({'error': f'{i+1}번째 항목 ({name}): 사유를 입력하세요.'}), 400

    try:
        from services.adjustment_service import process_adjustment_batch
        result = process_adjustment_batch(
            current_app.db, date_str, items,
            created_by=current_user.username)
        _log_action('batch_adjustment',
                     detail=f'{date_str} 재고조정 {result.get("count", 0)}건 '
                            f'(증가 {result.get("increase_count", 0)}건, '
                            f'감소 {result.get("decrease_count", 0)}건, '
                            f'항목 {len(items)}건)',
                     new_value={'date': date_str, 'batch_ts': result.get('batch_ts'),
                                'count': result.get('count', 0)})
        return jsonify({
            'success': True,
            'count': result.get('count', 0),
            'increase_count': result.get('increase_count', 0),
            'decrease_count': result.get('decrease_count', 0),
            'warnings': result.get('warnings', []),
        })
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        return jsonify({'error': f'재고 조정 중 오류: {e}'}), 500
