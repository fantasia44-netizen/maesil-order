"""
transfer.py — 창고 이동 관리 Blueprint.
수동 이동 입력 + 엑셀 일괄 이동.
"""
import os
from datetime import datetime
from services.tz_utils import today_kst

import pandas as pd
from flask import (
    Blueprint, render_template, request, current_app,
    flash, redirect, url_for, jsonify,
)
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename

from auth import role_required, _log_action
from services.storage_helper import backup_to_storage
from db_utils import get_db

transfer_bp = Blueprint('transfer', __name__, url_prefix='/transfer')

ALLOWED_EXT = {'xlsx', 'xls'}


def _allowed(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXT


@transfer_bp.route('/')
@role_required('admin', 'manager', 'logistics', 'general')
def index():
    """창고 이동 폼 (수동 + 엑셀)"""
    db = get_db()
    locations = []
    try:
        locations, _ = db.query_filter_options()
    except Exception:
        pass
    return render_template('transfer/index.html', locations=locations)


@transfer_bp.route('/api/products')
@role_required('admin', 'manager', 'logistics', 'general')
def api_products():
    """출발 창고 기준 재고 품목 목록 JSON (자동완성용)"""
    location = request.args.get('location', '')
    if not location:
        return jsonify([])
    try:
        from services.excel_io import build_stock_snapshot
        all_data = get_db().query_stock_by_location(location)
        snapshot = build_stock_snapshot(all_data)
        products = []
        for name, info in snapshot.items():
            if info['total'] > 0:
                products.append({
                    'name': name,
                    'qty': info['total'],
                    'unit': info.get('unit', '개'),
                })
        products.sort(key=lambda x: x['name'])
        return jsonify(products)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@transfer_bp.route('/manual', methods=['POST'])
@role_required('admin', 'manager', 'logistics', 'general')
def manual():
    """수동 창고 이동 (단건 — 하위 호환)"""
    product_name = request.form.get('product_name', '').strip()
    qty = request.form.get('qty', 0, type=int)
    from_location = request.form.get('from_location', '').strip()
    to_location = request.form.get('to_location', '').strip()
    date_str = request.form.get('date', today_kst())

    if not product_name or qty <= 0 or not from_location or not to_location:
        flash('품목명, 수량, 출발/도착 창고를 모두 입력하세요.', 'danger')
        return redirect(url_for('transfer.index'))

    if from_location == to_location:
        flash('출발 창고와 도착 창고가 같습니다.', 'danger')
        return redirect(url_for('transfer.index'))

    lot_number = request.form.get('lot_number', '').strip() or None
    grade = request.form.get('grade', '').strip() or None

    try:
        from services.transfer_service import process_manual_transfer
        result = process_manual_transfer(
            get_db(), product_name, qty,
            from_location, to_location, date_str,
            lot_number=lot_number, grade=grade,
            created_by=current_user.username,
        )

        if result.get('warnings'):
            for w in result['warnings']:
                flash(w, 'warning')

        _log_action('manual_transfer',
                     detail=f'{date_str} {product_name} x{qty} '
                            f'{from_location}→{to_location} '
                            f'({result.get("moved_count", 0)}건 처리)')
        flash(f"창고 이동 완료: {result.get('moved_count', 0)}건 처리", 'success')
    except ValueError as e:
        flash(str(e), 'danger')
    except Exception as e:
        flash(f'창고 이동 중 오류: {e}', 'danger')

    return redirect(url_for('transfer.index'))


@transfer_bp.route('/batch', methods=['POST'])
@role_required('admin', 'manager', 'logistics', 'general')
def batch():
    """다건 일괄 창고 이동 (JSON)"""
    data = request.get_json(silent=True)
    if not data:
        return jsonify({'error': '요청 데이터가 없습니다.'}), 400

    items = data.get('items', [])
    date_str = data.get('date', today_kst())

    if not items:
        return jsonify({'error': '이동 항목이 없습니다.'}), 400

    # 유효성 검증
    for i, item in enumerate(items):
        name = str(item.get('product_name', '')).strip()
        qty = item.get('qty', 0)
        from_loc = str(item.get('from_location', '')).strip()
        to_loc = str(item.get('to_location', '')).strip()
        if not name or not from_loc or not to_loc:
            return jsonify({'error': f'{i+1}번째 항목: 품목명/출발/도착 창고를 모두 입력하세요.'}), 400
        try:
            qty_val = float(qty)
            if qty_val <= 0:
                raise ValueError
        except (ValueError, TypeError):
            return jsonify({'error': f'{i+1}번째 항목 ({name}): 수량이 올바르지 않습니다.'}), 400
        if from_loc == to_loc:
            return jsonify({'error': f'{i+1}번째 항목 ({name}): 출발/도착 창고가 같습니다.'}), 400

    try:
        from services.transfer_service import process_manual_transfer

        total_count = 0
        all_warnings = []

        for i, item in enumerate(items):
            result = process_manual_transfer(
                get_db(),
                str(item['product_name']).strip(),
                float(item['qty']),
                str(item['from_location']).strip(),
                str(item['to_location']).strip(),
                date_str,
                lot_number=str(item.get('lot_number', '')).strip() or None,
                grade=str(item.get('grade', '')).strip() or None,
                created_by=current_user.username,
            )
            total_count += result.get('moved_count', 0)
            all_warnings.extend(result.get('warnings', []))

        _log_action('batch_transfer',
                     detail=f'{date_str} 일괄 창고이동 {total_count}건 처리 '
                            f'(항목 {len(items)}건)')
        return jsonify({
            'success': True,
            'count': total_count,
            'warnings': all_warnings,
        })
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        return jsonify({'error': f'창고 이동 중 오류: {e}'}), 500


@transfer_bp.route('/excel', methods=['POST'])
@role_required('admin', 'manager', 'logistics', 'general')
def excel():
    """엑셀 일괄 창고 이동 — 비활성화 (추후 재구현)"""
    return jsonify({'error': '엑셀 업로드 기능은 비활성화되었습니다. 추후 재구현 예정입니다.'}), 410
