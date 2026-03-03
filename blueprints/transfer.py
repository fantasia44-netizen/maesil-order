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

from auth import role_required

transfer_bp = Blueprint('transfer', __name__, url_prefix='/transfer')

ALLOWED_EXT = {'xlsx', 'xls'}


def _allowed(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXT


@transfer_bp.route('/')
@role_required('admin', 'manager', 'logistics', 'general')
def index():
    """창고 이동 폼 (수동 + 엑셀)"""
    db = current_app.db
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
        all_data = current_app.db.query_stock_by_location(location)
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
    mode = request.form.get('mode', '신규입력')

    if not product_name or qty <= 0 or not from_location or not to_location:
        flash('품목명, 수량, 출발/도착 창고를 모두 입력하세요.', 'danger')
        return redirect(url_for('transfer.index'))

    if from_location == to_location:
        flash('출발 창고와 도착 창고가 같습니다.', 'danger')
        return redirect(url_for('transfer.index'))

    try:
        from services.transfer_service import process_manual_transfer
        result = process_manual_transfer(
            current_app.db, product_name, qty,
            from_location, to_location, date_str, mode,
        )

        if result.get('warnings'):
            for w in result['warnings']:
                flash(w, 'warning')

        flash(f"창고 이동 완료: {result.get('moved_count', 0)}건 처리"
              + (f", 기존 {result.get('deleted_count', 0)}건 삭제" if mode == '수정입력' else ''),
              'success')
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
    mode = data.get('mode', '신규입력')

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

    # DataFrame 으로 변환하여 기존 process_transfer_excel 재활용
    rows = []
    for item in items:
        rows.append({
            '품목명': str(item['product_name']).strip(),
            '현재창고위치': str(item['from_location']).strip(),
            '이동창고위치': str(item['to_location']).strip(),
            '수량입력': float(item['qty']) if float(item['qty']) != int(float(item['qty'])) else int(float(item['qty'])),
        })
    df = pd.DataFrame(rows)

    try:
        from services.transfer_service import process_transfer_excel
        result = process_transfer_excel(current_app.db, df, date_str, mode)

        return jsonify({
            'success': True,
            'count': result.get('count', 0),
            'warnings': result.get('warnings', []),
            'deleted_count': result.get('deleted_count', 0),
        })
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        return jsonify({'error': f'창고 이동 중 오류: {e}'}), 500


@transfer_bp.route('/excel', methods=['POST'])
@role_required('admin', 'manager', 'logistics', 'general')
def excel():
    """엑셀 일괄 창고 이동"""
    file = request.files.get('file')
    if not file or not _allowed(file.filename):
        flash('엑셀 파일(.xlsx/.xls)을 선택하세요.', 'danger')
        return redirect(url_for('transfer.index'))

    date_str = request.form.get('date', today_kst())
    mode = request.form.get('mode', '신규입력')

    upload_dir = current_app.config['UPLOAD_FOLDER']
    os.makedirs(upload_dir, exist_ok=True)
    filename = secure_filename(file.filename)
    filepath = os.path.join(upload_dir, filename)
    file.save(filepath)

    try:
        df = pd.read_excel(filepath)
        required_cols = {'품목명', '현재창고위치', '이동창고위치', '수량입력'}
        if not required_cols.issubset(set(df.columns)):
            missing = required_cols - set(df.columns)
            flash(f'필수 컬럼 누락: {", ".join(missing)}', 'danger')
            return redirect(url_for('transfer.index'))

        from services.transfer_service import process_transfer_excel
        result = process_transfer_excel(current_app.db, df, date_str, mode)

        if result.get('warnings'):
            for w in result['warnings']:
                flash(w, 'warning')

        flash(f"엑셀 이동 완료: {result.get('count', 0)}건 처리"
              + (f", 기존 {result.get('deleted_count', 0)}건 삭제" if mode == '수정입력' else ''),
              'success')
    except KeyError as e:
        flash(f'엑셀 컬럼 오류: {e}', 'danger')
    except ValueError as e:
        flash(str(e), 'danger')
    except Exception as e:
        flash(f'엑셀 이동 처리 중 오류: {e}', 'danger')
    finally:
        if os.path.exists(filepath):
            os.remove(filepath)

    return redirect(url_for('transfer.index'))
