"""
set_assembly.py — 세트작업 관리 Blueprint.
BOM 기반 세트 조립: 단품 FIFO 차감 → 세트 산출, 부재료 차감, 이력 조회, 엑셀 다운로드.
"""
import io
import json
from datetime import datetime

import pandas as pd
from flask import (
    Blueprint, render_template, request, current_app,
    flash, redirect, url_for, send_file, jsonify,
)
from flask_login import login_required, current_user

from auth import role_required
from models import INV_TYPE_LABELS

set_assembly_bp = Blueprint('set_assembly', __name__, url_prefix='/set-assembly')


@set_assembly_bp.route('/')
@role_required('admin', 'manager', 'sales', 'logistics', 'production', 'general')
def index():
    """세트작업 폼 + 이력 조회"""
    db = current_app.db

    # 위치 목록
    locations = []
    try:
        locations, _ = db.query_filter_options()
    except Exception:
        pass

    # BOM 데이터 로드 (채널별 세트 목록)
    bom_data = {}
    try:
        raw = db.query_master_table('bom_master')
        for row in raw:
            ch = row.get('channel', '')
            sn = row.get('set_name', '')
            if ch and sn:
                if ch not in bom_data:
                    bom_data[ch] = []
                bom_data[ch].append(sn)
    except Exception as e:
        flash(f'BOM 데이터 로드 실패: {e}', 'danger')

    # 이력 조회
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')

    history = []
    if date_from or date_to:
        try:
            raw = db.query_stock_ledger(
                date_to=date_to or '9999-12-31',
                date_from=date_from or None,
                type_list=['SET_OUT', 'SET_IN'],
                order_desc=True,
            )
            history = raw
        except Exception as e:
            flash(f'세트작업 이력 조회 중 오류: {e}', 'danger')

    return render_template('set_assembly/index.html',
                           history=history,
                           locations=locations,
                           bom_data_json=json.dumps(bom_data, ensure_ascii=False),
                           date_from=date_from,
                           date_to=date_to,
                           type_labels=INV_TYPE_LABELS)


@set_assembly_bp.route('/api/products')
@role_required('admin', 'manager', 'sales', 'logistics', 'production', 'general')
def api_products():
    """창고별 재고 품목 목록 JSON 반환 (부재료 자동완성용)"""
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


@set_assembly_bp.route('/process', methods=['POST'])
@role_required('admin', 'manager', 'sales', 'logistics', 'production', 'general')
def process():
    """세트작업 처리"""
    date_str = request.form.get('date', datetime.now().strftime('%Y-%m-%d'))
    set_name = request.form.get('set_name', '').strip()
    channel = request.form.get('channel', '').strip()
    location = request.form.get('location', '').strip()
    qty_str = request.form.get('qty', '1').strip()

    if not set_name or not channel or not location:
        flash('세트종류, 판매처, 창고위치를 모두 선택해주세요.', 'danger')
        return redirect(url_for('set_assembly.index'))

    try:
        qty = int(qty_str)
    except ValueError:
        flash('수량은 숫자로 입력해주세요.', 'danger')
        return redirect(url_for('set_assembly.index'))

    # 부재료 파싱
    sub_names = request.form.getlist('sub_material_name[]')
    sub_qtys = request.form.getlist('sub_material_qty[]')
    sub_materials = []
    for i in range(len(sub_names)):
        s_name = sub_names[i].strip() if i < len(sub_names) else ''
        try:
            s_qty = int(sub_qtys[i]) if i < len(sub_qtys) else 0
        except (ValueError, IndexError):
            s_qty = 0
        if s_name and s_qty > 0:
            sub_materials.append({'name': s_name, 'qty': s_qty})

    try:
        from services.set_assembly_service import process_set_assembly
        result = process_set_assembly(
            current_app.db, date_str, set_name, channel, location, qty,
            sub_materials=sub_materials
        )

        if result.get('warnings'):
            for w in result['warnings']:
                flash(w, 'warning')

        if result.get('shortage'):
            for s in result['shortage']:
                flash(f'⚠️ {s}', 'danger')

        if result.get('success'):
            msg = (f"세트작업 완료: {set_name} x{qty} ({channel}) — "
                   f"단품 차감 {result.get('set_out_count', 0)}건, "
                   f"세트 산출 {result.get('set_in_count', 0)}건, "
                   f"구성품 {result.get('component_count', 0)}종 총 {result.get('total_deducted', 0)}개 차감")
            if result.get('sub_out_count', 0) > 0:
                msg += f", 부재료 {result.get('sub_out_count', 0)}건 차감"
            flash(msg, 'success')
    except Exception as e:
        flash(f'세트작업 처리 중 오류: {e}', 'danger')

    return redirect(url_for('set_assembly.index'))


@set_assembly_bp.route('/delete', methods=['POST'])
@role_required('admin', 'manager')
def delete():
    """세트작업 이력 삭제 (해당일 SET_OUT + SET_IN 전부 삭제)"""
    db = current_app.db
    date_str = request.form.get('delete_date', '').strip()

    if not date_str:
        flash('삭제할 날짜를 선택해주세요.', 'danger')
        return redirect(url_for('set_assembly.index'))

    try:
        cnt1 = db.delete_stock_ledger_by(date_str, 'SET_OUT')
        cnt2 = db.delete_stock_ledger_by(date_str, 'SET_IN')
        total = (cnt1 or 0) + (cnt2 or 0)
        if total > 0:
            flash(f'{date_str} 세트작업 이력 {total}건 삭제 완료', 'success')
        else:
            flash(f'{date_str} 에 삭제할 세트작업 이력이 없습니다.', 'warning')
    except Exception as e:
        flash(f'세트작업 이력 삭제 중 오류: {e}', 'danger')

    return redirect(url_for('set_assembly.index'))


@set_assembly_bp.route('/export')
@role_required('admin', 'manager', 'sales', 'logistics', 'production', 'general')
def export():
    """세트작업 이력 엑셀 다운로드"""
    db = current_app.db

    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')

    try:
        raw = db.query_stock_ledger(
            date_to=date_to or '9999-12-31',
            date_from=date_from or None,
            type_list=['SET_OUT', 'SET_IN'],
            order_desc=True,
        )

        if not raw:
            flash('다운로드할 세트작업 이력이 없습니다.', 'warning')
            return redirect(url_for('set_assembly.index'))

        df = pd.DataFrame(raw)

        col_map = {
            'transaction_date': '일자',
            'type': '유형',
            'product_name': '품목명',
            'qty': '수량',
            'location': '창고',
            'category': '종류',
            'unit': '단위',
            'expiry_date': '소비기한',
            'memo': '비고',
        }
        export_cols = [c for c in col_map.keys() if c in df.columns]
        df = df[export_cols].rename(columns=col_map)

        if '유형' in df.columns:
            df['유형'] = df['유형'].map(lambda x: INV_TYPE_LABELS.get(x, x))

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='세트작업이력')
        output.seek(0)

        fname = f"세트작업이력_{date_from or 'all'}_{date_to or 'all'}.xlsx"
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=fname,
        )
    except Exception as e:
        flash(f'세트작업 이력 다운로드 중 오류: {e}', 'danger')
        return redirect(url_for('set_assembly.index'))
