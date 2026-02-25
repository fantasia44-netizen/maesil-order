"""
history.py — 이력 관리 Blueprint.
재고 수불장 개별 항목 검색, 수정, 삭제 — 관리자 전용.
"""
from flask import (
    Blueprint, render_template, request, current_app,
    flash, redirect, url_for, jsonify,
)
from flask_login import login_required, current_user

from auth import role_required, _log_action
from models import INV_TYPE_LABELS

history_bp = Blueprint('history', __name__, url_prefix='/history')


@history_bp.route('/')
@role_required('admin', 'manager', 'logistics', 'production', 'general')
def index():
    """이력 검색 폼 + 결과 표시"""
    db = current_app.db

    # 필터 파라미터
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    location = request.args.get('location', '전체')
    record_type = request.args.get('type', '')
    product_name = request.args.get('product_name', '')

    locations, categories = [], []
    try:
        locations, categories = db.query_filter_options()
    except Exception:
        pass

    results = []
    searched = False

    if date_from or date_to or product_name:
        searched = True
        try:
            type_list = [record_type] if record_type else None
            raw = db.query_stock_ledger(
                date_to=date_to or '9999-12-31',
                date_from=date_from or None,
                location=location if location != '전체' else None,
                type_list=type_list,
                order_desc=True,
            )

            # 품목명 필터 (DB에서 LIKE 지원이 어려우므로 Python 필터)
            if product_name:
                search_term = product_name.replace(' ', '').lower()
                raw = [r for r in raw
                       if search_term in str(r.get('product_name', '')).replace(' ', '').lower()]

            results = raw[:500]  # 최대 500건 표시
        except Exception as e:
            flash(f'이력 조회 중 오류: {e}', 'danger')

    return render_template('history/index.html',
                           date_from=date_from, date_to=date_to,
                           location=location, record_type=record_type,
                           product_name=product_name,
                           locations=locations,
                           type_labels=INV_TYPE_LABELS,
                           records=results, searched=searched)


@history_bp.route('/edit/<int:row_id>', methods=['POST'])
@role_required('admin', 'manager', 'logistics', 'production', 'general')
def edit(row_id):
    """개별 이력 수정 (변경 전 데이터를 감사로그에 보존)"""
    db = current_app.db

    try:
        # 수정 전 데이터 조회 (롤백용)
        old_record = db.query_stock_ledger_by_id(row_id)
        old_value = None
        if old_record:
            old_value = {k: v for k, v in old_record.items()
                         if k not in ('id', 'created_at', 'is_deleted', 'deleted_at', 'deleted_by')}

        update_data = {}

        # 폼에서 수정 가능한 필드들
        for field in ['transaction_date', 'type', 'product_name', 'qty',
                       'location', 'category', 'food_type', 'expiry_date', 'storage_method',
                       'unit', 'lot_number', 'grade', 'manufacture_date', 'origin']:
            val = request.form.get(field)
            if val is not None:
                if field == 'qty':
                    update_data[field] = int(val)
                else:
                    update_data[field] = val.strip() if val.strip() else None

        if not update_data:
            flash('수정할 내용이 없습니다.', 'warning')
            return redirect(url_for('history.index'))

        db.update_stock_ledger(row_id, update_data)
        _log_action('edit_stock_ledger', target=str(row_id),
                     detail=str(update_data),
                     old_value=old_value, new_value=update_data)
        flash(f'이력 #{row_id} 수정 완료', 'success')
    except Exception as e:
        flash(f'수정 중 오류: {e}', 'danger')

    return redirect(url_for('history.index'))


@history_bp.route('/delete/<int:row_id>', methods=['POST'])
@role_required('admin')
def delete(row_id):
    """개별 이력 삭제 — 관리자/책임자만 (삭제 전 데이터 보존)"""
    db = current_app.db

    try:
        # 삭제 전 데이터 조회 (롤백용)
        old_record = db.query_stock_ledger_by_id(row_id)
        old_value = None
        if old_record:
            old_value = {k: v for k, v in old_record.items()
                         if k not in ('id', 'created_at')}

        db.delete_stock_ledger_by_id(row_id)
        _log_action('delete_stock_ledger', target=str(row_id),
                     old_value=old_value)
        flash(f'이력 #{row_id} 삭제 완료', 'success')
    except Exception as e:
        flash(f'삭제 중 오류: {e}', 'danger')

    return redirect(url_for('history.index'))
