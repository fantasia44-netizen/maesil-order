"""
history.py — 이력 관리 Blueprint.
재고 수불장 개별 항목 검색, 수정, 삭제 — 관리자 전용.
"""
import io

from flask import (
    Blueprint, render_template, request, current_app,
    flash, redirect, url_for, jsonify, send_file,
)
from flask_login import login_required, current_user

from auth import role_required, _log_action
from models import INV_TYPE_LABELS
from db_utils import get_db

history_bp = Blueprint('history', __name__, url_prefix='/history')


@history_bp.route('/')
@role_required('admin', 'manager', 'logistics', 'production', 'general')
def index():
    """이력 검색 폼 + 결과 표시"""
    db = get_db()

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
    """개별 이력 수정 (원본 블라인드 + 새 레코드 INSERT, 양방향 링크)"""
    db = get_db()

    try:
        update_data = {}

        # 폼에서 수정 가능한 필드들
        for field in ['transaction_date', 'type', 'product_name', 'qty',
                       'location', 'category', 'food_type', 'expiry_date', 'storage_method',
                       'unit', 'lot_number', 'grade', 'manufacture_date', 'origin']:
            val = request.form.get(field)
            if val is not None:
                if field == 'qty':
                    n = float(val)
                    update_data[field] = int(n) if n == int(n) else n
                else:
                    update_data[field] = val.strip() if val.strip() else None

        if not update_data:
            flash('수정할 내용이 없습니다.', 'warning')
            return redirect(url_for('history.index'))

        new_id = db.replace_stock_ledger(
            row_id, update_data, replaced_by_user=current_user.username)
        _log_action('replace_stock_ledger', target=str(row_id),
                     detail=str(update_data),
                     old_value=str(row_id), new_value=update_data)
        flash(f'이력 #{row_id} 수정 완료 (새 레코드 #{new_id})', 'success')
    except Exception as e:
        flash(f'수정 중 오류: {e}', 'danger')

    return redirect(url_for('history.index'))


@history_bp.route('/delete/<int:row_id>', methods=['POST'])
@role_required('admin')
def delete(row_id):
    """개별 이력 블라인드 처리 — 관리자 전용 (원본 DB 보존)"""
    db = get_db()

    try:
        old_record = db.query_stock_ledger_by_id(row_id)
        db.blind_stock_ledger(row_id, blinded_by=current_user.username)
        _log_action('blind_stock_ledger', target=str(row_id),
                     old_value=old_record)
        flash(f'이력 #{row_id} 삭제 완료', 'success')
    except Exception as e:
        flash(f'삭제 처리 중 오류: {e}', 'danger')

    return redirect(url_for('history.index'))


@history_bp.route('/excel')
@role_required('admin', 'manager', 'logistics', 'production', 'general')
def excel():
    """이력 엑셀 다운로드 — 현재 검색 조건으로 조회 후 .xlsx 반환"""
    import pandas as pd
    from openpyxl.utils import get_column_letter

    db = get_db()

    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    location = request.args.get('location', '전체')
    record_type = request.args.get('type', '')
    product_name = request.args.get('product_name', '')

    if not (date_from or date_to or product_name):
        flash('검색 조건을 입력한 후 엑셀 다운로드를 이용하세요.', 'warning')
        return redirect(url_for('history.index'))

    try:
        type_list = [record_type] if record_type else None
        raw = db.query_stock_ledger(
            date_to=date_to or '9999-12-31',
            date_from=date_from or None,
            location=location if location != '전체' else None,
            type_list=type_list,
            order_desc=True,
        )

        if product_name:
            search_term = product_name.replace(' ', '').lower()
            raw = [r for r in raw
                   if search_term in str(r.get('product_name', '')).replace(' ', '').lower()]

        results = raw[:500]

        if not results:
            flash('엑셀로 출력할 데이터가 없습니다.', 'warning')
            return redirect(url_for('history.index',
                                     date_from=date_from, date_to=date_to,
                                     type=record_type, product_name=product_name))

        df = pd.DataFrame(results)

        col_map = {
            'transaction_date': '날짜',
            'type': '유형',
            'product_name': '품목명',
            'qty': '수량',
            'unit': '단위',
            'location': '위치',
            'category': '카테고리',
            'food_type': '식품유형',
            'storage_method': '보관방법',
            'manufacture_date': '제조일',
            'expiry_date': '소비기한',
        }

        # 유형 코드 → 한글 라벨 변환
        if 'type' in df.columns:
            df['type'] = df['type'].map(lambda t: INV_TYPE_LABELS.get(t, t))

        use_cols = [c for c in col_map if c in df.columns]
        df_out = df[use_cols].rename(columns=col_map)

        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine='openpyxl') as writer:
            df_out.to_excel(writer, index=False, sheet_name='이력관리')

            ws = writer.sheets['이력관리']
            for i, col_name in enumerate(df_out.columns, 1):
                max_len = max(
                    len(str(col_name)) * 2,
                    df_out.iloc[:, i - 1].astype(str).str.len().max() if len(df_out) > 0 else 0
                )
                ws.column_dimensions[get_column_letter(i)].width = min(max_len + 4, 40)

        buf.seek(0)
        period = date_from or 'start'
        fname = f"이력관리_{period}~{date_to or 'now'}.xlsx"
        return send_file(buf,
                         mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                         as_attachment=True, download_name=fname)

    except Exception as e:
        flash(f'엑셀 생성 중 오류: {e}', 'danger')
        return redirect(url_for('history.index',
                                 date_from=date_from, date_to=date_to,
                                 type=record_type, product_name=product_name))
