"""
revenue.py — 매출 관리 Blueprint.
일일매출 엑셀 업로드, 조회, 엑셀 다운로드, 단가 재적용.
"""
import os
import io
import json
import unicodedata
from datetime import datetime

import pandas as pd
from flask import (
    Blueprint, render_template, request, current_app,
    flash, redirect, url_for, send_file, jsonify,
)
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename

from auth import role_required, _log_action
from models import REVENUE_CATEGORIES

# 카테고리 → 가격표 컬럼 매핑 (aggregator.py 와 동일)
_CATEGORY_PRICE_COL = {
    "일반매출": "네이버판매가",
    "쿠팡매출": "쿠팡판매가",
    "로켓": "로켓판매가",
    "N배송(용인)": "네이버판매가",
}

revenue_bp = Blueprint('revenue', __name__, url_prefix='/revenue')

ALLOWED_EXT = {'xlsx', 'xls'}


def _allowed(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXT


@revenue_bp.route('/')
@role_required('admin', 'manager', 'sales', 'general')
def index():
    """매출 조회"""
    db = current_app.db

    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    category = request.args.get('category', '전체')

    data = []
    total_revenue = 0

    try:
        data = db.query_revenue(
            date_from=date_from or None,
            date_to=date_to or None,
            category=category if category != '전체' else None,
        )
        total_revenue = sum(r.get('revenue', 0) for r in data)
    except Exception as e:
        flash(f'매출 조회 중 오류: {e}', 'danger')

    return render_template('revenue/index.html',
                           revenues=data, total_revenue=total_revenue,
                           date_from=date_from, date_to=date_to,
                           category=category,
                           categories=REVENUE_CATEGORIES)


@revenue_bp.route('/import', methods=['POST'])
@role_required('admin', 'manager', 'sales', 'general')
def import_revenue():
    """매출 엑셀 업로드"""
    file = request.files.get('file')
    if not file or not _allowed(file.filename):
        flash('엑셀 파일(.xlsx/.xls)을 선택하세요.', 'danger')
        return redirect(url_for('revenue.index'))

    upload_date = request.form.get('date', datetime.now().strftime('%Y-%m-%d'))
    mode = request.form.get('mode', '신규입력')

    upload_dir = current_app.config['UPLOAD_FOLDER']
    os.makedirs(upload_dir, exist_ok=True)
    filename = secure_filename(file.filename)
    filepath = os.path.join(upload_dir, filename)
    file.save(filepath)

    try:
        from services.excel_io import parse_revenue_payload
        df = pd.read_excel(filepath)
        payload, total_rev = parse_revenue_payload(df, upload_date)

        if not payload:
            flash('엑셀에서 유효한 매출 데이터가 없습니다.', 'warning')
            return redirect(url_for('revenue.index'))

        db = current_app.db

        # 수정입력: 해당일 기존 매출 삭제 후 재입력
        if mode == '수정입력':
            deleted = db.delete_revenue_by_date(date_from=upload_date, date_to=upload_date)
            flash(f'기존 매출 {deleted}건 삭제 후 재입력합니다.', 'info')

        db.upsert_revenue(payload)
        flash(f'매출 {len(payload)}건 등록 완료 (합계: {total_rev:,}원)', 'success')
    except Exception as e:
        flash(f'매출 업로드 중 오류: {e}', 'danger')
    finally:
        if os.path.exists(filepath):
            os.remove(filepath)

    return redirect(url_for('revenue.index'))


@revenue_bp.route('/export')
@role_required('admin', 'manager', 'sales', 'general')
def export():
    """매출 데이터 엑셀 다운로드"""
    db = current_app.db

    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    category = request.args.get('category', '전체')

    try:
        data = db.query_revenue(
            date_from=date_from or None,
            date_to=date_to or None,
            category=category if category != '전체' else None,
        )

        if not data:
            flash('다운로드할 데이터가 없습니다.', 'warning')
            return redirect(url_for('revenue.index'))

        df = pd.DataFrame(data)

        # 컬럼 정리
        col_map = {
            'revenue_date': '매출일자',
            'product_name': '품목명',
            'category': '매출구분',
            'qty': '수량',
            'unit_price': '단가',
            'revenue': '매출액',
        }
        export_cols = [c for c in col_map.keys() if c in df.columns]
        df = df[export_cols].rename(columns=col_map)

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='매출')
        output.seek(0)

        fname = f"매출_{date_from or 'all'}_{date_to or 'all'}.xlsx"
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=fname,
        )
    except Exception as e:
        flash(f'매출 다운로드 중 오류: {e}', 'danger')
        return redirect(url_for('revenue.index'))


@revenue_bp.route('/delete/<int:revenue_id>', methods=['POST'])
@role_required('admin')
def delete_revenue(revenue_id):
    """매출 1건 삭제 (admin 전용)"""
    try:
        # 삭제 전 데이터 보존 (되돌리기용)
        old_record = None
        try:
            res = current_app.db.client.table("daily_revenue").select("*").eq("id", revenue_id).execute()
            old_record = res.data[0] if res.data else None
        except Exception:
            pass
        current_app.db.delete_revenue_by_id(revenue_id)
        _log_action('delete_revenue', target=str(revenue_id), old_value=old_record)
        flash('매출 삭제 완료', 'success')
    except Exception as e:
        flash(f'매출 삭제 중 오류: {e}', 'danger')

    # 기존 필터 유지하며 리다이렉트
    date_from = request.form.get('date_from', '')
    date_to = request.form.get('date_to', '')
    category = request.form.get('category', '')
    return redirect(url_for('revenue.index',
                            date_from=date_from, date_to=date_to, category=category))


@revenue_bp.route('/stats')
@role_required('admin', 'manager', 'sales', 'general')
def stats():
    """매출 통계 + 그래프"""
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    category = request.args.get('category', '전체')

    stats_data = None
    if date_from or date_to:
        try:
            from services.revenue_service import get_revenue_stats
            stats_data = get_revenue_stats(
                current_app.db,
                date_from=date_from or None,
                date_to=date_to or None,
                category=category if category != '전체' else None,
            )
        except Exception as e:
            flash(f'통계 조회 중 오류: {e}', 'danger')

    return render_template('revenue/stats.html',
                           date_from=date_from, date_to=date_to,
                           category=category,
                           categories=REVENUE_CATEGORIES,
                           stats=stats_data,
                           stats_json=json.dumps(stats_data, ensure_ascii=False) if stats_data else '{}')


# ── 단가 재적용 (가격표 → 매출 단가 일괄 갱신) ──

@revenue_bp.route('/reapply-prices', methods=['POST'])
@role_required('admin', 'manager')
def reapply_prices():
    """가격표(master_prices)의 현재 단가를 매출 데이터에 일괄 재적용.

    대상: _CATEGORY_PRICE_COL에 정의된 카테고리만 (거래처매출 등 수동 입력은 제외).
    로직: revenue = qty × new_unit_price 로 재계산.
    """
    db = current_app.db
    date_from = request.form.get('date_from', '').strip()
    date_to = request.form.get('date_to', '').strip()

    if not date_from or not date_to:
        flash('단가 재적용할 기간(시작일, 종료일)을 지정하세요.', 'warning')
        return redirect(url_for('revenue.index'))

    def _norm(t):
        return unicodedata.normalize('NFC', str(t).strip())

    try:
        # 1) 현재 가격표 로드
        price_map = db.query_price_table()  # {product_name: {네이버판매가, 쿠팡판매가, 로켓판매가}}

        if not price_map:
            flash('가격표(master_prices)에 등록된 가격이 없습니다.', 'warning')
            return redirect(url_for('revenue.index'))

        # 2) 대상 매출 데이터 조회
        revenues = db.query_revenue(date_from=date_from, date_to=date_to)
        if not revenues:
            flash('해당 기간에 매출 데이터가 없습니다.', 'info')
            return redirect(url_for('revenue.index',
                                    date_from=date_from, date_to=date_to))

        # 3) 단가 재적용
        updated_list = []
        skipped = 0
        no_price = set()

        for rev in revenues:
            cat = rev.get('category', '')
            price_col = _CATEGORY_PRICE_COL.get(cat)
            if not price_col:
                # 거래처매출, 오배송, 클레임 등 수동 카테고리 → 건너뜀
                skipped += 1
                continue

            pname = _norm(rev.get('product_name', ''))
            price_info = price_map.get(pname)
            if not price_info:
                no_price.add(pname)
                skipped += 1
                continue

            new_unit_price = float(price_info.get(price_col, 0) or 0)
            old_unit_price = float(rev.get('unit_price', 0) or 0)

            # 가격이 동일하면 건너뜀
            if abs(new_unit_price - old_unit_price) < 0.01:
                skipped += 1
                continue

            qty = float(rev.get('qty', 0) or 0)
            new_revenue = round(qty * new_unit_price)

            updated_list.append({
                'revenue_date': rev['revenue_date'],
                'product_name': rev['product_name'],
                'category': cat,
                'qty': rev['qty'],
                'unit_price': int(new_unit_price),
                'revenue': int(new_revenue),
            })

        # 4) upsert (conflict key: revenue_date, product_name, category)
        if updated_list:
            db.upsert_revenue(updated_list)
            _log_action('reapply_prices',
                        detail=f'{date_from}~{date_to} 단가 재적용: {len(updated_list)}건 갱신')
            flash(f'단가 재적용 완료: {len(updated_list)}건 갱신 (건너뜀: {skipped}건)',
                  'success')
        else:
            flash(f'변경할 매출이 없습니다. (전체 {len(revenues)}건 중 단가 동일 또는 대상외)',
                  'info')

        if no_price:
            names = ', '.join(sorted(no_price)[:10])
            extra = f' 외 {len(no_price)-10}건' if len(no_price) > 10 else ''
            flash(f'가격표 미등록 품목: {names}{extra}', 'warning')

    except Exception as e:
        flash(f'단가 재적용 중 오류: {e}', 'danger')

    return redirect(url_for('revenue.index',
                            date_from=date_from, date_to=date_to))
