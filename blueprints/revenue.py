"""
revenue.py — 매출 관리 Blueprint.
일일매출 엑셀 업로드, 조회, 엑셀 다운로드, 단가 재적용.
"""
import os
import io
import json
import unicodedata
from datetime import datetime
from services.tz_utils import today_kst

import pandas as pd
from flask import (
    Blueprint, render_template, request, current_app,
    flash, redirect, url_for, send_file, jsonify,
)
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename

from auth import role_required, _log_action
from models import REVENUE_CATEGORIES

revenue_bp = Blueprint('revenue', __name__, url_prefix='/revenue')

ALLOWED_EXT = {'xlsx', 'xls'}


def _allowed(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXT


@revenue_bp.route('/')
@role_required('admin', 'ceo', 'manager', 'sales', 'general')
def index():
    """매출 조회 (order_transactions 기반)"""
    db = current_app.db

    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    category = request.args.get('category', '전체')

    data = []
    total_revenue = 0
    total_settlement = 0
    total_commission = 0

    try:
        data = db.query_revenue(
            date_from=date_from or None,
            date_to=date_to or None,
            category=category if category != '전체' else None,
        )
        total_revenue = sum(r.get('revenue', 0) for r in data)
        total_settlement = sum(r.get('settlement', 0) for r in data)
        total_commission = sum(r.get('commission', 0) for r in data)
    except Exception as e:
        flash(f'매출 조회 중 오류: {e}', 'danger')

    return render_template('revenue/index.html',
                           revenues=data,
                           total_revenue=total_revenue,
                           total_settlement=total_settlement,
                           total_commission=total_commission,
                           date_from=date_from, date_to=date_to,
                           category=category,
                           categories=REVENUE_CATEGORIES)


@revenue_bp.route('/import', methods=['POST'])
@role_required('admin', 'ceo', 'manager', 'sales', 'general')
def import_revenue():
    """매출 엑셀 업로드"""
    file = request.files.get('file')
    if not file or not _allowed(file.filename):
        flash('엑셀 파일(.xlsx/.xls)을 선택하세요.', 'danger')
        return redirect(url_for('revenue.index'))

    upload_date = request.form.get('date', today_kst())
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
        current_app.logger.info(f"[매출import성공] {upload_date} | {len(payload)}건 | 총매출 {total_rev:,}원 | 파일: {filename} | 사용자: {current_user.username}")
        flash(f'매출 {len(payload)}건 등록 완료 (합계: {total_rev:,}원)', 'success')
    except Exception as e:
        current_app.logger.error(f"[매출import실패] {upload_date} | 파일: {filename} | 사용자: {current_user.username} | {str(e)}")
        flash(f'매출 업로드 중 오류: {e}', 'danger')
    finally:
        if os.path.exists(filepath):
            os.remove(filepath)

    return redirect(url_for('revenue.index'))


@revenue_bp.route('/export')
@role_required('admin', 'ceo', 'manager', 'sales', 'general')
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
            'channel': '채널',
            'product_name': '품목명',
            'category': '매출구분',
            'qty': '수량',
            'revenue': '총매출',
            'settlement': '순매출(정산)',
            'commission': '수수료',
            'discount_amount': '할인액',
            'shipping_fee': '배송비',
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
        current_app.logger.info(
            f"[매출삭제] id={revenue_id} | "
            f"{old_record.get('revenue_date', '')} | {old_record.get('product_name', '')} | "
            f"{old_record.get('revenue', 0):,}원 | {old_record.get('category', '')} | "
            f"사용자: {current_user.username}"
            if old_record else
            f"[매출삭제] id={revenue_id} | 사용자: {current_user.username}"
        )
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
@role_required('admin', 'ceo', 'manager', 'sales', 'general')
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
