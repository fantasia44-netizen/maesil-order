"""
aggregation.py — 통합 집계 Blueprint (기존 집계프로그램 기능).
집계 파일 + BOM 파일 업로드 → 가공 → 결과 엑셀 다운로드.
출고/매출 반영: 집계 결과 파일을 바로 stock_ledger + daily_revenue 에 반영.
"""
import os
from datetime import datetime

import pandas as pd
from flask import (
    Blueprint, render_template, request, current_app,
    flash, redirect, url_for, send_file,
)
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename

from auth import role_required

aggregation_bp = Blueprint('aggregation', __name__, url_prefix='/aggregation')

ALLOWED_EXT = {'xlsx', 'xls', 'csv'}


def _allowed(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXT


@aggregation_bp.route('/')
@role_required('admin', 'manager', 'sales')
def index():
    """집계 업로드 폼"""
    output_dir = current_app.config['OUTPUT_FOLDER']
    result_files = []
    if os.path.exists(output_dir):
        result_files = sorted(
            [f for f in os.listdir(output_dir)
             if f.endswith('.xlsx') and (f.startswith('통합') or f.startswith('일일매출'))],
            reverse=True,
        )[:20]

    return render_template('aggregation/index.html', result_files=result_files)


@aggregation_bp.route('/process', methods=['POST'])
@role_required('admin', 'manager', 'sales')
def process():
    """집계 파일(들) + BOM 파일 + 옵션리스트 업로드 → 처리"""
    agg_files = request.files.getlist('agg_files')
    bom_file = request.files.get('bom_file')
    option_file = request.files.get('option_file')

    if not agg_files or all(f.filename == '' for f in agg_files):
        flash('집계 엑셀 파일을 하나 이상 선택하세요.', 'danger')
        return redirect(url_for('aggregation.index'))

    upload_dir = current_app.config['UPLOAD_FOLDER']
    output_dir = current_app.config['OUTPUT_FOLDER']
    os.makedirs(upload_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    # 집계 파일 저장
    agg_paths = []
    for f in agg_files:
        if f and f.filename and _allowed(f.filename):
            fname = secure_filename(f.filename)
            fpath = os.path.join(upload_dir, fname)
            f.save(fpath)
            agg_paths.append(fpath)

    if not agg_paths:
        flash('유효한 엑셀 파일이 없습니다.', 'danger')
        return redirect(url_for('aggregation.index'))

    # BOM 파일 저장 (필수)
    bom_path = None
    if bom_file and bom_file.filename and _allowed(bom_file.filename):
        bom_fname = secure_filename(bom_file.filename)
        bom_path = os.path.join(upload_dir, bom_fname)
        bom_file.save(bom_path)

    if not bom_path:
        flash('세트옵션BOM 파일을 선택하세요.', 'danger')
        # 업로드 파일 정리
        for p in agg_paths:
            if os.path.exists(p):
                os.remove(p)
        return redirect(url_for('aggregation.index'))

    # 옵션리스트 파일 저장 (선택)
    option_path = None
    if option_file and option_file.filename and _allowed(option_file.filename):
        opt_fname = secure_filename(option_file.filename)
        option_path = os.path.join(upload_dir, opt_fname)
        option_file.save(option_path)

    try:
        from services.aggregator import Aggregator

        aggregator = Aggregator()
        result = aggregator.run(agg_paths, option_path, bom_path, output_dir)

        if result.get('error'):
            flash(result['error'], 'danger')

        if result.get('success'):
            summary = result.get('summary', {})
            flash(f"집계 처리 완료: {summary.get('total_items', 0)}종, "
                  f"총 {summary.get('total_qty', 0):,}개", 'success')

        # 다운로드 링크 생성
        downloads = []
        for fpath in result.get('files', []):
            fname = os.path.basename(fpath)
            downloads.append({
                'name': fname,
                'url': url_for('aggregation.download', filename=fname),
            })

        # 최근 처리 결과 파일 목록
        result_files = sorted(
            [f for f in os.listdir(output_dir)
             if f.endswith('.xlsx') and (f.startswith('통합') or f.startswith('일일매출'))],
            reverse=True,
        )[:20]

        return render_template('aggregation/index.html',
                               result={'logs': result.get('logs', []),
                                       'downloads': downloads},
                               result_files=result_files)

    except Exception as e:
        flash(f'집계 처리 중 오류: {e}', 'danger')
        return redirect(url_for('aggregation.index'))
    finally:
        # 업로드 파일 정리
        for p in agg_paths:
            if os.path.exists(p):
                os.remove(p)
        if bom_path and os.path.exists(bom_path):
            os.remove(bom_path)
        if option_path and os.path.exists(option_path):
            os.remove(option_path)


@aggregation_bp.route('/download/<path:filename>')
@role_required('admin', 'manager', 'sales')
def download(filename):
    """집계 결과 파일 다운로드 (한글 파일명 지원)"""
    output_dir = os.path.abspath(current_app.config['OUTPUT_FOLDER'])
    safe_name = os.path.basename(filename)
    filepath = os.path.join(output_dir, safe_name)

    if not os.path.abspath(filepath).startswith(output_dir):
        from flask import abort
        abort(403)

    if not os.path.exists(filepath):
        flash('파일을 찾을 수 없습니다.', 'danger')
        return redirect(url_for('aggregation.index'))

    return send_file(
        filepath,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=safe_name,
    )


@aggregation_bp.route('/apply', methods=['POST'])
@role_required('admin', 'manager', 'sales')
def apply_results():
    """집계 결과 파일을 출고(stock_ledger) + 매출(daily_revenue)에 반영"""
    date_str = request.form.get('date', datetime.now().strftime('%Y-%m-%d'))
    mode = request.form.get('mode', '신규입력')
    selected_files = request.form.getlist('apply_files')

    if not selected_files:
        flash('반영할 파일을 선택하세요.', 'danger')
        return redirect(url_for('aggregation.index'))

    output_dir = os.path.abspath(current_app.config['OUTPUT_FOLDER'])

    # 선택된 파일 경로 구성 (보안 검증)
    file_paths = []
    for fname in selected_files:
        safe_name = os.path.basename(fname)
        fpath = os.path.join(output_dir, safe_name)
        if not os.path.abspath(fpath).startswith(output_dir):
            continue
        if os.path.exists(fpath):
            file_paths.append(fpath)

    if not file_paths:
        flash('선택된 파일을 찾을 수 없습니다.', 'danger')
        return redirect(url_for('aggregation.index'))

    try:
        from services.outbound_service import process_batch_outbound

        # 수정입력 + 매출: 해당일 매출 데이터도 삭제
        if mode == '수정입력':
            revenue_files_exist = any(
                os.path.basename(p).startswith('일일매출') for p in file_paths
            )
            if revenue_files_exist:
                db = current_app.db
                rev_deleted = db.delete_revenue_by_date(
                    date_from=date_str, date_to=date_str)
                if rev_deleted:
                    flash(f'기존 매출 {rev_deleted}건 삭제 후 재입력합니다.', 'info')

        result = process_batch_outbound(
            current_app.db, file_paths, date_str,
            mode=mode, force_shortage=True,
        )

        # 결과 메시지
        if result.get('deleted_count'):
            flash(f"기존 출고 {result['deleted_count']}건 삭제 후 재입력", 'info')

        for msg in result.get('results', []):
            flash(msg, 'success')

        for err in result.get('errors', []):
            flash(err, 'danger')

        if result.get('duplicate_warning'):
            flash(result['duplicate_warning'], 'warning')

        flash(f"출고/매출 반영 완료: 총 출고 {result.get('total_count', 0)}건", 'success')

    except Exception as e:
        flash(f'출고/매출 반영 중 오류: {e}', 'danger')

    return redirect(url_for('aggregation.index'))
