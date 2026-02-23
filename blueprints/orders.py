"""
orders.py — 온라인주문처리 Blueprint (기존 1611.py 기능).
주문서 + 옵션 파일 업로드 → 가공 → 결과 엑셀 다운로드.
"""
import os
from datetime import datetime

from flask import (
    Blueprint, render_template, request, current_app,
    flash, redirect, url_for, send_file, abort,
)
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename

from auth import role_required

orders_bp = Blueprint('orders', __name__, url_prefix='/orders')

ALLOWED_EXT = {'xlsx', 'xls', 'csv'}


def _allowed(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXT


@orders_bp.route('/')
@role_required('admin', 'manager', 'sales')
def index():
    """온라인주문처리 업로드 폼"""
    output_dir = current_app.config['OUTPUT_FOLDER']
    result_files = []
    if os.path.exists(output_dir):
        result_files = sorted(
            [f for f in os.listdir(output_dir)
             if f.endswith('.xlsx') and not f.startswith('집계')
             and not f.startswith('통합')],
            reverse=True,
        )[:20]

    return render_template('orders/index.html', result_files=result_files)


@orders_bp.route('/process', methods=['POST'])
@role_required('admin', 'manager', 'sales')
def process():
    """주문서 + 옵션 파일 업로드 → 처리"""
    # 폼 데이터 수집
    platform = request.form.get('platform', '스마트스토어')
    action = request.form.get('action', 'invoice')

    order_file = request.files.get('order_file')
    option_file = request.files.get('option_file')
    invoice_file = request.files.get('invoice_file')

    if not order_file or not _allowed(order_file.filename):
        flash('주문서 엑셀 파일(.xlsx/.xls/.csv)을 선택하세요.', 'danger')
        return redirect(url_for('orders.index'))

    if not option_file or not option_file.filename or not _allowed(option_file.filename):
        flash('옵션리스트 엑셀 파일(.xlsx/.xls/.csv)을 선택하세요.', 'danger')
        return redirect(url_for('orders.index'))

    # 플랫폼 → 모드 매핑
    mode = platform
    if platform == '옥션G마켓':
        mode = '옥션/G마켓'

    # 액션 → 처리유형 매핑
    action_map = {
        'invoice': '송장',
        'realpacking': '리얼패킹',
        'external_batch': '외부일괄',
    }
    target_type = action_map.get(action, '송장')

    upload_dir = current_app.config['UPLOAD_FOLDER']
    output_dir = current_app.config['OUTPUT_FOLDER']
    os.makedirs(upload_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    # 파일 저장
    order_path = os.path.join(upload_dir, secure_filename(order_file.filename))
    order_file.save(order_path)

    option_path = os.path.join(upload_dir, secure_filename(option_file.filename))
    option_file.save(option_path)

    invoice_path = None
    if invoice_file and invoice_file.filename and _allowed(invoice_file.filename):
        invoice_path = os.path.join(upload_dir, secure_filename(invoice_file.filename))
        invoice_file.save(invoice_path)

    try:
        from services.order_processor import OrderProcessor

        processor = OrderProcessor()
        result = processor.run(mode, order_path, option_path,
                               invoice_path, target_type, output_dir)

        if result.get('error'):
            flash(result['error'], 'danger')

        if result.get('success'):
            flash(f"[{platform}] {target_type} 처리 완료!", 'success')

        # 다운로드 링크 생성
        downloads = []
        for fpath in result.get('files', []):
            fname = os.path.basename(fpath)
            downloads.append({
                'name': fname,
                'url': url_for('orders.download', filename=fname),
            })

        # 최근 처리 결과 파일 목록
        result_files = sorted(
            [f for f in os.listdir(output_dir)
             if f.endswith('.xlsx') and not f.startswith('집계')
             and not f.startswith('통합')],
            reverse=True,
        )[:20]

        return render_template('orders/index.html',
                               result={'logs': result.get('logs', []),
                                       'downloads': downloads},
                               result_files=result_files)

    except Exception as e:
        flash(f'온라인주문처리 중 오류: {e}', 'danger')
        return redirect(url_for('orders.index'))
    finally:
        # 업로드 파일 정리
        if os.path.exists(order_path):
            os.remove(order_path)
        if os.path.exists(option_path):
            os.remove(option_path)
        if invoice_path and os.path.exists(invoice_path):
            os.remove(invoice_path)


@orders_bp.route('/download/<filename>')
@role_required('admin', 'manager', 'sales')
def download(filename):
    """처리 결과 파일 다운로드"""
    output_dir = current_app.config['OUTPUT_FOLDER']
    filepath = os.path.join(output_dir, secure_filename(filename))

    if not os.path.exists(filepath):
        flash('파일을 찾을 수 없습니다.', 'danger')
        return redirect(url_for('orders.index'))

    return send_file(
        filepath,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=filename,
    )
