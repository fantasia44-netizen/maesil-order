"""
orders.py — 주문 처리 Blueprint (기존 1611.py 기능).
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

ALLOWED_EXT = {'xlsx', 'xls'}


def _allowed(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXT


@orders_bp.route('/')
@role_required('admin', 'manager', 'sales')
def index():
    """주문 처리 업로드 폼"""
    # 기존 결과 파일 목록
    output_dir = current_app.config['OUTPUT_FOLDER']
    result_files = []
    if os.path.exists(output_dir):
        result_files = sorted(
            [f for f in os.listdir(output_dir)
             if f.startswith('주문') and f.endswith('.xlsx')],
            reverse=True,
        )[:20]

    return render_template('orders/index.html', result_files=result_files)


@orders_bp.route('/process', methods=['POST'])
@role_required('admin', 'manager', 'sales')
def process():
    """주문서 + 옵션 파일 업로드 → 처리"""
    order_file = request.files.get('order_file')
    option_file = request.files.get('option_file')

    if not order_file or not _allowed(order_file.filename):
        flash('주문서 엑셀 파일(.xlsx/.xls)을 선택하세요.', 'danger')
        return redirect(url_for('orders.index'))

    upload_dir = current_app.config['UPLOAD_FOLDER']
    output_dir = current_app.config['OUTPUT_FOLDER']
    os.makedirs(upload_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    order_filename = secure_filename(order_file.filename)
    order_path = os.path.join(upload_dir, order_filename)
    order_file.save(order_path)

    option_path = None
    if option_file and option_file.filename and _allowed(option_file.filename):
        option_filename = secure_filename(option_file.filename)
        option_path = os.path.join(upload_dir, option_filename)
        option_file.save(option_path)

    try:
        from services.order_service import process_orders

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_filename = f"주문처리결과_{timestamp}.xlsx"
        output_path = os.path.join(output_dir, output_filename)

        result = process_orders(
            db=current_app.db,
            order_path=order_path,
            option_path=option_path,
            output_path=output_path,
        )

        if result.get('warnings'):
            for w in result['warnings']:
                flash(w, 'warning')

        flash(f"주문 처리 완료: {result.get('count', 0)}건 → {output_filename}", 'success')
        return redirect(url_for('orders.download', filename=output_filename))

    except Exception as e:
        flash(f'주문 처리 중 오류: {e}', 'danger')
    finally:
        # 업로드 파일 정리
        if os.path.exists(order_path):
            os.remove(order_path)
        if option_path and os.path.exists(option_path):
            os.remove(option_path)

    return redirect(url_for('orders.index'))


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
