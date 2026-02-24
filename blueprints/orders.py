"""
orders.py — 온라인주문처리 Blueprint (기존 1611.py 기능).
주문서 업로드 → 옵션매칭(DB) → 가공 → 결과 엑셀 다운로드.
매칭 실패 시 AJAX 팝업으로 옵션 등록 후 재처리 지원.
"""
import os
import uuid
from datetime import datetime

import pandas as pd
from flask import (
    Blueprint, render_template, request, current_app,
    flash, redirect, url_for, send_file, abort, jsonify, session,
)
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename

from auth import role_required

orders_bp = Blueprint('orders', __name__, url_prefix='/orders')

ALLOWED_EXT = {'xlsx', 'xls', 'csv'}


def _allowed(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXT


def _cleanup_file(path):
    """안전한 파일 삭제"""
    if path and os.path.exists(path):
        try:
            os.remove(path)
        except Exception:
            pass


def _get_result_files(output_dir):
    """최근 처리 결과 파일 목록"""
    if os.path.exists(output_dir):
        return sorted(
            [f for f in os.listdir(output_dir)
             if f.endswith('.xlsx') and not f.startswith('집계')
             and not f.startswith('통합')],
            reverse=True,
        )[:20]
    return []


def _import_option_file_to_db(filepath):
    """옵션 엑셀 파일을 option_master DB에 일괄 등록 (additive upsert)."""
    try:
        if filepath.lower().endswith('.csv'):
            try:
                df = pd.read_csv(filepath, encoding='utf-8-sig', header=None, dtype=str).fillna('')
            except Exception:
                df = pd.read_csv(filepath, encoding='cp949', header=None, dtype=str).fillna('')
        else:
            df = pd.read_excel(filepath, header=None, dtype=str).fillna('')

        if len(df.columns) < 6:
            return 0

        payload = []
        for _, row in df.iterrows():
            orig = str(row.iloc[0]).strip()
            if not orig:
                continue
            payload.append({
                'original_name': orig,
                'product_name': str(row.iloc[1]).strip(),
                'line_code': str(row.iloc[2]).strip(),
                'sort_order': int(v) if not pd.isna(v := pd.to_numeric(row.iloc[4], errors='coerce')) else 999,
                'barcode': str(row.iloc[5]).strip() if len(row) > 5 else '',
            })

        if payload:
            current_app.db.insert_option_master_batch(payload)
        return len(payload)
    except Exception:
        return 0


@orders_bp.route('/')
@role_required('admin', 'manager', 'sales')
def index():
    """온라인주문처리 업로드 폼"""
    output_dir = current_app.config['OUTPUT_FOLDER']
    result_files = _get_result_files(output_dir)

    # 옵션마스터 DB 건수 표시
    try:
        option_count = current_app.db.count_option_master()
    except Exception:
        option_count = 0

    return render_template('orders/index.html',
                           result_files=result_files,
                           option_count=option_count)


@orders_bp.route('/process', methods=['POST'])
@role_required('admin', 'manager', 'sales')
def process():
    """주문서 업로드 → 처리 (옵션은 DB에서 로드, 파일은 선택사항)"""
    platform = request.form.get('platform', '스마트스토어')
    action = request.form.get('action', 'invoice')

    order_file = request.files.get('order_file')
    option_file = request.files.get('option_file')
    invoice_file = request.files.get('invoice_file')

    if not order_file or not _allowed(order_file.filename):
        flash('주문서 엑셀 파일(.xlsx/.xls/.csv)을 선택하세요.', 'danger')
        return redirect(url_for('orders.index'))

    # 플랫폼 → 모드 매핑
    mode = platform
    if platform == '옥션G마켓':
        mode = '옥션/G마켓'

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

    # 주문서 파일 저장 (고유 이름으로 재처리 지원)
    unique_id = uuid.uuid4().hex[:8]
    order_ext = order_file.filename.rsplit('.', 1)[1].lower() if '.' in order_file.filename else 'xlsx'
    order_saved = f"order_{unique_id}.{order_ext}"
    order_path = os.path.join(upload_dir, order_saved)
    order_file.save(order_path)

    # 옵션 파일 (선택사항: 업로드 시 DB에 추가 등록)
    option_path = None
    option_source = 'db'
    if option_file and option_file.filename and _allowed(option_file.filename):
        opt_ext = option_file.filename.rsplit('.', 1)[1].lower() if '.' in option_file.filename else 'xlsx'
        option_path = os.path.join(upload_dir, f"option_{unique_id}.{opt_ext}")
        option_file.save(option_path)
        imported = _import_option_file_to_db(option_path)
        if imported > 0:
            flash(f'옵션리스트 {imported}건 DB에 추가 등록됨', 'info')

    # 송장 파일
    invoice_path = None
    if invoice_file and invoice_file.filename and _allowed(invoice_file.filename):
        invoice_path = os.path.join(upload_dir, secure_filename(invoice_file.filename))
        invoice_file.save(invoice_path)

    try:
        from services.order_processor import OrderProcessor

        processor = OrderProcessor()
        result = processor.run(mode, order_path, option_path,
                               invoice_path, target_type, output_dir,
                               db=current_app.db, option_source=option_source)

        # 미매칭 항목 발견 → 모달 팝업으로 등록 유도
        if result.get('unmatched'):
            # 재처리를 위해 세션에 컨텍스트 보관
            session['order_reprocess'] = {
                'order_path': order_path,
                'invoice_path': invoice_path,
                'mode': mode,
                'target_type': target_type,
                'platform': platform,
            }

            try:
                option_count = current_app.db.count_option_master()
            except Exception:
                option_count = 0

            return render_template('orders/index.html',
                                   unmatched_items=result['unmatched'],
                                   result_files=_get_result_files(output_dir),
                                   option_count=option_count)

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

        # 성공 시 파일 정리
        _cleanup_file(order_path)
        _cleanup_file(option_path)

        try:
            option_count = current_app.db.count_option_master()
        except Exception:
            option_count = 0

        return render_template('orders/index.html',
                               result={'logs': result.get('logs', []),
                                       'downloads': downloads},
                               result_files=_get_result_files(output_dir),
                               option_count=option_count)

    except Exception as e:
        flash(f'온라인주문처리 중 오류: {e}', 'danger')
        _cleanup_file(order_path)
        _cleanup_file(option_path)
        return redirect(url_for('orders.index'))
    finally:
        _cleanup_file(option_path)
        if invoice_path:
            _cleanup_file(invoice_path)


# ================================================================
# AJAX API: 옵션 검색 / 등록 / 재처리
# ================================================================

@orders_bp.route('/api/option-search')
@role_required('admin', 'manager', 'sales')
def api_option_search():
    """옵션마스터 검색 API (AJAX)"""
    keyword = request.args.get('q', '').strip()
    if not keyword or len(keyword) < 2:
        return jsonify([])
    try:
        results = current_app.db.search_option_master(keyword)
        return jsonify([{
            'id': r['id'],
            'original_name': r.get('original_name', ''),
            'product_name': r.get('product_name', ''),
            'line_code': r.get('line_code', '0'),
            'sort_order': r.get('sort_order', 999),
            'barcode': r.get('barcode', ''),
        } for r in results[:20]])
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@orders_bp.route('/api/option-register', methods=['POST'])
@role_required('admin', 'manager', 'sales')
def api_option_register():
    """옵션마스터 등록 API (AJAX) — 미매칭 항목을 기존 옵션에 매핑 또는 신규 등록"""
    data = request.get_json()
    if not data:
        return jsonify({'error': '데이터가 없습니다.'}), 400

    original_name = data.get('original_name', '').strip()
    product_name = data.get('product_name', '').strip()
    line_code = data.get('line_code', '0').strip()
    sort_order = int(data.get('sort_order', 999))
    barcode = data.get('barcode', '').strip()

    if not original_name or not product_name:
        return jsonify({'error': '원문명과 품목명은 필수입니다.'}), 400

    try:
        current_app.db.insert_option_master({
            'original_name': original_name,
            'product_name': product_name,
            'line_code': line_code,
            'sort_order': sort_order,
            'barcode': barcode,
        })
        return jsonify({'success': True, 'message': f'{original_name} → {product_name} 등록 완료'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@orders_bp.route('/api/reprocess', methods=['POST'])
@role_required('admin', 'manager', 'sales')
def api_reprocess():
    """옵션 등록 후 재처리 API (AJAX)"""
    reprocess = session.get('order_reprocess')
    if not reprocess:
        return jsonify({'error': '재처리 데이터가 없습니다. 주문서를 다시 업로드하세요.'}), 400

    order_path = reprocess['order_path']
    if not os.path.exists(order_path):
        session.pop('order_reprocess', None)
        return jsonify({'error': '주문서 파일이 만료되었습니다. 다시 업로드하세요.'}), 400

    try:
        from services.order_processor import OrderProcessor
        output_dir = current_app.config['OUTPUT_FOLDER']
        processor = OrderProcessor()
        result = processor.run(
            reprocess['mode'], order_path, None,
            reprocess.get('invoice_path'), reprocess['target_type'], output_dir,
            db=current_app.db, option_source='db'
        )

        if result.get('unmatched'):
            return jsonify({
                'success': False,
                'unmatched': result['unmatched'],
                'message': f"아직 {len(result['unmatched'])}건 미등록"
            })

        if result.get('error'):
            return jsonify({'success': False, 'error': result['error']})

        # 성공 → 정리
        session.pop('order_reprocess', None)
        _cleanup_file(order_path)

        downloads = [{'name': os.path.basename(f),
                      'url': url_for('orders.download', filename=os.path.basename(f))}
                     for f in result.get('files', [])]

        return jsonify({
            'success': True,
            'message': f"[{reprocess['platform']}] {reprocess['target_type']} 처리 완료!",
            'downloads': downloads,
            'logs': result.get('logs', [])
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@orders_bp.route('/api/cancel-reprocess', methods=['POST'])
@role_required('admin', 'manager', 'sales')
def api_cancel_reprocess():
    """재처리 취소 시 임시 파일 정리"""
    reprocess = session.pop('order_reprocess', None)
    if reprocess:
        _cleanup_file(reprocess.get('order_path'))
    return jsonify({'success': True})


# ================================================================
# 파일 다운로드
# ================================================================

@orders_bp.route('/download/<path:filename>')
@role_required('admin', 'manager', 'sales')
def download(filename):
    """처리 결과 파일 다운로드 (한글 파일명 지원)"""
    output_dir = os.path.abspath(current_app.config['OUTPUT_FOLDER'])
    safe_name = os.path.basename(filename)
    filepath = os.path.join(output_dir, safe_name)

    if not os.path.abspath(filepath).startswith(output_dir):
        abort(403)

    if not os.path.exists(filepath):
        flash('파일을 찾을 수 없습니다.', 'danger')
        return redirect(url_for('orders.index'))

    return send_file(
        filepath,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=safe_name,
    )
