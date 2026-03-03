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
    """최근 처리 결과 파일 목록 — 파일 수정일 기준 최신순, 최대 100건"""
    if os.path.exists(output_dir):
        files = [
            f for f in os.listdir(output_dir)
            if (f.endswith('.xlsx') or f.endswith('.xls')) and not f.startswith('집계')
            and not f.startswith('통합')
        ]
        # 파일 수정일 기준 최신순 정렬
        files.sort(key=lambda f: os.path.getmtime(os.path.join(output_dir, f)), reverse=True)
        return files[:100]
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

    # ── 채널 자동감지: 파일 컬럼으로 실제 채널 판별 (빠르게 헤더만) ──
    try:
        from services.order_processor import OrderProcessor
        from services.channel_config import detect_channel

        detected = None
        try:
            _det_df = None
            ext = order_ext.lower()

            if ext == 'csv':
                _det_df = pd.read_csv(order_path, encoding='utf-8-sig', nrows=0)
            else:
                # 암호화 여부를 파일 매직넘버로 빠르게 판별
                _is_ole2 = False
                with open(order_path, 'rb') as f:
                    _is_ole2 = f.read(4) == b'\xd0\xcf\x11\xe0'

                if _is_ole2:
                    # OLE2 = 암호화 엑셀 (스마트스토어) → 복호화 후 헤더만
                    import io, msoffcrypto
                    with open(order_path, 'rb') as f:
                        dec = msoffcrypto.OfficeFile(f)
                        dec.load_key(password='1111')
                        buf = io.BytesIO()
                        dec.decrypt(buf)
                        buf.seek(0)
                        _det_df = pd.read_excel(buf, header=0, nrows=0)
                else:
                    # 일반 엑셀 → nrows=0 으로 헤더만 (매우 빠름)
                    try:
                        _det_df = pd.read_excel(order_path, header=0, nrows=0)
                    except Exception:
                        _det_df = pd.read_excel(order_path, header=2, nrows=0)

            if _det_df is not None and len(_det_df.columns) > 3:
                detected = detect_channel(_det_df)

            if detected and detected != mode:
                # 해미애찬은 스마트스토어와 동일 포맷이므로 자동감지 시 교정하지 않음
                if mode == '해미애찬' and detected == '스마트스토어':
                    pass  # 사용자 선택 유지
                else:
                    flash(f'⚠️ 선택: [{mode}] → 파일 감지: [{detected}] — [{detected}]로 자동 교정합니다.', 'warning')
                    mode = detected
        except Exception:
            pass  # 감지 실패해도 처리는 계속

        processor = OrderProcessor()
        result = processor.run(mode, order_path, option_path,
                               invoice_path, target_type, output_dir,
                               db=current_app.db, option_source=option_source,
                               save_to_db=True,
                               uploaded_by=current_user.username if current_user.is_authenticated else '')

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
                                       'downloads': downloads,
                                       'db_result': result.get('db_result')},
                               result_files=_get_result_files(output_dir),
                               option_count=option_count)

    except Exception as e:
        flash(f'온라인주문처리 중 오류: {e}', 'danger')
        _cleanup_file(order_path)
        _cleanup_file(option_path)
        return redirect(url_for('orders.index'))
    finally:
        _cleanup_file(option_path)
        # 재처리 대기 중이면 invoice 삭제하지 않음
        if invoice_path and not session.get('order_reprocess'):
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
            db=current_app.db, option_source='db',
            save_to_db=True,
            uploaded_by=current_user.username if current_user.is_authenticated else ''
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
        _cleanup_file(reprocess.get('invoice_path'))

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
        _cleanup_file(reprocess.get('invoice_path'))
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


# ================================================================
# Phase 1: 주문 관리 (검색/상세/수정/취소)
# ================================================================

@orders_bp.route('/manage')
@role_required('admin', 'manager', 'sales')
def manage():
    """주문 관리 페이지"""
    return render_template('orders/manage.html')


@orders_bp.route('/api/orders')
@role_required('admin', 'manager', 'sales')
def api_orders():
    """주문 목록 조회 API (확장 검색: 송장번호/수취인명 지원)"""
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')
    channel = request.args.get('channel')
    status = request.args.get('status')
    search = request.args.get('search')
    search_field = request.args.get('search_field', 'all')
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 50))
    offset = (page - 1) * per_page

    orders = current_app.db.query_order_transactions_extended(
        date_from=date_from, date_to=date_to,
        channel=channel, status=status,
        search=search, search_field=search_field,
        limit=per_page, offset=offset
    )
    # DEBUG: 첫 주문의 recipient_name 확인
    if orders:
        o0 = orders[0]
        print(f"[DEBUG] api_orders: recipient_name={o0.get('recipient_name','MISSING')}, invoice_no={o0.get('invoice_no','MISSING')}, keys_sample={list(o0.keys())[:5]}")
    return jsonify({'orders': orders, 'page': page, 'per_page': per_page})


@orders_bp.route('/api/orders/<int:order_id>')
@role_required('admin', 'manager', 'sales')
def api_order_detail(order_id):
    """주문 상세 조회 (배송정보 + 변경이력)"""
    txn = current_app.db.query_order_transaction_by_id(order_id)
    if not txn:
        return jsonify({'error': '주문을 찾을 수 없습니다'}), 404

    shipping = current_app.db.query_order_shipping(txn['channel'], txn['order_no'])
    change_log = current_app.db.query_order_change_log(order_id)

    return jsonify({
        'transaction': txn,
        'shipping': shipping,
        'change_log': change_log
    })


@orders_bp.route('/api/orders/<int:order_id>/edit', methods=['POST'])
@role_required('admin', 'manager')
def api_order_edit(order_id):
    """주문 수정 (RPC) — 수량 변경 시 재고+매출 자동 역분개+재처리"""
    data = request.get_json()
    if not data:
        return jsonify({'error': '요청 데이터 없음'}), 400

    payload = data.get('payload', {})
    reason = data.get('reason', '')
    if not reason:
        return jsonify({'error': '변경 사유를 입력하세요'}), 400

    # 수량 변경 시 재고/매출 역분개 → 재처리
    order = current_app.db.query_order_transaction_by_id(order_id)
    new_qty = payload.get('qty')
    need_reprocess = (
        new_qty is not None
        and order
        and order.get('is_outbound_done')
        and int(new_qty) != int(order.get('qty', 0))
    )

    if need_reprocess:
        from services.order_to_stock_service import (
            reverse_order_stock, process_single_order_realtime
        )
        # 1. 기존 출고 역분개
        reversal = reverse_order_stock(current_app.db, order_id)

        # 2. 필드 변경 (RPC)
        result = current_app.db.cancel_or_edit_order(
            order_id=order_id, change_type='수정',
            payload=payload, reason=reason,
            user=current_user.username if current_user.is_authenticated else ''
        )

        # 3. 새 수량으로 재처리
        reprocess = process_single_order_realtime(current_app.db, order_id)
        result['reversal'] = reversal
        result['reprocess'] = reprocess
    else:
        result = current_app.db.cancel_or_edit_order(
            order_id=order_id, change_type='수정',
            payload=payload, reason=reason,
            user=current_user.username if current_user.is_authenticated else ''
        )

    return jsonify(result)


@orders_bp.route('/api/orders/<int:order_id>/cancel', methods=['POST'])
@role_required('admin', 'manager')
def api_order_cancel(order_id):
    """주문 취소/환불 (RPC) — 출고 처리된 주문은 재고+매출 자동 역분개"""
    data = request.get_json()
    if not data:
        return jsonify({'error': '요청 데이터 없음'}), 400

    change_type = data.get('type', '취소')
    if change_type not in ('취소', '환불'):
        return jsonify({'error': '올바르지 않은 변경 유형'}), 400

    reason = data.get('reason', '')
    if not reason:
        return jsonify({'error': '취소/환불 사유를 입력하세요'}), 400

    # 취소 전 주문 데이터 확보
    order = current_app.db.query_order_transaction_by_id(order_id)

    # RPC로 상태 변경
    result = current_app.db.cancel_or_edit_order(
        order_id=order_id,
        change_type=change_type,
        payload={},
        reason=reason,
        user=current_user.username if current_user.is_authenticated else ''
    )

    # 출고 처리된 주문이면 재고+매출 역분개 (실시간 반영)
    if result.get('success') and order and order.get('is_outbound_done'):
        try:
            from services.order_to_stock_service import reverse_order_stock
            reversal = reverse_order_stock(current_app.db, order_id)
            result['reversal'] = reversal
        except Exception as e:
            result['reversal_error'] = str(e)

    return jsonify(result)


# ================================================================
# 송장 관리 API
# ================================================================

@orders_bp.route('/api/shipping/search')
@role_required('admin', 'manager', 'sales')
def api_shipping_search():
    """송장번호/수취인명으로 주문 검색"""
    keyword = request.args.get('keyword', '').strip()
    field = request.args.get('field', 'all')  # all, invoice, name

    if not keyword or len(keyword) < 2:
        return jsonify({'error': '검색어를 2글자 이상 입력하세요'}), 400

    results = current_app.db.search_order_shipping(keyword, field=field)
    return jsonify({'results': results})


@orders_bp.route('/api/shipping/update-invoice', methods=['POST'])
@role_required('admin', 'manager')
def api_update_invoice():
    """송장번호 업데이트 (단건 or 일괄)"""
    data = request.get_json()
    if not data:
        return jsonify({'error': '요청 데이터 없음'}), 400

    updates = data.get('updates', [])
    if not updates:
        # 단건 업데이트
        channel = data.get('channel')
        order_no = data.get('order_no')
        invoice_no = data.get('invoice_no')
        courier = data.get('courier')
        if not all([channel, order_no, invoice_no]):
            return jsonify({'error': '필수 항목 누락'}), 400
        ok = current_app.db.update_order_shipping_invoice(
            channel, order_no, invoice_no, courier,
            shipping_status='발송'
        )
        return jsonify({'success': ok, 'updated': 1 if ok else 0})
    else:
        # 일괄 업데이트
        count = current_app.db.bulk_update_shipping_invoices(updates)
        return jsonify({'success': True, 'updated': count})


@orders_bp.route('/api/reprocess-revenue', methods=['POST'])
@role_required('admin')
def api_reprocess_revenue():
    """출고 완료됐지만 매출 누락된 주문의 매출만 재생성"""
    data = request.get_json() or {}
    date_from = data.get('date_from')
    date_to = data.get('date_to')

    from services.order_to_stock_service import reprocess_revenue_only
    result = reprocess_revenue_only(current_app.db, date_from=date_from, date_to=date_to)
    return jsonify(result)


@orders_bp.route('/api/import-runs')
@role_required('admin', 'manager', 'sales')
def api_import_runs():
    """업로드 이력 목록"""
    runs = current_app.db.query_import_runs(limit=50)
    return jsonify({'runs': runs})


@orders_bp.route('/api/import-runs/<int:run_id>')
@role_required('admin', 'manager', 'sales')
def api_import_run_detail(run_id):
    """업로드 상세 결과"""
    run = current_app.db.query_import_run_by_id(run_id)
    if not run:
        return jsonify({'error': '업로드 이력을 찾을 수 없습니다'}), 404
    return jsonify(run)


# ================================================================
# N배송 수동입력
# ================================================================

@orders_bp.route('/n-delivery')
@role_required('admin', 'manager', 'sales')
def n_delivery():
    """N배송 수동입력 페이지"""
    # 옵션마스터에서 품목 목록 로드
    products = []
    try:
        opt_list = current_app.db.query_option_master_as_list()
        if opt_list:
            seen = set()
            for o in opt_list:
                name = str(o.get('품목명', '')).strip()
                if name and name.lower() not in ('standard_name', 'product_name', '품목명') and name not in seen:
                    seen.add(name)
                    products.append({
                        'name': name,
                        'barcode': o.get('바코드', ''),
                        'line_code': o.get('라인코드', 0),
                        'sort_order': o.get('출력순서', 999),
                    })
    except Exception:
        pass

    return render_template('orders/n_delivery.html', products=products)


# ================================================================
# Phase 2: 주문 → 출고+매출 자동처리
# ================================================================

@orders_bp.route('/api/process-outbound', methods=['POST'])
@role_required('admin', 'manager')
def api_process_outbound():
    """미처리 주문 자동 출고+매출 처리 (Phase 2)"""
    data = request.get_json() or {}
    date_from = data.get('date_from')
    date_to = data.get('date_to')
    channel = data.get('channel') or None
    force_shortage = data.get('force_shortage', False)

    if not date_from or not date_to:
        return jsonify({'error': '날짜 범위를 지정하세요'}), 400

    try:
        from services.order_to_stock_service import process_orders_to_stock
        result = process_orders_to_stock(
            current_app.db,
            date_from=date_from,
            date_to=date_to,
            channel=channel,
            force_shortage=force_shortage,
        )
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@orders_bp.route('/api/outbound-status')
@role_required('admin', 'manager', 'sales')
def api_outbound_status():
    """출고 처리 현황 (미처리/완료 건수)"""
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')
    try:
        summary = current_app.db.query_outbound_summary(
            date_from=date_from, date_to=date_to)
        return jsonify(summary)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@orders_bp.route('/api/n-delivery', methods=['POST'])
@role_required('admin', 'manager', 'sales')
def api_n_delivery():
    """N배송 수동입력 저장"""
    data = request.get_json()
    if not data:
        return jsonify({'error': '요청 데이터 없음'}), 400

    items = data.get('items', [])
    order_date = data.get('order_date', '')
    if not items:
        return jsonify({'error': '입력할 항목이 없습니다'}), 400
    if not order_date:
        return jsonify({'error': '매출일자를 입력하세요'}), 400

    db = current_app.db
    username = current_user.username if current_user.is_authenticated else ''

    # import_runs 생성
    import_run_id = db.create_import_run(
        channel='N배송_수동',
        filename=f'수동입력_{order_date}',
        file_hash=None,
        uploaded_by=username,
        total_rows=len(items),
    )
    if not import_run_id:
        return jsonify({'error': 'import_runs 생성 실패'}), 500

    # 주문 배열 구성
    import hashlib, json
    orders = []
    for i, item in enumerate(items):
        product_name = item.get('product_name', '')
        qty = int(item.get('qty', 0))
        if not product_name or qty <= 0:
            continue

        order_no = f"NDEL_{order_date.replace('-', '')}_{i+1:03d}"
        raw_data = {"product_name": product_name, "qty": qty, "order_date": order_date, "source": "N배송_수동"}
        raw_hash = hashlib.sha256(json.dumps(raw_data, sort_keys=True, ensure_ascii=False).encode()).hexdigest()

        transaction = {
            "channel": "N배송_수동",
            "order_date": order_date,
            "order_no": order_no,
            "line_no": 1,
            "original_option": "",
            "original_product": product_name,
            "raw_data": raw_data,
            "raw_hash": raw_hash,
            "parser_version": "1.0",
            "product_name": product_name,
            "barcode": item.get('barcode', ''),
            "line_code": int(item.get('line_code', 0)),
            "sort_order": int(item.get('sort_order', 999)),
            "qty": qty,
            "unit_price": 0,
            "total_amount": 0,
            "discount_amount": 0,
            "settlement": 0,
            "commission": 0,
        }
        orders.append({"transaction": transaction, "shipping": None})

    if not orders:
        return jsonify({'error': '유효한 입력 항목이 없습니다'}), 400

    result = db.upsert_order_batch(import_run_id, orders)
    return jsonify({
        'success': True,
        'message': f'N배송 {len(orders)}건 저장 완료',
        'result': result
    })
