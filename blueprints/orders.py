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

from auth import role_required, _log_action
from services.storage_helper import backup_to_storage
from db_utils import get_db
from services.channel_config import is_naver, resolve_channel

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
            get_db().insert_option_master_batch(payload)
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
        option_count = get_db().count_option_master()
    except Exception:
        option_count = 0

    return render_template('orders/index.html',
                           result_files=result_files,
                           option_count=option_count)


@orders_bp.route('/process', methods=['POST'])
@role_required('admin', 'manager', 'sales')
def process():
    """주문서 업로드 → 처리 (옵션은 DB에서 로드, 파일은 선택사항)"""
    platform = request.form.get('platform', '스마트스토어_배마마')
    action = request.form.get('action', 'invoice')
    collection_date = request.form.get('collection_date', '').strip() or None

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
    backup_to_storage(get_db(), order_path, 'upload', 'orders')

    # 옵션 파일 (선택사항: 업로드 시 DB에 추가 등록)
    option_path = None
    option_source = 'db'
    if option_file and option_file.filename and _allowed(option_file.filename):
        opt_ext = option_file.filename.rsplit('.', 1)[1].lower() if '.' in option_file.filename else 'xlsx'
        option_path = os.path.join(upload_dir, f"option_{unique_id}.{opt_ext}")
        option_file.save(option_path)
        backup_to_storage(get_db(), option_path, 'upload', 'orders')
        imported = _import_option_file_to_db(option_path)
        if imported > 0:
            flash(f'옵션리스트 {imported}건 DB에 추가 등록됨', 'info')

    # 송장 파일
    invoice_path = None
    if invoice_file and invoice_file.filename and _allowed(invoice_file.filename):
        invoice_path = os.path.join(upload_dir, secure_filename(invoice_file.filename))
        invoice_file.save(invoice_path)
        backup_to_storage(get_db(), invoice_path, 'upload', 'orders')

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
                # 네이버 채널 간에는 동일 포맷이므로 자동감지 시 교정하지 않음
                if is_naver(mode) and is_naver(detected):
                    pass  # 사용자 선택 유지
                else:
                    flash(f'⚠️ 선택: [{mode}] → 파일 감지: [{detected}] — [{detected}]로 자동 교정합니다.', 'warning')
                    mode = detected
        except Exception:
            pass  # 감지 실패해도 처리는 계속

        processor = OrderProcessor()
        # 리얼패킹/외부일괄은 송장 후처리이므로 DB 저장·재고차감 불필요
        should_save = target_type == '송장'
        result = processor.run(mode, order_path, option_path,
                               invoice_path, target_type, output_dir,
                               db=get_db(), option_source=option_source,
                               save_to_db=should_save,
                               uploaded_by=current_user.username if current_user.is_authenticated else '',
                               collection_date=collection_date)

        # 미매칭 항목 발견 → 모달 팝업으로 등록 유도
        if result.get('unmatched'):
            # 재처리를 위해 세션에 컨텍스트 보관
            session['order_reprocess'] = {
                'order_path': order_path,
                'invoice_path': invoice_path,
                'mode': mode,
                'target_type': target_type,
                'platform': platform,
                'collection_date': collection_date,
            }

            try:
                option_count = get_db().count_option_master()
            except Exception:
                option_count = 0

            return render_template('orders/index.html',
                                   unmatched_items=result['unmatched'],
                                   result_files=_get_result_files(output_dir),
                                   option_count=option_count)

        if result.get('error'):
            flash(result['error'], 'danger')

        if result.get('success'):
            db_res = result.get('db_result') or {}
            _log_action('process_order',
                         detail=f'[{platform}] {target_type} 처리 완료 — '
                                f'신규 {db_res.get("inserted", 0)}건, '
                                f'갱신 {db_res.get("updated", 0)}건')
            flash(f"[{platform}] {target_type} 처리 완료!", 'success')

        # 다채널 중복 경고 표시
        db_res = result.get('db_result') or {}
        cross_skip = db_res.get('cross_channel_skipped', 0)
        if cross_skip:
            flash(f"⚠️ 다른 채널에 이미 등록된 동일 주문 {cross_skip}건이 자동 스킵되었습니다. "
                  f"같은 주문을 여러 채널에 등록하면 재고가 이중 차감됩니다.", 'warning')

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
            option_count = get_db().count_option_master()
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
        results = get_db().search_option_master(keyword)
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
        get_db().insert_option_master({
            'original_name': original_name,
            'product_name': product_name,
            'line_code': line_code,
            'sort_order': sort_order,
            'barcode': barcode,
        })
        # 등록 성공 후 캐시 강제 갱신 (재처리 대비)
        match_key = original_name.replace(' ', '').upper()
        print(f"[OPTION-REG] OK: '{original_name}' -> '{product_name}' match_key='{match_key}'")
        _log_action('register_option',
                     detail=f'옵션 등록: {original_name} → {product_name} '
                            f'(라인:{line_code}, 순서:{sort_order})')
        return jsonify({'success': True, 'message': f'{original_name} -> {product_name} 등록 완료'})
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

        # 옵션 등록 직후 재처리 — 캐시 완전 우회하여 DB에서 직접 로드
        fresh_opts = get_db().query_option_master_as_list(use_cache=False)
        print(f"[REPROCESS] 옵션마스터 DB 직접 로드: {len(fresh_opts)}건 (캐시 우회)")

        processor = OrderProcessor()
        result = processor.run(
            reprocess['mode'], order_path, None,
            reprocess.get('invoice_path'), reprocess['target_type'], output_dir,
            db=get_db(), option_source='db',
            save_to_db=True,
            uploaded_by=current_user.username if current_user.is_authenticated else '',
            collection_date=reprocess.get('collection_date'),
            opt_list_override=fresh_opts,
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

        _log_action('reprocess_order',
                     detail=f'[{reprocess["platform"]}] {reprocess["target_type"]} 재처리 완료')
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
    outbound_filter = request.args.get('outbound', '')
    search = request.args.get('search')
    search_field = request.args.get('search_field', 'all')
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 50))
    offset = (page - 1) * per_page

    # outbound 필터 매핑: done → is_outbound_done=True, no_invoice/invoice_only → 후처리
    db_outbound = None
    if outbound_filter == 'done':
        db_outbound = 'done'
    elif outbound_filter in ('no_invoice', 'invoice_only'):
        db_outbound = 'not_done'  # 미출고만 조회 후 invoice 유무로 필터

    orders = get_db().query_order_transactions_extended(
        date_from=date_from, date_to=date_to,
        channel=channel, status=status,
        outbound=db_outbound,
        search=search, search_field=search_field,
        limit=per_page * 3 if outbound_filter in ('no_invoice', 'invoice_only') else per_page,
        offset=offset if outbound_filter not in ('no_invoice', 'invoice_only') else 0
    )

    # 송장 유무 기반 후처리 필터
    if outbound_filter == 'no_invoice':
        orders = [o for o in orders if not o.get('invoice_no')][:per_page]
    elif outbound_filter == 'invoice_only':
        orders = [o for o in orders if o.get('invoice_no')][:per_page]
    # DEBUG: 첫 주문의 recipient_name 확인
    if orders:
        o0 = orders[0]
        print(f"[DEBUG] api_orders: recipient_name={o0.get('recipient_name','MISSING')}, invoice_no={o0.get('invoice_no','MISSING')}, keys_sample={list(o0.keys())[:5]}")
    return jsonify({'orders': orders, 'page': page, 'per_page': per_page})


@orders_bp.route('/api/orders/<int:order_id>')
@role_required('admin', 'manager', 'sales')
def api_order_detail(order_id):
    """주문 상세 조회 (배송정보 + 변경이력)"""
    db = get_db()
    txn = db.query_order_transaction_by_id(order_id)
    if not txn:
        return jsonify({'error': '주문을 찾을 수 없습니다'}), 404

    shipping = db.query_order_shipping(txn['channel'], txn['order_no'])
    change_log = db.query_order_change_log(order_id)

    # 패킹 이력 조회 (해당 주문의 완료된 패킹 작업)
    packing_job = None
    if txn.get('order_no'):
        try:
            pj = db.client.table("packing_jobs").select("id,status,username,started_at,completed_at,video_duration_ms") \
                .eq("order_no", txn['order_no']).eq("status", "completed") \
                .order("completed_at", desc=True).limit(1).execute()
            if pj.data:
                packing_job = pj.data[0]
        except Exception:
            pass

    return jsonify({
        'transaction': txn,
        'shipping': shipping,
        'change_log': change_log,
        'packing_job': packing_job,
    })


@orders_bp.route('/api/products')
@role_required('admin', 'manager', 'sales')
def api_product_list():
    """stock_ledger의 고유 품목명 목록 반환 (품목 정정용)"""
    try:
        db = get_db()
        raw = db.client.table("stock_ledger") \
            .select("product_name") \
            .eq("status", "active") \
            .execute()
        names = sorted(set(
            r['product_name'] for r in (raw.data or [])
            if r.get('product_name')
        ))
        return jsonify({'products': names})
    except Exception as e:
        return jsonify({'error': str(e), 'products': []}), 500


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

    # 품목/수량 변경 시 재고/매출 역분개 → 재처리
    order = get_db().query_order_transaction_by_id(order_id)
    new_qty = payload.get('qty')
    new_product = payload.get('product_name')
    need_reprocess = (
        order
        and order.get('is_outbound_done')
        and (
            (new_qty is not None and int(new_qty) != int(order.get('qty', 0)))
            or (new_product and new_product != order.get('product_name', ''))
        )
    )

    if need_reprocess:
        from services.order_to_stock_service import (
            reverse_order_stock, process_single_order_realtime
        )
        # 1. 기존 출고 역분개
        reversal = reverse_order_stock(get_db(), order_id)

        # 2. 필드 변경 (RPC)
        result = get_db().cancel_or_edit_order(
            order_id=order_id, change_type='수정',
            payload=payload, reason=reason,
            user=current_user.username if current_user.is_authenticated else ''
        )

        # 3. 새 수량으로 재처리
        reprocess = process_single_order_realtime(get_db(), order_id)
        result['reversal'] = reversal
        result['reprocess'] = reprocess
    else:
        result = get_db().cancel_or_edit_order(
            order_id=order_id, change_type='수정',
            payload=payload, reason=reason,
            user=current_user.username if current_user.is_authenticated else ''
        )

    _log_action('edit_order', target=str(order_id),
                 old_value={'product_name': order.get('product_name', '') if order else '',
                            'qty': order.get('qty', 0) if order else 0},
                 detail=f'주문수정 #{order_id}: {reason}')
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
    order = get_db().query_order_transaction_by_id(order_id)

    # RPC로 상태 변경
    result = get_db().cancel_or_edit_order(
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
            reversal = reverse_order_stock(get_db(), order_id)
            result['reversal'] = reversal
        except Exception as e:
            result['reversal_error'] = str(e)

    _log_action('cancel_order', target=str(order_id),
                 old_value={'product_name': order.get('product_name', '') if order else '',
                            'qty': order.get('qty', 0) if order else 0,
                            'channel': order.get('channel', '') if order else ''},
                 detail=f'주문{change_type} #{order_id}: {reason}')
    return jsonify(result)


@orders_bp.route('/api/orders/bulk-cancel', methods=['POST'])
@role_required('admin', 'manager')
def api_orders_bulk_cancel():
    """주문 일괄 취소 — 선택된 주문들을 한번에 취소 (재고+매출 자동 역분개)"""
    data = request.get_json()
    if not data:
        return jsonify({'error': '요청 데이터 없음'}), 400

    order_ids = data.get('order_ids', [])
    change_type = data.get('type', '취소')
    reason = data.get('reason', '')

    if not order_ids or not isinstance(order_ids, list):
        return jsonify({'error': '취소할 주문을 선택하세요'}), 400
    if len(order_ids) > 200:
        return jsonify({'error': '한 번에 최대 200건까지 취소할 수 있습니다'}), 400
    if change_type not in ('취소', '환불'):
        return jsonify({'error': '올바르지 않은 변경 유형'}), 400
    if not reason:
        return jsonify({'error': '취소/환불 사유를 입력하세요'}), 400

    username = current_user.username if current_user.is_authenticated else ''
    results = []
    success_count = 0
    fail_count = 0
    total_stock_reversed = 0
    total_revenue_reversed = 0

    for oid in order_ids:
        try:
            order = get_db().query_order_transaction_by_id(int(oid))
            if not order:
                results.append({'order_id': oid, 'ok': False, 'error': '주문 없음'})
                fail_count += 1
                continue
            if order.get('status') != '정상':
                results.append({'order_id': oid, 'ok': False,
                                'error': f'이미 {order.get("status")} 상태'})
                fail_count += 1
                continue

            result = get_db().cancel_or_edit_order(
                order_id=int(oid), change_type=change_type,
                payload={}, reason=reason, user=username
            )

            if result.get('success') and order.get('is_outbound_done'):
                try:
                    from services.order_to_stock_service import reverse_order_stock
                    reversal = reverse_order_stock(get_db(), int(oid))
                    total_stock_reversed += reversal.get('stock_reversed', 0)
                    total_revenue_reversed += reversal.get('revenue_reversed', 0)
                    result['reversal'] = reversal
                except Exception as e:
                    result['reversal_error'] = str(e)

            if result.get('success'):
                success_count += 1
                _log_action('cancel_order', target=str(oid),
                            old_value={'product_name': order.get('product_name', ''),
                                       'qty': order.get('qty', 0),
                                       'channel': order.get('channel', '')},
                            detail=f'일괄{change_type} #{oid}: {reason}')
            else:
                fail_count += 1

            results.append({
                'order_id': oid,
                'ok': result.get('success', False),
                'error': result.get('error', ''),
                'reversal': result.get('reversal'),
                'reversal_error': result.get('reversal_error'),
            })
        except Exception as e:
            results.append({'order_id': oid, 'ok': False, 'error': str(e)})
            fail_count += 1

    return jsonify({
        'success': True,
        'total': len(order_ids),
        'success_count': success_count,
        'fail_count': fail_count,
        'total_stock_reversed': total_stock_reversed,
        'total_revenue_reversed': total_revenue_reversed,
        'results': results,
    })


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

    results = get_db().search_order_shipping(keyword, field=field)
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
        ok = get_db().update_order_shipping_invoice(
            channel, order_no, invoice_no, courier,
            shipping_status='발송'
        )
        if ok:
            _log_action('update_invoice',
                         detail=f'송장 업데이트: {channel}/{order_no} → {invoice_no}')
        return jsonify({'success': ok, 'updated': 1 if ok else 0})
    else:
        # 일괄 업데이트
        count = get_db().bulk_update_shipping_invoices(updates)
        if count > 0:
            _log_action('bulk_update_invoice',
                         detail=f'송장 일괄 업데이트 {count}건')
        return jsonify({'success': True, 'updated': count})


@orders_bp.route('/api/reprocess-revenue', methods=['POST'])
@role_required('admin')
def api_reprocess_revenue():
    """매출 재처리 — 더 이상 daily_revenue 사전계산을 하지 않으므로 비활성화.
    매출은 order_transactions에서 실시간 조회합니다."""
    return jsonify({
        'success': True,
        'message': '매출은 order_transactions에서 실시간 집계됩니다. 별도 재처리 불필요.',
        'revenue_count': 0, 'revenue_total': 0, 'processed_orders': 0,
        'errors': [], 'logs': [],
    })


@orders_bp.route('/api/import-runs')
@role_required('admin', 'manager', 'sales')
def api_import_runs():
    """업로드 이력 목록"""
    runs = get_db().query_import_runs(limit=50)
    return jsonify({'runs': runs})


@orders_bp.route('/api/import-runs/<int:run_id>')
@role_required('admin', 'manager', 'sales')
def api_import_run_detail(run_id):
    """업로드 상세 결과"""
    run = get_db().query_import_run_by_id(run_id)
    if not run:
        return jsonify({'error': '업로드 이력을 찾을 수 없습니다'}), 404
    return jsonify(run)


# ================================================================
# Import Run 롤백 (일괄취소)
# ================================================================

@orders_bp.route('/import-runs')
@role_required('admin', 'manager')
def import_runs_page():
    """업로드 이력 관리 페이지 (롤백 기능 포함)"""
    return render_template('orders/import_runs.html')


@orders_bp.route('/api/import-runs/<int:run_id>/impact')
@role_required('admin', 'manager')
def api_import_run_impact(run_id):
    """import_run 취소 영향 범위 미리보기"""
    result = get_db().get_import_run_impact(run_id)
    if result.get('error'):
        return jsonify({'error': result['error']}), 404
    return jsonify(result)


@orders_bp.route('/api/import-runs/<int:run_id>/cancel', methods=['POST'])
@role_required('admin', 'manager')
def api_cancel_import_run(run_id):
    """import_run 일괄취소 (롤백)"""
    db = get_db()
    cancelled_by = current_user.name or current_user.username

    # 영향 범위 먼저 확인
    impact = db.get_import_run_impact(run_id)
    if impact.get('error'):
        return jsonify({'error': impact['error']}), 404

    # 활성 주문 없으면 취소할 것이 없음
    if impact.get('active_count', 0) == 0:
        return jsonify({'error': '취소할 정상 주문이 없습니다.'}), 400

    # 롤백 실행
    result = db.cancel_import_run(run_id, cancelled_by)

    if result.get('error'):
        return jsonify({'error': result['error']}), 500

    # 감사 로그
    try:
        _log_action(
            action='cancel_import_run',
            target=f'import_run #{run_id}',
            detail=(
                f"import_run 일괄취소: "
                f"취소 {result['cancelled_orders']}건, "
                f"출고완료 건너뜀 {result['skipped_outbound']}건"
            ),
            new_value={
                'run_id': run_id,
                'cancelled_orders': result['cancelled_orders'],
                'skipped_outbound': result['skipped_outbound'],
            }
        )
    except Exception:
        pass

    return jsonify({
        'success': True,
        'cancelled_orders': result['cancelled_orders'],
        'skipped_outbound': result['skipped_outbound'],
    })


# ================================================================
# N배송 수동입력
# ================================================================

@orders_bp.route('/n-delivery')
@role_required('admin', 'manager', 'sales')
def n_delivery():
    """N배송 수동입력 페이지"""
    # BOM 세트옵션만 로드 (전체 품목 X)
    products = []
    try:
        bom_list = get_db().query_bom_master_all()
        if bom_list:
            seen = set()
            for b in bom_list:
                name = str(b.get('set_name', '')).strip()
                if name and name not in seen:
                    seen.add(name)
                    products.append({'name': name})
            products.sort(key=lambda x: x['name'])
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
            get_db(),
            date_from=date_from,
            date_to=date_to,
            channel=channel,
            force_shortage=force_shortage,
        )
        _log_action('process_outbound',
                     detail=f'주문→출고 자동처리 {date_from}~{date_to} '
                            f'(채널: {channel or "전체"}, '
                            f'출고 {result.get("outbound_count", 0)}건)')
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
        summary = get_db().query_outbound_summary(
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

    db = get_db()
    username = current_user.username if current_user.is_authenticated else ''

    # import_runs 생성 (반환: tuple (id, error_msg))
    import_run_id, run_err = db.create_import_run(
        channel='N배송_수동',
        filename=f'수동입력_{order_date}',
        file_hash=None,
        uploaded_by=username,
        total_rows=len(items),
    )
    if not import_run_id:
        return jsonify({'error': f'import_runs 생성 실패: {run_err}'}), 500

    # 단가 테이블 로드 (매출 자동 반영)
    import hashlib, json
    from services.product_name import canonical
    price_map = db.query_price_table()

    # 주문 배열 구성
    orders = []
    for i, item in enumerate(items):
        product_name_raw = item.get('product_name', '')
        qty = int(item.get('qty', 0))
        if not product_name_raw or qty <= 0:
            continue

        # 매칭/저장용 키: 전사 표준 canonical 적용 (공백 드리프트 방지)
        product_name = canonical(product_name_raw)

        # 단가 조회: canonical 키로 조회 (호환: 원본으로도 재시도)
        prices = price_map.get(product_name, {}) or price_map.get(product_name_raw, {})
        unit_price = prices.get('네이버판매가', 0)
        total_amount = unit_price * qty

        order_no = f"NDEL_{order_date.replace('-', '')}_{i+1:03d}"
        raw_data = {"product_name": product_name_raw, "qty": qty, "order_date": order_date, "source": "N배송_수동"}
        raw_hash = hashlib.sha256(json.dumps(raw_data, sort_keys=True, ensure_ascii=False).encode()).hexdigest()

        transaction = {
            "channel": "N배송_수동",
            "order_date": order_date,
            "order_no": order_no,
            "line_no": 1,
            "original_option": "",
            "original_product": product_name_raw,
            "raw_data": raw_data,
            "raw_hash": raw_hash,
            "parser_version": "1.0",
            "product_name": product_name,
            "barcode": item.get('barcode', ''),
            "line_code": int(item.get('line_code', 0)),
            "sort_order": int(item.get('sort_order', 999)),
            "qty": qty,
            "unit_price": unit_price,
            "total_amount": total_amount,
            "discount_amount": 0,
            "settlement": total_amount,
            "commission": 0,
            "status": "정상",
        }
        orders.append({"transaction": transaction, "shipping": None})

    if not orders:
        return jsonify({'error': '유효한 입력 항목이 없습니다'}), 400

    result = db.upsert_order_batch(import_run_id, orders)

    ins = result.get('inserted', 0)
    upd = result.get('updated', 0)
    fail = result.get('failed', 0)
    rpc_err = result.get('rpc_error', '')
    errors = result.get('errors', [])

    # 저장 실패 체크
    if ins + upd == 0:
        err_detail = f"inserted={ins}, updated={upd}, failed={fail}"
        if rpc_err:
            err_detail += f", RPC오류: {rpc_err}"
        if errors:
            err_detail += f", errors: {errors[:3]}"
        return jsonify({
            'success': False,
            'error': f'저장 실패 ({err_detail})',
            'result': result
        })

    # 실시간 출고 처리 (재고차감) — skip_outbound 시 출고완료 표시만
    skip_outbound = data.get('skip_outbound', False)
    rt_msg = ''
    if ins + upd > 0:
        if skip_outbound:
            # 기존 출고완료 건: 재고차감 안 하고 출고완료만 표시
            try:
                from services.channel_config import CHANNEL_REVENUE_MAP
                rev_cat = CHANNEL_REVENUE_MAP.get('N배송_수동', 'N배송')
                new_ids = []
                check = db.client.table('order_transactions').select('id') \
                    .eq('import_run_id', import_run_id).execute()
                new_ids = [r['id'] for r in (check.data or [])]
                if new_ids:
                    db.mark_orders_outbound_done(new_ids, order_date, rev_cat)
                rt_msg = f' (기존 출고완료 처리 — 재고차감 없음)'
            except Exception as e:
                rt_msg = f' (⚠️ 출고완료 표시 실패: {e})'
        else:
            try:
                from services.order_to_stock_service import process_realtime_outbound
                rt = process_realtime_outbound(db, import_run_id)
                result['realtime'] = rt
                oc = rt.get('outbound_count', 0)
                fo = rt.get('failed_orders', 0)
                shortage = rt.get('shortage_messages', []) or []
                rt_msg = f' (출고 {oc}건'
                if fo:
                    rt_msg += f', ⚠️ 재고부족 {fo}건 스킵'
                rt_msg += ')'
                if shortage:
                    result['shortage_messages'] = shortage
            except Exception as rt_err:
                result['realtime_error'] = str(rt_err)
                rt_msg = f' (⚠️ 출고 자동처리 실패: {rt_err})'

    _log_action('n_delivery',
                 detail=f'N배송 수동입력 {order_date}: {ins}건 저장, {upd}건 갱신{rt_msg}')
    return jsonify({
        'success': True,
        'message': f'N배송 {ins}건 저장, {upd}건 갱신{rt_msg}',
        'result': result
    })


@orders_bp.route('/api/n-delivery/list')
@role_required('admin', 'manager', 'sales')
def api_n_delivery_list():
    """N배송 수동입력 목록 조회 (날짜 범위)."""
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    if not date_from or not date_to:
        return jsonify({'error': '날짜 범위를 지정하세요'}), 400

    db = get_db()
    try:
        res = db.client.table('order_transactions').select(
            'id,order_date,order_no,product_name,qty,unit_price,total_amount,status,channel,is_outbound_done'
        ).like('order_no', 'NDEL%') \
         .gte('order_date', date_from) \
         .lte('order_date', date_to) \
         .order('order_date', desc=True) \
         .limit(500).execute()

        rows = res.data or []
        return jsonify({'items': rows, 'count': len(rows)})
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'trace': traceback.format_exc()}), 500


@orders_bp.route('/api/n-delivery/<int:tx_id>', methods=['PUT'])
@role_required('admin', 'manager', 'sales')
def api_n_delivery_update(tx_id):
    """N배송 수동입력 건 수정 (수량/품목 변경). 감사로그 기록."""
    data = request.get_json()
    if not data:
        return jsonify({'error': '요청 데이터 없음'}), 400

    db = get_db()
    username = current_user.username if current_user.is_authenticated else ''

    try:
        # 기존 데이터 조회
        old = db.client.table('order_transactions').select('*').eq('id', tx_id).execute()
        if not old.data:
            return jsonify({'error': '해당 건을 찾을 수 없습니다'}), 404

        old_row = old.data[0]
        # N배송 수동입력 건만 수정 가능 (order_no 'NDEL' 패턴 또는 channel 체크)
        order_no = old_row.get('order_no', '')
        if not order_no.startswith('NDEL') and old_row.get('channel') != 'N배송_수동':
            return jsonify({'error': 'N배송 수동입력 건만 수정 가능합니다'}), 403

        # 변경할 필드 구성
        update_data = {}
        new_qty = data.get('qty')
        new_product = data.get('product_name')

        if new_qty is not None:
            new_qty = int(new_qty)
            if new_qty < 0:
                return jsonify({'error': '수량은 0 이상이어야 합니다'}), 400

            # 수량 0 → 취소 처리
            if new_qty == 0:
                update_data['qty'] = 0
                update_data['total_amount'] = 0
                update_data['settlement'] = 0
                update_data['status'] = '취소'
                update_data['status_reason'] = f'수량 0 취소 (by {username})'
            else:
                update_data['qty'] = new_qty
                # 금액도 재계산
                up = old_row.get('unit_price', 0) or 0
                update_data['total_amount'] = up * new_qty
                update_data['settlement'] = up * new_qty

        if new_product and new_product != old_row.get('product_name'):
            from services.product_name import canonical
            new_product_canon = canonical(new_product)
            update_data['product_name'] = new_product_canon
            # 단가 재조회 (canonical 키)
            price_map = db.query_price_table()
            prices = price_map.get(new_product_canon, {}) or price_map.get(new_product, {})
            new_up = prices.get('네이버판매가', 0)
            update_data['unit_price'] = new_up
            qty = new_qty if new_qty else old_row.get('qty', 0)
            update_data['total_amount'] = new_up * qty
            update_data['settlement'] = new_up * qty

        if not update_data:
            return jsonify({'error': '변경할 내용이 없습니다'}), 400

        # 출고 처리 여부 확인 (수정 전)
        was_outbound_done = old_row.get('is_outbound_done', False)
        is_cancel = update_data.get('status') == '취소'

        # ★ 재고 처리를 DB 업데이트 전에 수행 (reverse가 원본 qty를 참조하므로)
        stock_msg = ''
        if was_outbound_done:
            try:
                from services.order_to_stock_service import reverse_order_stock, process_single_order_realtime
                # 1) 기존 출고 역분개 (SALES_RETURN) — 아직 원본 qty가 DB에 남아 있음
                rev_result = reverse_order_stock(db, tx_id)
                rev_cnt = rev_result.get('stock_reversed', 0)

                if is_cancel:
                    stock_msg = f' (취소 — 재고 복원 {rev_cnt}건)'
                else:
                    # 수정: 역분개 후 DB 업데이트 후 재출고 (아래에서 처리)
                    stock_msg = f' (재고: 복원 {rev_cnt}건)'
            except Exception as stk_err:
                stock_msg = f' (⚠️ 재고 재처리 실패: {stk_err})'

        # DB 업데이트
        db.client.table('order_transactions').update(update_data).eq('id', tx_id).execute()

        # 감사 로그 기록
        action = 'N배송_취소' if is_cancel else 'N배송_수정'
        db.insert_audit_log({
            'action': action,
            'user_name': username,
            'target': f'order_transactions#{tx_id}',
            'detail': f'{old_row.get("product_name")} 수량:{old_row.get("qty")}→{update_data.get("qty", old_row.get("qty"))}',
            'old_value': {
                'product_name': old_row.get('product_name'),
                'qty': old_row.get('qty'),
                'unit_price': old_row.get('unit_price'),
                'total_amount': old_row.get('total_amount'),
            },
            'new_value': update_data,
        })

        if is_cancel:
            return jsonify({'success': True, 'message': f'취소 처리 완료{stock_msg}'})

        # 수정(취소 아닌 경우): 변경된 수량/품목으로 재출고
        if was_outbound_done:
            try:
                from services.order_to_stock_service import process_single_order_realtime
                re_result = process_single_order_realtime(db, tx_id)
                out_cnt = re_result.get('outbound_count', 0)
                stock_msg += f' → 재출고 {out_cnt}건)'
                stock_msg = stock_msg.replace(') →', ' →')  # 괄호 정리
            except Exception as stk_err:
                stock_msg += f' (⚠️ 재출고 실패: {stk_err})'

        return jsonify({'success': True, 'message': f'수정 완료{stock_msg}'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@orders_bp.route('/api/n-delivery/reprocess', methods=['POST'])
@role_required('admin', 'manager')
def api_n_delivery_reprocess():
    """기존 N배송 미처리 건 일괄 출고 재처리.

    is_outbound_done=false인 NDEL 주문을 찾아서 출고 처리.
    """
    db = get_db()
    try:
        # NDEL% 주문 중 미처리 건 조회
        res = db.client.table('order_transactions').select(
            'id,order_no,product_name,qty,order_date,channel,is_outbound_done'
        ).like('order_no', 'NDEL%') \
         .eq('is_outbound_done', False) \
         .eq('status', '정상') \
         .limit(500).execute()

        pending = res.data or []
        if not pending:
            return jsonify({'success': True, 'message': '미처리 건 없음', 'processed': 0})

        from services.order_to_stock_service import process_single_order_realtime

        processed = 0
        errors = []
        for order in pending:
            oid = order['id']
            try:
                result = process_single_order_realtime(db, oid)
                if result.get('outbound_count', 0) > 0:
                    processed += 1
                if result.get('errors'):
                    errors.extend(result['errors'])
            except Exception as e:
                errors.append(f"주문 {order.get('order_no')}: {e}")

        msg = f'N배송 미처리 {len(pending)}건 중 {processed}건 출고 처리 완료'
        if errors:
            msg += f' (오류 {len(errors)}건)'
        _log_action('n_delivery_reprocess',
                     detail=f'N배송 미처리 재처리: {len(pending)}건 중 {processed}건 출고 완료')
        return jsonify({'success': True, 'message': msg,
                        'processed': processed, 'total': len(pending),
                        'errors': errors[:10]})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ================================================================
#  로켓매출 수동입력
# ================================================================

@orders_bp.route('/rocket-manual')
@role_required('admin', 'manager', 'sales')
def rocket_manual():
    """로켓매출 수동입력 페이지"""
    products = []
    locations = []
    try:
        price_map = get_db().query_price_table()
        for name, prices in price_map.items():
            rp = prices.get('로켓판매가', 0)
            if rp and rp > 0:
                products.append({'name': name, 'price': int(rp)})
        products.sort(key=lambda x: x['name'])
    except Exception:
        pass
    try:
        locations, _ = get_db().query_filter_options()
    except Exception:
        locations = ['넥스원', '해서']
    return render_template('orders/rocket_manual.html',
                           products=products, locations=locations)


@orders_bp.route('/api/rocket-manual', methods=['POST'])
@role_required('admin', 'manager', 'sales')
def api_rocket_manual():
    """로켓매출 수동입력 저장 → daily_revenue upsert + 재고차감."""
    data = request.get_json()
    if not data:
        return jsonify({'error': '요청 데이터 없음'}), 400

    items = data.get('items', [])
    revenue_date = data.get('revenue_date', '')
    warehouse = data.get('warehouse', '넥스원')
    if not items:
        return jsonify({'error': '입력할 항목이 없습니다'}), 400
    if not revenue_date:
        return jsonify({'error': '매출일자를 입력하세요'}), 400

    db = get_db()
    username = current_user.username if current_user.is_authenticated else ''
    price_map = db.query_price_table()

    revenue_payload = []
    stock_items = []

    from services.product_name import canonical
    for item in items:
        product_name_raw = str(item.get('product_name', '')).strip()
        qty = int(item.get('qty', 0))
        invoice_no = str(item.get('invoice_no', '')).strip()
        if not product_name_raw or qty <= 0:
            continue

        # 전사 표준 canonical 적용 — 공백 드리프트 차단
        product_name = canonical(product_name_raw)
        prices = price_map.get(product_name, {}) or price_map.get(product_name_raw, {})
        unit_price = prices.get('로켓판매가', 0)
        revenue = int(unit_price * qty)

        revenue_payload.append({
            'revenue_date': revenue_date,
            'product_name': product_name,
            'category': '로켓',
            'channel': '',
            'qty': qty,
            'unit_price': int(unit_price),
            'revenue': revenue,
            'invoice_no': invoice_no,
            'warehouse': warehouse,
        })
        stock_items.append({
            'product_name': product_name,
            'qty': qty,
            'unit_price': int(unit_price),
            'unit': '개',
        })

    if not revenue_payload:
        return jsonify({'error': '유효한 입력 항목이 없습니다'}), 400

    try:
        # 1. 매출 기록 (daily_revenue) — 연결오류 시 자동 재시도
        db._retry_on_disconnect(db.upsert_revenue, revenue_payload)

        # 2. 재고차감 (stock_ledger SALES_OUT)
        # ★ daily_revenue는 이미 저장됨. stock_ledger 실패 시 정합성 깨짐 →
        #   사용자에게 명시적으로 표시 (silent warning 금지)
        stock_msg = ''
        stock_critical = False
        try:
            from services.outbound_service import process_single_outbound
            result = db._retry_on_disconnect(
                process_single_outbound, db, revenue_date, warehouse, stock_items,
                memo=f'로켓매출 (rocket)'
            )
            if result.get('success'):
                stock_msg = f', 재고차감 {result.get("count", 0)}건'
            else:
                shortage = result.get('shortage', [])
                stock_msg = f' ⚠️ 재고 부족(매출만 기록됨): {", ".join(shortage[:3])}'
                stock_critical = True
                current_app.logger.error(
                    f'[로켓수동입력 재고부족] {revenue_date} | {warehouse} | '
                    f'shortage={shortage}'
                )
        except Exception as stk_err:
            stock_msg = f' ⚠️ 재고차감 실패(매출만 기록됨): {stk_err}'
            stock_critical = True
            current_app.logger.error(f'[로켓수동입력 재고차감 오류] {stk_err}')

        # 3. 감사 로그
        _log_action('rocket_manual',
                     detail=f'{revenue_date} 로켓매출 {len(revenue_payload)}건 저장 '
                            f'({warehouse}){stock_msg}')

        return jsonify({
            'success': True,
            'critical': stock_critical,
            'message': f'로켓매출 {len(revenue_payload)}건 저장 완료{stock_msg}',
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@orders_bp.route('/api/rocket-manual/list')
@role_required('admin', 'manager', 'sales')
def api_rocket_manual_list():
    """로켓매출 수동입력 목록 조회."""
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    if not date_from or not date_to:
        return jsonify({'error': '날짜 범위를 지정하세요'}), 400

    try:
        res = get_db().client.table('daily_revenue').select(
            'id,revenue_date,product_name,qty,unit_price,revenue,invoice_no,warehouse'
        ).eq('category', '로켓') \
         .gte('revenue_date', date_from) \
         .lte('revenue_date', date_to) \
         .order('revenue_date', desc=True) \
         .limit(500).execute()
        rows = res.data or []
        return jsonify({'items': rows, 'count': len(rows)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@orders_bp.route('/api/rocket-manual/add-product', methods=['POST'])
@role_required('admin', 'manager', 'sales')
def api_rocket_add_product():
    """로켓매출 상품 추가 (products + master_prices 동기화, 띄어쓰기 정규화)"""
    data = request.get_json()
    name = (data.get('product_name') or '').strip()
    price = data.get('rocket_price', 0)
    if not name:
        return jsonify({'success': False, 'error': '상품명을 입력하세요.'})
    if not price or price <= 0:
        return jsonify({'success': False, 'error': '판매가를 입력하세요.'})

    name_norm = name.replace(' ', '').replace('\u3000', '')
    db = get_db()
    try:
        # products 테이블 우선 (정규화 비교)
        existing_p = db.client.table('products').select('product_name') \
            .eq('name_normalized', name_norm).limit(1).execute()
        if existing_p.data:
            real_name = existing_p.data[0]['product_name']
            db.client.table('products').update({'rocket_price': float(price)}) \
                .eq('product_name', real_name).execute()
        else:
            db.client.table('products').insert({
                'product_name': name, 'name_normalized': name_norm,
                'rocket_price': float(price), 'material_type': '완제품',
            }).execute()

        # master_prices 동기화 (하위호환)
        existing = db.client.table('master_prices').select('id') \
            .eq('품목명', name).limit(1).execute()
        if existing.data:
            db.client.table('master_prices').update({'로켓판매가': price}) \
                .eq('품목명', name).execute()
        else:
            db.client.table('master_prices').insert({
                '품목명': name, '로켓판매가': price
            }).execute()

        # product_costs에도 없으면 추가 (하위호환)
        pc = db.client.table('product_costs').select('product_name') \
            .eq('product_name', name).limit(1).execute()
        if not pc.data:
            db.upsert_product_cost(product_name=name, cost_price=0, material_type='완제품')

        return jsonify({'success': True, 'message': f'"{name}" 로켓 판매가 {price:,}원 설정 완료'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@orders_bp.route('/api/rocket-manual/<int:rev_id>', methods=['PUT'])
@role_required('admin', 'manager')
def api_rocket_manual_update(rev_id):
    """로켓매출 수정/삭제."""
    data = request.get_json()
    if not data:
        return jsonify({'error': '요청 데이터 없음'}), 400

    db = get_db()
    username = current_user.username if current_user.is_authenticated else ''

    try:
        old = db.client.table('daily_revenue').select('*').eq('id', rev_id).execute()
        if not old.data:
            return jsonify({'error': '해당 건을 찾을 수 없습니다'}), 404
        old_row = old.data[0]
        if old_row.get('category') != '로켓':
            return jsonify({'error': '로켓매출 건만 수정 가능합니다'}), 403

        action = data.get('action')
        new_qty = data.get('qty')
        new_product = data.get('product_name')

        # 삭제
        if action == 'delete' or (new_qty is not None and int(new_qty) == 0):
            # 재고 복원 (삭제 시 기존 출고분 되돌리기)
            stock_msg = ''
            try:
                from services.product_name import canonical
                old_product = canonical(old_row.get('product_name', ''))
                old_qty = int(old_row.get('qty', 0))
                old_warehouse = old_row.get('warehouse', '해서')
                old_date = old_row.get('revenue_date', '')
                if old_qty > 0 and old_product:
                    db.client.table('stock_ledger').insert({
                        'transaction_date': old_date,
                        'type': 'SALES_RETURN',
                        'product_name': old_product,
                        'qty': old_qty,  # 양수 = 재고 복원
                        'location': old_warehouse,
                        'memo': f'로켓매출 삭제 복원 (rev#{rev_id})',
                        'created_by': username,
                    }).execute()
                    stock_msg = f', 재고 +{old_qty} 복원({old_warehouse})'
            except Exception as stk_err:
                stock_msg = f' (재고복원 실패: {stk_err})'
                current_app.logger.warning(f'로켓 삭제 재고복원 오류: {stk_err}')

            db.delete_revenue_by_id(rev_id)
            db.insert_audit_log({
                'action': '로켓매출_삭제',
                'user_name': username,
                'target': f'daily_revenue#{rev_id}',
                'detail': f'{old_row.get("product_name")} qty:{old_row.get("qty")} 삭제{stock_msg}',
                'old_value': old_row,
            })
            return jsonify({'success': True, 'message': f'삭제 완료{stock_msg}'})

        # 수정
        from services.product_name import canonical
        update_data = {}
        if new_product and new_product != old_row.get('product_name'):
            new_product = canonical(new_product)
            update_data['product_name'] = new_product
            # 단가가 별도 전송되지 않은 경우에만 자동 조회
            if data.get('unit_price') is None:
                price_map = db.query_price_table()
                prices = price_map.get(new_product, {})
                update_data['unit_price'] = int(prices.get('로켓판매가', 0))

        # 단가 직접 수정
        new_unit_price = data.get('unit_price')
        if new_unit_price is not None:
            update_data['unit_price'] = int(new_unit_price)

        if new_qty is not None:
            new_qty = int(new_qty)
            if new_qty <= 0:
                return jsonify({'error': '수량은 1 이상이어야 합니다 (삭제는 0)'}), 400
            update_data['qty'] = new_qty

        # 송장번호
        new_invoice = data.get('invoice_no')
        if new_invoice is not None:
            update_data['invoice_no'] = new_invoice

        # 출고창고
        new_warehouse = data.get('warehouse')
        if new_warehouse is not None:
            update_data['warehouse'] = new_warehouse

        if not update_data:
            return jsonify({'error': '변경할 내용이 없습니다'}), 400

        up = update_data.get('unit_price', old_row.get('unit_price', 0))
        q = update_data.get('qty', old_row.get('qty', 0))
        update_data['revenue'] = int(up * q)

        db.client.table('daily_revenue').update(update_data).eq('id', rev_id).execute()

        # 재고 조정 — 상품/수량/창고 변경 시 stock_ledger 반영
        stock_msg = ''
        try:
            old_product = canonical(old_row.get('product_name', ''))
            old_qty = int(old_row.get('qty', 0))
            old_warehouse = old_row.get('warehouse', '해서')
            old_date = old_row.get('revenue_date', '')

            final_product = canonical(update_data.get('product_name', old_product))
            final_qty = update_data.get('qty', old_qty)
            final_warehouse = update_data.get('warehouse', old_warehouse)

            product_changed = final_product != old_product
            qty_changed = final_qty != old_qty
            warehouse_changed = final_warehouse != old_warehouse

            if product_changed or warehouse_changed:
                # 상품 or 창고 변경: 기존 것 복원 + 새 것 차감
                if old_qty > 0:
                    db.client.table('stock_ledger').insert({
                        'transaction_date': old_date,
                        'type': 'SALES_RETURN',
                        'product_name': old_product,
                        'qty': old_qty,
                        'location': old_warehouse,
                        'memo': f'로켓매출 수정 복원 (rev#{rev_id})',
                        'created_by': username,
                    }).execute()
                if final_qty > 0:
                    db.client.table('stock_ledger').insert({
                        'transaction_date': old_date,
                        'type': 'SALES_OUT',
                        'product_name': final_product,
                        'qty': -final_qty,
                        'location': final_warehouse,
                        'memo': f'로켓매출 수정 재차감 (rev#{rev_id})',
                        'created_by': username,
                    }).execute()
                stock_msg = f', 재고 이동: {old_product}({old_warehouse})+{old_qty} → {final_product}({final_warehouse})-{final_qty}'
            elif qty_changed:
                # 수량만 변경: 차이만큼 조정
                diff = final_qty - old_qty
                if diff != 0:
                    db.client.table('stock_ledger').insert({
                        'transaction_date': old_date,
                        'type': 'SALES_OUT' if diff > 0 else 'SALES_RETURN',
                        'product_name': old_product,
                        'qty': -abs(diff) if diff > 0 else abs(diff),
                        'location': final_warehouse,
                        'memo': f'로켓매출 수량수정 {"추가차감" if diff > 0 else "부분복원"} (rev#{rev_id})',
                        'created_by': username,
                    }).execute()
                    stock_msg = f', 재고 {"-%d 추가차감" % abs(diff) if diff > 0 else "+%d 복원" % abs(diff)}({final_warehouse})'
        except Exception as stk_err:
            stock_msg = f' (재고조정 실패: {stk_err})'
            current_app.logger.warning(f'로켓 수정 재고조정 오류: {stk_err}')

        db.insert_audit_log({
            'action': '로켓매출_수정',
            'user_name': username,
            'target': f'daily_revenue#{rev_id}',
            'detail': f'{old_row.get("product_name")} qty:{old_row.get("qty")}→{update_data.get("qty", old_row.get("qty"))}{stock_msg}',
            'old_value': {'product_name': old_row.get('product_name'),
                          'qty': old_row.get('qty'), 'revenue': old_row.get('revenue'),
                          'warehouse': old_row.get('warehouse')},
            'new_value': update_data,
        })
        return jsonify({'success': True, 'message': f'수정 완료{stock_msg}'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ================================================================
# 출고 정합성 검사 API
# ================================================================

@orders_bp.route('/api/integrity-check', methods=['GET'])
@login_required
@role_required('admin', 'manager')
def integrity_check():
    """출고 정합성 검사: is_outbound_done=True인데 SALES_OUT 없는 유령 출고 + 다채널 중복 감지.

    Returns JSON:
        {
            ghost_outbound: [{id, channel, order_no, product_name, qty, order_date}, ...],
            cross_channel_duplicates: [{order_no, channels: [...]}, ...],
            summary: str
        }
    """
    db = get_db()
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')

    results = {'ghost_outbound': [], 'cross_channel_duplicates': [], 'summary': ''}

    try:
        # 1. 유령 출고 검사: is_outbound_done=True + SALES_OUT 미존재
        q = db.client.table('order_transactions').select(
            'id, channel, order_no, product_name, qty, order_date, is_outbound_done'
        ).eq('is_outbound_done', True)
        if date_from:
            q = q.gte('order_date', date_from)
        if date_to:
            q = q.lte('order_date', date_to)
        done_orders = q.limit(2000).execute()

        for order in (done_orders.data or []):
            oid = order['id']
            sl_check = db.client.table('stock_ledger').select('id').eq(
                'status', 'active'
            ).eq('type', 'SALES_OUT'
            ).like('event_uid', f'%:{oid}:%').limit(1).execute()
            if not sl_check.data:
                sl_check2 = db.client.table('stock_ledger').select('id').eq(
                    'status', 'active'
                ).eq('type', 'SALES_OUT'
                ).like('event_uid', f'%:{oid}:0').limit(1).execute()
                if not sl_check2.data:
                    results['ghost_outbound'].append({
                        'id': oid,
                        'channel': order.get('channel', ''),
                        'order_no': order.get('order_no', ''),
                        'product_name': order.get('product_name', ''),
                        'qty': order.get('qty', 0),
                        'order_date': order.get('order_date', ''),
                    })

        # 2. 다채널 중복 감지: 같은 order_no가 여러 채널에 존재
        q2 = db.client.table('order_transactions').select(
            'id, channel, order_no, product_name, qty'
        )
        if date_from:
            q2 = q2.gte('order_date', date_from)
        if date_to:
            q2 = q2.lte('order_date', date_to)
        all_orders = q2.limit(5000).execute()

        from collections import defaultdict
        by_ono = defaultdict(list)
        for r in (all_orders.data or []):
            by_ono[r['order_no']].append(r)

        for ono, rows in by_ono.items():
            channels = set(r['channel'] for r in rows)
            if len(channels) > 1:
                results['cross_channel_duplicates'].append({
                    'order_no': ono,
                    'channels': [{'id': r['id'], 'channel': r['channel'],
                                  'product_name': r['product_name'], 'qty': r['qty']}
                                 for r in rows],
                })

        ghost_n = len(results['ghost_outbound'])
        dup_n = len(results['cross_channel_duplicates'])
        if ghost_n == 0 and dup_n == 0:
            results['summary'] = '정합성 이상 없음'
        else:
            parts = []
            if ghost_n:
                parts.append(f'유령출고 {ghost_n}건')
            if dup_n:
                parts.append(f'다채널중복 {dup_n}건')
            results['summary'] = f'문제 발견: {", ".join(parts)}'

    except Exception as e:
        results['summary'] = f'검사 오류: {e}'

    return jsonify(results)


# ════════════════════════════════════════════════════════════
# 상품관리 (주문·판매용 — 조회 + 신규추가 + 수정, 삭제 불가)
# ════════════════════════════════════════════════════════════

@orders_bp.route('/products')
@login_required
@role_required('admin', 'manager', 'sales')
def products_page():
    """상품관리 페이지"""
    return render_template('orders/products.html')


@orders_bp.route('/api/product-manage', methods=['GET'])
@login_required
@role_required('admin', 'manager', 'sales')
def api_products_list():
    """상품 목록 조회 (페이지네이션 + 검색 + 필터)"""
    db = get_db()
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    keyword = request.args.get('keyword', '').strip()
    material_type = request.args.get('material_type', '')
    category = request.args.get('category', '')
    storage = request.args.get('storage_method', '')

    try:
        q = db.client.table('products').select('*').eq('is_active', True)

        # 키워드 검색 (상품명, SKU, 바코드)
        if keyword:
            q = q.or_(f'product_name.ilike.%{keyword}%,sku.ilike.%{keyword}%,barcode.ilike.%{keyword}%')

        # 기본: 완제품만 (영업용). 종류 필터 선택 시 해당 종류만
        if material_type:
            q = q.eq('material_type', material_type)
        else:
            q = q.eq('material_type', '완제품')
        if storage:
            q = q.eq('storage_method', storage)

        result = q.execute()
        data = result.data or []

        # SKU 숫자 정렬
        def sku_key(x):
            try:
                return (0, int(x.get('sku') or 0))
            except (ValueError, TypeError):
                return (1, 0)
        data.sort(key=sku_key)

        # 페이지네이션
        offset = (page - 1) * per_page
        page_data = data[offset:offset + per_page]

        return jsonify({'ok': True, 'data': page_data, 'page': page, 'per_page': per_page, 'total': len(data)})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})


@orders_bp.route('/api/product-manage', methods=['POST'])
@login_required
@role_required('admin', 'manager', 'sales')
def api_products_add():
    """신규 상품 추가 (띄어쓰기 정규화 + 중복 체크)"""
    db = get_db()
    data = request.get_json()
    name = (data.get('product_name') or '').strip()
    if not name:
        return jsonify({'ok': False, 'error': '상품명을 입력하세요.'})

    name_norm = name.replace(' ', '').replace('\u3000', '')

    try:
        # 정규화된 이름으로 중복 체크
        dup = db.client.table('products').select('product_name') \
            .eq('name_normalized', name_norm).limit(1).execute()
        if dup.data:
            existing = dup.data[0]['product_name']
            return jsonify({'ok': False, 'error': f'이미 등록된 상품입니다: "{existing}" (띄어쓰기 다른 동일 상품)'})

        row = {
            'product_name': name,
            'name_normalized': name_norm,
            'cost_price': float(data.get('cost_price', 0) or 0),
            'unit': data.get('unit', '개'),
            'memo': data.get('memo', ''),
            'weight': float(data.get('weight', 0) or 0),
            'weight_unit': data.get('weight_unit', 'g'),
            'cost_type': data.get('cost_type', '매입'),
            'material_type': data.get('material_type', '완제품'),
            'food_type': data.get('food_type', ''),
            'storage_method': data.get('storage_method', ''),
            'barcode': data.get('barcode', ''),
            'sku': data.get('sku', ''),
        }
        db.client.table('products').insert(row).execute()
        # product_costs에도 동기 삽입 (하위호환)
        db.upsert_product_cost(product_name=name, cost_price=row['cost_price'],
            unit=row['unit'], memo=row['memo'], weight=row['weight'],
            weight_unit=row['weight_unit'], cost_type=row['cost_type'],
            material_type=row['material_type'], food_type=row['food_type'],
            storage_method=row['storage_method'])
        return jsonify({'ok': True, 'message': f'"{name}" 등록 완료'})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})


@orders_bp.route('/api/product-manage/<product_name>', methods=['PUT'])
@login_required
@role_required('admin', 'manager', 'sales')
def api_products_update(product_name):
    """상품 수정 (삭제 불가)"""
    db = get_db()
    data = request.get_json()
    try:
        update = {}
        for field in ['cost_price', 'unit', 'memo', 'weight', 'weight_unit',
                       'cost_type', 'material_type', 'food_type', 'storage_method',
                       'barcode', 'sku', 'naver_price', 'coupang_price', 'rocket_price',
                       'self_mall_price']:
            if field in data:
                if field in ('cost_price', 'weight', 'naver_price', 'coupang_price',
                             'rocket_price', 'self_mall_price'):
                    update[field] = float(data[field] or 0)
                else:
                    update[field] = data[field]
        if update:
            db.client.table('products').update(update).eq('product_name', product_name).execute()

            # option_master 동기화: SKU → sort_order, barcode
            sync_option = {}
            if 'sku' in data:
                try:
                    sync_option['sort_order'] = int(data['sku'])
                except (ValueError, TypeError):
                    pass
            if 'barcode' in data:
                sync_option['barcode'] = str(data['barcode'] or '')
            if sync_option:
                db.client.table('option_master').update(sync_option) \
                    .eq('product_name', product_name).execute()

        return jsonify({'ok': True, 'message': f'"{product_name}" 수정 완료 (옵션 동기화됨)'})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})


# ════════════════════════════════════════════════════════════
# 옵션관리 (주문·판매용 — 조회 + 수정만, 삭제/동기화 불가)
# ════════════════════════════════════════════════════════════

@orders_bp.route('/options')
@login_required
@role_required('admin', 'manager', 'sales')
def options_page():
    """옵션관리 페이지 (영업용)"""
    return render_template('orders/options.html')


@orders_bp.route('/api/options', methods=['GET'])
@login_required
@role_required('admin', 'manager', 'sales')
def api_options_list():
    """옵션 목록 조회"""
    db = get_db()
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    keyword = request.args.get('keyword', '').strip()

    try:
        q = db.client.table('option_master').select('*') \
            .or_('is_deleted.is.null,is_deleted.eq.false')

        if keyword:
            q = q.or_(f'original_name.ilike.%{keyword}%,product_name.ilike.%{keyword}%')

        q = q.order('sort_order').order('product_name')
        offset = (page - 1) * per_page
        q = q.range(offset, offset + per_page - 1)
        result = q.execute()

        return jsonify({'ok': True, 'data': result.data or [], 'page': page, 'per_page': per_page})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})


@orders_bp.route('/api/options/<int:option_id>', methods=['PUT'])
@login_required
@role_required('admin', 'manager', 'sales')
def api_options_update(option_id):
    """옵션 수정 (삭제 불가) — products 동기화 포함"""
    db = get_db()
    data = request.get_json()
    allowed_fields = ['product_name', 'line_code', 'sort_order', 'barcode']
    update = {k: data[k] for k in allowed_fields if k in data}

    if not update:
        return jsonify({'ok': False, 'error': '수정할 항목이 없습니다.'})

    try:
        db.update_option_master(option_id, update)

        # products 동기화: 옵션에서 수정한 상품 정보를 products에도 반영
        product_name = data.get('product_name')
        if product_name:
            product_name = product_name.strip()
            name_norm = product_name.replace(' ', '').replace('\u3000', '')
            p = db.client.table('products').select('id') \
                .eq('name_normalized', name_norm).limit(1).execute()

            if p.data:
                sync = {}
                if 'sort_order' in data:
                    sync['sku'] = str(data['sort_order'])
                if 'barcode' in data:
                    sync['barcode'] = str(data['barcode'] or '')
                if sync:
                    db.client.table('products').update(sync) \
                        .eq('name_normalized', name_norm).execute()
            else:
                # products에 없으면 신규 추가
                db.client.table('products').insert({
                    'product_name': product_name,
                    'name_normalized': name_norm,
                    'sku': str(data.get('sort_order', '')),
                    'barcode': str(data.get('barcode', '')),
                    'material_type': '완제품',
                    'unit': '개',
                    'is_active': True,
                }).execute()

        return jsonify({'ok': True, 'message': '수정 완료 (상품정보 동기화됨)'})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})
