"""
shipping.py — 송장관리 블루프린트.

CJ 택배 연동, 마켓 송장등록, 배송상태 추적.
marketplace.py에서 분리 (2026-03-23).
"""
import os
import logging
from flask import (Blueprint, render_template, request, jsonify,
                   send_file, g)
from flask_login import current_user
from auth import role_required, _log_action
from services.tz_utils import today_kst
from db_utils import get_db

logger = logging.getLogger(__name__)

shipping_bp = Blueprint('shipping', __name__, url_prefix='/shipping')


# ── 송장관리 페이지 ──

@shipping_bp.route('/')
@role_required('admin', 'general')
def index():
    """송장관리 페이지."""
    from config import Config
    db = get_db()
    mgr = g.marketplace
    active_channels = mgr.get_active_channels()
    channel_labels = getattr(Config, 'CHANNEL_LABELS', {})

    return render_template('marketplace/shipping.html',
                           active_channels=active_channels,
                           channel_labels=channel_labels,
                           today=today_kst())


# ── CJ 송장 업로드 (엑셀 매칭) ──

@shipping_bp.route('/api/cj-tracking-upload', methods=['POST'])
@role_required('admin', 'general')
def cj_tracking_upload():
    """CJ 송장결과 파일 업로드 → 이름+전화뒷4자리로 매칭 → order_shipping 반영."""
    from services.invoice_matching_service import parse_cj_excel, match_invoices_to_orders

    file = request.files.get('file')
    if not file:
        return jsonify({'ok': False, 'error': '파일이 없습니다.'})

    filename = file.filename.lower()
    if not filename.endswith(('.xlsx', '.xls')):
        return jsonify({'ok': False, 'error': 'xlsx 또는 xls 파일만 지원합니다.'})

    try:
        cj_map = parse_cj_excel(file.read(), filename)
        db = get_db()
        result = match_invoices_to_orders(db, cj_map, date_range_days=14)

        _log_action(f'CJ 송장파일 업로드: {file.filename} — '
                    f'매칭 {result["matched"]}건, DB반영 {result["updated"]}건')

        return jsonify({
            'ok': result['matched'] > 0,
            'total': result['total'],
            'matched': result['matched'],
            'success': result['updated'],
            'skipped': result['skipped'],
            'errors': result['errors'],
        })
    except ValueError as e:
        return jsonify({'ok': False, 'error': str(e)})
    except Exception as e:
        logger.error(f'[CJ Upload] 오류: {e}', exc_info=True)
        return jsonify({'ok': False, 'error': str(e)})


# ── CJ 송장 자동 생성 ──

@shipping_bp.route('/api/cj-orders-without-invoice', methods=['GET'])
@role_required('admin', 'general')
def cj_orders_without_invoice():
    """송장 미배정 주문 현황 조회."""
    try:
        from services.cj_shipping_service import query_orders_without_invoice
        db = get_db()
        channel = request.args.get('channel') or None
        orders = query_orders_without_invoice(db, channel=channel, limit=500)

        by_channel = {}
        for o in orders:
            ch = o['channel']
            by_channel[ch] = by_channel.get(ch, 0) + 1

        return jsonify({
            'total': len(orders),
            'by_channel': by_channel,
            'orders': orders[:50],
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@shipping_bp.route('/api/cj-generate-invoices', methods=['POST'])
@role_required('admin', 'general')
def cj_generate_invoices():
    """CJ 운송장 채번 + 예약접수 일괄 처리."""
    try:
        from services.cj_shipping_service import (
            query_orders_without_invoice, generate_cj_invoices
        )
        db = get_db()
        channel = request.form.get('channel') or None
        limit = int(request.form.get('limit', 100))

        orders = query_orders_without_invoice(db, channel=channel, limit=limit)
        if not orders:
            return jsonify({'total': 0, 'success': 0, 'failed': 0,
                           'message': '송장 미배정 주문이 없습니다.'})

        result = generate_cj_invoices(db, orders)
        _log_action(f'CJ 송장 자동생성: {result["success"]}/{result["total"]}건 성공')
        return jsonify(result)

    except Exception as e:
        logger.error(f'[CJ Generate] 오류: {e}', exc_info=True)
        return jsonify({'error': str(e)}), 500


@shipping_bp.route('/api/cj-check-booking', methods=['POST'])
@role_required('admin', 'general')
def cj_check_booking():
    """CJ 예약접수 상태 확인."""
    try:
        from services.cj_shipping_service import check_cj_booking_status
        db = get_db()
        channel = request.form.get('channel') or None
        result = check_cj_booking_status(db, channel=channel)
        return jsonify(result)
    except Exception as e:
        logger.error(f'[CJ Check] 오류: {e}', exc_info=True)
        return jsonify({'error': str(e)}), 500


# ── 운송상태 동기화 ──

@shipping_bp.route('/api/sync-shipping-status', methods=['POST'])
@role_required('admin', 'manager', 'general')
def sync_shipping_status_route():
    """마켓 API에서 주문 상태 폴링 → DB 반영."""
    try:
        from services.shipping_status_service import sync_shipping_status
        db = get_db()
        mgr = g.marketplace
        channel = request.form.get('channel') or None

        result = sync_shipping_status(db, mgr, channel)
        summary = result.get('summary', {})
        _log_action(f'배송상태 동기화: {summary.get("total_updated", 0)}건 갱신')
        return jsonify(result)
    except Exception as e:
        logger.error(f'[ShippingStatus] 동기화 오류: {e}', exc_info=True)
        return jsonify({'error': str(e)}), 500


@shipping_bp.route('/api/delivery-status-summary', methods=['GET'])
@role_required('admin', 'manager', 'general')
def delivery_status_summary():
    """채널별 배송상태 집계."""
    try:
        db = get_db()
        channel = request.args.get('channel') or None
        summary = db.query_delivery_status_summary(channel)
        return jsonify({'summary': summary})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── 마켓별 송장등록 파일 다운로드 ──

@shipping_bp.route('/api/download-invoice-file/<channel>', methods=['GET'])
@role_required('admin', 'general')
def download_invoice_file(channel):
    """마켓 관리자 수동 업로드용 송장등록 엑셀 파일 다운로드."""
    try:
        from services.marketplace_invoice_file_service import generate_marketplace_invoice_file
        db = get_db()
        filepath = generate_marketplace_invoice_file(db, channel)

        return send_file(
            filepath,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=os.path.basename(filepath),
        )
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        logger.error(f'[InvoiceFile] 생성 오류: {e}', exc_info=True)
        return jsonify({'error': str(e)}), 500


# ── 마켓 송장 push ──

@shipping_bp.route('/push-invoices', methods=['POST'])
@role_required('admin', 'general')
def push_invoices_route():
    """마켓플레이스에 송장번호 일괄 전송 (발송처리)."""
    try:
        db = get_db()
        mgr = g.marketplace
        channel = request.form.get('channel', '')

        if not channel:
            return jsonify({'error': '채널을 선택하세요.'}), 400

        from services.marketplace_sync_service import push_invoices
        result = push_invoices(db, mgr, channel, triggered_by=current_user.username)

        _log_action(f'송장 전송: {channel} — '
                    f'{result.get("success", 0)}/{result.get("total", 0)}건 성공')
        return jsonify(result)

    except Exception as e:
        logger.error(f'[Push] 송장 전송 오류: {e}', exc_info=True)
        return jsonify({'error': str(e)}), 500


@shipping_bp.route('/api/pending-invoices')
@role_required('admin', 'general')
def api_pending_invoices():
    """채널별 push 대기 건수 조회."""
    try:
        db = get_db()
        counts = {}
        for ch in ['스마트스토어_배마마', '스마트스토어_해미애찬', '자사몰', '쿠팡']:
            pending = db.query_pending_invoice_push(channel=ch)
            mapped = sum(1 for p in pending if p.get('api_order_id'))
            if mapped > 0:
                counts[ch] = mapped
        return jsonify({'counts': counts})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── 송장 라벨 출력 ──

@shipping_bp.route('/api/label-ready-count')
@role_required('admin', 'general')
def api_label_ready_count():
    """라벨 출력 건수 — 송장번호 기준 (미출력/출력완료 분리)."""
    try:
        db = get_db()
        date = request.args.get('date') or today_kst()
        q = db.client.table("order_shipping").select("invoice_no,label_printed") \
            .neq("invoice_no", "") \
            .not_.is_("invoice_no", "null") \
            .eq("courier", "CJ대한통운") \
            .gte("created_at", f"{date}T00:00:00") \
            .limit(2000)
        res = q.execute()

        # 송장번호 기준 중복 제거
        inv_status = {}
        for i in (res.data or []):
            inv = i.get('invoice_no', '')
            if inv and inv not in inv_status:
                inv_status[inv] = i.get('label_printed', False)

        total = len(inv_status)
        printed = sum(1 for v in inv_status.values() if v)
        unprinted = total - printed
        return jsonify({'count': total, 'unprinted': unprinted, 'printed': printed})
    except Exception as e:
        return jsonify({'count': 0, 'unprinted': 0, 'printed': 0, 'error': str(e)})


@shipping_bp.route('/api/label-list')
@role_required('admin', 'general')
def api_label_list():
    """라벨 출력 대상 목록 조회 (송장번호 기준 그룹핑)."""
    try:
        db = get_db()
        channel = request.args.get('channel', '')
        date = request.args.get('date', '') or today_kst()
        status = request.args.get('status', 'all')

        shipments = _query_shipments_for_label(db, channel, date, status)

        # 송장번호 기준 그룹핑 (1 송장 = 1 라벨)
        inv_map = {}
        for s in shipments:
            inv = s.get('invoice_no', '')
            if not inv:
                continue
            if inv not in inv_map:
                inv_map[inv] = {
                    'invoice_no': inv,
                    'channel': s.get('channel', ''),
                    'order_nos': [],
                    'name': s.get('receiver_name', ''),
                    'address': s.get('receiver_addr', ''),
                    'product_name': s.get('product_name', ''),
                    'total_qty': s.get('total_qty', 0),
                    'memo': s.get('memo', ''),
                    'label_printed': s.get('label_printed', False),
                    'label_printed_at': s.get('label_printed_at', ''),
                }
            order_no = s.get('order_no', '')
            if order_no and order_no not in inv_map[inv]['order_nos']:
                inv_map[inv]['order_nos'].append(order_no)

        items = []
        for inv, data in inv_map.items():
            data['order_no'] = data['order_nos'][0] if data['order_nos'] else ''
            data['order_count'] = len(data['order_nos'])
            del data['order_nos']
            items.append(data)

        return jsonify({'items': items, 'total': len(items)})
    except Exception as e:
        return jsonify({'items': [], 'error': str(e)})


@shipping_bp.route('/api/label-print', methods=['POST'])
@role_required('admin', 'general')
def api_label_print():
    """CJ 표준운송장 라벨 PDF 생성.

    body JSON:
      - invoice_nos: ['...']   — 직접 지정
      - channel, date, status  — 조건부 조회
    """
    from flask import Response
    from services.courier.cj_label_generator import generate_labels_from_db_and_api
    from services.courier.cj_client import CJCourierClient

    data = request.get_json(silent=True) or {}
    invoice_nos = data.get('invoice_nos', [])

    db = get_db()

    # 직접 송장번호 지정
    if invoice_nos:
        shipments = _query_shipments_by_invoice(db, invoice_nos)
    else:
        # 조건부 조회
        channel = data.get('channel', '')
        date = data.get('date', '') or today_kst()
        status = data.get('status', '미출력')
        shipments = _query_shipments_for_label(db, channel, date, status)

    # 송장번호 기준 중복 제거 (1 송장 = 1 라벨)
    seen = set()
    unique_shipments = []
    for s in shipments:
        inv = s.get('invoice_no', '')
        if inv and inv not in seen:
            seen.add(inv)
            unique_shipments.append(s)
    shipments = unique_shipments

    if not shipments:
        return jsonify({'ok': False, 'error': '출력할 송장이 없습니다.'})

    # CJ 클라이언트 (주소정제용)
    cj = CJCourierClient(
        cust_id=os.environ.get('CJ_CUST_ID', ''),
        biz_reg_num=os.environ.get('CJ_BIZ_REG_NUM', ''),
        test_mode=os.environ.get('CJ_USE_PROD', 'false').lower() != 'true',
        use_prod=os.environ.get('CJ_USE_PROD', 'false').lower() == 'true',
    )

    # 발송인 정보
    sender = {
        'name': os.environ.get('CJ_SENDER_NAME', '배마마'),
        'phone': os.environ.get('CJ_SENDER_PHONE', ''),
        'address': os.environ.get('CJ_SENDER_ADDRESS', ''),
    }

    result = generate_labels_from_db_and_api(shipments, cj_client=cj, sender=sender)

    if not result.get('ok'):
        return jsonify({'ok': False, 'error': result.get('error', '라벨 생성 실패')})

    # 출력 완료 표시 (label_printed 컬럼이 있으면)
    printed_invoices = [s.get('invoice_no') for s in shipments if s.get('invoice_no')]
    if printed_invoices:
        try:
            from datetime import datetime, timezone
            now_iso = datetime.now(timezone.utc).isoformat()
            for inv in printed_invoices:
                db.client.table("order_shipping").update({
                    "label_printed": True,
                    "label_printed_at": now_iso,
                }).eq("invoice_no", inv).execute()
        except Exception as e:
            # label_printed 컬럼이 없으면 무시 (마이그레이션 전)
            logger.info(f'label_printed 업데이트 스킵 (컬럼 미존재 가능): {e}')

    _log_action(f'라벨 출력: {result["count"]}건')

    return Response(
        result['pdf_bytes'],
        mimetype='application/pdf',
        headers={
            'Content-Disposition': f'inline; filename=cj_labels_{result["count"]}.pdf',
        }
    )


def _enrich_with_products(db, shipments: list) -> list:
    """order_transactions에서 상품명·수량 조회해서 shipment에 추가."""
    if not shipments:
        return shipments
    for ship in shipments:
        order_no = ship.get('order_no', '')
        channel = ship.get('channel', '')
        if not order_no:
            continue
        try:
            q = db.client.table("order_transactions") \
                .select("product_name,qty") \
                .eq("order_no", order_no) \
                .eq("status", "정상")
            if channel:
                q = q.eq("channel", channel)
            txn = q.execute()
            if txn.data:
                items = []
                for t in txn.data:
                    pname = t.get('product_name', '')
                    qty = t.get('qty', 1)
                    if pname:
                        items.append(f'{pname} x{qty}' if qty and int(qty) > 1 else pname)
                ship['product_name'] = ', '.join(items[:3])
                if len(items) > 3:
                    ship['product_name'] += f' 외 {len(items)-3}건'
                ship['product_count'] = len(items)
                ship['total_qty'] = sum(int(t.get('qty', 1) or 1) for t in txn.data)
        except Exception as e:
            logger.debug(f'[Label] 상품 조회 실패 ({order_no}): {e}')
    return shipments


def _query_shipments_by_invoice(db, invoice_nos: list) -> list:
    """송장번호로 배송정보 조회 (order_shipping 기반)."""
    if not invoice_nos:
        return []
    try:
        results = []
        for i in range(0, len(invoice_nos), 50):
            chunk = invoice_nos[i:i+50]
            res = db.client.table("order_shipping") \
                .select("*") \
                .in_("invoice_no", chunk) \
                .execute()
            if res.data:
                results.extend(res.data)

        shipments = []
        for r in results:
            shipments.append({
                'invoice_no': r.get('invoice_no', ''),
                'courier': r.get('courier', ''),
                'order_no': r.get('order_no', ''),
                'receiver_name': r.get('name', ''),
                'receiver_phone': r.get('phone', ''),
                'receiver_addr': r.get('address', ''),
                'receiver_zipcode': '',
                'product_name': '',
                'memo': r.get('memo', ''),
                'channel': r.get('channel', ''),
                'label_printed': r.get('label_printed', False),
                'label_printed_at': r.get('label_printed_at', ''),
            })
        return _enrich_with_products(db, shipments)
    except Exception as e:
        logger.error(f'[Label] 송장 조회 오류: {e}')
        return []


def _query_shipments_for_label(db, channel: str, date: str, status: str) -> list:
    """조건부 배송정보 조회 (라벨 출력용)."""
    try:
        q = db.client.table("order_shipping") \
            .select("*") \
            .neq("invoice_no", "") \
            .not_.is_("invoice_no", "null") \
            .eq("courier", "CJ대한통운")

        if channel:
            q = q.eq("channel", channel)

        if date:
            q = q.gte("created_at", f"{date}T00:00:00")

        # label_printed 필터 (컬럼 존재 시)
        if status == '미출력':
            try:
                q = q.or_("label_printed.is.null,label_printed.eq.false")
            except Exception:
                pass
        elif status == '출력완료':
            try:
                q = q.eq("label_printed", True)
            except Exception:
                pass

        q = q.order("id").limit(200)
        res = q.execute()

        shipments = []
        for r in (res.data or []):
            shipments.append({
                'invoice_no': r.get('invoice_no', ''),
                'courier': r.get('courier', ''),
                'order_no': r.get('order_no', ''),
                'receiver_name': r.get('name', ''),
                'receiver_phone': r.get('phone', ''),
                'receiver_addr': r.get('address', ''),
                'receiver_zipcode': '',
                'product_name': '',
                'memo': r.get('memo', ''),
                'channel': r.get('channel', ''),
                'label_printed': r.get('label_printed', False),
                'label_printed_at': r.get('label_printed_at', ''),
            })
        return _enrich_with_products(db, shipments)
    except Exception as e:
        logger.error(f'[Label] 조건부 조회 오류: {e}')
        return []
