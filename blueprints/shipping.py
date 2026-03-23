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
