"""marketplace.py -- 마켓플레이스 API 연동 Blueprint.

네이버 스마트스토어 / 쿠팡 / Cafe24 API 설정, 동기화, 교차검증.
"""
import logging
from flask import (Blueprint, render_template, request, current_app,
                   jsonify, flash, redirect, url_for)
from flask_login import login_required, current_user
from auth import role_required, _log_action
from services.tz_utils import today_kst, days_ago_kst

logger = logging.getLogger(__name__)


def _cafe24_callback_url() -> str:
    """Cafe24 OAuth redirect_uri — 항상 HTTPS."""
    u = url_for('cafe24_oauth_redirect', _external=True)
    return u.replace('http://', 'https://', 1)

marketplace_bp = Blueprint('marketplace', __name__, url_prefix='/marketplace')


@marketplace_bp.route('/')
@role_required('admin', 'general')
def index():
    """API 연동 설정 대시보드."""
    db = current_app.db
    mgr = current_app.marketplace

    channels = mgr.get_all_channels()

    # 각 채널의 DB config 로드
    configs = {}
    for ch in channels:
        ch_name = ch['channel']
        rows = db.query_marketplace_api_configs(channel=ch_name)
        configs[ch_name] = rows[0] if rows else {}

    # 최근 동기화 로그
    recent_logs = db.query_api_sync_logs(limit=10)

    return render_template('marketplace/index.html',
                           channels=channels,
                           configs=configs,
                           recent_logs=recent_logs)


@marketplace_bp.route('/config/<channel>', methods=['POST'])
@role_required('admin')
def save_config(channel):
    """채널 API 설정 저장."""
    db = current_app.db
    mgr = current_app.marketplace

    payload = {
        'channel': channel,
        'client_id': request.form.get('client_id', '').strip(),
        'client_secret': request.form.get('client_secret', '').strip(),
        'is_active': request.form.get('is_active') == 'on',
    }

    # 채널별 추가 필드
    if channel == '쿠팡':
        payload['vendor_id'] = request.form.get('vendor_id', '').strip()
    elif channel == '자사몰':
        payload['mall_id'] = request.form.get('mall_id', '').strip()

    db.upsert_marketplace_api_config(payload)
    _log_action(f'마켓플레이스 API 설정 저장: {channel}')

    # 클라이언트 재로드
    mgr._load_configs(db)

    flash(f'{channel} API 설정이 저장되었습니다.', 'success')
    return redirect(url_for('marketplace.index'))


@marketplace_bp.route('/test/<channel>', methods=['POST'])
@role_required('admin')
def test_connection(channel):
    """API 연결 테스트."""
    db = current_app.db
    mgr = current_app.marketplace
    client = mgr.get_client(channel)

    if not client:
        return jsonify({'success': False, 'message': f'{channel} 클라이언트 없음'})

    result = client.test_connection(db)

    # Cafe24 OAuth 인증이 필요한 경우 올바른 redirect_uri로 auth_url 재생성
    if result.get('auth_url'):
        callback_url = _cafe24_callback_url()
        result['auth_url'] = client.get_auth_url(callback_url, state='connect')

    return jsonify(result)


@marketplace_bp.route('/oauth/authorize/<channel>')
@role_required('admin')
def oauth_authorize(channel):
    """OAuth2 인증 시작 (Cafe24 등)."""
    mgr = current_app.marketplace
    client = mgr.get_client(channel)
    if not client:
        flash(f'{channel} 클라이언트 없음', 'danger')
        return redirect(url_for('marketplace.index'))

    callback_url = _cafe24_callback_url()
    auth_url = client.get_auth_url(callback_url, state='connect')
    return redirect(auth_url)


@marketplace_bp.route('/oauth/callback/<channel>')
@role_required('admin')
def oauth_callback(channel):
    """OAuth2 콜백 — 인가 코드를 토큰으로 교환."""
    db = current_app.db
    mgr = current_app.marketplace
    client = mgr.get_client(channel)

    code = request.args.get('code', '')
    error = request.args.get('error', '')

    if error:
        flash(f'{channel} OAuth 인증 거부: {error}', 'danger')
        return redirect(url_for('marketplace.index'))

    if not code:
        flash(f'{channel} 인가 코드가 없습니다', 'danger')
        return redirect(url_for('marketplace.index'))

    callback_url = _cafe24_callback_url()
    result = client.exchange_code(db, code, callback_url)

    if result is True:
        mgr._load_configs(db)
        _log_action(f'{channel} OAuth 인증 완료')
        flash(f'{channel} OAuth 인증 성공!', 'success')
    else:
        flash(f'{channel} 토큰 교환 실패: {result}', 'danger')

    return redirect(url_for('marketplace.index'))


@marketplace_bp.route('/sync', methods=['POST'])
@role_required('admin', 'general')
def sync():
    """수동 동기화 실행."""
    try:
        db = current_app.db
        mgr = current_app.marketplace

        channel = request.form.get('channel', '')
        sync_type = request.form.get('sync_type', 'orders')
        date_from = request.form.get('date_from', days_ago_kst(7))
        date_to = request.form.get('date_to', today_kst())

        from services.marketplace_sync_service import sync_orders, sync_settlements

        results = {}

        if sync_type in ('orders', 'all'):
            results['orders'] = sync_orders(
                db, mgr, channel, date_from, date_to,
                triggered_by=current_user.username
            )

        if sync_type in ('settlements', 'all'):
            results['settlements'] = sync_settlements(
                db, mgr, channel, date_from, date_to,
                triggered_by=current_user.username
            )

        _log_action(f'마켓플레이스 동기화: {channel} {sync_type} {date_from}~{date_to}')
        return jsonify(results)

    except Exception as e:
        logger.error(f'[Sync] 동기화 오류: {e}', exc_info=True)
        return jsonify({'error': str(e)}), 500


@marketplace_bp.route('/diag/<channel>')
@role_required('admin')
def diag(channel):
    """API 진단 — 실제 API 응답 확인."""
    db = current_app.db
    mgr = current_app.marketplace
    client = mgr.get_client(channel)

    if not client:
        return jsonify({'error': f'{channel} 클라이언트 없음'})

    info = {
        'channel': channel,
        'is_ready': client.is_ready,
        'config_keys': list(client.config.keys()),
        'has_token': bool(client.config.get('access_token')),
        'token_expires': client.config.get('token_expires_at', ''),
    }

    # 토큰 갱신 시도
    try:
        refresh_ok = client.refresh_token(db)
        info['refresh_ok'] = refresh_ok
    except Exception as e:
        info['refresh_error'] = str(e)

    # 주문 조회 (최근 1일, 최대 3건)
    try:
        from services.tz_utils import today_kst, days_ago_kst
        orders = client.fetch_orders(days_ago_kst(1), today_kst())
        info['orders_count'] = len(orders)
        if orders:
            info['sample_order'] = {k: str(v)[:50] for k, v in orders[0].items()}
    except Exception as e:
        info['orders_error'] = str(e)

    return jsonify(info)


@marketplace_bp.route('/sync/log')
@role_required('admin', 'general')
def sync_log():
    """동기화 이력."""
    db = current_app.db
    channel = request.args.get('channel', '')
    logs = db.query_api_sync_logs(channel=channel or None, limit=100)
    return render_template('marketplace/sync_log.html', logs=logs, channel=channel)


@marketplace_bp.route('/validation')
@role_required('admin', 'general')
def validation():
    """교차검증 대시보드."""
    db = current_app.db
    channel = request.args.get('channel', '')
    date_from = request.args.get('date_from', days_ago_kst(7))
    date_to = request.args.get('date_to', today_kst())

    validation_result = None
    if channel:
        from services.marketplace_validation_service import validate_orders
        validation_result = validate_orders(db, channel, date_from, date_to)

    return render_template('marketplace/validation.html',
                           channel=channel,
                           date_from=date_from,
                           date_to=date_to,
                           result=validation_result)


@marketplace_bp.route('/api/validation/run', methods=['POST'])
@role_required('admin', 'general')
def run_validation():
    """교차검증 실행 (AJAX)."""
    db = current_app.db
    channel = request.form.get('channel', '')
    date_from = request.form.get('date_from', days_ago_kst(7))
    date_to = request.form.get('date_to', today_kst())

    if not channel:
        return jsonify({'error': '채널을 선택하세요'}), 400

    from services.marketplace_validation_service import validate_orders, validate_settlements

    result = {
        'orders': validate_orders(db, channel, date_from, date_to),
        'settlements': validate_settlements(db, channel, date_from, date_to),
    }

    _log_action(f'마켓플레이스 교차검증: {channel} {date_from}~{date_to}')
    return jsonify(result)
