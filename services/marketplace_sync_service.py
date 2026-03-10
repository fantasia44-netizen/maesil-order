"""
marketplace_sync_service.py — 마켓플레이스 API 동기화 오케스트레이션.

주문/정산 데이터를 API에서 가져와 api_orders / api_settlements에 저장.
"""
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def sync_orders(db, marketplace_mgr, channel, date_from, date_to,
                triggered_by='system'):
    """채널 주문 데이터 API 동기화.

    Returns:
        dict: {fetched, new, updated, errors, log_id}
    """
    client = marketplace_mgr.get_client(channel)
    if not client or not client.is_ready:
        return {'fetched': 0, 'new': 0, 'updated': 0,
                'error': f'{channel} 클라이언트 미준비'}

    # 동기화 로그 시작
    log = db.insert_api_sync_log({
        'channel': channel,
        'sync_type': 'orders',
        'status': 'running',
        'date_from': date_from,
        'date_to': date_to,
        'triggered_by': triggered_by,
    })
    log_id = log['id'] if log else None

    try:
        # 토큰 리프레시
        if not client.refresh_token(db):
            _finish_log(db, log_id, 'error', error_message='토큰 갱신 실패')
            return {'fetched': 0, 'new': 0, 'updated': 0,
                    'error': '토큰 갱신 실패', 'log_id': log_id}

        # 주문 조회
        orders = client.fetch_orders(date_from, date_to)
        fetched = len(orders)

        if not orders:
            _finish_log(db, log_id, 'success', fetched=0)
            return {'fetched': 0, 'new': 0, 'updated': 0, 'log_id': log_id}

        # DB 저장
        result = db.upsert_api_orders_batch(orders)

        _finish_log(db, log_id, 'success',
                    fetched=fetched,
                    new=result.get('new', 0),
                    updated=result.get('updated', 0))

        # config의 last_synced_at 업데이트
        db.upsert_marketplace_api_config({
            'channel': channel,
            'last_synced_at': datetime.now(timezone.utc).isoformat(),
        })

        logger.info(f'[동기화] {channel} 주문 {fetched}건 (신규 {result.get("new", 0)})')
        return {
            'fetched': fetched,
            'new': result.get('new', 0),
            'updated': result.get('updated', 0),
            'log_id': log_id,
        }

    except Exception as e:
        logger.error(f'[동기화] {channel} 주문 오류: {e}')
        _finish_log(db, log_id, 'error', error_message=str(e))
        return {'fetched': 0, 'new': 0, 'updated': 0,
                'error': str(e), 'log_id': log_id}


def sync_settlements(db, marketplace_mgr, channel, date_from, date_to,
                     triggered_by='system'):
    """채널 정산 데이터 API 동기화.

    Returns:
        dict: {fetched, new, updated, errors, log_id}
    """
    client = marketplace_mgr.get_client(channel)
    if not client or not client.is_ready:
        return {'fetched': 0, 'new': 0, 'updated': 0,
                'error': f'{channel} 클라이언트 미준비'}

    log = db.insert_api_sync_log({
        'channel': channel,
        'sync_type': 'settlements',
        'status': 'running',
        'date_from': date_from,
        'date_to': date_to,
        'triggered_by': triggered_by,
    })
    log_id = log['id'] if log else None

    try:
        if not client.refresh_token(db):
            _finish_log(db, log_id, 'error', error_message='토큰 갱신 실패')
            return {'fetched': 0, 'new': 0, 'updated': 0,
                    'error': '토큰 갱신 실패', 'log_id': log_id}

        settlements = client.fetch_settlements(date_from, date_to)
        fetched = len(settlements)

        if not settlements:
            _finish_log(db, log_id, 'success', fetched=0)
            return {'fetched': 0, 'new': 0, 'updated': 0, 'log_id': log_id}

        db.upsert_api_settlements_batch(settlements)

        _finish_log(db, log_id, 'success', fetched=fetched, new=fetched)

        logger.info(f'[동기화] {channel} 정산 {fetched}건')
        return {'fetched': fetched, 'new': fetched, 'updated': 0, 'log_id': log_id}

    except Exception as e:
        logger.error(f'[동기화] {channel} 정산 오류: {e}')
        _finish_log(db, log_id, 'error', error_message=str(e))
        return {'fetched': 0, 'new': 0, 'updated': 0,
                'error': str(e), 'log_id': log_id}


def sync_all_channels(db, marketplace_mgr, date_from, date_to,
                      triggered_by='system'):
    """전체 활성 채널 동기화.

    Returns:
        list: [{channel, orders, settlements}]
    """
    results = []
    for channel in marketplace_mgr.get_active_channels():
        r = {
            'channel': channel,
            'orders': sync_orders(db, marketplace_mgr, channel,
                                  date_from, date_to, triggered_by),
            'settlements': sync_settlements(db, marketplace_mgr, channel,
                                            date_from, date_to, triggered_by),
        }
        results.append(r)
    return results


def _finish_log(db, log_id, status, fetched=0, new=0, updated=0,
                error_message=None):
    """동기화 로그 완료 처리."""
    if not log_id:
        return
    update = {
        'status': status,
        'finished_at': datetime.now(timezone.utc).isoformat(),
        'records_fetched': fetched,
        'records_new': new,
        'records_updated': updated,
    }
    if error_message:
        update['error_message'] = error_message[:500]
    db.update_api_sync_log(log_id, update)
