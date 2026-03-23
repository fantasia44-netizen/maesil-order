"""
health_monitor.py — 자체 모니터링 서비스.

외부 도구(Sentry 등) 없이 최소한의 모니터링:
1. 헬스체크: DB 연결, 외부 API 토큰 상태
2. 에러 집계: 최근 에러 기록 + 카운트
3. 일일 리포트: 주문수집/송장/에러 요약
"""
import logging
import os
from collections import deque
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# 최근 에러 버퍼 (메모리, 최대 100건)
_error_buffer = deque(maxlen=100)
_daily_stats = {
    'errors': 0,
    'api_calls': 0,
    'orders_collected': 0,
    'invoices_pushed': 0,
    'date': None,
}


def record_error(source: str, message: str, details: str = ''):
    """에러 기록."""
    now = datetime.now()
    _error_buffer.append({
        'time': now.isoformat()[:19],
        'source': source,
        'message': str(message)[:200],
        'details': str(details)[:500],
    })
    _reset_daily_if_needed()
    _daily_stats['errors'] += 1
    logger.error(f'[Monitor] {source}: {message}')


def record_stat(key: str, count: int = 1):
    """일일 통계 기록."""
    _reset_daily_if_needed()
    if key in _daily_stats:
        _daily_stats[key] += count


def _reset_daily_if_needed():
    """날짜 바뀌면 일일 통계 리셋."""
    today = datetime.now().strftime('%Y-%m-%d')
    if _daily_stats['date'] != today:
        _daily_stats['errors'] = 0
        _daily_stats['api_calls'] = 0
        _daily_stats['orders_collected'] = 0
        _daily_stats['invoices_pushed'] = 0
        _daily_stats['date'] = today


def check_health(db=None, marketplace_mgr=None):
    """시스템 헬스체크.

    Returns:
        {status: 'ok'|'degraded'|'down', checks: [...], summary: str}
    """
    checks = []
    overall = 'ok'

    # 1. DB 연결
    try:
        if db and hasattr(db, 'client') and db.client:
            res = db.client.table('order_transactions').select('id').limit(1).execute()
            checks.append({'name': 'DB연결', 'status': 'ok', 'detail': 'Supabase 정상'})
        else:
            checks.append({'name': 'DB연결', 'status': 'warn', 'detail': 'DB 인스턴스 없음'})
            overall = 'degraded'
    except Exception as e:
        checks.append({'name': 'DB연결', 'status': 'error', 'detail': str(e)[:100]})
        overall = 'down'

    # 2. 마켓플레이스 API 토큰
    if marketplace_mgr:
        for ch in ['스마트스토어_배마마', '쿠팡', '자사몰']:
            client = marketplace_mgr.get_client(ch)
            if client:
                ready = client.is_ready
                checks.append({
                    'name': f'마켓API({ch})',
                    'status': 'ok' if ready else 'warn',
                    'detail': '연결됨' if ready else '토큰 만료/미설정',
                })
                if not ready:
                    overall = 'degraded' if overall == 'ok' else overall

    # 3. CJ 택배 API
    try:
        cj_cust = os.getenv('CJ_CUST_ID', '')
        if cj_cust:
            checks.append({'name': 'CJ택배', 'status': 'ok', 'detail': f'고객ID: {cj_cust[:4]}...'})
        else:
            checks.append({'name': 'CJ택배', 'status': 'warn', 'detail': 'CJ_CUST_ID 미설정'})
    except Exception as e:
        checks.append({'name': 'CJ택배', 'status': 'error', 'detail': str(e)[:100]})

    # 4. 최근 에러
    recent_errors = len([e for e in _error_buffer
                        if e['time'] > (datetime.now() - timedelta(hours=1)).isoformat()[:19]])
    checks.append({
        'name': '최근에러(1h)',
        'status': 'ok' if recent_errors == 0 else ('warn' if recent_errors < 5 else 'error'),
        'detail': f'{recent_errors}건',
    })
    if recent_errors >= 5:
        overall = 'degraded'

    summary = f"상태: {overall} | DB/API/CJ 체크 {len(checks)}항목 | 에러 {recent_errors}건/1h"

    return {
        'status': overall,
        'checks': checks,
        'summary': summary,
        'timestamp': datetime.now().isoformat()[:19],
    }


def get_daily_report():
    """일일 리포트."""
    _reset_daily_if_needed()
    return {
        **_daily_stats,
        'recent_errors': list(_error_buffer)[-10:],
        'error_buffer_size': len(_error_buffer),
    }


def get_recent_errors(limit=20):
    """최근 에러 목록."""
    return list(_error_buffer)[-limit:]
