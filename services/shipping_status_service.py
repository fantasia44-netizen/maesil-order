"""
shipping_status_service.py — 마켓플레이스 운송상태 자동 추적 서비스.

각 채널 API에서 주문 상태를 폴링 → 정규화 → DB 반영.
"""
import logging
from datetime import datetime

from services.channel_config import PLATFORM_MAP

logger = logging.getLogger(__name__)

# 채널별 원본 상태 → 정규화 상태 매핑
STATUS_MAP = {
    'naver': {
        'PAYED': '발송대기',
        'PAYMENT_WAITING': '결제대기',
        'DELIVERING': '배송중',
        'DELIVERED': '배송완료',
        'PURCHASE_DECIDED': '구매확정',
        'EXCHANGED': '교환완료',
        'CANCELED': '취소',
        'RETURNED': '반품완료',
        'NOT_YET': '발송대기',
    },
    'coupang': {
        'ACCEPT': '발송대기',
        'INSTRUCT': '발송대기',
        'DEPARTURE': '발송완료',
        'DELIVERING': '배송중',
        'FINAL_DELIVERY': '배송완료',
        'NONE_TRACKING': '배송중',
    },
    'cafe24': {
        'N00': '입금대기',
        'N10': '결제완료',
        'N20': '상품준비중',
        'N22': '발송완료',
        'N30': '배송중',
        'N40': '배송완료',
        'N50': '구매확정',
    },
}

# register_invoice로 발송처리한 뒤 = 최소 '발송완료'
# '배송완료' 또는 '구매확정'이면 더 이상 추적 불필요
TRACKING_DONE_STATUSES = {'배송완료', '구매확정', '취소', '반품완료', '교환완료'}


def normalize_status(platform: str, raw_status: str) -> str:
    """플랫폼별 원본 상태를 정규화된 상태로 변환."""
    platform_map = STATUS_MAP.get(platform, {})
    return platform_map.get(raw_status, raw_status)


def sync_shipping_status(db, marketplace_mgr, channel=None):
    """발송 완료 but 배송완료 아닌 주문의 상태를 마켓 API로 업데이트.

    Args:
        db: SupabaseDB 인스턴스
        marketplace_mgr: MarketplaceManager 인스턴스
        channel: 특정 채널만 (None이면 전체)

    Returns:
        dict: {channels: [{channel, total, updated, errors}], summary: {...}}
    """
    channels = [channel] if channel else marketplace_mgr.get_active_channels()
    results = []

    for ch in channels:
        ch_result = {'channel': ch, 'total': 0, 'updated': 0, 'errors': []}
        platform = PLATFORM_MAP.get(ch, '')

        client = marketplace_mgr.get_client(ch)
        if not client:
            ch_result['errors'].append('클라이언트 없음')
            results.append(ch_result)
            continue

        if not client.is_ready:
            try:
                client.refresh_token(db)
            except Exception:
                pass
            if not client.is_ready:
                ch_result['errors'].append('인증 미완료')
                results.append(ch_result)
                continue

        try:
            # 추적 대상: 발송완료 but 배송완료 아닌 건
            tracking_orders = db.query_shipped_orders_for_tracking(channel=ch, limit=200)
            ch_result['total'] = len(tracking_orders)

            if not tracking_orders:
                results.append(ch_result)
                continue

            # API에서 상태 조회
            order_ids = [o['api_order_id'] for o in tracking_orders if o.get('api_order_id')]
            if not order_ids:
                ch_result['errors'].append('api_order_id 매핑 없음')
                results.append(ch_result)
                continue

            statuses = client.fetch_order_statuses(order_ids)

            # 상태 맵 구성: api_order_id → raw_status
            status_lookup = {}
            for s in statuses:
                status_lookup[s['api_order_id']] = s.get('status_raw', '')

            # DB 업데이트 목록 구성
            updates = []
            now = datetime.utcnow().isoformat()
            for order in tracking_orders:
                aoid = order.get('api_order_id', '')
                if aoid not in status_lookup:
                    continue

                raw_status = status_lookup[aoid]
                normalized = normalize_status(platform, raw_status)

                # 현재 상태와 같으면 스킵
                if normalized == order.get('delivery_status'):
                    continue

                updates.append({
                    'channel': order['channel'],
                    'order_no': order['order_no'],
                    'delivery_status': normalized,
                    'delivery_status_raw': raw_status,
                    'delivery_status_updated_at': now,
                })

            if updates:
                ch_result['updated'] = db.bulk_update_delivery_status(updates)

        except Exception as e:
            logger.error(f'[ShippingStatus] {ch} 동기화 오류: {e}', exc_info=True)
            ch_result['errors'].append(str(e))

        results.append(ch_result)

    total_updated = sum(r['updated'] for r in results)
    total_tracked = sum(r['total'] for r in results)
    return {
        'channels': results,
        'summary': {
            'total_tracked': total_tracked,
            'total_updated': total_updated,
        },
    }
