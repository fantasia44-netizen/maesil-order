"""
settlement_service.py -- 플랫폼 정산 서비스.

autotool의 order_transactions 데이터(같은 Supabase)를 활용하여
채널별 매출을 집계하고 platform_settlements에 저장.
수수료 자동 계산 (platform_fee_config 참조).
"""
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def sync_platform_settlements(db, channel, date_from, date_to):
    """주문 데이터(order_transactions)에서 채널별 매출 집계 → platform_settlements 저장.

    Args:
        db: SupabaseDB 인스턴스
        channel: 채널명 (smartstore, coupang, oasis, 11st, kakao 등)
        date_from: 시작일 (YYYY-MM-DD)
        date_to: 종료일 (YYYY-MM-DD)

    Returns:
        dict: {created_count, updated_count, total_settlement}
    """
    # 1. 주문 데이터 조회 (autotool의 order_transactions)
    orders = db.query_order_transactions(
        date_from=date_from,
        date_to=date_to,
        channel=channel,
        status='구매확정',
        limit=10000,
    )

    if not orders:
        logger.info(f"[정산] {channel} {date_from}~{date_to}: 주문 데이터 없음")
        return {'created_count': 0, 'updated_count': 0, 'total_settlement': 0}

    # 2. 수수료율 조회
    fee_configs = db.query_platform_fee_config(channel=channel)
    sales_commission_rate = 0
    for fc in fee_configs:
        if fc.get('fee_type') == 'sales_commission':
            sales_commission_rate = float(fc.get('rate', 0))
            break

    # 3. 날짜별로 집계
    by_date = {}
    for order in orders:
        order_date = str(order.get('order_date', ''))[:10]
        if not order_date:
            continue

        if order_date not in by_date:
            by_date[order_date] = {
                'gross_sales': 0,
                'delivery_fee': 0,
                'order_count': 0,
                'order_details': [],
            }

        # 결제금액 (payment_amount 또는 total_amount)
        amount = int(order.get('payment_amount', 0) or order.get('total_amount', 0) or 0)
        delivery = int(order.get('delivery_fee', 0) or 0)

        by_date[order_date]['gross_sales'] += amount
        by_date[order_date]['delivery_fee'] += delivery
        by_date[order_date]['order_count'] += 1
        by_date[order_date]['order_details'].append({
            'order_no': order.get('order_no', ''),
            'product_name': order.get('product_name', ''),
            'amount': amount,
        })

    # 4. 정산 레코드 생성/업데이트
    created = 0
    updated = 0
    total_settlement = 0

    for settle_date, data in by_date.items():
        gross = data['gross_sales']
        platform_fee = int(gross * sales_commission_rate / 100)
        net = gross - platform_fee

        payload = {
            'channel': channel,
            'settlement_date': settle_date,
            'settlement_period_from': settle_date,
            'settlement_period_to': settle_date,
            'gross_sales': gross,
            'platform_fee': platform_fee,
            'delivery_fee': data['delivery_fee'],
            'net_settlement': net,
            'order_details': data['order_details'][:50],  # JSON 크기 제한
            'fee_details': {
                'sales_commission_rate': sales_commission_rate,
                'sales_commission': platform_fee,
            },
            'api_reference': f"{channel}_{settle_date}",
            'synced_at': datetime.now(timezone.utc).isoformat(),
        }

        # upsert (insert_platform_settlement은 upsert 지원)
        db.insert_platform_settlement(payload)
        created += 1
        total_settlement += net

    logger.info(
        f"[정산] {channel} {date_from}~{date_to}: "
        f"{created}건 동기화, 총 정산금 {total_settlement:,}원"
    )

    return {
        'created_count': created,
        'updated_count': updated,
        'total_settlement': total_settlement,
    }


def sync_all_channels(db, date_from, date_to):
    """전체 채널 일괄 정산 동기화.

    Returns:
        list: [{channel, created_count, total_settlement, error?}]
    """
    channels = ['smartstore', 'coupang', 'oasis', '11st', 'kakao']
    results = []

    for ch in channels:
        try:
            r = sync_platform_settlements(db, ch, date_from, date_to)
            results.append({'channel': ch, **r})
        except Exception as e:
            logger.error(f"[정산] {ch} 동기화 오류: {e}")
            results.append({'channel': ch, 'error': str(e)})

    return results


def get_settlement_summary(db, date_from=None, date_to=None):
    """플랫폼 정산 현황 요약 (대시보드용).

    Returns:
        dict: {
            total_settlements, matched_count, pending_count,
            total_gross, total_fee, total_net,
            by_channel: [{channel, gross, fee, net, count, match_status}]
        }
    """
    settlements = db.query_platform_settlements(
        date_from=date_from, date_to=date_to)

    total_gross = 0
    total_fee = 0
    total_net = 0
    matched = 0
    pending = 0
    by_channel = {}

    for s in settlements:
        gross = s.get('gross_sales', 0)
        fee = s.get('platform_fee', 0)
        net = s.get('net_settlement', 0)
        ch = s.get('channel', '기타')
        status = s.get('match_status', 'pending')

        total_gross += gross
        total_fee += fee
        total_net += net

        if status == 'matched':
            matched += 1
        else:
            pending += 1

        if ch not in by_channel:
            by_channel[ch] = {
                'channel': ch,
                'gross': 0,
                'fee': 0,
                'net': 0,
                'count': 0,
                'matched': 0,
                'pending': 0,
            }
        by_channel[ch]['gross'] += gross
        by_channel[ch]['fee'] += fee
        by_channel[ch]['net'] += net
        by_channel[ch]['count'] += 1
        if status == 'matched':
            by_channel[ch]['matched'] += 1
        else:
            by_channel[ch]['pending'] += 1

    return {
        'total_settlements': len(settlements),
        'matched_count': matched,
        'pending_count': pending,
        'total_gross': total_gross,
        'total_fee': total_fee,
        'total_net': total_net,
        'by_channel': sorted(by_channel.values(), key=lambda x: -x['net']),
    }


# ── 채널 표시명 ──
CHANNEL_DISPLAY = {
    'smartstore': '스마트스토어',
    'coupang': '쿠팡',
    'oasis': '오아시스',
    '11st': '11번가',
    'kakao': '카카오',
    'auction': '옥션',
    'gmarket': 'G마켓',
}


def get_channel_display(channel):
    """채널 코드 → 표시명."""
    return CHANNEL_DISPLAY.get(channel, channel)


def compare_api_vs_calculated_settlements(db, channel, date_from, date_to):
    """API 정산(실제 마켓플레이스 데이터) vs 계산 정산(엑셀 주문 기반) 비교.

    Returns:
        dict: {channel, summary, daily_comparison}
    """
    api_settlements = db.query_api_settlements(
        channel=channel, date_from=date_from, date_to=date_to)
    platform_settlements = db.query_platform_settlements(
        channel=channel, date_from=date_from, date_to=date_to)

    # 날짜별 인덱싱
    api_by_date = {}
    for s in api_settlements:
        d = str(s.get('settlement_date', ''))[:10]
        if d not in api_by_date:
            api_by_date[d] = {'gross': 0, 'fee': 0, 'net': 0}
        api_by_date[d]['gross'] += int(s.get('gross_sales', 0))
        api_by_date[d]['fee'] += int(s.get('total_commission', 0))
        api_by_date[d]['net'] += int(s.get('net_settlement', 0))

    plat_by_date = {}
    for s in platform_settlements:
        d = str(s.get('settlement_date', ''))[:10]
        if d not in plat_by_date:
            plat_by_date[d] = {'gross': 0, 'fee': 0, 'net': 0}
        plat_by_date[d]['gross'] += int(s.get('gross_sales', 0))
        plat_by_date[d]['fee'] += int(s.get('platform_fee', 0))
        plat_by_date[d]['net'] += int(s.get('net_settlement', 0))

    all_dates = sorted(set(list(api_by_date.keys()) + list(plat_by_date.keys())))
    daily = []
    total_diff = 0

    for d in all_dates:
        api = api_by_date.get(d, {'gross': 0, 'fee': 0, 'net': 0})
        plat = plat_by_date.get(d, {'gross': 0, 'fee': 0, 'net': 0})
        diff = api['net'] - plat['net']
        total_diff += diff
        daily.append({
            'date': d,
            'api_gross': api['gross'],
            'api_fee': api['fee'],
            'api_net': api['net'],
            'calc_gross': plat['gross'],
            'calc_fee': plat['fee'],
            'calc_net': plat['net'],
            'diff': diff,
        })

    return {
        'channel': channel,
        'summary': {
            'api_total_net': sum(a['gross'] for a in api_by_date.values()) if api_by_date else 0,
            'calc_total_net': sum(p['gross'] for p in plat_by_date.values()) if plat_by_date else 0,
            'total_diff': total_diff,
            'days_compared': len(all_dates),
        },
        'daily_comparison': daily,
    }
