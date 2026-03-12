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


def sync_revenue_fees(db, marketplace_mgr, channel, date_from, date_to,
                      triggered_by='system'):
    """쿠팡 매출내역(revenue-history) 동기화 — 일별 집계 → api_settlements 저장.

    쿠팡 ordersheets에 없는 수수료/정산 데이터를 revenue-history에서 가져와
    saleDate 기준으로 일별 집계하여 api_settlements에 저장한다.

    Note: ordersheets orderId와 revenue-history orderId는 서로 다른 ID 체계이므로
    개별 주문 매칭이 불가능하다. 따라서 일별 합산으로 정산 데이터를 관리한다.

    Returns:
        dict: {fetched, saved, items_total, error}
    """
    client = marketplace_mgr.get_client(channel)
    if not client or not client.is_ready:
        return {'fetched': 0, 'saved': 0, 'error': f'{channel} 클라이언트 미준비'}

    # revenue-history는 현재 쿠팡만 지원
    if not hasattr(client, 'fetch_revenue_history'):
        return {'fetched': 0, 'saved': 0, 'error': f'{channel}은 매출내역 API 미지원'}

    log = db.insert_api_sync_log({
        'channel': channel,
        'sync_type': 'revenue_fees',
        'status': 'running',
        'date_from': date_from,
        'date_to': date_to,
        'triggered_by': triggered_by,
    })
    log_id = log['id'] if log else None

    try:
        if not client.refresh_token(db):
            _finish_log(db, log_id, 'error', error_message='토큰 갱신 실패')
            return {'fetched': 0, 'saved': 0, 'error': '토큰 갱신 실패'}

        records = client.fetch_revenue_history(date_from, date_to)
        fetched = len(records)

        if not records:
            _finish_log(db, log_id, 'success', fetched=0)
            return {'fetched': 0, 'saved': 0}

        # ── saleDate 기준 일별 집계 ──
        daily = {}  # {saleDate: {totals...}}
        items_total = 0

        for record in records:
            sale_date = str(record.get('saleDate', ''))[:10]
            if not sale_date:
                continue

            delivery_fee = record.get('deliveryFee', {})
            del_amount = int(delivery_fee.get('amount', 0))
            del_fee = int(delivery_fee.get('fee', 0))
            del_fee_vat = int(delivery_fee.get('feeVat', 0))
            del_settlement = int(delivery_fee.get('settlementAmount', 0))

            if sale_date not in daily:
                daily[sale_date] = {
                    'sale_amount': 0,
                    'service_fee': 0,
                    'service_fee_vat': 0,
                    'settlement_amount': 0,
                    'courantee_fee': 0,
                    'courantee_fee_vat': 0,
                    'store_fee_discount': 0,
                    'seller_discount_coupon': 0,
                    'downloadable_coupon': 0,
                    'coupang_discount_coupon': 0,
                    'delivery_fee': 0,
                    'delivery_fee_commission': 0,
                    'delivery_settlement': 0,
                    'item_count': 0,
                    'order_count': 0,
                }

            d = daily[sale_date]
            d['order_count'] += 1
            d['delivery_fee'] += del_amount
            d['delivery_fee_commission'] += del_fee + del_fee_vat
            d['delivery_settlement'] += del_settlement

            for item in record.get('items', []):
                d['sale_amount'] += int(item.get('saleAmount', 0))
                d['service_fee'] += int(item.get('serviceFee', 0))
                d['service_fee_vat'] += int(item.get('serviceFeeVat', 0))
                d['settlement_amount'] += int(item.get('settlementAmount', 0))
                d['courantee_fee'] += int(item.get('couranteeFee', 0))
                d['courantee_fee_vat'] += int(item.get('couranteeFeeVat', 0))
                d['store_fee_discount'] += int(item.get('storeFeeDiscount', 0))
                d['seller_discount_coupon'] += int(item.get('sellerDiscountCoupon', 0))
                d['downloadable_coupon'] += int(item.get('downloadableCoupon', 0))
                d['coupang_discount_coupon'] += int(item.get('coupangDiscountCoupon', 0))
                d['item_count'] += 1
                items_total += 1

        # ── api_settlements로 변환 후 upsert ──
        settlements = []
        for sale_date, d in sorted(daily.items()):
            total_commission = d['service_fee'] + d['service_fee_vat']
            settlements.append({
                'channel': channel,
                'settlement_date': sale_date,
                'settlement_id': f'revenue_{sale_date}',
                'gross_sales': d['sale_amount'],
                'total_commission': total_commission,
                'shipping_fee_income': d['delivery_fee'],
                'shipping_fee_cost': d['delivery_fee_commission'],
                'coupon_discount': (d['seller_discount_coupon'] +
                                    d['downloadable_coupon'] +
                                    d['coupang_discount_coupon']),
                'point_discount': 0,
                'other_deductions': d['courantee_fee'] + d['courantee_fee_vat'],
                'net_settlement': d['settlement_amount'] + d['delivery_settlement'],
                'fee_breakdown': {
                    'source': 'revenue-history',
                    'service_fee': d['service_fee'],
                    'service_fee_vat': d['service_fee_vat'],
                    'courantee_fee': d['courantee_fee'],
                    'courantee_fee_vat': d['courantee_fee_vat'],
                    'store_fee_discount': d['store_fee_discount'],
                    'delivery_fee': d['delivery_fee'],
                    'delivery_fee_commission': d['delivery_fee_commission'],
                    'delivery_settlement': d['delivery_settlement'],
                    'item_count': d['item_count'],
                    'order_count': d['order_count'],
                },
            })

        db.upsert_api_settlements_batch(settlements)
        saved = len(settlements)

        _finish_log(db, log_id, 'success', fetched=fetched, new=saved)
        logger.info(f'[동기화] {channel} 매출내역 {fetched}건 → '
                     f'{saved}일 정산 저장 (아이템 {items_total}건)')
        return {'fetched': fetched, 'saved': saved, 'items_total': items_total}

    except Exception as e:
        logger.error(f'[동기화] {channel} 매출내역 오류: {e}')
        _finish_log(db, log_id, 'error', error_message=str(e))
        return {'fetched': 0, 'saved': 0, 'error': str(e)}


def sync_ad_costs(db, ad_client, date_from, date_to, triggered_by='system'):
    """네이버 검색광고 일별 광고비 동기화 → api_settlements 저장.

    Returns:
        dict: {fetched, saved, total_cost, error}
    """
    if not ad_client or not ad_client.is_ready:
        return {'fetched': 0, 'saved': 0, 'error': '광고 API 클라이언트 미준비'}

    log = db.insert_api_sync_log({
        'channel': '스마트스토어',
        'sync_type': 'ad_costs',
        'status': 'running',
        'date_from': date_from,
        'date_to': date_to,
        'triggered_by': triggered_by,
    })
    log_id = log['id'] if log else None

    try:
        records = ad_client.fetch_daily_ad_cost(date_from, date_to)
        fetched = len(records)

        if not records:
            _finish_log(db, log_id, 'success', fetched=0)
            return {'fetched': 0, 'saved': 0}

        # 일별 집계
        daily = {}
        for r in records:
            d = r['date']
            if d not in daily:
                daily[d] = {
                    'cost': 0, 'clicks': 0, 'impressions': 0,
                    'conversions': 0, 'conv_amt': 0, 'campaigns': [],
                }
            daily[d]['cost'] += r['cost']
            daily[d]['clicks'] += r['clicks']
            daily[d]['impressions'] += r['impressions']
            daily[d]['conversions'] += r['conversions']
            daily[d]['conv_amt'] += r['conversion_amount']
            daily[d]['campaigns'].append({
                'id': r['campaign_id'],
                'name': r['campaign_name'],
                'cost': r['cost'],
                'clicks': r['clicks'],
            })

        # api_settlements에 저장 (channel=스마트스토어, settlement_id=ad_cost_날짜)
        settlements = []
        total_cost = 0
        for dt, v in sorted(daily.items()):
            total_cost += v['cost']
            settlements.append({
                'channel': '스마트스토어',
                'settlement_date': dt,
                'settlement_id': f'ad_cost_{dt}',
                'gross_sales': 0,
                'total_commission': 0,
                'shipping_fee_income': 0,
                'shipping_fee_cost': 0,
                'coupon_discount': 0,
                'point_discount': 0,
                'other_deductions': v['cost'],
                'net_settlement': -v['cost'],
                'fee_breakdown': {
                    'source': 'naver-searchad',
                    'ad_cost': v['cost'],
                    'clicks': v['clicks'],
                    'impressions': v['impressions'],
                    'conversions': v['conversions'],
                    'conversion_amount': v['conv_amt'],
                    'campaigns': v['campaigns'],
                },
            })

        db.upsert_api_settlements_batch(settlements)
        saved = len(settlements)

        _finish_log(db, log_id, 'success', fetched=fetched, new=saved)
        logger.info(f'[동기화] 네이버광고 {fetched}건 → {saved}일 저장 '
                     f'(총 {total_cost:,}원)')
        return {'fetched': fetched, 'saved': saved, 'total_cost': total_cost}

    except Exception as e:
        logger.error(f'[동기화] 네이버광고 오류: {e}')
        _finish_log(db, log_id, 'error', error_message=str(e))
        return {'fetched': 0, 'saved': 0, 'error': str(e)}


def sync_all_channels(db, marketplace_mgr, date_from, date_to,
                      triggered_by='system', ad_client=None):
    """전체 활성 채널 동기화.

    Returns:
        list: [{channel, orders, settlements, revenue_fees}]
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
        # 쿠팡: 매출내역으로 수수료 업데이트
        if channel == '쿠팡':
            r['revenue_fees'] = sync_revenue_fees(
                db, marketplace_mgr, channel,
                date_from, date_to, triggered_by)
        results.append(r)

    # 네이버 검색광고 비용 동기화
    if ad_client and ad_client.is_ready:
        ad_result = sync_ad_costs(db, ad_client, date_from, date_to, triggered_by)
        results.append({'channel': '네이버광고', 'ad_costs': ad_result})

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


# ================================================================
# 송장 Push (발송처리)
# ================================================================

def push_invoices(db, marketplace_mgr, channel, triggered_by='system'):
    """마켓플레이스에 송장번호 일괄 전송 (발송처리).

    1. order_shipping에서 대기 중 송장 조회
    2. api_orders와 매핑하여 마켓 주문ID 확보
    3. client.register_invoice() 호출
    4. 성공 건: shipping_status='발송' 업데이트
    5. api_sync_log에 이력 기록

    Returns:
        dict: {total, success, failed, errors, log_id}
    """
    from config import Config

    client = marketplace_mgr.get_client(channel)
    if not client or not client.is_ready:
        return {'total': 0, 'success': 0, 'failed': 0,
                'error': f'{channel} 클라이언트 미준비'}

    # 동기화 로그
    log = db.insert_api_sync_log({
        'channel': channel,
        'sync_type': 'invoice_push',
        'status': 'running',
        'triggered_by': triggered_by,
    })
    log_id = log.get('id') if log else None

    try:
        # 토큰 갱신
        client.refresh_token(db)

        # 대기 중 송장 조회
        pending = db.query_pending_invoice_push(channel=channel)
        if not pending:
            _finish_log(db, log_id, 'success', fetched=0)
            return {'total': 0, 'success': 0, 'failed': 0, 'log_id': log_id}

        # api_order_id 있는 건만 필터 (마켓 매핑 필수)
        pushable = [p for p in pending if p.get('api_order_id')]
        if not pushable:
            _finish_log(db, log_id, 'success', fetched=len(pending),
                        error_message=f'api_orders 매핑 없는 건 {len(pending)}개')
            return {'total': len(pending), 'success': 0,
                    'failed': len(pending), 'log_id': log_id,
                    'error': 'api_orders 매핑 없음'}

        # 택배사 코드 변환
        courier_name = Config.DEFAULT_COURIER
        courier_codes = Config.COURIER_CODES.get(courier_name, {})

        # 채널별 택배사 코드 결정
        channel_key_map = {
            '스마트스토어': 'naver', '해미애찬': 'naver',
            '쿠팡': 'coupang', '자사몰': 'cafe24',
        }
        code_key = channel_key_map.get(channel, 'naver')
        courier_code = courier_codes.get(code_key, 'CJGLS')

        # register_invoice 입력 데이터 구성
        invoice_data = []
        for p in pushable:
            invoice_data.append({
                'api_order_id': p['api_order_id'],
                'api_line_id': p['api_line_id'],
                'invoice_no': p['invoice_no'],
                'courier_code': courier_code,
                'raw_data': p.get('raw_data', {}),
            })

        # API 호출
        results = client.register_invoice(invoice_data)

        # 결과 처리
        success_count = 0
        fail_count = 0
        errors = []
        status_updates = []

        # order_no 매핑 (결과에서 shipping_status 업데이트용)
        api_to_order = {}
        for p in pushable:
            api_to_order[p['api_order_id']] = (p['channel'], p['order_no'])

        for r in results:
            if r.get('success'):
                success_count += 1
                pair = api_to_order.get(r['api_order_id'])
                if pair:
                    status_updates.append({
                        'channel': pair[0],
                        'order_no': pair[1],
                        'shipping_status': '발송',
                    })
            else:
                fail_count += 1
                if r.get('error'):
                    errors.append(f"{r['api_order_id']}: {r['error']}")

        # shipping_status 업데이트
        if status_updates:
            db.bulk_update_shipping_status(status_updates)

        _finish_log(db, log_id, 'success',
                    fetched=len(pushable),
                    new=success_count,
                    updated=fail_count,
                    error_message='; '.join(errors[:10]) if errors else None)

        logger.info(f'[{channel}] 송장 push 완료: '
                    f'{success_count}/{len(pushable)} 성공')

        return {
            'total': len(pushable),
            'success': success_count,
            'failed': fail_count,
            'errors': errors[:20],
            'log_id': log_id,
        }

    except Exception as e:
        logger.error(f'[{channel}] 송장 push 오류: {e}')
        _finish_log(db, log_id, 'error', error_message=str(e)[:500])
        return {'total': 0, 'success': 0, 'failed': 0,
                'error': str(e), 'log_id': log_id}
