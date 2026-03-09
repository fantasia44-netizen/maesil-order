"""
tax_invoice_service.py -- 세금계산서 비즈니스 로직.
팝빌에서 세금계산서 조회/동기화 + 발행 기능.
"""
import uuid
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════
# Popbill 세금계산서 동기화 (매출/매입 가져오기)
# ══════════════════════════════════════════

def _fmt_date(yyyymmdd):
    """YYYYMMDD → YYYY-MM-DD."""
    if not yyyymmdd or len(str(yyyymmdd)) != 8:
        return str(yyyymmdd) if yyyymmdd else ''
    s = str(yyyymmdd)
    return f'{s[:4]}-{s[4:6]}-{s[6:8]}'


def _map_tax_type(popbill_tax_type):
    """팝빌 과세유형 → DB 값."""
    return {'T': '과세', 'N': '면세', 'Z': '영세'}.get(popbill_tax_type, '과세')


def _map_status(state_code):
    """팝빌 상태코드 → DB status.
    3xx: 발행 / 4xx: 국세청 전송 완료 / 2xx: 임시저장
    """
    if not state_code:
        return 'issued'
    code = int(state_code) if str(state_code).isdigit() else 0
    if code >= 300:
        return 'issued'
    elif code >= 200:
        return 'draft'
    return 'issued'


def _get_val(obj, key, default=''):
    """팝빌 반환 객체에서 값을 안전하게 가져오기 (dict 또는 object)."""
    if isinstance(obj, dict):
        return obj.get(key, default) or default
    return getattr(obj, key, default) or default


def sync_tax_invoices(db, popbill_svc, start_date, end_date, direction='SELL'):
    """팝빌에서 세금계산서 목록을 조회하여 DB에 동기화.

    Args:
        db: SupabaseDB
        popbill_svc: PopbillService 인스턴스
        start_date: 시작일 (YYYYMMDD)
        end_date: 종료일 (YYYYMMDD)
        direction: 'SELL'(매출) / 'BUY'(매입)

    Returns:
        dict: {new_count, skipped_count, updated_count, total_fetched}
    """
    if not popbill_svc.is_ready:
        raise RuntimeError('Popbill SDK가 초기화되지 않았습니다.')

    new_count = 0
    skipped = 0
    updated = 0
    total_fetched = 0
    db_direction = 'sales' if direction == 'SELL' else 'purchase'

    # 페이지네이션으로 전체 조회
    page = 1
    per_page = 100

    while True:
        try:
            result = popbill_svc.search_invoices(
                direction=direction,
                start_date=start_date,
                end_date=end_date,
                page=page,
                per_page=per_page,
            )
        except Exception as e:
            logger.error(f"팝빌 세금계산서 조회 오류 (page={page}): {e}")
            raise

        items = getattr(result, 'list', None) or []
        total_fetched += len(items)

        for item in items:
            # 팝빌 SDK는 ntsconfirmNum (소문자 c) 또는 ntsConfirmNum 둘 다 가능
            nts_num = _get_val(item, 'ntsconfirmNum') or _get_val(item, 'ntsConfirmNum')
            mgt_key = _get_val(item, 'invoicerMgtKey') if direction == 'SELL' \
                else _get_val(item, 'invoiceeMgtKey')
            write_date_raw = _get_val(item, 'writeDate')
            issue_date_raw = _get_val(item, 'issueDate')
            tax_type_code = _get_val(item, 'taxType', 'T')
            state_code = _get_val(item, 'stateCode')

            supply_cost = int(_get_val(item, 'supplyCostTotal', '0') or '0')
            tax_total = int(_get_val(item, 'taxTotal', '0') or '0')
            total_amount = int(_get_val(item, 'totalAmount', '0') or '0')

            # 공급자/공급받는자 정보
            invoicer_corp_num = _get_val(item, 'invoicerCorpNum')
            invoicer_corp_name = _get_val(item, 'invoicerCorpName')
            invoicer_ceo = _get_val(item, 'invoicerCEOName')
            invoicee_corp_num = _get_val(item, 'invoiceeCorpNum')
            invoicee_corp_name = _get_val(item, 'invoiceeCorpName')
            invoicee_ceo = _get_val(item, 'invoiceeCEOName')

            # 중복 체크
            existing_id = db.check_tax_invoice_exists(
                invoice_number=nts_num if nts_num else None,
                mgt_key=mgt_key if mgt_key else None,
            )

            if existing_id:
                # 기존 건: 상태 업데이트만 (팝빌 상태가 바뀔 수 있음)
                new_status = _map_status(state_code)
                try:
                    db.update_tax_invoice(existing_id, {
                        'status': new_status,
                        'invoice_number': nts_num or None,
                    })
                    updated += 1
                except Exception:
                    pass
                skipped += 1
                continue

            # 신규 건 DB 저장
            payload = {
                'direction': db_direction,
                'invoice_number': nts_num,
                'mgt_key': mgt_key,
                'write_date': _fmt_date(write_date_raw),
                'issue_date': _fmt_date(issue_date_raw),
                'tax_type': _map_tax_type(tax_type_code),
                'supplier_corp_num': invoicer_corp_num,
                'supplier_corp_name': invoicer_corp_name,
                'supplier_ceo_name': invoicer_ceo,
                'buyer_corp_num': invoicee_corp_num,
                'buyer_corp_name': invoicee_corp_name,
                'buyer_ceo_name': invoicee_ceo,
                'supply_cost_total': supply_cost,
                'tax_total': tax_total,
                'total_amount': total_amount,
                'status': _map_status(state_code),
                'registered_by': 'popbill_sync',
            }

            try:
                invoice_id = db.insert_tax_invoice(payload)
                if invoice_id:
                    new_count += 1
                    logger.info(f"세금계산서 동기화 신규: ID={invoice_id}, "
                                f"방향={db_direction}, 금액={total_amount:,}")
                else:
                    skipped += 1
            except Exception as e:
                logger.error(f"세금계산서 동기화 저장 실패: {nts_num or mgt_key} — {e}")
                skipped += 1

        # 다음 페이지 여부
        total_count = getattr(result, 'total', 0) or 0
        if total_fetched >= total_count or len(items) < per_page:
            break
        page += 1

    logger.info(f"세금계산서 동기화 완료 ({direction}): "
                f"전체 {total_fetched}건, 신규 {new_count}건, "
                f"업데이트 {updated}건, 스킵 {skipped}건")

    return {
        'new_count': new_count,
        'skipped_count': skipped,
        'updated_count': updated,
        'total_fetched': total_fetched,
    }


def sync_all_tax_invoices(db, popbill_svc, start_date=None, end_date=None):
    """매출 + 매입 세금계산서 일괄 동기화.

    Args:
        db: SupabaseDB
        popbill_svc: PopbillService
        start_date: YYYYMMDD (기본: 3개월 전)
        end_date: YYYYMMDD (기본: 오늘)

    Returns:
        dict: {sell: {...}, buy: {...}}
    """
    from services.tz_utils import today_kst, days_ago_kst

    if not end_date:
        end_date = today_kst().replace('-', '')
    if not start_date:
        start_date = days_ago_kst(90).replace('-', '')

    results = {}
    for direction in ['SELL', 'BUY']:
        try:
            r = sync_tax_invoices(db, popbill_svc, start_date, end_date, direction)
            results[direction.lower()] = r
        except Exception as e:
            logger.error(f"세금계산서 동기화 오류 ({direction}): {e}")
            results[direction.lower()] = {'error': str(e)}

    return results


def generate_mgt_key():
    """팝빌 관리번호 생성 (유니크, 24자 이내).
    형식: AT + YYYYMMDDHHMMSS + 8자리 UUID = 24자
    """
    now = datetime.now().strftime('%Y%m%d%H%M%S')
    short_uid = uuid.uuid4().hex[:8].upper()
    return f'AT{now}{short_uid}'


def build_invoice_from_trade(db, partner_id, trade_date, items, tax_type='과세'):
    """거래 데이터 → 세금계산서 발행 데이터 빌드.

    Args:
        db: SupabaseDB
        partner_id: 거래처 ID (business_partners)
        trade_date: 작성일자 (YYYY-MM-DD)
        items: list of {product_name, qty, unit_price}
        tax_type: '과세' | '면세' | '영세'

    Returns:
        dict: issue_sales_invoice()에 전달할 형태
    """
    # 거래처 조회 (business_partners 테이블)
    partner = db.query_partner_by_id(partner_id)
    if not partner:
        raise ValueError(f'거래처 ID {partner_id}를 찾을 수 없습니다')

    # 우리 사업장 정보
    my_biz = db.query_default_business()

    # 면세/영세는 부가세 0%
    vat_rate = 0.1 if tax_type == '과세' else 0

    detail_items = []
    supply_total = 0
    for item in items:
        qty = int(item.get('qty', 0))
        unit_cost = int(item.get('unit_price', 0))
        supply = qty * unit_cost
        tax = int(supply * vat_rate)
        supply_total += supply
        detail_items.append({
            'name': item.get('product_name', ''),
            'qty': qty,
            'unit_cost': unit_cost,
            'supply_cost': supply,
            'tax': tax,
        })

    tax_total = int(supply_total * vat_rate)
    write_date = trade_date.replace('-', '')  # YYYYMMDD

    return {
        'write_date': write_date,
        'tax_type': tax_type,
        'mgt_key': generate_mgt_key(),
        'buyer_corp_num': partner.get('business_number', '').replace('-', ''),
        'buyer_corp_name': partner.get('partner_name', ''),
        'buyer_ceo_name': partner.get('representative', ''),
        'buyer_addr': partner.get('address', ''),
        'buyer_biz_type': partner.get('type', ''),
        'buyer_biz_class': partner.get('business_item', ''),
        'buyer_email': partner.get('email', ''),
        'supplier_corp_num': my_biz.get('business_number', '').replace('-', '') if my_biz else '',
        'supplier_corp_name': my_biz.get('business_name', '') if my_biz else '',
        'supplier_ceo_name': my_biz.get('representative', '') if my_biz else '',
        'supplier_addr': my_biz.get('address', '') if my_biz else '',
        'supplier_biz_type': my_biz.get('biz_type', '') if my_biz else '',
        'supplier_biz_class': my_biz.get('biz_class', '') if my_biz else '',
        'supplier_email': my_biz.get('email', '') if my_biz else '',
        'items': detail_items,
        'supply_cost_total': supply_total,
        'tax_total': tax_total,
        'total_amount': supply_total + tax_total,
    }


def save_invoice_to_db(db, invoice_data, direction, popbill_result=None, registered_by=''):
    """발행 결과를 DB에 저장.

    Returns:
        int: 생성된 tax_invoices.id
    """
    write_date = invoice_data['write_date']
    if len(write_date) == 8:
        write_date = f'{write_date[:4]}-{write_date[4:6]}-{write_date[6:8]}'

    payload = {
        'direction': direction,
        'invoice_number': popbill_result.get('nts_confirm_num', '') if popbill_result else '',
        'mgt_key': invoice_data.get('mgt_key', ''),
        'write_date': write_date,
        'issue_date': write_date,
        'tax_type': invoice_data.get('tax_type', '과세'),
        'supplier_corp_num': invoice_data.get('supplier_corp_num', ''),
        'supplier_corp_name': invoice_data.get('supplier_corp_name', ''),
        'supplier_ceo_name': invoice_data.get('supplier_ceo_name', ''),
        'buyer_corp_num': invoice_data.get('buyer_corp_num', ''),
        'buyer_corp_name': invoice_data.get('buyer_corp_name', ''),
        'buyer_ceo_name': invoice_data.get('buyer_ceo_name', ''),
        'supply_cost_total': invoice_data.get('supply_cost_total', 0),
        'tax_total': invoice_data.get('tax_total', 0),
        'total_amount': invoice_data.get('total_amount', 0),
        'items': invoice_data.get('items', []),
        'status': 'issued' if popbill_result else 'draft',
        'registered_by': registered_by,
    }
    invoice_id = db.insert_tax_invoice(payload)
    logger.info(f"세금계산서 DB 저장: ID={invoice_id}, 방향={direction}")

    # ── 자동 전표 생성 ──
    if invoice_id:
        try:
            from services.journal_service import (
                create_sales_invoice_journal, create_purchase_invoice_journal,
            )
            if direction == 'sales':
                create_sales_invoice_journal(db, invoice_id, created_by=registered_by or 'system')
            elif direction == 'purchase':
                create_purchase_invoice_journal(db, invoice_id, created_by=registered_by or 'system')
        except Exception as e:
            logger.error(f"세금계산서 자동 전표 생성 실패 (ID={invoice_id}): {e}")

    return invoice_id
