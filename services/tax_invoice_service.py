"""
tax_invoice_service.py -- 세금계산서 비즈니스 로직.
"""
import uuid
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


def generate_mgt_key():
    """팝빌 관리번호 생성 (유니크, 24자 이내).
    형식: AT + YYYYMMDDHHMMSS + 8자리 UUID = 24자
    """
    now = datetime.now().strftime('%Y%m%d%H%M%S')
    short_uid = uuid.uuid4().hex[:8].upper()
    return f'AT{now}{short_uid}'


def build_invoice_from_trade(db, partner_id, trade_date, items):
    """거래 데이터 → 세금계산서 발행 데이터 빌드.

    Args:
        db: SupabaseDB
        partner_id: 거래처 ID (business_partners)
        trade_date: 작성일자 (YYYY-MM-DD)
        items: list of {product_name, qty, unit_price}

    Returns:
        dict: issue_sales_invoice()에 전달할 형태
    """
    # 거래처 조회 (business_partners 테이블)
    partner = db.query_partner_by_id(partner_id)
    if not partner:
        raise ValueError(f'거래처 ID {partner_id}를 찾을 수 없습니다')

    # 우리 사업장 정보
    my_biz = db.query_default_business()

    detail_items = []
    supply_total = 0
    for item in items:
        qty = int(item.get('qty', 0))
        unit_cost = int(item.get('unit_price', 0))
        supply = qty * unit_cost
        tax = int(supply * 0.1)  # 부가세 10%
        supply_total += supply
        detail_items.append({
            'name': item.get('product_name', ''),
            'qty': qty,
            'unit_cost': unit_cost,
            'supply_cost': supply,
            'tax': tax,
        })

    tax_total = int(supply_total * 0.1)
    write_date = trade_date.replace('-', '')  # YYYYMMDD

    return {
        'write_date': write_date,
        'mgt_key': generate_mgt_key(),
        'buyer_corp_num': partner.get('business_number', '').replace('-', ''),
        'buyer_corp_name': partner.get('partner_name', ''),
        'buyer_ceo_name': partner.get('representative', ''),
        'buyer_addr': partner.get('address', ''),
        'buyer_biz_type': partner.get('type', ''),
        'buyer_biz_class': partner.get('business_item', ''),
        'buyer_email': partner.get('email', ''),
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
    return invoice_id
