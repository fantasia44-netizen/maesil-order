"""
invoice_matching_service.py — CJ 송장번호 ↔ 주문 매칭 서비스.

CJ 엑셀 업로드 / 향후 CJ API 모두 이 서비스를 통해 매칭.
입력 방식이 달라도 동일 DB, 동일 로직, 중복 스킵.
"""
import logging
import re
from datetime import date, timedelta

from services.channel_config import PLATFORM_MAP

logger = logging.getLogger(__name__)


def _clean_phone(phone_str: str) -> str:
    """전화번호에서 숫자만 추출."""
    return re.sub(r'[^0-9]', '', str(phone_str or ''))


def _make_match_key(name: str, phone: str) -> str:
    """이름 + 전화번호 뒷4자리로 매칭 키 생성."""
    name = str(name or '').strip()
    phone = _clean_phone(phone)
    suffix = phone[-4:] if len(phone) >= 4 else phone
    return f"{name}_{suffix}"


def _extract_recipient_from_raw(channel: str, raw_data: dict) -> tuple:
    """api_orders.raw_data에서 채널별 수취인 이름+전화번호 추출.

    Returns:
        (name, phone) tuple
    """
    platform = PLATFORM_MAP.get(channel, '')
    name = phone = ''

    if platform == 'naver':
        po = raw_data.get('productOrder', {})
        sa = po.get('shippingAddress', {})
        name = sa.get('name', '')
        phone = sa.get('tel1', '') or sa.get('tel2', '')
    elif platform == 'coupang':
        rcv = raw_data.get('receiver', {})
        name = rcv.get('name', '')
        phone = rcv.get('safeNumber', '') or rcv.get('receiverNumber', '')
    elif platform == 'cafe24':
        name = raw_data.get('shipping_name', '') or raw_data.get('buyer_name', '')
        phone = raw_data.get('shipping_phone', '') or raw_data.get('buyer_cellphone', '')

    return name, phone


def match_invoices_to_orders(db, invoice_map: dict, date_range_days=7):
    """송장번호 맵을 주문과 매칭하여 order_shipping 업데이트.

    Args:
        db: SupabaseDB 인스턴스
        invoice_map: {match_key(이름_전화뒷4): invoice_no, ...}
        date_range_days: 매칭 범위 (기본 7일)

    Returns:
        dict: {total, matched, updated, skipped, errors, details}
    """
    if not invoice_map:
        return {'total': 0, 'matched': 0, 'updated': 0, 'skipped': 0, 'errors': []}

    today = date.today().strftime('%Y-%m-%d')
    date_from = (date.today() - timedelta(days=date_range_days)).strftime('%Y-%m-%d')

    updates = []
    matched = 0
    seen_orders = set()
    errors = []

    # ── 방법 1: api_orders.raw_data에서 수취인 추출 ──
    try:
        api_rows = db.query_api_orders(date_from=date_from, date_to=today)
        for row in api_rows:
            ch = row.get('channel', '')
            raw = row.get('raw_data') or {}
            order_no = row.get('api_line_id') or row.get('api_order_id', '')
            name, phone = _extract_recipient_from_raw(ch, raw)

            if not name or not order_no:
                continue

            key = _make_match_key(name, phone)
            if key in invoice_map and order_no not in seen_orders:
                updates.append({
                    'channel': ch,
                    'order_no': order_no,
                    'invoice_no': invoice_map[key],
                    'courier': 'CJ대한통운',
                })
                matched += 1
                seen_orders.add(order_no)
    except Exception as e:
        logger.error(f'[InvoiceMatch] api_orders 매칭 오류: {e}', exc_info=True)
        errors.append(f'api_orders 매칭 오류: {e}')

    # ── 방법 2 (Fallback): order_shipping에서 직접 매칭 ──
    # 엑셀 업로드 주문은 api_orders에 없을 수 있으므로
    unmatched_keys = set(invoice_map.keys()) - {
        _make_match_key(
            _extract_recipient_from_raw(u['channel'], {})[0],
            ''
        ) for u in updates
    }

    if unmatched_keys:
        try:
            # order_shipping에서 최근 주문의 이름+전화 가져오기
            ship_rows = db.client.table("order_shipping") \
                .select("channel, order_no, name, phone") \
                .not_.is_("name", "null") \
                .gte("created_at", f"{date_from}T00:00:00") \
                .is_("invoice_no", "null") \
                .limit(5000) \
                .execute()

            for row in (ship_rows.data or []):
                name = row.get('name', '')
                phone = _clean_phone(row.get('phone', ''))
                order_no = row.get('order_no', '')
                ch = row.get('channel', '')

                if not name or not order_no or order_no in seen_orders:
                    continue

                key = _make_match_key(name, phone)
                if key in invoice_map:
                    updates.append({
                        'channel': ch,
                        'order_no': order_no,
                        'invoice_no': invoice_map[key],
                        'courier': 'CJ대한통운',
                    })
                    matched += 1
                    seen_orders.add(order_no)
        except Exception as e:
            logger.error(f'[InvoiceMatch] order_shipping fallback 오류: {e}', exc_info=True)
            errors.append(f'order_shipping fallback 오류: {e}')

    # ── DB 반영 ──
    updated = 0
    if updates:
        updated = db.bulk_update_shipping_invoices(updates)

    return {
        'total': len(invoice_map),
        'matched': matched,
        'updated': updated,
        'skipped': len(invoice_map) - matched,
        'errors': errors,
    }


def parse_cj_excel(file_bytes, filename='') -> dict:
    """CJ 택배 엑셀 파일을 파싱하여 매칭 맵 생성.

    Args:
        file_bytes: 엑셀 파일 BytesIO 또는 bytes
        filename: 원본 파일명

    Returns:
        dict: {match_key(이름_전화뒷4): invoice_no, ...}

    Raises:
        ValueError: 파싱 실패 시
    """
    import io
    import openpyxl

    if isinstance(file_bytes, bytes):
        file_bytes = io.BytesIO(file_bytes)

    wb = openpyxl.load_workbook(file_bytes, read_only=False)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    wb.close()

    if len(rows) < 2:
        raise ValueError('데이터가 없습니다.')

    headers = [str(c or '').replace('\r\n', '').replace(' ', '') for c in rows[0]]

    # 컬럼 감지: 이름으로 먼저, 없으면 CJ 표준 인덱스
    col_invoice = col_name = col_phone = None

    for i, h in enumerate(headers):
        if '운송장번호' in h:
            col_invoice = i
        elif h == '받는분' and col_name is None:
            col_name = i
        elif '받는분전화' in h or '받는분휴대' in h:
            col_phone = i

    # CJ 표준 40컬럼 폴백
    if col_invoice is None and len(headers) >= 22:
        col_invoice = 7
    if col_name is None and len(headers) >= 22:
        col_name = 20
    if col_phone is None and len(headers) >= 22:
        col_phone = 21

    if col_invoice is None or col_name is None or col_phone is None:
        raise ValueError('필수 컬럼(운송장번호, 받는분, 받는분전화번호)을 찾을 수 없습니다.')

    cj_map = {}
    for row in rows[1:]:
        row_list = list(row)
        inv_no = str(row_list[col_invoice] or '').strip()
        name = str(row_list[col_name] or '').strip()
        phone = _clean_phone(row_list[col_phone])

        if not inv_no or not name or not phone:
            continue
        key = _make_match_key(name, phone)
        cj_map[key] = inv_no

    if not cj_map:
        raise ValueError('유효한 송장 데이터가 없습니다.')

    return cj_map
