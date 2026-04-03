"""
services/courier/cj_label_generator.py — CJ대한통운 표준운송장 라벨 PDF 생성.

라벨 규격: 123mm x 100mm (감열지)
바코드: CODE128A (분류코드), CODE128C (운송장번호)
참조: 표준운송장 가이드(CJ대한통운)1_5인치_new_251105

마스킹 규칙:
  - 이름: 2번째 글자 '*' (예: 홍*동)
  - 전화번호: 뒷 4자리 '****' (예: 010-1234-****)
"""
import io
import logging
import re
from datetime import datetime

from reportlab.lib.units import mm
from reportlab.lib import colors
from reportlab.pdfgen import canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.graphics.barcode import code128

log = logging.getLogger(__name__)

# ── 라벨 크기 ──
LABEL_W = 100.0 * mm  # 283.46pt
LABEL_H = 125.0 * mm  # 354.33pt

# ── 폰트 등록 ──
_FONT_REGISTERED = False


def _register_fonts():
    """한글 폰트 등록 (malgun / nanum 자동 탐색)."""
    global _FONT_REGISTERED
    if _FONT_REGISTERED:
        return

    import os
    font_paths = {
        'Korean': [
            "C:/Windows/Fonts/malgun.ttf",
            "/usr/share/fonts/truetype/nanum/NanumGothic.ttf",
            "/System/Library/Fonts/AppleSDGothicNeo.ttc",
        ],
        'KoreanBold': [
            "C:/Windows/Fonts/malgunbd.ttf",
            "/usr/share/fonts/truetype/nanum/NanumGothicBold.ttf",
        ],
    }
    for font_name, paths in font_paths.items():
        for fp in paths:
            if os.path.exists(fp):
                try:
                    pdfmetrics.registerFont(TTFont(font_name, fp))
                    break
                except Exception:
                    pass
        else:
            # Bold 실패 시 일반 폰트로 폴백
            if font_name == 'KoreanBold':
                try:
                    pdfmetrics.registerFont(TTFont('KoreanBold',
                        font_paths['Korean'][0]))
                except Exception:
                    pass

    _FONT_REGISTERED = True


# ══════════════════════════════════════════
# 마스킹
# ══════════════════════════════════════════

def _mask_name(name: str) -> str:
    """이름 2번째 글자 마스킹: 홍길동 → 홍*동"""
    if not name:
        return ''
    name = name.strip()
    if len(name) <= 1:
        return name
    return name[0] + '*' + name[2:] if len(name) >= 3 else name[0] + '*'


def _mask_phone(phone: str) -> str:
    """전화번호 뒷 4자리 마스킹: 010-1234-5678 → 010-1234-****"""
    if not phone:
        return ''
    digits = re.sub(r'[^0-9]', '', phone)
    if len(digits) >= 8:
        visible = digits[:-4]
        # 포맷팅
        if len(digits) == 11:
            return f'{visible[:3]}-{visible[3:7]}-****'
        elif len(digits) == 10:
            return f'{visible[:2]}-{visible[2:6]}-****'
        else:
            return visible + '-****'
    return phone


def _split_address(addr: str, max_len: int = 28) -> tuple:
    """긴 주소를 2줄로 분리."""
    if not addr or len(addr) <= max_len:
        return (addr or '', '')
    # 공백 기준 분리
    mid = len(addr) // 2
    sp = addr.rfind(' ', 0, mid + 5)
    if sp == -1:
        sp = mid
    return (addr[:sp].strip(), addr[sp:].strip())


# ══════════════════════════════════════════
# 바코드 헬퍼
# ══════════════════════════════════════════

def _draw_barcode_128(c, x, y, value, bar_width=0.33*mm, bar_height=12*mm):
    """CODE128 바코드를 캔버스에 직접 그리기."""
    if not value:
        return
    try:
        bc = code128.Code128(
            value,
            barWidth=bar_width,
            barHeight=bar_height,
            humanReadable=False,
        )
        bc.drawOn(c, x, y)
    except Exception as e:
        log.warning(f'바코드 생성 실패 ({value}): {e}')
        c.setFont('Korean', 8)
        c.drawString(x, y + 2*mm, f'[{value}]')


# ══════════════════════════════════════════
# 단일 라벨 렌더링
# ══════════════════════════════════════════

def _render_label(c, shipment: dict):
    """하나의 운송장 라벨을 현재 페이지에 렌더링.

    shipment dict 필드:
        invoice_no      : 운송장번호 (12자리)
        sender_name     : 보내는 분
        sender_phone    : 보내는 분 전화
        sender_addr     : 보내는 분 주소
        receiver_name   : 받는 분
        receiver_phone  : 받는 분 전화
        receiver_addr   : 받는 분 주소 (전체)
        receiver_zipcode: 우편번호
        product_name    : 상품명
        memo            : 배송메모
        dest_code       : 분류코드 (3자리 지역코드)
        sub_dest_code   : 서브분류코드
        short_addr      : 주소약칭
        branch          : 배달점소명
        sm_code         : 배달사원 별칭
        order_no        : 주문번호
        box_type        : 박스 크기
    """
    _register_fonts()

    inv = shipment.get('invoice_no', '')
    dest_code = shipment.get('dest_code', '')
    sub_dest = shipment.get('sub_dest_code', '')
    short_addr = shipment.get('short_addr', '')
    branch = shipment.get('branch', '')
    sm_code = shipment.get('sm_code', '')

    recv_name = _mask_name(shipment.get('receiver_name', ''))
    recv_phone = _mask_phone(shipment.get('receiver_phone', ''))
    recv_addr = shipment.get('receiver_addr', '')
    recv_zip = shipment.get('receiver_zipcode', '')

    send_name = shipment.get('sender_name', '')
    send_phone = shipment.get('sender_phone', '')
    send_addr = shipment.get('sender_addr', '')

    product = shipment.get('product_name', '')
    memo = shipment.get('memo', '')
    order_no = shipment.get('order_no', '')
    today = datetime.now().strftime('%Y-%m-%d')

    # 라벨 크기: 100mm(W) x 125mm(H)
    LW = LABEL_W  # 100mm
    LH = LABEL_H  # 125mm

    # ── 외곽 테두리 ──
    c.setStrokeColor(colors.black)
    c.setLineWidth(0.8)
    c.rect(2*mm, 2*mm, LW - 4*mm, LH - 4*mm)

    # ══════════════════════════════════════
    # 상단 영역: 분류코드 (y: 90~121mm)
    # ══════════════════════════════════════
    top_y = 90 * mm

    # 수평 구분선
    c.setLineWidth(0.5)
    c.line(2*mm, top_y, LW - 2*mm, top_y)

    # 분류코드 영역 (큰 텍스트 + 바코드)
    c.setFont('KoreanBold', 36)
    dest_display = dest_code
    c.drawString(6*mm, 102*mm, dest_display)

    # 서브 분류코드 (옆에 작게)
    if sub_dest:
        c.setFont('KoreanBold', 18)
        c.drawString(6*mm + c.stringWidth(dest_display, 'KoreanBold', 36) + 3*mm,
                     102*mm, sub_dest)

    # 주소약칭 (분류코드 아래)
    c.setFont('KoreanBold', 13)
    c.drawString(6*mm, 93*mm, short_addr[:18] if short_addr else '')

    # 오른쪽: 분류코드 바코드 (CODE128A)
    if dest_code:
        bc_value = f'{dest_code}{sub_dest}' if sub_dest else dest_code
        _draw_barcode_128(c, 58*mm, 95*mm, bc_value,
                         bar_width=0.45*mm, bar_height=20*mm)

    # ══════════════════════════════════════
    # 배달점소·별칭 (y: 82~90mm)
    # ══════════════════════════════════════
    c.line(2*mm, 82*mm, LW - 2*mm, 82*mm)

    c.setFont('KoreanBold', 11)
    branch_line = branch or ''
    if sm_code:
        branch_line += f'  [{sm_code}]'
    c.drawString(6*mm, 84*mm, branch_line[:28])

    # ══════════════════════════════════════
    # 받는분 영역 (y: 52~82mm)
    # ══════════════════════════════════════
    c.line(2*mm, 52*mm, LW - 2*mm, 52*mm)

    c.setFont('Korean', 7)
    c.drawString(4*mm, 78*mm, '받는분')

    c.setFont('KoreanBold', 12)
    c.drawString(18*mm, 76*mm, recv_name)

    c.setFont('Korean', 10)
    c.drawString(48*mm, 76*mm, recv_phone)

    # 우편번호
    if recv_zip:
        c.setFont('Korean', 8)
        c.drawString(80*mm, 76*mm, f'({recv_zip})')

    # 주소 (최대 3줄)
    addr1, addr2 = _split_address(recv_addr, 30)
    c.setFont('Korean', 9)
    c.drawString(6*mm, 68*mm, addr1)
    if addr2:
        addr2a, addr2b = _split_address(addr2, 30)
        c.drawString(6*mm, 62*mm, addr2a)
        if addr2b:
            c.drawString(6*mm, 56*mm, addr2b)

    # ══════════════════════════════════════
    # 보내는분 영역 (y: 40~52mm)
    # ══════════════════════════════════════
    c.line(2*mm, 40*mm, LW - 2*mm, 40*mm)

    c.setFont('Korean', 7)
    c.drawString(4*mm, 48*mm, '보내는분')

    c.setFont('Korean', 9)
    send_line = f'{send_name}  {send_phone}'
    c.drawString(18*mm, 48*mm, send_line[:35])

    c.setFont('Korean', 7)
    c.drawString(6*mm, 43*mm, send_addr[:40] if send_addr else '')

    # ══════════════════════════════════════
    # 상품/메모 영역 (y: 32~40mm)
    # ══════════════════════════════════════
    c.line(2*mm, 32*mm, LW - 2*mm, 32*mm)

    c.setFont('Korean', 8)
    prod_line = product[:22] if product else ''
    if memo:
        prod_line += f' ({memo[:12]})'
    c.drawString(6*mm, 35*mm, prod_line)

    # 주문번호 (오른쪽)
    if order_no:
        c.setFont('Korean', 6)
        c.drawString(70*mm, 35*mm, f'#{order_no}')

    # ══════════════════════════════════════
    # 하단: 운송장번호 바코드 (y: 2~32mm)
    # ══════════════════════════════════════
    if inv:
        _draw_barcode_128(c, 10*mm, 12*mm, inv,
                         bar_width=0.38*mm, bar_height=15*mm)

        # 운송장번호 텍스트
        c.setFont('KoreanBold', 11)
        if len(inv) == 12:
            inv_display = f'{inv[:4]}-{inv[4:8]}-{inv[8:]}'
        else:
            inv_display = inv
        c.drawString(10*mm, 6*mm, inv_display)

    # 출력일시 (오른쪽 하단)
    c.setFont('Korean', 6)
    c.drawString(75*mm, 6*mm, today)


# ══════════════════════════════════════════
# Public API
# ══════════════════════════════════════════

def generate_label_pdf(shipments: list[dict]) -> bytes:
    """운송장 라벨 PDF 생성 (여러 장 가능).

    Args:
        shipments: 각 dict에 라벨 인쇄 정보 포함
            필수: invoice_no, receiver_name, receiver_phone, receiver_addr
            권장: dest_code, short_addr, branch, sm_code,
                  sender_name, sender_phone, sender_addr,
                  product_name, memo, order_no

    Returns:
        PDF 바이트열
    """
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=(LABEL_W, LABEL_H))

    for i, ship in enumerate(shipments):
        if i > 0:
            c.showPage()
        _render_label(c, ship)

    c.save()
    buf.seek(0)
    return buf.read()


def generate_labels_from_db_and_api(
    shipments_data: list[dict],
    cj_client=None,
    sender: dict = None,
) -> dict:
    """DB 배송 데이터 + CJ API 주소정제를 결합해 라벨 PDF 생성.

    Args:
        shipments_data: DB에서 조회한 배송 정보 리스트
            필수: invoice_no, receiver_name, receiver_phone,
                  receiver_addr (또는 receiver_address)
        cj_client: CJCourierClient 인스턴스 (주소정제용)
        sender: 발송인 정보 dict (name, phone, address)

    Returns:
        {'ok': True, 'pdf_bytes': bytes, 'count': int}
        or {'ok': False, 'error': str}
    """
    if not shipments_data:
        return {'ok': False, 'error': '라벨 생성할 데이터가 없습니다.'}

    sender = sender or {}
    label_data = []

    for ship in shipments_data:
        recv_addr = (ship.get('receiver_addr', '')
                     or ship.get('receiver_address', '')
                     or ship.get('address', ''))

        # CJ API 주소정제로 분류코드/배달점소 조회
        dest_code = ship.get('dest_code', '')
        sub_dest = ship.get('sub_dest_code', '')
        short_addr = ship.get('short_addr', '')
        branch = ship.get('branch', '')
        sm_code = ship.get('sm_code', '')

        if cj_client and recv_addr and not dest_code:
            try:
                refined = cj_client.refine_address(recv_addr)
                if refined.get('ok'):
                    dest_code = refined.get('dest_code', '')
                    sub_dest = refined.get('sub_dest_code', '')
                    short_addr = refined.get('short_addr', '')
                    branch = refined.get('branch', '')
                    sm_code = refined.get('sm_code', '')
            except Exception as e:
                log.warning(f'주소정제 실패 ({recv_addr[:30]}): {e}')

        label_data.append({
            'invoice_no': ship.get('invoice_no', ''),
            'receiver_name': ship.get('receiver_name', ''),
            'receiver_phone': ship.get('receiver_phone', ''),
            'receiver_addr': recv_addr,
            'receiver_zipcode': ship.get('receiver_zipcode', '')
                               or ship.get('zipcode', ''),
            'sender_name': sender.get('name', ''),
            'sender_phone': sender.get('phone', ''),
            'sender_addr': sender.get('address', ''),
            'product_name': ship.get('product_name', '')
                           or ship.get('item_name', ''),
            'memo': ship.get('memo', '') or ship.get('delivery_memo', ''),
            'order_no': ship.get('order_no', ''),
            'box_type': ship.get('box_type', ''),
            'dest_code': dest_code,
            'sub_dest_code': sub_dest,
            'short_addr': short_addr,
            'branch': branch,
            'sm_code': sm_code,
        })

    try:
        pdf_bytes = generate_label_pdf(label_data)
        return {'ok': True, 'pdf_bytes': pdf_bytes, 'count': len(label_data)}
    except Exception as e:
        log.error(f'라벨 PDF 생성 오류: {e}')
        return {'ok': False, 'error': f'PDF 생성 실패: {e}'}
