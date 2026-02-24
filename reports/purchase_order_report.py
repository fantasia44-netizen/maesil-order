"""
purchase_order_report.py -- 물품 발주서 PDF 생성.
DB 접근 금지. 데이터는 caller가 제공.
"""
from datetime import datetime
from reports import (HAS_REPORTLAB, A4, mm, colors,
                     SimpleDocTemplate, Table, TableStyle,
                     Paragraph, Spacer, ParagraphStyle)
from reports.pdf_common import register_font, page_footer


def generate_purchase_order_pdf(path, my_biz, supplier, items,
                                order_date="", delivery_note="",
                                caution_text=""):
    """물품 발주서 PDF 생성.

    my_biz   : dict -- business_name, business_number, representative,
                       address, contact, fax, email  (발주처 = 본사)
    supplier : dict -- partner_name, business_number, representative,
                       address, phone, fax, email     (공급업체)
    items    : list[dict] -- product_name, unit, qty, request_date, note
    order_date    : str  -- 발주일자
    delivery_note : str  -- 입고기한 등 비고
    caution_text  : str  -- 주의사항
    """
    if not HAS_REPORTLAB:
        raise RuntimeError("reportlab 패키지가 필요합니다.")

    font_name = register_font()
    margin = 20 * mm
    page_w, page_h = A4
    usable_w = page_w - 2 * margin

    doc = SimpleDocTemplate(path, pagesize=A4,
                            leftMargin=margin, rightMargin=margin,
                            topMargin=margin, bottomMargin=margin)
    elements = []
    footer_fn = lambda c, d: page_footer(c, d, font_name)

    # ─── 스타일 ───
    title_style = ParagraphStyle('POTitle', fontName=font_name, fontSize=20,
                                  alignment=1, spaceAfter=6 * mm,
                                  spaceBefore=2 * mm)
    label_style = ParagraphStyle('Label', fontName=font_name, fontSize=9,
                                  textColor=colors.Color(0.2, 0.2, 0.2))
    value_style = ParagraphStyle('Value', fontName=font_name, fontSize=9)
    section_style = ParagraphStyle('Section', fontName=font_name, fontSize=10,
                                    spaceAfter=2 * mm, spaceBefore=4 * mm)
    note_style = ParagraphStyle('Note', fontName=font_name, fontSize=8,
                                 textColor=colors.Color(0.3, 0.3, 0.3),
                                 leading=12)
    caution_style = ParagraphStyle('Caution', fontName=font_name, fontSize=8,
                                    textColor=colors.Color(0.2, 0.2, 0.2),
                                    leading=13, leftIndent=5 * mm)
    date_style = ParagraphStyle('DateRight', fontName=font_name, fontSize=9,
                                 alignment=2, spaceAfter=3 * mm)

    # ─── 제목 ───
    elements.append(Paragraph("물 품 발 주 서", title_style))

    # ─── 발주일자 ───
    if not order_date:
        order_date = datetime.now().strftime('%Y-%m-%d')
    elements.append(Paragraph(f"발주일자: {order_date}", date_style))

    # ─── Section 1: 발주처 (본사) ───
    def _info_table(title_text, rows_data, width):
        """정보 섹션 테이블."""
        data = []
        header_para = Paragraph(
            f"<b>{title_text}</b>",
            ParagraphStyle('IH', fontName=font_name, fontSize=10,
                           alignment=1, textColor=colors.white))
        data.append([header_para, ""])
        for lbl, val in rows_data:
            lp = Paragraph(f"  {lbl}", label_style)
            vp = Paragraph(str(val or ''), value_style)
            data.append([lp, vp])
        t = Table(data, colWidths=[width * 0.30, width * 0.70])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.Color(0.2, 0.3, 0.5)),
            ('SPAN', (0, 0), (1, 0)),
            ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, -1), font_name),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('GRID', (0, 0), (-1, -1), 0.4, colors.Color(0.6, 0.6, 0.6)),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 3),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
            ('LEFTPADDING', (0, 0), (-1, -1), 4),
        ]))
        return t

    # 발주처 정보
    orderer_info = [
        ("상호", my_biz.get('business_name', '')),
        ("사업자번호", my_biz.get('business_number', '')),
        ("대표자명", my_biz.get('representative', '')),
        ("회사주소", my_biz.get('address', '')),
        ("연락처", my_biz.get('contact', '')),
        ("팩스", my_biz.get('fax', '')),
        ("E-Mail", my_biz.get('email', '')),
    ]
    if delivery_note:
        orderer_info.insert(2, ("입고기한", delivery_note))

    elements.append(_info_table("발 주 처", orderer_info, usable_w))
    elements.append(Spacer(1, 4 * mm))

    # ─── Section 2: 발주내역 ───
    elements.append(Paragraph(
        "<b><font color='#334477'>발 주 내 역</font></b>", section_style))

    headers = ["No", "물품명", "단위", "수량", "입고요청일", "비고"]
    cw = [usable_w * 0.06, usable_w * 0.30, usable_w * 0.10,
          usable_w * 0.10, usable_w * 0.18, usable_w * 0.26]

    table_data = [headers]
    for i, item in enumerate(items, 1):
        table_data.append([
            str(i),
            item.get('product_name', ''),
            item.get('unit', ''),
            str(item.get('qty', '')),
            item.get('request_date', ''),
            item.get('note', ''),
        ])

    # 빈 행 채우기 (최소 5행)
    while len(table_data) < 6:
        table_data.append(["", "", "", "", "", ""])

    tbl = Table(table_data, colWidths=cw, repeatRows=1)
    last_r = len(table_data) - 1
    tbl.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (-1, -1), font_name),
        ('FONTSIZE', (0, 0), (-1, 0), 9),
        ('FONTSIZE', (0, 1), (-1, -1), 8),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('ALIGN', (1, 1), (1, -1), 'LEFT'),
        ('ALIGN', (5, 1), (5, -1), 'LEFT'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('GRID', (0, 0), (-1, -1), 0.4, colors.Color(0.5, 0.5, 0.5)),
        ('BACKGROUND', (0, 0), (-1, 0), colors.Color(0.2, 0.3, 0.5)),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1),
         [colors.white, colors.Color(0.96, 0.96, 0.96)]),
        ('TOPPADDING', (0, 0), (-1, -1), 3),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
    ]))
    elements.append(tbl)

    # ─── 주의사항 ───
    if caution_text:
        elements.append(Spacer(1, 4 * mm))
        elements.append(Paragraph(
            "<b><font color='#334477'>주 의 사 항</font></b>", section_style))
        # 줄바꿈 처리
        caution_lines = caution_text.replace('\r\n', '\n').split('\n')
        for line in caution_lines:
            elements.append(Paragraph(line, caution_style))
    elements.append(Spacer(1, 5 * mm))

    # ─── Section 3: 공급업체 ───
    supplier_info = [
        ("상호", supplier.get('partner_name', '')),
        ("사업자번호", supplier.get('business_number', '')),
        ("대표자명", supplier.get('representative', '')),
        ("회사주소", supplier.get('address', '')),
        ("연락처", supplier.get('phone', '')),
        ("팩스", supplier.get('fax', '')),
        ("E-Mail", supplier.get('email', '')),
    ]
    elements.append(_info_table("공 급 업 체", supplier_info, usable_w))

    # ─── 하단: 서명 영역 ───
    elements.append(Spacer(1, 10 * mm))
    sign_style = ParagraphStyle('Sign', fontName=font_name, fontSize=10,
                                 alignment=2, spaceBefore=5 * mm)
    biz_name = my_biz.get('business_name', '')
    rep_name = my_biz.get('representative', '')
    elements.append(Paragraph(
        f"{order_date}", sign_style))
    elements.append(Paragraph(
        f"발주처: {biz_name}   대표 {rep_name}  (인)", sign_style))

    doc.build(elements, onFirstPage=footer_fn, onLaterPages=footer_fn)
