"""
invoice_report.py — 거래명세서 PDF 생성.
DB 접근 금지. 데이터는 caller가 제공.
"""
from reports import (HAS_REPORTLAB, A4, mm, colors,
                     SimpleDocTemplate, Table, TableStyle,
                     Paragraph, Spacer, ParagraphStyle)
from reports.pdf_common import register_font, page_footer


def _fmt_num(val):
    """숫자를 천단위 포맷."""
    try:
        n = int(val)
        return f"{n:,}"
    except (ValueError, TypeError):
        return str(val) if val else ""


def generate_invoice_pdf(path, my_biz, partner, trades, trade_date=""):
    """거래명세서 PDF 생성.

    my_biz : dict — business_name, business_number, representative, address, contact, fax
    partner: dict — partner_name, business_number, address, contact1
    trades : list[dict] — product_name, qty, unit, unit_price, amount, memo
    trade_date: str — 거래일자
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
    title_style = ParagraphStyle('InvTitle', fontName=font_name, fontSize=18,
                                  alignment=1, spaceAfter=5 * mm)
    label_style = ParagraphStyle('Label', fontName=font_name, fontSize=9,
                                  textColor=colors.Color(0.2, 0.2, 0.2))
    value_style = ParagraphStyle('Value', fontName=font_name, fontSize=9)
    note_style = ParagraphStyle('Note', fontName=font_name, fontSize=8,
                                 textColor=colors.Color(0.3, 0.3, 0.3))

    # ─── 제목 ───
    elements.append(Paragraph("거 래 명 세 서", title_style))
    elements.append(Spacer(1, 3 * mm))

    # ─── 공급자 / 공급받는자 정보 ───
    my_name = my_biz.get('business_name', '')
    my_bnum = my_biz.get('business_number', '')
    my_rep = my_biz.get('representative', '')
    my_addr = my_biz.get('address', '')
    my_tel = my_biz.get('contact', '')
    my_fax = my_biz.get('fax', '')

    ptn_name = partner.get('partner_name', '')
    ptn_bnum = partner.get('business_number', '')
    ptn_addr = partner.get('address', '')
    ptn_tel = partner.get('contact1', '')

    half_w = usable_w * 0.48
    gap_w = usable_w * 0.04

    def _info_table(title_text, rows_data):
        """공급자/공급받는자 정보 테이블."""
        data = []
        header_para = Paragraph(f"<b>{title_text}</b>", ParagraphStyle(
            'IH', fontName=font_name, fontSize=10, alignment=1,
            textColor=colors.white))
        data.append([header_para, ""])
        for label, val in rows_data:
            lp = Paragraph(f"  {label}", label_style)
            vp = Paragraph(str(val), value_style)
            data.append([lp, vp])
        t = Table(data, colWidths=[half_w * 0.35, half_w * 0.65])
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

    supplier_info = [
        ("상호", my_name),
        ("사업자번호", my_bnum),
        ("대표자", my_rep),
        ("주소", my_addr),
        ("TEL", my_tel),
        ("FAX", my_fax),
    ]
    buyer_info = [
        ("상호", ptn_name),
        ("사업자번호", ptn_bnum),
        ("주소", ptn_addr),
        ("TEL", ptn_tel),
        ("", ""),  # 빈 행 (높이 맞춤)
        ("", ""),
    ]

    sup_table = _info_table("공 급 자", supplier_info)
    buy_table = _info_table("공 급 받 는 자", buyer_info)

    # 왼쪽: 공급받는자(매출처), 오른쪽: 공급자(본사)
    info_row = Table([[buy_table, "", sup_table]], colWidths=[half_w, gap_w, half_w])
    info_row.setStyle(TableStyle([
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('LEFTPADDING', (0, 0), (-1, -1), 0),
        ('RIGHTPADDING', (0, 0), (-1, -1), 0),
        ('TOPPADDING', (0, 0), (-1, -1), 0),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 0),
    ]))
    elements.append(info_row)
    elements.append(Spacer(1, 4 * mm))

    # ─── 거래일 ───
    if trade_date:
        date_style = ParagraphStyle('DateLine', fontName=font_name, fontSize=9,
                                     alignment=0, spaceAfter=2 * mm)
        elements.append(Paragraph(f"거래일자: {trade_date}", date_style))
        elements.append(Spacer(1, 2 * mm))

    # ─── 품목 테이블 ───
    headers = ["No", "품목명", "수량", "단위", "단가", "금액", "비고"]
    cw = [usable_w * 0.06, usable_w * 0.28, usable_w * 0.10, usable_w * 0.08,
          usable_w * 0.14, usable_w * 0.18, usable_w * 0.16]

    table_data = [headers]
    total_qty = 0
    total_amount = 0
    for i, t in enumerate(trades, 1):
        qty = t.get('qty', 0)
        amount = t.get('amount', 0)
        total_qty += qty
        total_amount += amount
        table_data.append([
            str(i),
            t.get('product_name', ''),
            _fmt_num(qty),
            t.get('unit', '개'),
            _fmt_num(t.get('unit_price', 0)),
            _fmt_num(amount),
            t.get('memo', ''),
        ])

    # 합계 행
    table_data.append([
        "", "합 계", _fmt_num(total_qty), "", "", _fmt_num(total_amount), ""
    ])

    tbl = Table(table_data, colWidths=cw, repeatRows=1)
    last_r = len(table_data) - 1
    tbl.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (-1, -1), font_name),
        ('FONTSIZE', (0, 0), (-1, 0), 9),
        ('FONTSIZE', (0, 1), (-1, -1), 8),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('ALIGN', (1, 1), (1, -1), 'LEFT'),  # 품목명 좌측정렬
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('GRID', (0, 0), (-1, -1), 0.4, colors.Color(0.5, 0.5, 0.5)),
        ('BACKGROUND', (0, 0), (-1, 0), colors.Color(0.2, 0.3, 0.5)),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('ROWBACKGROUNDS', (0, 1), (-1, -2),
         [colors.white, colors.Color(0.96, 0.96, 0.96)]),
        ('BACKGROUND', (0, last_r), (-1, last_r), colors.Color(0.9, 0.9, 0.9)),
        ('FONTSIZE', (0, last_r), (-1, last_r), 9),
        ('TOPPADDING', (0, 0), (-1, -1), 3),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
    ]))
    elements.append(tbl)

    # ─── 합계 금액 요약 ───
    elements.append(Spacer(1, 5 * mm))
    total_style = ParagraphStyle('Total', fontName=font_name, fontSize=11,
                                  alignment=2, spaceAfter=3 * mm)
    elements.append(Paragraph(
        f"<b>합계 금액:  {_fmt_num(total_amount)} 원</b>", total_style))

    # ─── 하단 안내문구 ───
    elements.append(Spacer(1, 10 * mm))
    elements.append(Paragraph(
        "위 금액을 청구합니다.", note_style))

    doc.build(elements, onFirstPage=footer_fn, onLaterPages=footer_fn)
