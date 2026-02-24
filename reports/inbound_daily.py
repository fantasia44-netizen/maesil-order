"""
inbound_daily.py — 입고일지 PDF 생성.
DB 접근 금지. 데이터는 caller가 제공.
"""
from reports import (HAS_REPORTLAB, A4, mm, colors,
                     SimpleDocTemplate, Table, TableStyle,
                     Paragraph, Spacer,
                     ParagraphStyle)
from reports.pdf_common import (register_font, build_header, page_footer,
                                 build_warnings_section, make_data_table)


def generate_inbound_log_pdf(path, config, df_inbound, warnings=None):
    """입고일지 PDF 생성.
    df_inbound: INBOUND rows DataFrame
    """
    if not HAS_REPORTLAB:
        raise RuntimeError("reportlab 패키지가 필요합니다.")
    target_date = config['target_date']
    approvals = config['approvals']
    title = config.get('title', '입고일지')
    include_warnings = config.get('include_warnings', True)

    font_name = register_font()
    page_w, page_h = A4
    margin = 15 * mm
    usable_w = page_w - 2 * margin
    doc = SimpleDocTemplate(path, pagesize=A4,
                            leftMargin=margin, rightMargin=margin,
                            topMargin=margin, bottomMargin=margin)
    elements = []
    footer_fn = lambda c, d: page_footer(c, d, font_name)

    build_header(elements, title, f"입고일자: {target_date}", approvals, font_name, usable_w)

    # Section A: 입고 내역
    sec_style = ParagraphStyle('SecA', fontName=font_name, fontSize=10, alignment=0,
                                textColor=colors.Color(0.1, 0.2, 0.5))
    elements.append(Paragraph("A. 입고 내역 (INBOUND)", sec_style))
    elements.append(Spacer(1, 2 * mm))

    if not df_inbound.empty:
        headers = ["품목명", "입고수량", "단위", "창고", "종류", "보관방법",
                   "소비기한", "제조일", "이력번호"]
        cw = [usable_w * 0.18, usable_w * 0.10, usable_w * 0.07, usable_w * 0.10,
              usable_w * 0.10, usable_w * 0.10, usable_w * 0.12, usable_w * 0.12,
              usable_w * 0.11]
        data = [headers]
        for _, r in df_inbound.iterrows():
            data.append([
                str(r.get('product_name', '')),
                f"{int(r.get('qty', 0)):,}",
                str(r.get('unit', '')) if str(r.get('unit', '')) not in ('', 'nan') else '개',
                str(r.get('location', '')),
                str(r.get('category', '')) if str(r.get('category', '')) not in ('', 'nan') else '',
                str(r.get('storage_method', '')) if str(r.get('storage_method', '')) not in ('', 'nan') else '',
                str(r.get('expiry_date', '')) if str(r.get('expiry_date', '')) not in ('', 'nan') else '',
                str(r.get('manufacture_date', '')) if str(r.get('manufacture_date', '')) not in ('', 'nan') else '',
                str(r.get('lot_number', '')) if str(r.get('lot_number', '')) not in ('', 'nan') else '',
            ])
        elements.append(make_data_table(data, cw, font_name))
    else:
        nd = ParagraphStyle('ND', fontName=font_name, fontSize=8, alignment=0)
        elements.append(Paragraph("  (해당 일자 입고 내역 없음)", nd))

    elements.append(Spacer(1, 6 * mm))

    # Section B: 요약
    sec_b_style = ParagraphStyle('SecB', fontName=font_name, fontSize=10, alignment=0)
    elements.append(Paragraph("B. 요약", sec_b_style))
    elements.append(Spacer(1, 2 * mm))
    summary_style = ParagraphStyle('Sum', fontName=font_name, fontSize=9, alignment=0)
    n_items = len(df_inbound)
    total_qty = int(df_inbound['qty'].sum()) if not df_inbound.empty else 0
    n_products = df_inbound['product_name'].nunique() if not df_inbound.empty else 0
    elements.append(Paragraph(f"- 입고 건수: {n_items}건 (품목 수: {n_products})", summary_style))
    elements.append(Paragraph(f"- 총 입고수량: {total_qty:,}", summary_style))

    if include_warnings and warnings:
        elements.append(Spacer(1, 4 * mm))
        build_warnings_section(elements, warnings, font_name, usable_w)

    doc.build(elements, onFirstPage=footer_fn, onLaterPages=footer_fn)
