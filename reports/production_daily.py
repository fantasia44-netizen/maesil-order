"""
production_daily.py — [템플릿3] 생산일지 PDF 생성.
DB 접근 금지. 데이터는 caller가 제공.
"""
from reports import (HAS_REPORTLAB, A4, mm, colors,
                     SimpleDocTemplate, Table, TableStyle,
                     Paragraph, Spacer,
                     ParagraphStyle)
from reports.pdf_common import (register_font, build_header, page_footer,
                                 build_warnings_section, make_data_table)


def generate_production_log_pdf(path, config, df_prod, df_out, warnings=None):
    """생산일지 PDF 생성.
    df_prod: PRODUCTION rows DataFrame
    df_out: PROD_OUT rows DataFrame
    """
    if not HAS_REPORTLAB:
        raise RuntimeError("reportlab 패키지가 필요합니다.")
    target_date = config['target_date']
    approvals = config['approvals']
    title = config.get('title', '생산일지')
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

    build_header(elements, title, f"생산일자: {target_date}", approvals, font_name, usable_w)

    # Section A: 생산실적
    sec_a_style = ParagraphStyle('SecA', fontName=font_name, fontSize=10, alignment=0,
                                  textColor=colors.Color(0.1, 0.2, 0.5))
    elements.append(Paragraph("A. 생산실적 (PRODUCTION)", sec_a_style))
    elements.append(Spacer(1, 2 * mm))

    if not df_prod.empty:
        prod_headers = ["품목명", "생산수량", "단위", "창고", "소비기한", "제조일", "이력번호"]
        cw_prod = [usable_w * 0.22, usable_w * 0.12, usable_w * 0.08, usable_w * 0.12,
                   usable_w * 0.15, usable_w * 0.15, usable_w * 0.16]
        prod_data = [prod_headers]
        for _, r in df_prod.iterrows():
            prod_data.append([
                str(r['product_name']),
                f"{int(r['qty']):,}",
                str(r['unit']) if str(r['unit']) not in ('', 'nan') else '개',
                str(r['location']),
                str(r['expiry_date']) if str(r['expiry_date']) not in ('', 'nan') else '',
                str(r['manufacture_date']) if str(r['manufacture_date']) not in ('', 'nan') else '',
                str(r.get('lot_number', '')) if str(r.get('lot_number', '')) not in ('', 'nan') else '',
            ])
        elements.append(make_data_table(prod_data, cw_prod, font_name))
    else:
        nd = ParagraphStyle('ND', fontName=font_name, fontSize=8, alignment=0)
        elements.append(Paragraph("  (해당 일자 생산 실적 없음)", nd))

    elements.append(Spacer(1, 6 * mm))

    # Section B: 원료사용
    sec_b_style = ParagraphStyle('SecB', fontName=font_name, fontSize=10, alignment=0,
                                  textColor=colors.Color(0.5, 0.2, 0.1))
    elements.append(Paragraph("B. 원료/부재료 사용 (PROD_OUT)", sec_b_style))
    elements.append(Spacer(1, 2 * mm))

    if not df_out.empty:
        out_headers = ["품목명", "사용수량", "단위", "종류", "원산지", "소비기한"]
        cw_out = [usable_w * 0.25, usable_w * 0.13, usable_w * 0.08,
                  usable_w * 0.15, usable_w * 0.19, usable_w * 0.20]
        out_data = [out_headers]
        for _, r in df_out.iterrows():
            out_data.append([
                str(r['product_name']),
                f"{abs(int(r['qty'])):,}",
                str(r['unit']) if str(r['unit']) not in ('', 'nan') else '개',
                str(r['category']) if str(r['category']) not in ('', 'nan') else '',
                str(r['origin']) if str(r['origin']) not in ('', 'nan') else '',
                str(r['expiry_date']) if str(r['expiry_date']) not in ('', 'nan') else '',
            ])
        elements.append(make_data_table(out_data, cw_out, font_name))
    else:
        nd = ParagraphStyle('ND', fontName=font_name, fontSize=8, alignment=0)
        elements.append(Paragraph("  (해당 일자 원료 사용 내역 없음)", nd))

    elements.append(Spacer(1, 6 * mm))

    # Section C: 요약
    sec_c_style = ParagraphStyle('SecC', fontName=font_name, fontSize=10, alignment=0)
    elements.append(Paragraph("C. 요약", sec_c_style))
    elements.append(Spacer(1, 2 * mm))
    summary_style = ParagraphStyle('Sum', fontName=font_name, fontSize=9, alignment=0)
    n_prod = len(df_prod)
    n_out = len(df_out)
    total_prod_qty = int(df_prod['qty'].sum()) if not df_prod.empty else 0
    total_out_qty = abs(int(df_out['qty'].sum())) if not df_out.empty else 0
    elements.append(Paragraph(f"- 생산 품목: {n_prod}건 (총 생산수량: {total_prod_qty:,})", summary_style))
    elements.append(Paragraph(f"- 원료 사용: {n_out}건 (총 사용수량: {total_out_qty:,})", summary_style))

    if include_warnings and warnings:
        elements.append(Spacer(1, 4 * mm))
        build_warnings_section(elements, warnings, font_name, usable_w)

    doc.build(elements, onFirstPage=footer_fn, onLaterPages=footer_fn)
