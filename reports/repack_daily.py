"""
repack_daily.py — [템플릿4] 소분작업일지 PDF 생성.
생산일지와 동일한 형식: A.산출실적 / B.원료/부재료 사용 / C.요약
DB 접근 금지. 데이터는 caller가 제공.
"""
from reports import (HAS_REPORTLAB, A4, mm, colors,
                     SimpleDocTemplate, Table, TableStyle,
                     Paragraph, Spacer,
                     ParagraphStyle)
from reports.pdf_common import (register_font, build_header, page_footer,
                                 build_warnings_section, make_data_table)


def generate_repack_log_pdf(path, config, df, warnings=None):
    """소분작업일지 PDF 생성.
    df: REPACK_OUT + REPACK_IN rows DataFrame
    생산일지 형식: A.산출실적(제품) / B.원료/부재료 사용 / C.요약
    """
    if not HAS_REPORTLAB:
        raise RuntimeError("reportlab 패키지가 필요합니다.")
    target_date = config['target_date']
    approvals = config['approvals']
    title = config.get('title', '소분작업일지')
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

    build_header(elements, title, f"작업일자: {target_date}", approvals, font_name, usable_w)

    df_in = df[df['type'] == 'REPACK_IN']
    df_out = df[df['type'] == 'REPACK_OUT']
    df_out_main = df_out[~df_out['memo'].str.contains('부자재', na=False)]
    df_out_sub = df_out[df_out['memo'].str.contains('부자재', na=False)]

    # ═══ Section A: 산출실적 (제품) ═══
    sec_a_style = ParagraphStyle('SecA', fontName=font_name, fontSize=10, alignment=0,
                                  textColor=colors.Color(0.1, 0.2, 0.5))
    elements.append(Paragraph("A. 산출실적 (REPACK_IN)", sec_a_style))
    elements.append(Spacer(1, 2 * mm))

    if not df_in.empty:
        in_headers = ["품목명", "산출수량", "단위", "창고", "소분일", "소비기한"]
        cw_in = [usable_w * 0.25, usable_w * 0.12, usable_w * 0.08, usable_w * 0.13,
                 usable_w * 0.20, usable_w * 0.22]
        in_data = [in_headers]
        for _, r in df_in.iterrows():
            in_data.append([
                str(r['product_name']),
                f"{int(r['qty']):,}",
                str(r['unit']) if str(r['unit']) not in ('', 'nan') else '개',
                str(r['location']),
                str(r['transaction_date']) if str(r.get('transaction_date', '')) not in ('', 'nan') else target_date,
                str(r['expiry_date']) if str(r['expiry_date']) not in ('', 'nan') else '',
            ])
        elements.append(make_data_table(in_data, cw_in, font_name))
    else:
        nd = ParagraphStyle('ND', fontName=font_name, fontSize=8, alignment=0)
        elements.append(Paragraph("  (해당 일자 산출 실적 없음)", nd))

    elements.append(Spacer(1, 6 * mm))

    # ═══ Section B: 원료/부재료 사용 ═══
    sec_b_style = ParagraphStyle('SecB', fontName=font_name, fontSize=10, alignment=0,
                                  textColor=colors.Color(0.5, 0.2, 0.1))
    elements.append(Paragraph("B. 원료/부재료 사용 (REPACK_OUT)", sec_b_style))
    elements.append(Spacer(1, 2 * mm))

    # B-1) 원료(투입품) 사용
    df_out_all = df_out  # 원료 + 부자재 합쳐서 표시
    if not df_out_all.empty:
        out_headers = ["품목명", "사용수량", "단위", "종류", "원산지", "소비기한"]
        cw_out = [usable_w * 0.25, usable_w * 0.13, usable_w * 0.08,
                  usable_w * 0.15, usable_w * 0.19, usable_w * 0.20]
        out_data = [out_headers]
        for _, r in df_out_all.iterrows():
            cat_str = str(r.get('category', '')).strip()
            if cat_str in ('', 'nan', 'None'):
                cat_str = ''
            origin_str = str(r.get('origin', '')).strip()
            if origin_str in ('', 'nan', 'None'):
                origin_str = ''
            out_data.append([
                str(r['product_name']),
                f"{abs(int(r['qty'])):,}",
                str(r['unit']) if str(r['unit']) not in ('', 'nan') else '개',
                cat_str,
                origin_str,
                str(r['expiry_date']) if str(r['expiry_date']) not in ('', 'nan') else '',
            ])
        elements.append(make_data_table(out_data, cw_out, font_name))
    else:
        nd = ParagraphStyle('ND', fontName=font_name, fontSize=8, alignment=0)
        elements.append(Paragraph("  (해당 일자 원료/부재료 사용 내역 없음)", nd))

    elements.append(Spacer(1, 6 * mm))

    # ═══ Section C: 요약 ═══
    sec_c_style = ParagraphStyle('SecC', fontName=font_name, fontSize=10, alignment=0)
    elements.append(Paragraph("C. 요약", sec_c_style))
    elements.append(Spacer(1, 2 * mm))

    doc_nos = sorted(df['repack_doc_no'].unique())
    doc_nos = [d for d in doc_nos if d and str(d) not in ('', 'nan')]
    n_in = len(df_in)
    n_out_main = len(df_out_main)
    n_out_sub = len(df_out_sub)
    total_in_qty = int(df_in['qty'].sum()) if not df_in.empty else 0
    total_out_qty = abs(int(df_out_main['qty'].sum())) if not df_out_main.empty else 0
    total_sub_qty = abs(int(df_out_sub['qty'].sum())) if not df_out_sub.empty else 0

    summary_style = ParagraphStyle('Sum', fontName=font_name, fontSize=9, alignment=0)
    elements.append(Paragraph(f"- 소분번호: {len(doc_nos)}건", summary_style))
    elements.append(Paragraph(f"- 산출 품목: {n_in}건 (총 산출수량: {total_in_qty:,})", summary_style))
    elements.append(Paragraph(f"- 원료 사용: {n_out_main}건 (총 사용수량: {total_out_qty:,})", summary_style))
    if n_out_sub > 0:
        elements.append(Paragraph(f"- 부자재 사용: {n_out_sub}건 (총 사용수량: {total_sub_qty:,})", summary_style))

    if include_warnings and warnings:
        elements.append(Spacer(1, 4 * mm))
        build_warnings_section(elements, warnings, font_name, usable_w)

    doc.build(elements, onFirstPage=footer_fn, onLaterPages=footer_fn)
