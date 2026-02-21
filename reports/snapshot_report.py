"""
snapshot_report.py — [템플릿1] 재고현황 PDF 생성.
DB 접근 금지. 데이터는 caller가 제공.
"""
import pandas as pd
from reports import (HAS_REPORTLAB, A4, mm, colors,
                     SimpleDocTemplate, Table, TableStyle,
                     Paragraph, Spacer, PageBreak,
                     ParagraphStyle)
from reports.pdf_common import (register_font, build_header, page_footer,
                                 build_warnings_section, make_data_table)


def generate_stock_snapshot_pdf(path, config, snapshot_df, warnings=None):
    """재고현황 PDF 생성.
    path: 저장 경로
    config: {target_date, approvals, title, include_warnings}
    snapshot_df: pandas DataFrame (품목별 그룹 잔고)
    warnings: list of warning dicts (optional)
    """
    if not HAS_REPORTLAB:
        raise RuntimeError("reportlab 패키지가 필요합니다.")

    target_date = config['target_date']
    approvals = config['approvals']
    title = config.get('title', '재고현황')
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

    # --- 요약 페이지 ---
    build_header(elements, title, f"기준일: {target_date}", approvals, font_name, usable_w)

    cat_summary = snapshot_df.groupby('category').agg(
        품목수=('product_name', 'nunique'),
        총수량=('qty', 'sum')
    ).reset_index().rename(columns={'category': '종류'})
    sum_data = [["종류", "품목 수", "총 수량"]]
    for _, r in cat_summary.iterrows():
        sum_data.append([r['종류'] or '(미분류)', str(int(r['품목수'])), f"{int(r['총수량']):,}"])
    sum_data.append(["합계", str(int(cat_summary['품목수'].sum())), f"{int(cat_summary['총수량'].sum()):,}"])
    cw_sum = [usable_w * 0.4, usable_w * 0.3, usable_w * 0.3]
    sum_table = make_data_table(sum_data, cw_sum, font_name, header_font=9, data_font=8, padding=3)
    sum_table.setStyle(TableStyle([
        ('FONTNAME', (0, len(sum_data) - 1), (-1, len(sum_data) - 1), font_name),
        ('BACKGROUND', (0, len(sum_data) - 1), (-1, len(sum_data) - 1), colors.Color(0.9, 0.9, 0.9)),
    ]))
    elements.append(sum_table)

    if include_warnings and warnings:
        warn_note = ParagraphStyle('WN', fontName=font_name, fontSize=8,
                                    textColor=colors.Color(0.7, 0, 0))
        elements.append(Spacer(1, 3 * mm))
        elements.append(Paragraph(f"※ 데이터 경고: {len(warnings)}건 (상세 페이지 말미 참조)", warn_note))

    elements.append(PageBreak())

    # --- 상세 페이지 ---
    headers = ["품목명", "창고", "수량", "단위", "보관방법", "소비기한", "제조일", "원산지"]
    cw = [usable_w * 0.20, usable_w * 0.10, usable_w * 0.10, usable_w * 0.07,
          usable_w * 0.10, usable_w * 0.13, usable_w * 0.13, usable_w * 0.17]

    categories = snapshot_df['category'].fillna('(미분류)').unique()
    categories = sorted(categories)
    for cat in categories:
        cat_df = snapshot_df[snapshot_df['category'].fillna('(미분류)') == cat]
        cat_style = ParagraphStyle('CatH', fontName=font_name, fontSize=9,
                                    textColor=colors.white, backColor=colors.Color(0.3, 0.4, 0.6))
        elements.append(Spacer(1, 2 * mm))
        cat_banner_data = [[Paragraph(f"  [ {cat} ]  ({len(cat_df)}건)", cat_style)]]
        cat_banner = Table(cat_banner_data, colWidths=[usable_w])
        cat_banner.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, -1), colors.Color(0.3, 0.4, 0.6)),
            ('TOPPADDING', (0, 0), (-1, -1), 3),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
        ]))
        elements.append(cat_banner)

        table_data = [headers]
        cat_total_qty = 0
        for _, r in cat_df.iterrows():
            qty_val = int(r['qty'])
            cat_total_qty += qty_val
            unit = r['unit'] if pd.notna(r.get('unit')) and str(r['unit']).strip() not in ('', 'nan') else '개'
            table_data.append([
                str(r['product_name']),
                str(r['location']),
                f"{qty_val:,}",
                unit,
                str(r['storage_method']) if str(r['storage_method']) not in ('', 'nan') else '',
                str(r['expiry_date']) if str(r['expiry_date']) not in ('', 'nan') else '',
                str(r['manufacture_date']) if str(r['manufacture_date']) not in ('', 'nan') else '',
                str(r['origin']) if str(r['origin']) not in ('', 'nan') else '',
            ])
        table_data.append(["소계", "", f"{cat_total_qty:,}", "", "", "", "", ""])
        t = make_data_table(table_data, cw, font_name)
        last_row = len(table_data) - 1
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, last_row), (-1, last_row), colors.Color(0.92, 0.92, 0.92)),
            ('FONTSIZE', (0, last_row), (-1, last_row), 7),
        ]))
        elements.append(t)

    if include_warnings and warnings:
        elements.append(PageBreak())
        build_warnings_section(elements, warnings, font_name, usable_w)

    doc.build(elements, onFirstPage=footer_fn, onLaterPages=footer_fn)
