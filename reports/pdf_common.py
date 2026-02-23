"""
pdf_common.py — 공통 PDF 인프라 (폰트, 헤더, 푸터, 경고 섹션, 데이터 테이블).
DB 접근 금지.
"""
import os
from datetime import datetime
from reports import (HAS_REPORTLAB, A4, landscape, mm, colors,
                     SimpleDocTemplate, Table, TableStyle,
                     Paragraph, Spacer, PageBreak, KeepTogether,
                     ParagraphStyle, pdfmetrics, TTFont)


def register_font():
    """한글 폰트 등록. 등록된 폰트명 반환."""
    font_paths = [
        # Windows
        "C:/Windows/Fonts/malgun.ttf",
        "C:/Windows/Fonts/gulim.ttc",
        "C:/Windows/Fonts/batang.ttc",
        # Linux (Docker: fonts-nanum 패키지)
        "/usr/share/fonts/truetype/nanum/NanumGothic.ttf",
        "/usr/share/fonts/truetype/nanum/NanumBarunGothic.ttf",
        "/usr/share/fonts/truetype/nanum/NanumMyeongjo.ttf",
        # macOS
        "/System/Library/Fonts/AppleSDGothicNeo.ttc",
    ]
    for fp in font_paths:
        if os.path.exists(fp):
            try:
                pdfmetrics.registerFont(TTFont('Korean', fp))
                return 'Korean'
            except:
                pass
    return 'Helvetica'


def build_header(elements, title, subtitle, approvals, font_name, page_width):
    """제목(왼쪽) + 결재란(오른쪽) 헤더 행 + 부제 구성."""
    approval_header = list(approvals.keys())
    approval_values = [v if v else "" for v in approvals.values()]
    approval_data = [approval_header, approval_values]
    col_w = 22 * mm
    approval_table = Table(approval_data, colWidths=[col_w] * len(approval_header))
    approval_table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (-1, -1), font_name),
        ('FONTSIZE', (0, 0), (-1, 0), 8),
        ('FONTSIZE', (0, 1), (-1, 1), 9),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
        ('BACKGROUND', (0, 0), (-1, 0), colors.Color(0.85, 0.85, 0.85)),
        ('BOTTOMPADDING', (0, 1), (-1, 1), 18),
        ('TOPPADDING', (0, 1), (-1, 1), 4),
    ]))
    title_style = ParagraphStyle('Title', fontName=font_name, fontSize=16, alignment=0)
    title_para = Paragraph(title, title_style)
    approval_width = col_w * len(approval_header)
    title_width = page_width - approval_width
    header_row = Table([[title_para, approval_table]],
                       colWidths=[title_width, approval_width])
    header_row.setStyle(TableStyle([
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('ALIGN', (0, 0), (0, 0), 'LEFT'),
        ('ALIGN', (1, 0), (1, 0), 'RIGHT'),
        ('LEFTPADDING', (0, 0), (-1, -1), 0),
        ('RIGHTPADDING', (0, 0), (-1, -1), 0),
        ('TOPPADDING', (0, 0), (-1, -1), 0),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 0),
    ]))
    elements.append(header_row)
    date_style = ParagraphStyle('Date', fontName=font_name, fontSize=9,
                                 alignment=2, spaceAfter=2 * mm)
    elements.append(Paragraph(
        f"{subtitle}  |  출력일: {datetime.now().strftime('%Y-%m-%d %H:%M')}", date_style))
    elements.append(Spacer(1, 3 * mm))


def page_footer(canvas, doc, font_name):
    """페이지 번호 푸터."""
    canvas.saveState()
    canvas.setFont(font_name, 8)
    canvas.drawCentredString(doc.pagesize[0] / 2, 10 * mm, f"- {doc.page} -")
    canvas.restoreState()


def build_warnings_section(elements, warnings, font_name, page_width):
    """PDF에 경고(Exceptions) 섹션 추가."""
    if not warnings:
        note_style = ParagraphStyle('Note', fontName=font_name, fontSize=8, alignment=0)
        elements.append(Spacer(1, 3 * mm))
        elements.append(Paragraph("※ 데이터 이상 없음", note_style))
        return
    elements.append(Spacer(1, 5 * mm))
    sec_style = ParagraphStyle('WarnTitle', fontName=font_name, fontSize=9, alignment=0,
                                textColor=colors.Color(0.7, 0, 0))
    elements.append(Paragraph(
        f"※ 데이터 경고 ({len(warnings)}건) — 값을 수정하지 않고 표시만 합니다", sec_style))
    elements.append(Spacer(1, 2 * mm))
    warn_data = [["품목명", "필드", "상태"]]
    for w in warnings[:50]:
        warn_data.append([w["product_name"], w["field"], w["issue"]])
    cw = [page_width * 0.45, page_width * 0.25, page_width * 0.30]
    warn_table = Table(warn_data, colWidths=cw, repeatRows=1)
    warn_table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (-1, -1), font_name),
        ('FONTSIZE', (0, 0), (-1, -1), 7),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('GRID', (0, 0), (-1, -1), 0.3, colors.Color(0.6, 0.6, 0.6)),
        ('BACKGROUND', (0, 0), (-1, 0), colors.Color(0.95, 0.85, 0.85)),
        ('TOPPADDING', (0, 0), (-1, -1), 1),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 1),
    ]))
    elements.append(warn_table)
    if len(warnings) > 50:
        elements.append(Paragraph(f"... 외 {len(warnings) - 50}건 생략", sec_style))


def make_data_table(table_data, col_widths, font_name,
                     header_font=8, data_font=7, padding=2):
    """공통 데이터 테이블 생성 헬퍼."""
    t = Table(table_data, colWidths=col_widths, repeatRows=1)
    t.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (-1, -1), font_name),
        ('FONTSIZE', (0, 0), (-1, 0), header_font),
        ('FONTSIZE', (0, 1), (-1, -1), data_font),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('GRID', (0, 0), (-1, -1), 0.4, colors.Color(0.5, 0.5, 0.5)),
        ('BACKGROUND', (0, 0), (-1, 0), colors.Color(0.2, 0.3, 0.5)),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1),
         [colors.white, colors.Color(0.96, 0.96, 0.96)]),
        ('TOPPADDING', (0, 0), (-1, -1), padding),
        ('BOTTOMPADDING', (0, 0), (-1, -1), padding),
    ]))
    return t


def build_legacy_pdf(path, title, approvals, cols, target_date, rows,
                      fit_one_page=False):
    """레거시 PDF 생성 (탭1/수불장 직접 출력용)."""
    font_name = register_font()
    doc = SimpleDocTemplate(path, pagesize=landscape(A4),
                            leftMargin=15*mm, rightMargin=15*mm,
                            topMargin=15*mm, bottomMargin=15*mm)
    elements = []

    # 결재란
    approval_header = list(approvals.keys())
    approval_values = [v if v else "" for v in approvals.values()]
    approval_data = [approval_header, approval_values]
    col_w = 25 * mm
    approval_table = Table(approval_data, colWidths=[col_w] * len(approval_header))
    approval_table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (-1, -1), font_name),
        ('FONTSIZE', (0, 0), (-1, 0), 9),
        ('FONTSIZE', (0, 1), (-1, 1), 10),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
        ('BACKGROUND', (0, 0), (-1, 0), colors.Color(0.85, 0.85, 0.85)),
        ('BOTTOMPADDING', (0, 1), (-1, 1), 20),
        ('TOPPADDING', (0, 1), (-1, 1), 5),
    ]))

    title_style = ParagraphStyle('Title', fontName=font_name, fontSize=18, alignment=0)
    title_para = Paragraph(title, title_style)
    available_width = landscape(A4)[0] - 30 * mm
    approval_width = col_w * len(approval_header)
    title_width = available_width - approval_width
    header_row = Table([[title_para, approval_table]],
                       colWidths=[title_width, approval_width])
    header_row.setStyle(TableStyle([
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('ALIGN', (0, 0), (0, 0), 'LEFT'),
        ('ALIGN', (1, 0), (1, 0), 'RIGHT'),
        ('LEFTPADDING', (0, 0), (-1, -1), 0),
        ('RIGHTPADDING', (0, 0), (-1, -1), 0),
        ('TOPPADDING', (0, 0), (-1, -1), 0),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 0),
    ]))
    elements.append(header_row)

    date_style = ParagraphStyle('Date', fontName=font_name, fontSize=10,
                                 alignment=2, spaceAfter=3 * mm)
    elements.append(Paragraph(
        f"기준일: {target_date}  |  출력일: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        date_style))
    elements.append(Spacer(1, 5 * mm))

    col_headers = list(cols)
    table_data = [col_headers] + [list(r) for r in rows]
    num_cols = len(col_headers)
    col_width = available_width / num_cols

    data_font_size = 8
    header_font_size = 9
    row_padding = 4
    if fit_one_page:
        page_h = landscape(A4)[1] - 30 * mm
        header_area = 40 * mm
        avail_h = page_h - header_area
        row_count = len(table_data)
        needed_h = row_count * 5.5 * mm
        if needed_h > avail_h:
            scale = float(avail_h / needed_h)
            data_font_size = max(5, int(8 * scale))
            header_font_size = max(6, int(9 * scale))
            row_padding = max(1, int(4 * scale))

    data_table = Table(table_data, colWidths=[col_width] * num_cols, repeatRows=1)
    data_table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (-1, -1), font_name),
        ('FONTSIZE', (0, 0), (-1, 0), header_font_size),
        ('FONTSIZE', (0, 1), (-1, -1), data_font_size),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
        ('BACKGROUND', (0, 0), (-1, 0), colors.Color(0.2, 0.3, 0.5)),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1),
         [colors.white, colors.Color(0.95, 0.95, 0.95)]),
        ('TOPPADDING', (0, 0), (-1, -1), row_padding),
        ('BOTTOMPADDING', (0, 0), (-1, -1), row_padding),
    ]))
    elements.append(data_table)
    doc.build(elements)
