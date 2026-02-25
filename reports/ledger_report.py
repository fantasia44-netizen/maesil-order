"""
ledger_report.py — [템플릿2] 수불부 PDF 생성.
DB 접근 금지. 데이터는 caller가 제공.
"""
from datetime import datetime
from reports import (HAS_REPORTLAB, A4, landscape, mm, colors,
                     SimpleDocTemplate, Table, TableStyle,
                     Paragraph, Spacer, PageBreak, KeepTogether,
                     ParagraphStyle,
                     BaseDocTemplate, Frame, PageTemplate, NextPageTemplate)
from reports.pdf_common import (register_font, build_header, page_footer,
                                 build_warnings_section, make_data_table)
from models import INV_TYPE_LABELS


def _safe_num(val):
    """qty를 숫자로 변환 (소수점 허용, 정수면 int 유지)."""
    n = float(val)
    return int(n) if n == int(n) else n


def _fmt_qty(val):
    """숫자를 천단위 포맷 (소수점은 소수 1자리)."""
    if isinstance(val, float) and val != int(val):
        return f"{val:,.1f}"
    return f"{int(val):,}"


def _unpack_gkey(gkey):
    """group_keys 4-tuple 또는 5-tuple 언패킹."""
    if len(gkey) == 5:
        return gkey[0], gkey[1], gkey[2], gkey[3], gkey[4]
    return gkey[0], gkey[1], gkey[2], gkey[3], ''


# ================================================================
# 다단 출력 헬퍼 함수
# ================================================================

def _draw_mc_header(canvas, config, font_name, page_w, page_h, margin):
    """다단 모드 첫 페이지 헤더를 캔버스에 직접 그리기."""
    date_from = config.get('date_from', '')
    date_to = config['date_to']
    title = config.get('title', '수불부')
    period_str = f"{date_from} ~ {date_to}" if date_from else f"~ {date_to}"
    approvals = config.get('approvals', {})

    y_top = page_h - margin

    # 제목
    canvas.setFont(font_name, 14)
    canvas.drawString(margin, y_top - 12, title)

    # 결재란 (오른쪽)
    labels = list(approvals.keys())
    n_appr = len(labels)
    col_w_a = 18 * mm
    appr_total_w = col_w_a * n_appr
    x_start = page_w - margin - appr_total_w

    canvas.setFont(font_name, 6.5)
    for i, label in enumerate(labels):
        x = x_start + i * col_w_a
        # 헤더 셀 (회색 배경)
        canvas.setStrokeColor(colors.black)
        canvas.setFillColor(colors.Color(0.85, 0.85, 0.85))
        canvas.rect(x, y_top - 10, col_w_a, 10, fill=1)
        canvas.setFillColor(colors.black)
        canvas.drawCentredString(x + col_w_a / 2, y_top - 8, label)
        # 값 셀 (빈칸)
        canvas.setFillColor(colors.white)
        canvas.rect(x, y_top - 24, col_w_a, 14, fill=1)

    # 기간 표시
    canvas.setFont(font_name, 7.5)
    canvas.setFillColor(colors.black)
    subtitle = f"기간: {period_str}  |  출력일: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    canvas.drawRightString(page_w - margin, y_top - 32, subtitle)


def _generate_multicolumn_pdf(path, config, prev_dict, period_groups,
                                sorted_keys, group_keys, num_cols, warnings=None):
    """다단 플랫테이블 PDF 생성 — 한장출력 스타일을 2~3단으로 분할."""
    font_name = register_font()
    margin = 15 * mm
    page_size = landscape(A4)
    page_w, page_h = page_size
    has_mfg = len(group_keys) > 4

    # 칼럼 너비 계산
    gap = 5 * mm
    total_gap = gap * (num_cols - 1)
    col_w = (page_w - 2 * margin - total_gap) / num_cols

    # 헤더/푸터 높이
    header_h = 36 * mm

    # 프레임 생성 헬퍼
    def make_frames(prefix, content_h, y_bottom):
        frames = []
        for i in range(num_cols):
            x = margin + i * (col_w + gap)
            frames.append(Frame(x, y_bottom, col_w, content_h,
                                id=f'{prefix}_c{i}',
                                leftPadding=0, rightPadding=0,
                                topPadding=0, bottomPadding=0))
        return frames

    fp_content_h = page_h - margin - margin - header_h
    fp_frames = make_frames('fp', fp_content_h, margin)

    lp_content_h = page_h - margin - margin
    lp_frames = make_frames('lp', lp_content_h, margin)

    def on_first_page(canvas, doc):
        canvas.saveState()
        _draw_mc_header(canvas, config, font_name, page_w, page_h, margin)
        canvas.setFont(font_name, 7)
        canvas.drawCentredString(page_w / 2, 8 * mm, f"- {doc.page} -")
        canvas.restoreState()

    def on_later_pages(canvas, doc):
        canvas.saveState()
        canvas.setFont(font_name, 7)
        canvas.drawCentredString(page_w / 2, 8 * mm, f"- {doc.page} -")
        canvas.restoreState()

    first_tmpl = PageTemplate(id='first', frames=fp_frames, onPage=on_first_page)
    later_tmpl = PageTemplate(id='later', frames=lp_frames, onPage=on_later_pages)

    doc = BaseDocTemplate(path, pagesize=page_size,
                          leftMargin=margin, rightMargin=margin,
                          topMargin=margin, bottomMargin=margin)
    doc.addPageTemplates([first_tmpl, later_tmpl])

    # ── 플랫 테이블 데이터 구축 (한장출력과 동일) ──
    if has_mfg:
        if num_cols >= 3:
            headers = ["품목명", "제조일", "전일", "입고", "출고", "잔고"]
            cw = [col_w * 0.30, col_w * 0.18, col_w * 0.13,
                  col_w * 0.13, col_w * 0.13, col_w * 0.13]
        else:
            headers = ["품목명", "제조일", "단위", "전일", "입고", "출고", "잔고"]
            cw = [col_w * 0.26, col_w * 0.16, col_w * 0.07,
                  col_w * 0.12, col_w * 0.12, col_w * 0.12, col_w * 0.15]
    else:
        if num_cols >= 3:
            headers = ["품목명", "창고", "전일", "입고", "출고", "잔고"]
            cw = [col_w * 0.30, col_w * 0.15, col_w * 0.14,
                  col_w * 0.14, col_w * 0.14, col_w * 0.13]
        else:
            headers = ["품목명", "창고", "단위", "전일", "입고", "출고", "잔고"]
            cw = [col_w * 0.26, col_w * 0.13, col_w * 0.07,
                  col_w * 0.13, col_w * 0.13, col_w * 0.13, col_w * 0.15]

    table_data = [headers]
    for gkey in sorted_keys:
        product_name, location, category, unit, mfg_date = _unpack_gkey(gkey)
        opening = prev_dict.get(gkey, 0)
        total_in = 0
        total_out = 0
        for row in period_groups.get(gkey, []):
            qty = _safe_num(row['qty'])
            if qty >= 0:
                total_in += qty
            else:
                total_out += abs(qty)
        closing = opening + total_in - total_out
        u = unit if str(unit) not in ('', 'nan') else '개'
        loc_str = str(location) if location and str(location) not in ('', 'nan', 'None') else ''
        mfg_str = str(mfg_date) if mfg_date and str(mfg_date) not in ('', 'nan', 'None') else ''

        if has_mfg:
            if num_cols >= 3:
                table_data.append([
                    product_name, mfg_str,
                    _fmt_qty(opening) if opening else "",
                    _fmt_qty(total_in) if total_in else "",
                    _fmt_qty(total_out) if total_out else "",
                    _fmt_qty(closing)
                ])
            else:
                table_data.append([
                    product_name, mfg_str, u,
                    _fmt_qty(opening) if opening else "",
                    _fmt_qty(total_in) if total_in else "",
                    _fmt_qty(total_out) if total_out else "",
                    _fmt_qty(closing)
                ])
        else:
            if num_cols >= 3:
                table_data.append([
                    product_name, loc_str,
                    _fmt_qty(opening) if opening else "",
                    _fmt_qty(total_in) if total_in else "",
                    _fmt_qty(total_out) if total_out else "",
                    _fmt_qty(closing)
                ])
            else:
                table_data.append([
                    product_name, loc_str, u,
                    _fmt_qty(opening) if opening else "",
                    _fmt_qty(total_in) if total_in else "",
                    _fmt_qty(total_out) if total_out else "",
                    _fmt_qty(closing)
                ])

    # ── 동적 폰트 크기 ──
    row_count = len(table_data)
    if num_cols >= 3:
        d_font, h_font, pad = 5.5, 6.5, 1
    else:
        d_font, h_font, pad = 6, 7, 1.5

    # 테이블 생성 (repeatRows=1 → 헤더가 각 프레임 상단에 반복)
    t = Table(table_data, colWidths=cw, repeatRows=1, splitByRow=True)
    t.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (-1, -1), font_name),
        ('FONTSIZE', (0, 0), (-1, 0), h_font),
        ('FONTSIZE', (0, 1), (-1, -1), d_font),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('ALIGN', (0, 1), (0, -1), 'LEFT'),  # 품목명 좌측 정렬
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('GRID', (0, 0), (-1, -1), 0.3, colors.Color(0.6, 0.6, 0.6)),
        ('BACKGROUND', (0, 0), (-1, 0), colors.Color(0.2, 0.3, 0.5)),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1),
         [colors.white, colors.Color(0.96, 0.96, 0.96)]),
        ('TOPPADDING', (0, 0), (-1, -1), pad),
        ('BOTTOMPADDING', (0, 0), (-1, -1), pad),
        ('LEFTPADDING', (0, 0), (-1, -1), 2),
        ('RIGHTPADDING', (0, 0), (-1, -1), 2),
    ]))

    elements = [NextPageTemplate('later'), t]
    doc.build(elements)


# ================================================================
# 메인 PDF 생성 함수
# ================================================================

def generate_ledger_pdf(path, config, prev_dict, period_groups, sorted_keys,
                         group_keys, warnings=None):
    """수불부 PDF 생성.
    prev_dict: {gkey: 전일재고}
    period_groups: {gkey: [row_series, ...]}
    sorted_keys: [gkey, ...]
    group_keys: ['product_name', 'location', 'category', 'unit'] (+ 'manufacture_date')
    """
    if not HAS_REPORTLAB:
        raise RuntimeError("reportlab 패키지가 필요합니다.")
    date_from = config.get('date_from', '')
    date_to = config['date_to']
    approvals = config['approvals']
    title = config.get('title', '수불부')
    include_warnings = config.get('include_warnings', True)
    fit_one_page = config.get('fit_one_page', False)
    multi_col = config.get('multi_col', 0)  # 0=off, 2=2단, 3=3단
    has_mfg = len(group_keys) > 4  # 제조일 분리 여부

    # ================================================================
    # 다단 출력 모드 — 사용자 선택 (2단 또는 3단)
    # ================================================================
    if multi_col in (2, 3):
        num_cols = multi_col
        _generate_multicolumn_pdf(path, config, prev_dict, period_groups,
                                   sorted_keys, group_keys, num_cols, warnings)
        return

    font_name = register_font()
    margin = 15 * mm

    if fit_one_page:
        page_size = landscape(A4)
    else:
        page_size = A4

    page_w, page_h = page_size
    usable_w = page_w - 2 * margin
    doc = SimpleDocTemplate(path, pagesize=page_size,
                            leftMargin=margin, rightMargin=margin,
                            topMargin=margin, bottomMargin=margin)
    elements = []
    footer_fn = lambda c, d: page_footer(c, d, font_name)
    period_str = f"{date_from} ~ {date_to}" if date_from else f"~ {date_to}"

    # ================================================================
    # 한 장 맞춤 모드 — 통합 테이블 1개로 전체 품목 표시
    # ================================================================
    if fit_one_page:
        build_header(elements, title, f"기간: {period_str}", approvals, font_name, usable_w)
        elements.append(Spacer(1, 3 * mm))

        if has_mfg:
            headers = ["품목명", "창고", "제조일", "단위", "전일재고", "입고", "출고", "종료일재고"]
            cw = [usable_w * 0.18, usable_w * 0.10, usable_w * 0.12, usable_w * 0.06,
                  usable_w * 0.12, usable_w * 0.12, usable_w * 0.12, usable_w * 0.12]
        else:
            headers = ["품목명", "창고", "단위", "전일재고", "입고", "출고", "종료일재고"]
            cw = [usable_w * 0.22, usable_w * 0.12, usable_w * 0.08,
                  usable_w * 0.14, usable_w * 0.14, usable_w * 0.14, usable_w * 0.14]

        table_data = [headers]
        for gkey in sorted_keys:
            product_name, location, category, unit, mfg_date = _unpack_gkey(gkey)
            opening = prev_dict.get(gkey, 0)
            total_in = 0
            total_out = 0
            for row in period_groups[gkey]:
                qty = _safe_num(row['qty'])
                if qty >= 0:
                    total_in += qty
                else:
                    total_out += abs(qty)
            closing = opening + total_in - total_out
            u = unit if str(unit) not in ('', 'nan') else '개'
            loc_str = str(location) if location and str(location) not in ('', 'nan', 'None') else ''
            mfg_str = str(mfg_date) if mfg_date and str(mfg_date) not in ('', 'nan', 'None') else ''
            if has_mfg:
                table_data.append([
                    product_name, loc_str, mfg_str, u,
                    _fmt_qty(opening) if opening else "",
                    _fmt_qty(total_in) if total_in else "",
                    _fmt_qty(total_out) if total_out else "",
                    _fmt_qty(closing)
                ])
            else:
                table_data.append([
                    product_name, loc_str, u,
                    _fmt_qty(opening) if opening else "",
                    _fmt_qty(total_in) if total_in else "",
                    _fmt_qty(total_out) if total_out else "",
                    _fmt_qty(closing)
                ])

        # 동적 폰트 축소
        row_count = len(table_data)
        avail_h = page_h - 2 * margin - 45 * mm  # 헤더 영역 제외
        needed_h = row_count * 5.5 * mm
        if needed_h > avail_h and row_count > 2:
            scale = float(avail_h / needed_h)
            d_font = max(5, int(7 * scale))
            h_font = max(6, int(8 * scale))
            pad = max(1, int(2 * scale))
        else:
            d_font, h_font, pad = 7, 8, 2

        t = make_data_table(table_data, cw, font_name, header_font=h_font, data_font=d_font, padding=pad)
        last_r = len(table_data) - 1
        # 홀짝 줄 배경
        elements.append(t)

        doc.build(elements, onFirstPage=footer_fn, onLaterPages=footer_fn)
        return

    # ================================================================
    # 일반 모드 (기존) — 요약 + 품목별 상세
    # ================================================================
    build_header(elements, title, f"기간: {period_str}", approvals, font_name, usable_w)

    loc_summary = {}
    for gkey in sorted_keys:
        product_name, location, category, unit, mfg_date = _unpack_gkey(gkey)
        opening = prev_dict.get(gkey, 0)
        total_in = 0
        total_out = 0
        for row in period_groups[gkey]:
            qty = _safe_num(row['qty'])
            if qty >= 0:
                total_in += qty
            else:
                total_out += abs(qty)
        closing = opening + total_in - total_out
        if location not in loc_summary:
            loc_summary[location] = {"items": set(), "opening": 0, "in": 0, "out": 0, "closing": 0}
        loc_summary[location]["items"].add(product_name)
        loc_summary[location]["opening"] += opening
        loc_summary[location]["in"] += total_in
        loc_summary[location]["out"] += total_out
        loc_summary[location]["closing"] += closing

    sum_data = [["창고", "품목 수", "전일재고", "입고합계", "출고합계", "종료일재고"]]
    grand = {"items": 0, "opening": 0, "in": 0, "out": 0, "closing": 0}
    for loc in sorted(loc_summary.keys()):
        s = loc_summary[loc]
        n_items = len(s["items"])
        sum_data.append([
            loc, str(n_items),
            _fmt_qty(s['opening']), _fmt_qty(s['in']), _fmt_qty(s['out']), _fmt_qty(s['closing'])
        ])
        grand["items"] += n_items
        grand["opening"] += s["opening"]
        grand["in"] += s["in"]
        grand["out"] += s["out"]
        grand["closing"] += s["closing"]
    sum_data.append(["합계", str(grand["items"]),
                     _fmt_qty(grand['opening']), _fmt_qty(grand['in']),
                     _fmt_qty(grand['out']), _fmt_qty(grand['closing'])])
    cw_sum = [usable_w * 0.20, usable_w * 0.12, usable_w * 0.17,
              usable_w * 0.17, usable_w * 0.17, usable_w * 0.17]
    sum_table = make_data_table(sum_data, cw_sum, font_name, header_font=9, data_font=8, padding=3)
    last_r = len(sum_data) - 1
    sum_table.setStyle(TableStyle([
        ('BACKGROUND', (0, last_r), (-1, last_r), colors.Color(0.9, 0.9, 0.9)),
    ]))
    elements.append(sum_table)

    if include_warnings and warnings:
        warn_note = ParagraphStyle('WN', fontName=font_name, fontSize=8,
                                    textColor=colors.Color(0.7, 0, 0))
        elements.append(Spacer(1, 3 * mm))
        elements.append(Paragraph(f"※ 데이터 경고: {len(warnings)}건 (문서 말미 참조)", warn_note))

    # --- 상세 페이지 ---
    headers = ["일자", "구분", "단위", "전일재고", "입고", "출고", "종료일재고", "증빙번호", "비고"]
    cw = [usable_w * 0.11, usable_w * 0.09, usable_w * 0.06, usable_w * 0.10,
          usable_w * 0.10, usable_w * 0.10, usable_w * 0.10, usable_w * 0.17, usable_w * 0.17]

    current_location = None
    for gkey in sorted_keys:
        product_name, location, category, unit, mfg_date = _unpack_gkey(gkey)
        running = prev_dict.get(gkey, 0)
        transactions = period_groups[gkey]

        if location != current_location:
            elements.append(PageBreak())
            current_location = location
            loc_style = ParagraphStyle('LocH', fontName=font_name, fontSize=10,
                                       textColor=colors.white)
            loc_banner_data = [[Paragraph(f"  창고: {location}", loc_style)]]
            loc_banner = Table(loc_banner_data, colWidths=[usable_w])
            loc_banner.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, -1), colors.Color(0.2, 0.3, 0.5)),
                ('TOPPADDING', (0, 0), (-1, -1), 4),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
            ]))
            elements.append(loc_banner)
            elements.append(Spacer(1, 2 * mm))

        item_block = []
        item_style = ParagraphStyle('ItemH', fontName=font_name, fontSize=8,
                                     textColor=colors.Color(0.1, 0.1, 0.4))
        # 배너에 제조일 표시 (5-tuple인 경우)
        item_label = f"▶ {product_name} ({category})"
        if has_mfg and mfg_date and str(mfg_date) not in ('', 'nan', 'None'):
            item_label += f"  [제조일: {mfg_date}]"
        item_banner_data = [[Paragraph(item_label, item_style)]]
        item_banner = Table(item_banner_data, colWidths=[usable_w])
        item_banner.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, -1), colors.Color(0.92, 0.94, 0.98)),
            ('TOPPADDING', (0, 0), (-1, -1), 2),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
        ]))
        item_block.append(item_banner)

        table_data = [headers]
        item_total_in = 0
        item_total_out = 0

        if running != 0:
            table_data.append([
                date_from if date_from else "",
                "전일이월",
                unit if str(unit) not in ('', 'nan') else '개',
                _fmt_qty(running), "", "", _fmt_qty(running), "", ""
            ])

        for row in transactions:
            qty = _safe_num(row['qty'])
            running += qty
            type_label = INV_TYPE_LABELS.get(row.get('type', ''), row.get('type', ''))
            in_qty = _fmt_qty(qty) if qty >= 0 else ""
            out_qty = _fmt_qty(abs(qty)) if qty < 0 else ""
            if qty >= 0:
                item_total_in += qty
            else:
                item_total_out += abs(qty)
            # 증빙번호: lot_number > repack_doc_no
            ref_no = str(row.get('lot_number', '')).strip()
            if ref_no in ('', 'nan', 'None'):
                ref_no = str(row.get('repack_doc_no', '')).strip()
            if ref_no in ('', 'nan', 'None'):
                ref_no = ''
            # 비고
            memo_val = str(row.get('memo', '')).strip()
            if memo_val in ('nan', 'None'):
                memo_val = ''
            table_data.append([
                str(row.get('transaction_date', '')),
                type_label,
                str(row.get('unit', '개')) if str(row.get('unit', '')) not in ('', 'nan') else '개',
                "", in_qty, out_qty, _fmt_qty(running),
                ref_no, memo_val
            ])

        table_data.append([
            "", "소계", "", "",
            _fmt_qty(item_total_in) if item_total_in else "",
            _fmt_qty(item_total_out) if item_total_out else "",
            _fmt_qty(running), "", ""
        ])
        t = make_data_table(table_data, cw, font_name)
        lr = len(table_data) - 1
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, lr), (-1, lr), colors.Color(0.92, 0.92, 0.92)),
        ]))
        if running != 0 or prev_dict.get(gkey, 0) != 0:
            carry_row = 1
            if len(table_data) > 2:
                t.setStyle(TableStyle([
                    ('BACKGROUND', (0, carry_row), (-1, carry_row), colors.Color(0.88, 0.93, 1.0)),
                ]))
        item_block.append(t)

        try:
            elements.append(KeepTogether(item_block))
        except:
            elements.extend(item_block)
        elements.append(Spacer(1, 2 * mm))

    if include_warnings and warnings:
        elements.append(PageBreak())
        build_warnings_section(elements, warnings, font_name, usable_w)

    doc.build(elements, onFirstPage=footer_fn, onLaterPages=footer_fn)
