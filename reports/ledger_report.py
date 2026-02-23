"""
ledger_report.py — [템플릿2] 수불부 PDF 생성.
DB 접근 금지. 데이터는 caller가 제공.
"""
from reports import (HAS_REPORTLAB, A4, landscape, mm, colors,
                     SimpleDocTemplate, Table, TableStyle,
                     Paragraph, Spacer, PageBreak, KeepTogether,
                     ParagraphStyle)
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
    has_mfg = len(group_keys) > 4  # 제조일 분리 여부

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
