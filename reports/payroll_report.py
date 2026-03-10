"""
payroll_report.py — 급여명세서 PDF 생성.
개별 명세서 및 전체 보고용 명세서 지원.
DB 접근 금지.
"""
from datetime import datetime
from reports import (HAS_REPORTLAB, A4, mm, colors,
                     SimpleDocTemplate, Table, TableStyle,
                     Paragraph, Spacer, PageBreak, KeepTogether,
                     ParagraphStyle)
from reports.pdf_common import register_font


def _fmt(val):
    n = int(val or 0)
    return f'{n:,}'


def _build_payslip_elements(p, font_name, biz_name='배마마'):
    """급여명세서 1인분의 element 리스트 생성."""
    elements = []
    styles = {
        'title': ParagraphStyle('T', fontName=font_name, fontSize=14,
                                 alignment=1, spaceAfter=2*mm),
        'subtitle': ParagraphStyle('ST', fontName=font_name, fontSize=9,
                                    alignment=1, spaceAfter=4*mm,
                                    textColor=colors.Color(0.4, 0.4, 0.4)),
        'section': ParagraphStyle('Sec', fontName=font_name, fontSize=9,
                                   spaceBefore=3*mm, spaceAfter=1*mm),
        'note': ParagraphStyle('N', fontName=font_name, fontSize=7,
                                textColor=colors.Color(0.5, 0.5, 0.5),
                                spaceBefore=3*mm),
    }

    # 제목
    elements.append(Paragraph(f'급 여 명 세 서', styles['title']))
    elements.append(Paragraph(
        f'{biz_name}  |  {p.get("pay_month", "")}  |  출력일: {datetime.now().strftime("%Y-%m-%d")}',
        styles['subtitle']))

    # 인적사항
    info_data = [
        ['성명', p.get('employee_name', ''), '부서', p.get('department', '')],
        ['직급', p.get('position', ''), '입사일', p.get('hire_date', '')],
    ]
    info_table = Table(info_data, colWidths=[25*mm, 55*mm, 25*mm, 55*mm])
    info_table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (-1, -1), font_name),
        ('FONTSIZE', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.Color(0.6, 0.6, 0.6)),
        ('BACKGROUND', (0, 0), (0, -1), colors.Color(0.92, 0.92, 0.92)),
        ('BACKGROUND', (2, 0), (2, -1), colors.Color(0.92, 0.92, 0.92)),
        ('TOPPADDING', (0, 0), (-1, -1), 3),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
    ]))
    elements.append(info_table)
    elements.append(Spacer(1, 3*mm))

    # 지급/공제 테이블 (좌: 지급, 우: 공제)
    pay_items = [
        ('기본급', _fmt(p.get('base_salary', 0))),
    ]
    for key, label in [
        ('position_allowance', '직급수당'),
        ('responsibility_allowance', '직책수당'),
        ('longevity_allowance', '근속수당'),
        ('meal_allowance', '식대(비과세)'),
        ('vehicle_allowance', '차량유지비(비과세)'),
        ('overtime_pay', '연장근로수당'),
        ('night_pay', '야간근로수당'),
        ('holiday_pay', '휴일근로수당'),
        ('bonus', '상여금/성과급'),
        ('other_allowance', '기타수당'),
    ]:
        val = int(p.get(key, 0) or 0)
        if val > 0:
            pay_items.append((label, _fmt(val)))

    deduct_items = [
        ('국민연금', _fmt(p.get('national_pension', 0))),
        ('건강보험', _fmt(p.get('health_insurance', 0))),
        ('장기요양보험', _fmt(p.get('long_term_care', 0))),
        ('고용보험', _fmt(p.get('employment_insurance', 0))),
        ('근로소득세', _fmt(p.get('income_tax', 0))),
        ('지방소득세', _fmt(p.get('local_income_tax', 0))),
    ]

    # 행 수 맞추기
    max_rows = max(len(pay_items), len(deduct_items))
    while len(pay_items) < max_rows:
        pay_items.append(('', ''))
    while len(deduct_items) < max_rows:
        deduct_items.append(('', ''))

    header_row = ['지급항목', '금액(원)', '공제항목', '금액(원)']
    table_data = [header_row]
    for i in range(max_rows):
        table_data.append([
            pay_items[i][0], pay_items[i][1],
            deduct_items[i][0], deduct_items[i][1],
        ])

    # 합계행
    table_data.append([
        '총 지급액', _fmt(p.get('gross_salary', 0)),
        '총 공제액', _fmt(p.get('total_deductions', 0)),
    ])

    col_widths = [35*mm, 45*mm, 35*mm, 45*mm]
    detail_table = Table(table_data, colWidths=col_widths, repeatRows=1)
    detail_table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (-1, -1), font_name),
        ('FONTSIZE', (0, 0), (-1, -1), 8),
        ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
        ('ALIGN', (3, 0), (3, -1), 'RIGHT'),
        ('GRID', (0, 0), (-1, -1), 0.4, colors.Color(0.5, 0.5, 0.5)),
        ('BACKGROUND', (0, 0), (-1, 0), colors.Color(0.2, 0.3, 0.5)),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('BACKGROUND', (0, -1), (-1, -1), colors.Color(0.9, 0.9, 0.9)),
        ('FONTSIZE', (0, -1), (-1, -1), 9),
        ('TOPPADDING', (0, 0), (-1, -1), 2),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
        ('ROWBACKGROUNDS', (0, 1), (-1, -2),
         [colors.white, colors.Color(0.97, 0.97, 0.97)]),
    ]))
    elements.append(detail_table)
    elements.append(Spacer(1, 4*mm))

    # 실수령액 강조
    net_data = [['실수령액', _fmt(p.get('net_salary', 0)) + '원']]
    net_table = Table(net_data, colWidths=[80*mm, 80*mm])
    net_table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (-1, -1), font_name),
        ('FONTSIZE', (0, 0), (-1, -1), 12),
        ('ALIGN', (0, 0), (0, 0), 'CENTER'),
        ('ALIGN', (1, 0), (1, 0), 'CENTER'),
        ('BOX', (0, 0), (-1, -1), 1, colors.Color(0.2, 0.3, 0.5)),
        ('BACKGROUND', (0, 0), (-1, -1), colors.Color(0.95, 0.95, 1)),
        ('TOPPADDING', (0, 0), (-1, -1), 6),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
    ]))
    elements.append(net_table)

    # 사업주 부담분 (보고용)
    employer_data = [
        ['사업주 부담', '국민연금', '건강보험', '장기요양', '고용보험', '산재보험', '합계'],
        ['금액(원)',
         _fmt(p.get('national_pension_employer', 0)),
         _fmt(p.get('health_insurance_employer', 0)),
         _fmt(p.get('long_term_care_employer', 0)),
         _fmt(p.get('employment_insurance_employer', 0)),
         _fmt(p.get('industrial_accident_insurance', 0)),
         _fmt(p.get('total_employer_cost', 0))],
    ]
    emp_widths = [25*mm] + [22*mm]*5 + [25*mm]
    emp_table = Table(employer_data, colWidths=emp_widths)
    emp_table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (-1, -1), font_name),
        ('FONTSIZE', (0, 0), (-1, -1), 7),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('GRID', (0, 0), (-1, -1), 0.3, colors.Color(0.6, 0.6, 0.6)),
        ('BACKGROUND', (0, 0), (-1, 0), colors.Color(0.85, 0.8, 0.9)),
        ('TOPPADDING', (0, 0), (-1, -1), 2),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
    ]))
    elements.append(Spacer(1, 3*mm))
    elements.append(emp_table)

    elements.append(Paragraph(
        '※ 본 명세서는 참고용이며 실제 급여와 차이가 있을 수 있습니다.',
        styles['note']))

    return elements


def generate_individual_payslip(path, payroll_record, biz_name='배마마'):
    """개별 급여명세서 PDF 생성."""
    font_name = register_font()
    doc = SimpleDocTemplate(path, pagesize=A4,
                            leftMargin=15*mm, rightMargin=15*mm,
                            topMargin=15*mm, bottomMargin=15*mm)
    elements = _build_payslip_elements(payroll_record, font_name, biz_name)
    doc.build(elements)


def generate_bulk_payslips(path, payroll_records, biz_name='배마마'):
    """전체 급여명세서 PDF 생성 (보고용, 직원별 페이지 구분)."""
    font_name = register_font()
    doc = SimpleDocTemplate(path, pagesize=A4,
                            leftMargin=15*mm, rightMargin=15*mm,
                            topMargin=15*mm, bottomMargin=15*mm)
    elements = []

    for i, record in enumerate(payroll_records):
        if i > 0:
            elements.append(PageBreak())
        elements.extend(_build_payslip_elements(record, font_name, biz_name))

    if not elements:
        elements.append(Paragraph('급여 데이터가 없습니다.',
                                   ParagraphStyle('Empty', fontName=font_name,
                                                   fontSize=12, alignment=1)))
    doc.build(elements)


def generate_payroll_summary(path, payroll_records, pay_month, biz_name='배마마'):
    """급여 총괄표 PDF (전체 직원 한페이지 요약)."""
    font_name = register_font()
    from reports import landscape
    doc = SimpleDocTemplate(path, pagesize=landscape(A4),
                            leftMargin=10*mm, rightMargin=10*mm,
                            topMargin=12*mm, bottomMargin=12*mm)
    elements = []

    title_style = ParagraphStyle('T', fontName=font_name, fontSize=14,
                                  alignment=1, spaceAfter=2*mm)
    sub_style = ParagraphStyle('S', fontName=font_name, fontSize=9,
                                alignment=1, spaceAfter=4*mm,
                                textColor=colors.Color(0.4, 0.4, 0.4))

    elements.append(Paragraph(f'{pay_month} 급여 총괄표', title_style))
    elements.append(Paragraph(
        f'{biz_name}  |  출력일: {datetime.now().strftime("%Y-%m-%d %H:%M")}',
        sub_style))

    headers = ['#', '성명', '부서', '기본급', '수당', '총지급액',
               '4대보험', '소득세', '총공제', '실수령액', '사업주부담']
    table_data = [headers]

    sum_base = sum_allow = sum_gross = sum_ins = sum_tax = 0
    sum_deduct = sum_net = sum_employer = 0

    for i, p in enumerate(payroll_records):
        base = int(p.get('base_salary', 0) or 0)
        allow = int(p.get('allowances', 0) or 0)
        gross = int(p.get('gross_salary', 0) or 0)
        ins = (int(p.get('national_pension', 0) or 0) +
               int(p.get('health_insurance', 0) or 0) +
               int(p.get('long_term_care', 0) or 0) +
               int(p.get('employment_insurance', 0) or 0))
        tax = (int(p.get('income_tax', 0) or 0) +
               int(p.get('local_income_tax', 0) or 0))
        deduct = int(p.get('total_deductions', 0) or 0)
        net = int(p.get('net_salary', 0) or 0)
        employer = int(p.get('total_employer_cost', 0) or 0)

        sum_base += base; sum_allow += allow; sum_gross += gross
        sum_ins += ins; sum_tax += tax; sum_deduct += deduct
        sum_net += net; sum_employer += employer

        table_data.append([
            str(i+1), p.get('employee_name', ''), p.get('department', ''),
            _fmt(base), _fmt(allow), _fmt(gross),
            _fmt(ins), _fmt(tax), _fmt(deduct), _fmt(net), _fmt(employer),
        ])

    # 합계행
    table_data.append([
        '', '합계', '', _fmt(sum_base), _fmt(sum_allow), _fmt(sum_gross),
        _fmt(sum_ins), _fmt(sum_tax), _fmt(sum_deduct),
        _fmt(sum_net), _fmt(sum_employer),
    ])

    page_w = landscape(A4)[0] - 20*mm
    col_widths = [
        page_w*0.03, page_w*0.08, page_w*0.08,
        page_w*0.09, page_w*0.08, page_w*0.10,
        page_w*0.09, page_w*0.09, page_w*0.09,
        page_w*0.10, page_w*0.09,
    ]

    t = Table(table_data, colWidths=col_widths, repeatRows=1)
    t.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (-1, -1), font_name),
        ('FONTSIZE', (0, 0), (-1, 0), 8),
        ('FONTSIZE', (0, 1), (-1, -1), 7),
        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
        ('ALIGN', (3, 1), (-1, -1), 'RIGHT'),
        ('ALIGN', (0, 1), (0, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('GRID', (0, 0), (-1, -1), 0.4, colors.Color(0.5, 0.5, 0.5)),
        ('BACKGROUND', (0, 0), (-1, 0), colors.Color(0.2, 0.3, 0.5)),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('BACKGROUND', (0, -1), (-1, -1), colors.Color(0.92, 0.92, 0.92)),
        ('FONTSIZE', (0, -1), (-1, -1), 8),
        ('ROWBACKGROUNDS', (0, 1), (-1, -2),
         [colors.white, colors.Color(0.97, 0.97, 0.97)]),
        ('TOPPADDING', (0, 0), (-1, -1), 2),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
    ]))
    elements.append(t)

    doc.build(elements)
