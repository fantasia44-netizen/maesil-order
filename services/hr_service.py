"""
hr_service.py -- 인건비/연차 관리 서비스 레이어.
직원 요약, 연차 달력, 급여 계산 (한국 급여체계) 로직 제공.
"""
from datetime import date, timedelta
from collections import defaultdict
import math


def get_employee_summary(db):
    """재직자 수, 총인건비, 연차 현황 요약.

    Args:
        db: SupabaseDB instance

    Returns:
        dict: {
            active_count, total_count,
            total_payroll, current_month,
            leave_summary: {granted, used, remaining}
        }
    """
    employees = db.query_employees()
    active = [e for e in employees if e.get('status') == '재직']

    # 현재 월 급여 합계
    today = date.today()
    current_month = today.strftime('%Y-%m')
    payroll = db.query_payroll(pay_month=current_month)
    total_payroll = sum(float(r.get('total_cost', 0)) for r in payroll)

    # 올해 연차 현황
    current_year = today.year
    all_leave = db.query_annual_leave(year=current_year)
    total_granted = sum(float(r.get('granted_days', 0)) for r in all_leave)
    total_used = sum(float(r.get('used_days', 0)) for r in all_leave)

    return {
        'active_count': len(active),
        'total_count': len(employees),
        'total_payroll': total_payroll,
        'current_month': current_month,
        'leave_summary': {
            'granted': total_granted,
            'used': total_used,
            'remaining': total_granted - total_used,
        },
    }


def get_leave_calendar(db, year, month):
    """월별 연차 사용 현황 (달력형 데이터).

    Args:
        db: SupabaseDB instance
        year: 연도 (int)
        month: 월 (int)

    Returns:
        list of dict: [{date, employee_id, employee_name, days, leave_type, memo}, ...]
    """
    # 해당 월의 모든 leave_records 조회
    records = db.query_leave_records(year=year)

    # 직원 이름 매핑
    employees = db.query_employees()
    emp_map = {e['id']: e.get('name', '') for e in employees}

    # 해당 월 필터링
    month_str = f"{year}-{month:02d}"
    result = []
    for r in records:
        leave_date = r.get('leave_date', '')
        if not leave_date or not leave_date.startswith(month_str):
            continue
        result.append({
            'date': leave_date,
            'employee_id': r.get('employee_id'),
            'employee_name': emp_map.get(r.get('employee_id'), ''),
            'days': r.get('days', 1),
            'leave_type': r.get('leave_type', '연차'),
            'memo': r.get('memo', ''),
        })

    # 날짜순 정렬
    result.sort(key=lambda x: x['date'])
    return result


# ══════════════════════════════════════════════════════
#  한국 급여 계산 엔진
# ══════════════════════════════════════════════════════

# 급여 항목 type → 키 매핑
COMPONENT_FIELD_MAP = {
    'base_salary': 'base_salary',
    'position_allowance': 'position_allowance',
    'responsibility_allowance': 'responsibility_allowance',
    'longevity_allowance': 'longevity_allowance',
    'meal_allowance': 'meal_allowance',
    'vehicle_allowance': 'vehicle_allowance',
    'overtime_pay': 'overtime_pay',
    'night_pay': 'night_pay',
    'holiday_pay': 'holiday_pay',
    'bonus': 'bonus',
    'other_allowance': 'other_allowance',
}

# 비과세 대상 항목과 해당 limit_type 매핑
NONTAXABLE_COMPONENT_MAP = {
    'meal_allowance': 'meal_allowance',
    'vehicle_allowance': 'vehicle_allowance',
    'childcare_allowance': 'childcare',
}

# 기본 비과세 한도 (DB에 없을 때 폴백, 원/월)
DEFAULT_NONTAXABLE_LIMITS = {
    'meal_allowance': 200000,
    'vehicle_allowance': 200000,
    'childcare': 200000,
}


def calculate_payroll(employee, salary_components, rate_map, nontax_map=None,
                      insurance_overrides=None):
    """직원 1인의 월 급여를 전체 계산.

    Args:
        employee: dict - 직원 정보 (base_salary, dependents_count 등)
        salary_components: list of dict - salary_components 테이블 레코드
        rate_map: dict - {insurance_type: {employee_rate, employer_rate, ...}}
        nontax_map: dict - {limit_type: monthly_limit} 비과세 한도
        insurance_overrides: list of dict - 개인별 보험요율 오버라이드
            [{insurance_type, employee_rate, employer_rate}, ...]

    Returns:
        dict - 급여 계산 결과 (모든 항목 포함)
    """
    # 개인별 오버라이드가 있으면 rate_map에 병합 (개인 우선)
    if insurance_overrides:
        rate_map = dict(rate_map)  # 원본 수정 방지
        for ov in insurance_overrides:
            ins_type = ov.get('insurance_type', '')
            if ins_type:
                merged = dict(rate_map.get(ins_type, {}))
                if ov.get('employee_rate') is not None:
                    merged['employee_rate'] = ov['employee_rate']
                if ov.get('employer_rate') is not None:
                    merged['employer_rate'] = ov['employer_rate']
                rate_map[ins_type] = merged
    if nontax_map is None:
        nontax_map = {}

    # ── 1. 급여 항목 집계 ──
    amounts = {field: 0 for field in COMPONENT_FIELD_MAP.values()}
    other_detail = {}

    # base_salary는 employee 테이블의 값을 기본으로
    amounts['base_salary'] = int(float(employee.get('base_salary', 0)))

    for comp in salary_components:
        ctype = comp.get('component_type', '')
        amount = int(float(comp.get('amount', 0)))

        if ctype == 'base_salary':
            # salary_components에 기본급이 있으면 그 값 사용
            amounts['base_salary'] = amount
        elif ctype in COMPONENT_FIELD_MAP:
            amounts[COMPONENT_FIELD_MAP[ctype]] = amount
        elif ctype == 'childcare_allowance':
            # childcare_allowance는 other_allowance에 포함
            amounts['other_allowance'] += amount
            other_detail['보육수당'] = amount
        else:
            # 기타 항목
            name = comp.get('component_name', ctype)
            amounts['other_allowance'] += amount
            other_detail[name] = amount

    # ── 2. 총 지급액 계산 ──
    gross_salary = sum(amounts.values())

    # ── 3. 비과세액 계산 ──
    nontaxable_amount = 0

    for comp in salary_components:
        ctype = comp.get('component_type', '')
        amount = int(float(comp.get('amount', 0)))
        is_taxable = comp.get('is_taxable', True)

        if not is_taxable:
            # 명시적으로 비과세 설정된 항목
            nontaxable_amount += amount
        elif ctype in NONTAXABLE_COMPONENT_MAP:
            # 비과세 한도 적용 대상
            limit_key = NONTAXABLE_COMPONENT_MAP[ctype]
            limit_amount = nontax_map.get(
                limit_key, DEFAULT_NONTAXABLE_LIMITS.get(limit_key, 0)
            )
            nontaxable_amount += min(amount, limit_amount)

    # 직원이 비과세 면제 대상인 경우
    if employee.get('is_tax_exempt'):
        nontaxable_amount = gross_salary

    # ── 4. 과세 대상액 ──
    taxable_amount = max(gross_salary - nontaxable_amount, 0)

    # ── 5. 4대보험 공제 계산 (근로자 부담분) ──
    np_result = _calc_national_pension(taxable_amount, rate_map)
    hi_result = _calc_health_insurance(taxable_amount, rate_map)
    ltc_result = _calc_long_term_care(hi_result['employee'], rate_map)
    ei_result = _calc_employment_insurance(taxable_amount, rate_map)
    ia_result = _calc_industrial_accident(taxable_amount, rate_map)

    national_pension = np_result['employee']
    health_insurance = hi_result['employee']
    long_term_care = ltc_result['employee']
    employment_insurance = ei_result['employee']

    # ── 6. 소득세/지방소득세 계산 ──
    dependents = int(employee.get('dependents_count', 1))
    # 과세 대상에서 4대보험 공제 후 기준으로 간이세액 계산
    income_tax = calculate_income_tax(taxable_amount, dependents)
    local_income_tax = _round_down_ten(income_tax * 0.1)

    # ── 7. 총 공제액 ──
    total_deductions = (
        national_pension + health_insurance + long_term_care +
        employment_insurance + income_tax + local_income_tax
    )

    # ── 8. 실수령액 ──
    net_salary = gross_salary - total_deductions

    # ── 9. 사업주 부담분 ──
    np_employer = np_result['employer']
    hi_employer = hi_result['employer']
    ltc_employer = ltc_result['employer']
    ei_employer = ei_result['employer']
    ia_employer = ia_result['employer']

    total_employer_cost = (
        np_employer + hi_employer + ltc_employer +
        ei_employer + ia_employer
    )

    # ── 10. 총 수당 합계 (기본급 제외) ──
    total_allowances = gross_salary - amounts['base_salary']

    return {
        # 지급 항목
        'base_salary': amounts['base_salary'],
        'position_allowance': amounts['position_allowance'],
        'responsibility_allowance': amounts['responsibility_allowance'],
        'longevity_allowance': amounts['longevity_allowance'],
        'meal_allowance': amounts['meal_allowance'],
        'vehicle_allowance': amounts['vehicle_allowance'],
        'overtime_pay': amounts['overtime_pay'],
        'night_pay': amounts['night_pay'],
        'holiday_pay': amounts['holiday_pay'],
        'bonus': amounts['bonus'],
        'other_allowance': amounts['other_allowance'],
        'other_allowance_detail': other_detail,
        'total_allowances': total_allowances,

        # 총액
        'gross_salary': gross_salary,
        'taxable_amount': taxable_amount,
        'nontaxable_amount': nontaxable_amount,

        # 4대보험 근로자 공제
        'national_pension': national_pension,
        'health_insurance': health_insurance,
        'long_term_care': long_term_care,
        'employment_insurance': employment_insurance,

        # 세금 공제
        'income_tax': income_tax,
        'local_income_tax': local_income_tax,

        # 합계
        'total_deductions': total_deductions,
        'net_salary': net_salary,

        # 사업주 부담분
        'national_pension_employer': np_employer,
        'health_insurance_employer': hi_employer,
        'long_term_care_employer': ltc_employer,
        'employment_insurance_employer': ei_employer,
        'industrial_accident_insurance': ia_employer,
        'total_employer_cost': total_employer_cost,
    }


def _round_down_ten(value):
    """10원 미만 절사 (한국 보험료/세금 관행)."""
    return int(value // 10) * 10


def _calc_national_pension(taxable_amount, rate_map):
    """국민연금 계산.
    - 기준소득월액 상/하한 적용
    - 근로자 4.5%, 사업주 4.5%
    """
    rate_info = rate_map.get('national_pension', {})
    emp_rate = float(rate_info.get('employee_rate', 4.5)) / 100
    er_rate = float(rate_info.get('employer_rate', 4.5)) / 100
    min_base = int(rate_info.get('min_base', 390000))
    max_base = int(rate_info.get('max_base', 6170000))

    # 기준소득월액 상/하한 적용
    base = taxable_amount
    if min_base > 0 and base < min_base:
        base = min_base
    if max_base > 0 and base > max_base:
        base = max_base

    employee = _round_down_ten(base * emp_rate)
    employer = _round_down_ten(base * er_rate)

    return {'employee': employee, 'employer': employer}


def _calc_health_insurance(taxable_amount, rate_map):
    """건강보험 계산.
    - 보수월액 기준
    - 근로자 3.545%, 사업주 3.545%
    """
    rate_info = rate_map.get('health_insurance', {})
    emp_rate = float(rate_info.get('employee_rate', 3.545)) / 100
    er_rate = float(rate_info.get('employer_rate', 3.545)) / 100

    employee = _round_down_ten(taxable_amount * emp_rate)
    employer = _round_down_ten(taxable_amount * er_rate)

    return {'employee': employee, 'employer': employer}


def _calc_long_term_care(health_insurance_amount, rate_map):
    """장기요양보험 계산.
    - 건강보험료의 12.95% (노사 각 부담)
    """
    rate_info = rate_map.get('long_term_care', {})
    emp_rate = float(rate_info.get('employee_rate', 12.95)) / 100
    er_rate = float(rate_info.get('employer_rate', 12.95)) / 100

    employee = _round_down_ten(health_insurance_amount * emp_rate)
    employer = _round_down_ten(health_insurance_amount * er_rate)

    return {'employee': employee, 'employer': employer}


def _calc_employment_insurance(taxable_amount, rate_map):
    """고용보험 계산.
    - 근로자 0.9%, 사업주 0.9% (실업급여)
    - 사업주는 추가로 고용안정/직업능력개발 부담 있으나 여기선 기본만
    """
    rate_info = rate_map.get('employment_insurance', {})
    emp_rate = float(rate_info.get('employee_rate', 0.9)) / 100
    er_rate = float(rate_info.get('employer_rate', 0.9)) / 100

    employee = _round_down_ten(taxable_amount * emp_rate)
    employer = _round_down_ten(taxable_amount * er_rate)

    return {'employee': employee, 'employer': employer}


def _calc_industrial_accident(taxable_amount, rate_map):
    """산재보험 계산.
    - 전액 사업주 부담
    - 업종별 상이, 평균요율 약 1.47%
    """
    rate_info = rate_map.get('industrial_accident', {})
    er_rate = float(rate_info.get('employer_rate', 1.47)) / 100

    employer = _round_down_ten(taxable_amount * er_rate)

    return {'employee': 0, 'employer': employer}


def calculate_income_tax(taxable_monthly, dependents_count=1):
    """간이세액표 기반 근로소득세 추정 계산.

    실제 간이세액표는 매우 세분화되어 있어, 여기서는
    월급여 기준 근사 계산을 사용합니다.

    한국 근로소득세 간이세액 계산 로직:
    1. 월 과세급여에서 근로소득공제 적용
    2. 인적공제 적용
    3. 세율 적용 (6%~45% 누진)

    Args:
        taxable_monthly: 월 과세급여 (원)
        dependents_count: 부양가족 수 (본인 포함, 기본 1)

    Returns:
        int: 근로소득세 (원, 10원 미만 절사)
    """
    if taxable_monthly <= 0:
        return 0

    # 부양가족 수 최소 1 (본인)
    dependents = max(dependents_count, 1)

    # ── 연간 환산 ──
    annual_salary = taxable_monthly * 12

    # ── 근로소득공제 (2025년 기준) ──
    if annual_salary <= 5_000_000:
        deduction = annual_salary * 0.70
    elif annual_salary <= 15_000_000:
        deduction = 3_500_000 + (annual_salary - 5_000_000) * 0.40
    elif annual_salary <= 45_000_000:
        deduction = 7_500_000 + (annual_salary - 15_000_000) * 0.15
    elif annual_salary <= 100_000_000:
        deduction = 12_000_000 + (annual_salary - 45_000_000) * 0.05
    else:
        deduction = 14_750_000 + (annual_salary - 100_000_000) * 0.02

    earned_income = annual_salary - deduction

    # ── 인적공제 (1인당 150만원) ──
    personal_deduction = dependents * 1_500_000

    # ── 국민연금/건강보험료 공제 (근사치) ──
    # 실제로는 연말정산 시 공제하지만 간이세액에서는 이미 반영
    insurance_deduction = annual_salary * 0.089  # 약 8.9% (4대보험 근로자분 합계 근사)

    # ── 과세표준 ──
    taxable_base = earned_income - personal_deduction - insurance_deduction
    # 근로소득세액공제를 위한 표준세액공제 130,000원
    standard_deduction = 130_000

    if taxable_base <= 0:
        return 0

    # ── 기본세율 적용 (2025년 기준 소득세율) ──
    if taxable_base <= 14_000_000:
        tax = taxable_base * 0.06
    elif taxable_base <= 50_000_000:
        tax = 840_000 + (taxable_base - 14_000_000) * 0.15
    elif taxable_base <= 88_000_000:
        tax = 6_240_000 + (taxable_base - 50_000_000) * 0.24
    elif taxable_base <= 150_000_000:
        tax = 15_360_000 + (taxable_base - 88_000_000) * 0.35
    elif taxable_base <= 300_000_000:
        tax = 37_060_000 + (taxable_base - 150_000_000) * 0.38
    elif taxable_base <= 500_000_000:
        tax = 94_060_000 + (taxable_base - 300_000_000) * 0.40
    elif taxable_base <= 1_000_000_000:
        tax = 174_060_000 + (taxable_base - 500_000_000) * 0.42
    else:
        tax = 384_060_000 + (taxable_base - 1_000_000_000) * 0.45

    # ── 근로소득세액공제 ──
    if tax <= 1_300_000:
        tax_credit = tax * 0.55
    else:
        tax_credit = 715_000 + (tax - 1_300_000) * 0.30

    # 세액공제 한도
    if annual_salary <= 33_000_000:
        max_credit = 740_000
    elif annual_salary <= 70_000_000:
        max_credit = 660_000
    else:
        max_credit = 500_000

    tax_credit = min(tax_credit, max_credit)

    # 표준세액공제
    tax -= tax_credit
    tax -= standard_deduction

    if tax <= 0:
        return 0

    # 월할
    monthly_tax = tax / 12

    return _round_down_ten(monthly_tax)
