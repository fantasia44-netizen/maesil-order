"""
hr_service.py -- 인건비/연차 관리 서비스 레이어.
직원 요약, 연차 달력 등 비즈니스 로직 제공.
"""
from datetime import date, timedelta
from collections import defaultdict


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
