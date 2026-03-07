"""
hr.py -- 인건비/연차 관리 Blueprint.
직원 관리, 급여 관리, 연차 관리.
직원/급여는 admin만, 연차는 admin+manager.
"""
from flask import (
    Blueprint, render_template, request, current_app,
    jsonify,
)
from flask_login import login_required, current_user

from auth import role_required, _log_action

hr_bp = Blueprint('hr', __name__, url_prefix='/hr')


# ══════════════════════════════════════════════
#  직원 관리
# ══════════════════════════════════════════════

@hr_bp.route('/employees')
@role_required('admin')
def employees():
    """직원 관리 메인 페이지"""
    return render_template('hr/employees.html')


@hr_bp.route('/api/employees')
@role_required('admin')
def api_employees():
    """직원 목록 JSON API"""
    db = current_app.db
    status = request.args.get('status', '')
    try:
        rows = db.query_employees(status=status or None)
        # 각 직원에 법정 연차일수 추가
        for r in rows:
            r['legal_leave_days'] = db.calculate_legal_leave_days(
                r.get('hire_date')
            )
        return jsonify({'success': True, 'employees': rows})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@hr_bp.route('/api/employees', methods=['POST'])
@role_required('admin')
def api_create_employee():
    """직원 등록"""
    db = current_app.db
    data = request.get_json()
    if not data:
        return jsonify({'error': '데이터가 없습니다.'}), 400

    name = (data.get('name') or '').strip()
    hire_date = (data.get('hire_date') or '').strip()

    if not name or not hire_date:
        return jsonify({'error': '이름과 입사일은 필수입니다.'}), 400

    try:
        base_salary = float(data.get('base_salary', 0))
    except (ValueError, TypeError):
        return jsonify({'error': '기본급이 올바르지 않습니다.'}), 400

    payload = {
        'name': name,
        'position': (data.get('position') or '').strip(),
        'department': (data.get('department') or '').strip(),
        'base_salary': base_salary,
        'hire_date': hire_date,
        'status': (data.get('status') or '재직').strip(),
        'memo': (data.get('memo') or '').strip(),
    }

    try:
        result = db.insert_employee(payload)
        _log_action('create_employee', target=name,
                     detail=f'입사일={hire_date}, 기본급={base_salary:,.0f}',
                     new_value=payload)
        return jsonify({'success': True, 'employee': result})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@hr_bp.route('/api/employees/<int:emp_id>', methods=['PUT'])
@role_required('admin')
def api_update_employee(emp_id):
    """직원 수정"""
    db = current_app.db
    data = request.get_json()
    if not data:
        return jsonify({'error': '데이터가 없습니다.'}), 400

    name = (data.get('name') or '').strip()
    hire_date = (data.get('hire_date') or '').strip()

    if not name or not hire_date:
        return jsonify({'error': '이름과 입사일은 필수입니다.'}), 400

    try:
        base_salary = float(data.get('base_salary', 0))
    except (ValueError, TypeError):
        return jsonify({'error': '기본급이 올바르지 않습니다.'}), 400

    payload = {
        'name': name,
        'position': (data.get('position') or '').strip(),
        'department': (data.get('department') or '').strip(),
        'base_salary': base_salary,
        'hire_date': hire_date,
        'status': (data.get('status') or '재직').strip(),
        'memo': (data.get('memo') or '').strip(),
    }

    try:
        result = db.update_employee(emp_id, payload)
        _log_action('update_employee', target=f'{name} (id={emp_id})',
                     detail=f'입사일={hire_date}, 기본급={base_salary:,.0f}',
                     new_value=payload)
        return jsonify({'success': True, 'employee': result})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@hr_bp.route('/api/employees/<int:emp_id>', methods=['DELETE'])
@role_required('admin')
def api_delete_employee(emp_id):
    """직원 삭제"""
    db = current_app.db
    try:
        db.delete_employee(emp_id)
        _log_action('delete_employee', target=f'id={emp_id}')
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ══════════════════════════════════════════════
#  급여 관리
# ══════════════════════════════════════════════

@hr_bp.route('/payroll')
@role_required('admin')
def payroll():
    """급여 관리 메인 페이지"""
    return render_template('hr/payroll.html')


@hr_bp.route('/api/payroll')
@role_required('admin')
def api_payroll():
    """급여 목록 JSON API"""
    db = current_app.db
    pay_month = request.args.get('pay_month', '')
    try:
        rows = db.query_payroll(pay_month=pay_month or None)

        # 직원 이름 매핑
        employees = db.query_employees()
        emp_map = {e['id']: e for e in employees}
        for r in rows:
            emp = emp_map.get(r.get('employee_id'), {})
            r['employee_name'] = emp.get('name', '')
            r['department'] = emp.get('department', '')
            r['position'] = emp.get('position', '')

        total_cost = sum(float(r.get('total_cost', 0)) for r in rows)

        return jsonify({
            'success': True,
            'payroll': rows,
            'total_cost': total_cost,
            'count': len(rows),
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@hr_bp.route('/api/payroll/<int:payroll_id>', methods=['PUT'])
@role_required('admin')
def api_update_payroll(payroll_id):
    """급여 1건 수정 (수당/메모 수정)"""
    db = current_app.db
    data = request.get_json()
    if not data:
        return jsonify({'error': '데이터가 없습니다.'}), 400

    try:
        base_salary = float(data.get('base_salary', 0))
        allowances = float(data.get('allowances', 0))
    except (ValueError, TypeError):
        return jsonify({'error': '금액이 올바르지 않습니다.'}), 400

    payload = {
        'base_salary': base_salary,
        'allowances': allowances,
        'total_cost': base_salary + allowances,
        'memo': (data.get('memo') or '').strip(),
    }

    try:
        result = db.update_payroll(payroll_id, payload)
        _log_action('update_payroll', target=f'id={payroll_id}',
                     detail=f'기본급={base_salary:,.0f}, 수당={allowances:,.0f}',
                     new_value=payload)
        return jsonify({'success': True, 'payroll': result})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@hr_bp.route('/api/payroll/generate', methods=['POST'])
@role_required('admin')
def api_generate_payroll():
    """월 급여 자동 생성"""
    db = current_app.db
    data = request.get_json() or {}
    pay_month = (data.get('pay_month') or '').strip()

    if not pay_month:
        return jsonify({'error': '대상 월을 지정해주세요.'}), 400

    try:
        count = db.generate_monthly_payroll(pay_month)
        _log_action('generate_payroll',
                     detail=f'대상월={pay_month}, 생성={count}건')
        return jsonify({'success': True, 'count': count})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@hr_bp.route('/api/payroll/sync-expenses', methods=['POST'])
@role_required('admin')
def api_sync_expenses():
    """급여 합계를 expenses에 인건비로 자동 반영"""
    db = current_app.db
    data = request.get_json() or {}
    pay_month = (data.get('pay_month') or '').strip()

    if not pay_month:
        return jsonify({'error': '대상 월을 지정해주세요.'}), 400

    try:
        result = db.sync_payroll_to_expenses(pay_month)
        _log_action('sync_payroll_to_expenses',
                     detail=f'대상월={pay_month}, 총액={result["total_cost"]:,.0f}, '
                            f'액션={result["action"]}')
        return jsonify({'success': True, **result})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ══════════════════════════════════════════════
#  연차 관리
# ══════════════════════════════════════════════

@hr_bp.route('/leave')
@role_required('admin', 'general')
def leave():
    """연차 관리 메인 페이지"""
    return render_template('hr/leave.html')


@hr_bp.route('/api/leave')
@role_required('admin', 'general')
def api_leave():
    """연차 현황 JSON API (직원별 연차 + 법정일수)"""
    db = current_app.db
    year = request.args.get('year', '')

    try:
        if not year:
            from datetime import date
            year = date.today().year

        year = int(year)
        employees = db.query_employees(status='재직')
        all_leave = db.query_annual_leave(year=year)

        # employee_id -> annual_leave 매핑
        leave_map = {r.get('employee_id'): r for r in all_leave}

        result = []
        for emp in employees:
            emp_id = emp['id']
            legal_days = db.calculate_legal_leave_days(emp.get('hire_date'))
            al = leave_map.get(emp_id, {})
            granted = float(al.get('granted_days', 0))
            used = float(al.get('used_days', 0))

            result.append({
                'employee_id': emp_id,
                'employee_name': emp.get('name', ''),
                'department': emp.get('department', ''),
                'hire_date': emp.get('hire_date', ''),
                'legal_days': legal_days,
                'granted_days': granted,
                'used_days': used,
                'remaining_days': granted - used,
            })

        return jsonify({'success': True, 'leave_data': result, 'year': year})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@hr_bp.route('/api/leave/grant', methods=['POST'])
@role_required('admin')
def api_grant_leave():
    """연차 부여일수 설정"""
    db = current_app.db
    data = request.get_json()
    if not data:
        return jsonify({'error': '데이터가 없습니다.'}), 400

    employee_id = data.get('employee_id')
    year = data.get('year')
    granted_days = data.get('granted_days', 0)

    if not employee_id or not year:
        return jsonify({'error': '직원과 연도는 필수입니다.'}), 400

    try:
        granted_days = float(granted_days)
    except (ValueError, TypeError):
        return jsonify({'error': '일수가 올바르지 않습니다.'}), 400

    try:
        result = db.update_annual_leave(employee_id, year, {
            'granted_days': granted_days,
        })
        _log_action('grant_leave',
                     target=f'employee_id={employee_id}',
                     detail=f'연도={year}, 부여={granted_days}일')
        return jsonify({'success': True, 'leave': result})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@hr_bp.route('/api/leave', methods=['POST'])
@role_required('admin', 'general')
def api_create_leave():
    """연차 사용 등록"""
    db = current_app.db
    data = request.get_json()
    if not data:
        return jsonify({'error': '데이터가 없습니다.'}), 400

    employee_id = data.get('employee_id')
    leave_date = (data.get('leave_date') or '').strip()

    if not employee_id or not leave_date:
        return jsonify({'error': '직원과 날짜는 필수입니다.'}), 400

    try:
        days = float(data.get('days', 1))
    except (ValueError, TypeError):
        days = 1

    payload = {
        'employee_id': int(employee_id),
        'leave_date': leave_date,
        'days': days,
        'leave_type': (data.get('leave_type') or '연차').strip(),
        'memo': (data.get('memo') or '').strip(),
    }

    try:
        result = db.insert_leave_record(payload)
        _log_action('create_leave_record',
                     target=f'employee_id={employee_id}',
                     detail=f'날짜={leave_date}, {days}일',
                     new_value=payload)
        return jsonify({'success': True, 'record': result})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@hr_bp.route('/api/leave/records')
@role_required('admin', 'general')
def api_leave_records():
    """연차 사용 기록 조회"""
    db = current_app.db
    employee_id = request.args.get('employee_id', '')
    year = request.args.get('year', '')

    try:
        rows = db.query_leave_records(
            employee_id=int(employee_id) if employee_id else None,
            year=int(year) if year else None,
        )

        # 직원 이름 매핑
        employees = db.query_employees()
        emp_map = {e['id']: e.get('name', '') for e in employees}
        for r in rows:
            r['employee_name'] = emp_map.get(r.get('employee_id'), '')

        return jsonify({'success': True, 'records': rows})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@hr_bp.route('/api/leave/calendar')
@role_required('admin', 'general')
def api_leave_calendar():
    """월별 연차 달력 데이터"""
    db = current_app.db
    year = request.args.get('year', '')
    month = request.args.get('month', '')

    if not year or not month:
        from datetime import date
        today = date.today()
        year = year or str(today.year)
        month = month or str(today.month)

    try:
        from services.hr_service import get_leave_calendar
        calendar_data = get_leave_calendar(db, int(year), int(month))
        return jsonify({
            'success': True,
            'calendar': calendar_data,
            'year': int(year),
            'month': int(month),
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
