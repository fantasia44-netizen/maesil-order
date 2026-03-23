"""
db/hr_repo.py — 인사/급여/연차 DB Repository.

db_supabase.py에서 분리 (2026-03-23).
메서드 20개.
"""
from .base import BaseRepo


class HrRepo(BaseRepo):
    """인사/급여/연차 DB Repository."""

    def query_employees(self, status=None):
        """직원 목록 조회. status='재직'/'퇴직' 필터 가능."""
        try:
            q = self.client.table("employees").select("*").or_("is_deleted.is.null,is_deleted.eq.false")
            if status:
                q = q.eq("status", status)
            q = q.order("name")
            res = q.execute()
            return res.data or []
        except Exception as e:
            print(f"[DB] query_employees error: {e}")
            return []


    def insert_employee(self, data):
        """직원 1명 등록."""
        res = self.client.table("employees").insert(data).execute()
        return res.data[0] if res.data else None


    def update_employee(self, emp_id, data, biz_id=None):
        """직원 정보 수정."""
        q = self.client.table("employees").update(data).eq("id", int(emp_id))
        res = self._with_biz(q, biz_id).execute()
        return res.data[0] if res.data else None


    def delete_employee(self, emp_id, biz_id=None):
        """직원 소프트 삭제."""
        q = self.client.table("employees").update(
            {"is_deleted": True}
        ).eq("id", int(emp_id))
        self._with_biz(q, biz_id).execute()

    # ── payroll_monthly (급여 관리) ──


    def query_payroll(self, pay_month=None):
        """급여 목록 조회. pay_month='2026-03' 필터."""
        try:
            q = self.client.table("payroll_monthly").select("*")
            if pay_month:
                q = q.eq("pay_month", pay_month)
            q = q.order("employee_id")
            res = q.execute()
            return res.data or []
        except Exception as e:
            print(f"[DB] query_payroll error: {e}")
            return []


    def insert_payroll(self, data):
        """급여 1건 등록."""
        res = self.client.table("payroll_monthly").insert(data).execute()
        return res.data[0] if res.data else None


    def update_payroll(self, payroll_id, data, biz_id=None):
        """급여 1건 수정."""
        q = self.client.table("payroll_monthly").update(data).eq("id", int(payroll_id))
        res = self._with_biz(q, biz_id).execute()
        return res.data[0] if res.data else None


    def generate_monthly_payroll(self, pay_month):
        """재직 직원의 기본급으로 월 급여 자동 생성.
        이미 해당 월에 급여가 있는 직원은 건너뜀.
        Returns: 생성 건수.
        """
        employees = self.query_employees(status='재직')
        if not employees:
            return 0

        existing = self.query_payroll(pay_month=pay_month)
        existing_emp_ids = {r.get('employee_id') for r in existing}

        inserted = 0
        for emp in employees:
            emp_id = emp.get('id')
            if emp_id in existing_emp_ids:
                continue
            base = float(emp.get('base_salary', 0))
            payload = {
                'employee_id': emp_id,
                'pay_month': pay_month,
                'base_salary': base,
                'allowances': 0,
                'total_cost': base,
                'memo': '',
            }
            self.insert_payroll(payload)
            inserted += 1

        return inserted


    def query_annual_leave(self, employee_id=None, year=None):
        """연차 현황 조회."""
        try:
            q = self.client.table("annual_leave").select("*")
            if employee_id:
                q = q.eq("employee_id", int(employee_id))
            if year:
                q = q.eq("leave_year", int(year))
            res = q.execute()
            return res.data or []
        except Exception as e:
            print(f"[DB] query_annual_leave error: {e}")
            return []


    def update_annual_leave(self, employee_id, year, data):
        """연차 현황 upsert (없으면 생성, 있으면 수정)."""
        existing = self.query_annual_leave(
            employee_id=employee_id, year=year
        )
        if existing:
            res = self.client.table("annual_leave").update(data).eq(
                "employee_id", int(employee_id)
            ).eq("leave_year", int(year)).execute()
            return res.data[0] if res.data else None
        else:
            data['employee_id'] = int(employee_id)
            data['leave_year'] = int(year)
            res = self.client.table("annual_leave").insert(data).execute()
            return res.data[0] if res.data else None


    def insert_leave_record(self, data):
        """연차 사용 기록 등록 + used_days 자동 업데이트."""
        res = self.client.table("leave_records").insert(data).execute()
        record = res.data[0] if res.data else None

        if record:
            # used_days 자동 업데이트
            emp_id = data.get('employee_id')
            leave_date = data.get('leave_date', '')
            year = int(leave_date[:4]) if leave_date else None
            days = float(data.get('days', 1))
            if emp_id and year:
                al = self.query_annual_leave(employee_id=emp_id, year=year)
                if al:
                    new_used = float(al[0].get('used_days', 0)) + days
                    self.update_annual_leave(emp_id, year, {
                        'used_days': new_used,
                    })
                else:
                    # annual_leave 레코드가 없으면 생성
                    self.update_annual_leave(emp_id, year, {
                        'granted_days': 0,
                        'used_days': days,
                    })

        return record


    def query_leave_records(self, employee_id=None, year=None):
        """연차 사용 기록 조회."""
        try:
            q = self.client.table("leave_records").select("*")
            if employee_id:
                q = q.eq("employee_id", int(employee_id))
            if year:
                date_from = f"{year}-01-01"
                date_to = f"{year}-12-31"
                q = q.gte("leave_date", date_from).lte("leave_date", date_to)
            q = q.order("leave_date", desc=True)
            res = q.execute()
            return res.data or []
        except Exception as e:
            print(f"[DB] query_leave_records error: {e}")
            return []

    @staticmethod

    def calculate_legal_leave_days(hire_date_str):
        """입사일 기반 법정 연차일수 계산.
        - 1년 미만: 매월 1일씩 (최대 11일)
        - 1년 이상: 15일 + 2년마다 1일 추가 (최대 25일)

        Args:
            hire_date_str: 'YYYY-MM-DD' 형식 입사일

        Returns:
            int: 법정 연차일수
        """
        from datetime import date
        if not hire_date_str:
            return 0
        try:
            hire = date.fromisoformat(str(hire_date_str)[:10])
        except (ValueError, TypeError):
            return 0

        today = date.today()
        delta = today - hire
        total_months = (today.year - hire.year) * 12 + (today.month - hire.month)

        if delta.days < 365:
            # 1년 미만: 매월 1일씩, 최대 11일
            return min(total_months, 11)
        else:
            # 1년 이상: 15일 기본
            years_worked = delta.days // 365
            # 2년마다 1일 추가 (최대 25일)
            extra = (years_worked - 1) // 2
            return min(15 + extra, 25)

    # ── salary_components (급여 항목 관리) ──


    def query_employee_insurance_overrides(self, employee_id):
        """직원의 개인별 보험요율 오버라이드 조회.
        Returns: list of dict [{insurance_type, employee_rate, employer_rate, notes}, ...]
        """
        try:
            res = self.client.table("employee_insurance_overrides") \
                .select("*").eq("employee_id", employee_id).or_("is_deleted.is.null,is_deleted.eq.false").execute()
            return res.data or []
        except Exception as e:
            print(f"[DB] query_employee_insurance_overrides error: {e}")
            return []


    def upsert_employee_insurance_override(self, employee_id, insurance_type,
                                            employee_rate, employer_rate, notes=''):
        """직원 보험요율 오버라이드 설정 (upsert).
        기존 레코드가 있으면 업데이트, 없으면 생성.
        """
        try:
            # 기존 레코드 확인
            res = self.client.table("employee_insurance_overrides") \
                .select("id").eq("employee_id", employee_id) \
                .eq("insurance_type", insurance_type).execute()
            payload = {
                'employee_id': employee_id,
                'insurance_type': insurance_type,
                'employee_rate': float(employee_rate),
                'employer_rate': float(employer_rate),
                'notes': notes,
            }
            if res.data:
                self.client.table("employee_insurance_overrides") \
                    .update(payload).eq("id", res.data[0]['id']).execute()
            else:
                self.client.table("employee_insurance_overrides") \
                    .insert(payload).execute()
        except Exception as e:
            print(f"[DB] upsert_employee_insurance_override error: {e}")
            raise


    def delete_employee_insurance_override(self, employee_id, insurance_type):
        """직원 보험요율 오버라이드 소프트 삭제 (기본값으로 복원)."""
        try:
            self.client.table("employee_insurance_overrides") \
                .update({"is_deleted": True}) \
                .eq("employee_id", employee_id) \
                .eq("insurance_type", insurance_type).execute()
        except Exception as e:
            print(f"[DB] delete_employee_insurance_override error: {e}")

    # ── enhanced payroll generation ──


    def generate_monthly_payroll_v2(self, pay_month):
        """한국 급여체계 기반 월 급여 자동 생성.
        각 직원의 salary_components, insurance_rates 기반 자동 계산.
        hire_date/retire_date 기반 대상자 필터 + 일할계산 + 근태차감.
        이미 해당 월에 급여가 있는 직원은 재계산하여 UPDATE.

        Args:
            pay_month: 'YYYY-MM' 형식 대상 월

        Returns:
            dict: {inserted: 신규건수, updated: 갱신건수, skipped: 스킵건수}
        """
        from services.hr_service import (
            calculate_payroll, calculate_proration_ratio,
            calculate_attendance_deductions
        )
        from datetime import datetime, timezone
        import calendar as cal_mod

        year = int(pay_month[:4])
        month = int(pay_month[5:7])
        cal_days = cal_mod.monthrange(year, month)[1]
        month_start = f'{year}-{month:02d}-01'
        month_end = f'{year}-{month:02d}-{cal_days:02d}'

        # 해당 월 재직 대상자: hire_date <= 월말 AND (retire_date IS NULL OR retire_date >= 월초)
        all_employees = self.query_employees()
        eligible = []
        for emp in all_employees:
            status = emp.get('status', '')
            hire = emp.get('hire_date', '')
            retire = emp.get('retire_date') or ''

            # 재직 또는 퇴사자 중 해당 월에 근무한 직원
            if status not in ('재직', '퇴사', '퇴직'):
                continue
            if not hire:
                if status == '재직':
                    eligible.append(emp)
                continue
            if hire > month_end:
                continue  # 아직 입사 전
            if retire and retire < month_start:
                continue  # 이미 퇴사
            eligible.append(emp)

        if not eligible:
            return {'inserted': 0, 'updated': 0, 'skipped': 0}

        existing = self.query_payroll(pay_month=pay_month)
        existing_map = {r.get('employee_id'): r for r in existing}

        # 보험 요율 조회
        insurance_rates = self.query_insurance_rates(year=year)
        rate_map = {r['insurance_type']: r for r in insurance_rates}

        # 비과세 한도 조회
        nontax_limits = self.query_nontaxable_limits(year=year)
        nontax_map = {r['limit_type']: r['monthly_limit'] for r in nontax_limits}

        inserted = 0
        updated = 0
        skipped = 0
        for emp in eligible:
            emp_id = emp.get('id')

            # 일할비율 계산
            proration = calculate_proration_ratio(
                emp.get('hire_date'), emp.get('retire_date'), year, month)
            if proration['ratio'] <= 0:
                skipped += 1
                continue

            # 근태 차감 계산 (결근/조퇴/무급/지각)
            leave_recs = self._query_leave_records_for_month(emp_id, year, month)
            att_result = calculate_attendance_deductions(
                leave_recs,
                int(float(emp.get('base_salary', 0))) * proration['ratio'],
                proration['calendar_days'])

            # 직원의 급여 항목 조회
            components = self.query_salary_components(emp_id, active_only=True)

            # 개인별 보험요율 오버라이드 조회
            overrides = self.query_employee_insurance_overrides(emp_id)

            # 급여 계산 (일할 + 근태차감 반영)
            result = calculate_payroll(
                emp, components, rate_map, nontax_map,
                insurance_overrides=overrides,
                proration_ratio=proration['ratio'],
                attendance_deduction=att_result['total_deduction'],
                attendance_detail=att_result['detail'],
                proration_days=proration['work_days'],
                calendar_days=proration['calendar_days'])

            payroll_data = {
                'base_salary': result['base_salary'],
                'allowances': result['total_allowances'],
                'total_cost': result['gross_salary'],
                'position_allowance': result['position_allowance'],
                'responsibility_allowance': result['responsibility_allowance'],
                'longevity_allowance': result['longevity_allowance'],
                'meal_allowance': result['meal_allowance'],
                'vehicle_allowance': result['vehicle_allowance'],
                'overtime_pay': result['overtime_pay'],
                'night_pay': result['night_pay'],
                'holiday_pay': result['holiday_pay'],
                'bonus': result['bonus'],
                'other_allowance': result['other_allowance'],
                'other_allowance_detail': result.get('other_allowance_detail', {}),
                'gross_salary': result['gross_salary'],
                'taxable_amount': result['taxable_amount'],
                'nontaxable_amount': result['nontaxable_amount'],
                'national_pension': result['national_pension'],
                'health_insurance': result['health_insurance'],
                'long_term_care': result['long_term_care'],
                'employment_insurance': result['employment_insurance'],
                'income_tax': result['income_tax'],
                'local_income_tax': result['local_income_tax'],
                'total_deductions': result['total_deductions'],
                'net_salary': result['net_salary'],
                'national_pension_employer': result['national_pension_employer'],
                'health_insurance_employer': result['health_insurance_employer'],
                'long_term_care_employer': result['long_term_care_employer'],
                'employment_insurance_employer': result['employment_insurance_employer'],
                'industrial_accident_insurance': result['industrial_accident_insurance'],
                'total_employer_cost': result['total_employer_cost'],
                'proration_ratio': result['proration_ratio'],
                'proration_days': result['proration_days'],
                'calendar_days': result['calendar_days'],
                'attendance_deduction': result['attendance_deduction'],
                'attendance_detail': result['attendance_detail'],
            }

            existing_payroll = existing_map.get(emp_id)
            if existing_payroll:
                # 기존 레코드가 confirmed가 아니면 재계산 UPDATE
                if existing_payroll.get('status') != 'confirmed':
                    payroll_data['updated_at'] = datetime.now(timezone.utc).isoformat()
                    self.update_payroll(existing_payroll['id'], payroll_data)
                    updated += 1
                else:
                    skipped += 1
            else:
                # 신규 레코드 INSERT
                payroll_data['employee_id'] = emp_id
                payroll_data['pay_month'] = pay_month
                payroll_data['status'] = 'draft'
                payroll_data['memo'] = ''
                self.insert_payroll(payroll_data)
                inserted += 1

        # 급여 → expenses 자동 동기화
        if inserted > 0 or updated > 0:
            try:
                self.sync_payroll_to_expenses(pay_month)
            except Exception:
                pass  # 동기화 실패해도 급여 생성 결과는 유지

        return {'inserted': inserted, 'updated': updated, 'skipped': skipped}


    def _query_leave_records_for_month(self, employee_id, year, month):
        """특정 직원의 해당 월 leave_records 조회 (결근/조퇴/무급/지각 포함)."""
        month_start = f'{year}-{month:02d}-01'
        import calendar as cal_mod
        cal_days = cal_mod.monthrange(year, month)[1]
        month_end = f'{year}-{month:02d}-{cal_days:02d}'
        try:
            res = self.client.table('leave_records') \
                .select('*') \
                .eq('employee_id', employee_id) \
                .gte('leave_date', month_start) \
                .lte('leave_date', month_end) \
                .execute()
            return res.data or []
        except Exception:
            return []


    def generate_bulk_payroll(self, from_month, to_month):
        """여러 월 급여 일괄 생성.

        Args:
            from_month: 시작월 'YYYY-MM'
            to_month: 종료월 'YYYY-MM'

        Returns:
            dict: {months: [{month, inserted, updated, skipped}, ...], total_inserted, total_updated}
        """
        from datetime import date
        results = []
        total_ins = 0
        total_upd = 0

        # 월 목록 생성
        fy, fm = int(from_month[:4]), int(from_month[5:7])
        ty, tm = int(to_month[:4]), int(to_month[5:7])
        y, m = fy, fm
        while (y, m) <= (ty, tm):
            pay_month = f'{y}-{m:02d}'
            r = self.generate_monthly_payroll_v2(pay_month)
            results.append({
                'month': pay_month,
                'inserted': r['inserted'],
                'updated': r['updated'],
                'skipped': r.get('skipped', 0),
            })
            total_ins += r['inserted']
            total_upd += r['updated']
            m += 1
            if m > 12:
                m = 1
                y += 1

        return {
            'months': results,
            'total_inserted': total_ins,
            'total_updated': total_upd,
        }


    def recalculate_payroll(self, payroll_id):
        """기존 급여 1건 재계산 (급여 항목/보험 요율 변경 시).

        Args:
            payroll_id: payroll_monthly ID

        Returns:
            dict: 업데이트된 급여 레코드
        """
        from services.hr_service import calculate_payroll
        from datetime import datetime, timezone

        # 기존 급여 조회
        try:
            res = self.client.table("payroll_monthly").select("*").eq(
                "id", int(payroll_id)
            ).execute()
            if not res.data:
                return None
            payroll = res.data[0]
        except Exception:
            return None

        emp_id = payroll.get('employee_id')
        pay_month = payroll.get('pay_month', '')
        year = int(pay_month[:4]) if pay_month else 2025

        # 직원 정보
        employees = self.query_employees()
        emp = next((e for e in employees if e['id'] == emp_id), None)
        if not emp:
            return None

        # 급여 항목 & 요율
        components = self.query_salary_components(emp_id, active_only=True)
        insurance_rates = self.query_insurance_rates(year=year)
        rate_map = {r['insurance_type']: r for r in insurance_rates}
        nontax_limits = self.query_nontaxable_limits(year=year)
        nontax_map = {r['limit_type']: r['monthly_limit'] for r in nontax_limits}

        # 개인별 보험요율 오버라이드
        overrides = self.query_employee_insurance_overrides(emp_id)

        result = calculate_payroll(emp, components, rate_map, nontax_map,
                                   insurance_overrides=overrides)

        update_data = {
            'base_salary': result['base_salary'],
            'allowances': result['total_allowances'],
            'total_cost': result['gross_salary'],
            'position_allowance': result['position_allowance'],
            'responsibility_allowance': result['responsibility_allowance'],
            'longevity_allowance': result['longevity_allowance'],
            'meal_allowance': result['meal_allowance'],
            'vehicle_allowance': result['vehicle_allowance'],
            'overtime_pay': result['overtime_pay'],
            'night_pay': result['night_pay'],
            'holiday_pay': result['holiday_pay'],
            'bonus': result['bonus'],
            'other_allowance': result['other_allowance'],
            'other_allowance_detail': result.get('other_allowance_detail', {}),
            'gross_salary': result['gross_salary'],
            'taxable_amount': result['taxable_amount'],
            'nontaxable_amount': result['nontaxable_amount'],
            'national_pension': result['national_pension'],
            'health_insurance': result['health_insurance'],
            'long_term_care': result['long_term_care'],
            'employment_insurance': result['employment_insurance'],
            'income_tax': result['income_tax'],
            'local_income_tax': result['local_income_tax'],
            'total_deductions': result['total_deductions'],
            'net_salary': result['net_salary'],
            'national_pension_employer': result['national_pension_employer'],
            'health_insurance_employer': result['health_insurance_employer'],
            'long_term_care_employer': result['long_term_care_employer'],
            'employment_insurance_employer': result['employment_insurance_employer'],
            'industrial_accident_insurance': result['industrial_accident_insurance'],
            'total_employer_cost': result['total_employer_cost'],
            'updated_at': datetime.now(timezone.utc).isoformat(),
        }

        return self.update_payroll(payroll_id, update_data)

    # ══════════════════════════════════════════════════════════
    # 회계 ERP 메서드 (은행/세금계산서/매칭/정산)
    # ══════════════════════════════════════════════════════════

    # ── codef_connections ──


