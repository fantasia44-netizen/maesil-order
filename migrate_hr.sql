-- migrate_hr.sql -- 인건비/연차 관리용 테이블 생성
-- 실행: Supabase SQL Editor에서 실행

-- 직원 마스터
CREATE TABLE IF NOT EXISTS employees (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    position TEXT DEFAULT '',
    department TEXT DEFAULT '',
    base_salary NUMERIC DEFAULT 0,
    hire_date DATE NOT NULL,
    status TEXT DEFAULT '재직',
    memo TEXT DEFAULT '',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 월별 급여 (인건비)
CREATE TABLE IF NOT EXISTS payroll_monthly (
    id BIGSERIAL PRIMARY KEY,
    employee_id BIGINT REFERENCES employees(id),
    pay_month TEXT NOT NULL,
    base_salary NUMERIC DEFAULT 0,
    allowances NUMERIC DEFAULT 0,
    total_cost NUMERIC DEFAULT 0,
    memo TEXT DEFAULT '',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(employee_id, pay_month)
);

-- 연차 현황 (연도별)
CREATE TABLE IF NOT EXISTS annual_leave (
    id BIGSERIAL PRIMARY KEY,
    employee_id BIGINT REFERENCES employees(id),
    leave_year INTEGER NOT NULL,
    granted_days NUMERIC DEFAULT 0,
    used_days NUMERIC DEFAULT 0,
    memo TEXT DEFAULT '',
    UNIQUE(employee_id, leave_year)
);

-- 연차 사용 기록
CREATE TABLE IF NOT EXISTS leave_records (
    id BIGSERIAL PRIMARY KEY,
    employee_id BIGINT REFERENCES employees(id),
    leave_date DATE NOT NULL,
    days NUMERIC DEFAULT 1,
    leave_type TEXT DEFAULT '연차',
    memo TEXT DEFAULT '',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 인덱스
CREATE INDEX IF NOT EXISTS idx_payroll_month ON payroll_monthly(pay_month);
CREATE INDEX IF NOT EXISTS idx_leave_records_emp ON leave_records(employee_id);
CREATE INDEX IF NOT EXISTS idx_leave_records_date ON leave_records(leave_date);
CREATE INDEX IF NOT EXISTS idx_employees_status ON employees(status);
