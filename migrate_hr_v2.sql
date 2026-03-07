-- migrate_hr_v2.sql -- 한국 급여체계 상세화 마이그레이션
-- 실행: Supabase SQL Editor에서 실행
-- 기존 migrate_hr.sql의 테이블 위에 추가/변경
-- ====================================================

-- ══════════════════════════════════════════════════════
--  1. employees 테이블 확장
-- ══════════════════════════════════════════════════════

-- 주민등록번호 뒷자리 (성별/생년 확인용)
ALTER TABLE employees ADD COLUMN IF NOT EXISTS resident_number_back TEXT DEFAULT '';

-- 급여 입금 계좌
ALTER TABLE employees ADD COLUMN IF NOT EXISTS bank_name TEXT DEFAULT '';
ALTER TABLE employees ADD COLUMN IF NOT EXISTS bank_account TEXT DEFAULT '';

-- 부양가족 수 (소득세 계산에 필요, 본인 포함)
ALTER TABLE employees ADD COLUMN IF NOT EXISTS dependents_count INTEGER DEFAULT 1;

-- 비과세 적용 여부
ALTER TABLE employees ADD COLUMN IF NOT EXISTS is_tax_exempt BOOLEAN DEFAULT false;

-- ══════════════════════════════════════════════════════
--  2. salary_components (급여 항목 설정)
-- ══════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS salary_components (
    id BIGSERIAL PRIMARY KEY,
    employee_id BIGINT NOT NULL REFERENCES employees(id) ON DELETE CASCADE,
    component_type TEXT NOT NULL,
    -- 'base_salary'            : 기본급
    -- 'position_allowance'     : 직급수당
    -- 'responsibility_allowance': 직책수당
    -- 'longevity_allowance'    : 근속수당
    -- 'meal_allowance'         : 식대 (비과세 한도 200,000원/월)
    -- 'vehicle_allowance'      : 차량유지비 (비과세 한도 200,000원/월)
    -- 'childcare_allowance'    : 보육수당
    -- 'overtime_pay'           : 연장근로수당
    -- 'night_pay'              : 야간근로수당
    -- 'holiday_pay'            : 휴일근로수당
    -- 'bonus'                  : 상여금/성과급
    -- 'other_allowance'        : 기타수당
    component_name TEXT NOT NULL,          -- 표시명 (한글)
    amount INTEGER DEFAULT 0,             -- 금액 (원, 정수)
    is_taxable BOOLEAN DEFAULT true,      -- 과세 여부
    is_fixed BOOLEAN DEFAULT true,        -- 고정급 여부 (vs 변동급)
    effective_from DATE,                  -- 적용 시작일
    effective_to DATE,                    -- 적용 종료일 (NULL = 현재 적용 중)
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- 인덱스
CREATE INDEX IF NOT EXISTS idx_salary_comp_emp ON salary_components(employee_id);
CREATE INDEX IF NOT EXISTS idx_salary_comp_type ON salary_components(component_type);
CREATE INDEX IF NOT EXISTS idx_salary_comp_effective ON salary_components(effective_from, effective_to);

-- ══════════════════════════════════════════════════════
--  3. insurance_rates (4대보험 요율)
-- ══════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS insurance_rates (
    id BIGSERIAL PRIMARY KEY,
    year INTEGER NOT NULL,
    insurance_type TEXT NOT NULL,
    -- 'national_pension'      : 국민연금
    -- 'health_insurance'      : 건강보험
    -- 'long_term_care'        : 장기요양보험
    -- 'employment_insurance'  : 고용보험
    -- 'industrial_accident'   : 산재보험
    employee_rate NUMERIC(6, 3) NOT NULL,  -- 근로자 부담률 (%)
    employer_rate NUMERIC(6, 3) NOT NULL,  -- 사업주 부담률 (%)
    min_base INTEGER DEFAULT 0,            -- 최저 기준보수월액 (원)
    max_base INTEGER DEFAULT 0,            -- 최고 기준보수월액 (원)
    notes TEXT DEFAULT '',
    created_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(year, insurance_type)
);

-- 2025년 4대보험 요율 삽입
-- 국민연금: 근로자 4.5%, 사업주 4.5% (합계 9%)
--   기준소득월액 하한 390,000원, 상한 6,170,000원 (2025년 7월~ 상한 변경 가능)
INSERT INTO insurance_rates (year, insurance_type, employee_rate, employer_rate, min_base, max_base, notes) VALUES
(2025, 'national_pension', 4.5, 4.5, 390000, 6170000, '국민연금 (기준소득월액 상/하한 2025년 기준)')
ON CONFLICT (year, insurance_type) DO NOTHING;

-- 건강보험: 근로자 3.545%, 사업주 3.545% (합계 7.09%)
INSERT INTO insurance_rates (year, insurance_type, employee_rate, employer_rate, min_base, max_base, notes) VALUES
(2025, 'health_insurance', 3.545, 3.545, 0, 0, '건강보험료율 7.09% (노사 각 3.545%)')
ON CONFLICT (year, insurance_type) DO NOTHING;

-- 장기요양보험: 건강보험료의 12.95% (노사 각 부담)
INSERT INTO insurance_rates (year, insurance_type, employee_rate, employer_rate, min_base, max_base, notes) VALUES
(2025, 'long_term_care', 12.95, 12.95, 0, 0, '장기요양보험 (건강보험료의 12.95%, 노사 각 부담)')
ON CONFLICT (year, insurance_type) DO NOTHING;

-- 고용보험: 근로자 0.9%, 사업주 0.9%~1.15% (150인 미만 사업장 기준)
INSERT INTO insurance_rates (year, insurance_type, employee_rate, employer_rate, min_base, max_base, notes) VALUES
(2025, 'employment_insurance', 0.9, 0.9, 0, 0, '고용보험 실업급여 (150인 미만 사업장, 사업주는 고용안정/직업능력개발 별도)')
ON CONFLICT (year, insurance_type) DO NOTHING;

-- 산재보험: 전액 사업주 부담 (업종별 상이, 평균요율 약 1.47%)
INSERT INTO insurance_rates (year, insurance_type, employee_rate, employer_rate, min_base, max_base, notes) VALUES
(2025, 'industrial_accident', 0, 1.47, 0, 0, '산재보험 (전액 사업주 부담, 업종별 상이, 평균요율)')
ON CONFLICT (year, insurance_type) DO NOTHING;

-- 2026년 요율 (2025년과 동일하게 초기 설정, 변경 시 업데이트)
INSERT INTO insurance_rates (year, insurance_type, employee_rate, employer_rate, min_base, max_base, notes) VALUES
(2026, 'national_pension', 4.5, 4.5, 390000, 6170000, '국민연금 (2026년, 확정 후 수정 필요)')
ON CONFLICT (year, insurance_type) DO NOTHING;

INSERT INTO insurance_rates (year, insurance_type, employee_rate, employer_rate, min_base, max_base, notes) VALUES
(2026, 'health_insurance', 3.545, 3.545, 0, 0, '건강보험료율 (2026년, 확정 후 수정 필요)')
ON CONFLICT (year, insurance_type) DO NOTHING;

INSERT INTO insurance_rates (year, insurance_type, employee_rate, employer_rate, min_base, max_base, notes) VALUES
(2026, 'long_term_care', 12.95, 12.95, 0, 0, '장기요양보험 (2026년, 확정 후 수정 필요)')
ON CONFLICT (year, insurance_type) DO NOTHING;

INSERT INTO insurance_rates (year, insurance_type, employee_rate, employer_rate, min_base, max_base, notes) VALUES
(2026, 'employment_insurance', 0.9, 0.9, 0, 0, '고용보험 실업급여 (2026년, 확정 후 수정 필요)')
ON CONFLICT (year, insurance_type) DO NOTHING;

INSERT INTO insurance_rates (year, insurance_type, employee_rate, employer_rate, min_base, max_base, notes) VALUES
(2026, 'industrial_accident', 0, 1.47, 0, 0, '산재보험 (2026년, 확정 후 수정 필요)')
ON CONFLICT (year, insurance_type) DO NOTHING;

-- ══════════════════════════════════════════════════════
--  4. payroll_monthly 테이블 확장
-- ══════════════════════════════════════════════════════

-- 지급 항목 상세
ALTER TABLE payroll_monthly ADD COLUMN IF NOT EXISTS position_allowance INTEGER DEFAULT 0;
ALTER TABLE payroll_monthly ADD COLUMN IF NOT EXISTS responsibility_allowance INTEGER DEFAULT 0;
ALTER TABLE payroll_monthly ADD COLUMN IF NOT EXISTS longevity_allowance INTEGER DEFAULT 0;
ALTER TABLE payroll_monthly ADD COLUMN IF NOT EXISTS meal_allowance INTEGER DEFAULT 0;
ALTER TABLE payroll_monthly ADD COLUMN IF NOT EXISTS vehicle_allowance INTEGER DEFAULT 0;
ALTER TABLE payroll_monthly ADD COLUMN IF NOT EXISTS overtime_pay INTEGER DEFAULT 0;
ALTER TABLE payroll_monthly ADD COLUMN IF NOT EXISTS night_pay INTEGER DEFAULT 0;
ALTER TABLE payroll_monthly ADD COLUMN IF NOT EXISTS holiday_pay INTEGER DEFAULT 0;
ALTER TABLE payroll_monthly ADD COLUMN IF NOT EXISTS bonus INTEGER DEFAULT 0;
ALTER TABLE payroll_monthly ADD COLUMN IF NOT EXISTS other_allowance INTEGER DEFAULT 0;
ALTER TABLE payroll_monthly ADD COLUMN IF NOT EXISTS other_allowance_detail JSONB DEFAULT '{}';

-- 총액 및 과세 구분
ALTER TABLE payroll_monthly ADD COLUMN IF NOT EXISTS gross_salary INTEGER DEFAULT 0;
ALTER TABLE payroll_monthly ADD COLUMN IF NOT EXISTS taxable_amount INTEGER DEFAULT 0;
ALTER TABLE payroll_monthly ADD COLUMN IF NOT EXISTS nontaxable_amount INTEGER DEFAULT 0;

-- 4대보험 근로자 공제
ALTER TABLE payroll_monthly ADD COLUMN IF NOT EXISTS national_pension INTEGER DEFAULT 0;
ALTER TABLE payroll_monthly ADD COLUMN IF NOT EXISTS health_insurance INTEGER DEFAULT 0;
ALTER TABLE payroll_monthly ADD COLUMN IF NOT EXISTS long_term_care INTEGER DEFAULT 0;
ALTER TABLE payroll_monthly ADD COLUMN IF NOT EXISTS employment_insurance INTEGER DEFAULT 0;

-- 세금 공제
ALTER TABLE payroll_monthly ADD COLUMN IF NOT EXISTS income_tax INTEGER DEFAULT 0;
ALTER TABLE payroll_monthly ADD COLUMN IF NOT EXISTS local_income_tax INTEGER DEFAULT 0;

-- 총 공제액 및 실수령액
ALTER TABLE payroll_monthly ADD COLUMN IF NOT EXISTS total_deductions INTEGER DEFAULT 0;
ALTER TABLE payroll_monthly ADD COLUMN IF NOT EXISTS net_salary INTEGER DEFAULT 0;

-- 사업주 부담분
ALTER TABLE payroll_monthly ADD COLUMN IF NOT EXISTS national_pension_employer INTEGER DEFAULT 0;
ALTER TABLE payroll_monthly ADD COLUMN IF NOT EXISTS health_insurance_employer INTEGER DEFAULT 0;
ALTER TABLE payroll_monthly ADD COLUMN IF NOT EXISTS long_term_care_employer INTEGER DEFAULT 0;
ALTER TABLE payroll_monthly ADD COLUMN IF NOT EXISTS employment_insurance_employer INTEGER DEFAULT 0;
ALTER TABLE payroll_monthly ADD COLUMN IF NOT EXISTS industrial_accident_insurance INTEGER DEFAULT 0;
ALTER TABLE payroll_monthly ADD COLUMN IF NOT EXISTS total_employer_cost INTEGER DEFAULT 0;

-- 급여 상태 (draft/confirmed/paid)
ALTER TABLE payroll_monthly ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'draft';
ALTER TABLE payroll_monthly ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now();

-- ══════════════════════════════════════════════════════
--  5. 비과세 한도 설정 테이블
-- ══════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS nontaxable_limits (
    id BIGSERIAL PRIMARY KEY,
    year INTEGER NOT NULL,
    limit_type TEXT NOT NULL,
    -- 'meal_allowance'    : 식대 비과세 한도
    -- 'vehicle_allowance' : 차량유지비/자가운전보조금 비과세 한도
    -- 'childcare'         : 보육수당 비과세 한도
    monthly_limit INTEGER NOT NULL,        -- 월 비과세 한도 (원)
    notes TEXT DEFAULT '',
    UNIQUE(year, limit_type)
);

-- 2025년 비과세 한도
INSERT INTO nontaxable_limits (year, limit_type, monthly_limit, notes) VALUES
(2025, 'meal_allowance', 200000, '식대 비과세 한도 월 20만원 (2023년~ 적용)'),
(2025, 'vehicle_allowance', 200000, '자가운전보조금 비과세 한도 월 20만원'),
(2025, 'childcare', 200000, '6세 이하 자녀 보육수당 비과세 한도 월 20만원')
ON CONFLICT (year, limit_type) DO NOTHING;

-- 2026년 비과세 한도 (동일 기준)
INSERT INTO nontaxable_limits (year, limit_type, monthly_limit, notes) VALUES
(2026, 'meal_allowance', 200000, '식대 비과세 한도 월 20만원'),
(2026, 'vehicle_allowance', 200000, '자가운전보조금 비과세 한도 월 20만원'),
(2026, 'childcare', 200000, '보육수당 비과세 한도 월 20만원')
ON CONFLICT (year, limit_type) DO NOTHING;

-- ══════════════════════════════════════════════════════
--  6. 추가 인덱스
-- ══════════════════════════════════════════════════════

CREATE INDEX IF NOT EXISTS idx_insurance_rates_year ON insurance_rates(year);
CREATE INDEX IF NOT EXISTS idx_nontaxable_limits_year ON nontaxable_limits(year);
CREATE INDEX IF NOT EXISTS idx_payroll_status ON payroll_monthly(status);
