-- 직원별 보험요율 오버라이드 테이블
-- 기본값은 insurance_rates에서 가져오고, 개인별 다른 요율 적용 시 이 테이블에 저장
CREATE TABLE IF NOT EXISTS employee_insurance_overrides (
    id BIGSERIAL PRIMARY KEY,
    employee_id BIGINT NOT NULL REFERENCES employees(id) ON DELETE CASCADE,
    insurance_type TEXT NOT NULL,           -- national_pension, health_insurance, long_term_care, employment_insurance, industrial_accident
    employee_rate NUMERIC(6,3) NOT NULL,   -- 근로자 부담 요율 (%)
    employer_rate NUMERIC(6,3) NOT NULL,   -- 사업주 부담 요율 (%)
    notes TEXT DEFAULT '',                 -- 사유 메모
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(employee_id, insurance_type)
);

-- 인덱스
CREATE INDEX IF NOT EXISTS idx_eio_employee ON employee_insurance_overrides(employee_id);

-- RLS 정책
ALTER TABLE employee_insurance_overrides ENABLE ROW LEVEL SECURITY;
CREATE POLICY "employee_insurance_overrides_all" ON employee_insurance_overrides
    FOR ALL USING (true) WITH CHECK (true);
