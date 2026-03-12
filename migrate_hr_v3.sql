-- migrate_hr_v3.sql -- 급여 일할계산 + 근태차감 + 퇴사일 마이그레이션
-- 실행: Supabase SQL Editor에서 실행
-- ====================================================

-- ══════════════════════════════════════════════════════
--  1. employees 테이블: 퇴사일 추가
-- ══════════════════════════════════════════════════════

ALTER TABLE employees ADD COLUMN IF NOT EXISTS retire_date DATE;

-- ══════════════════════════════════════════════════════
--  2. payroll_monthly 테이블: 일할계산 + 근태차감 컬럼
-- ══════════════════════════════════════════════════════

-- 일할계산 관련
ALTER TABLE payroll_monthly ADD COLUMN IF NOT EXISTS proration_ratio NUMERIC(6, 4) DEFAULT 1.0;
ALTER TABLE payroll_monthly ADD COLUMN IF NOT EXISTS proration_days INTEGER DEFAULT 0;
ALTER TABLE payroll_monthly ADD COLUMN IF NOT EXISTS calendar_days INTEGER DEFAULT 0;

-- 근태차감 관련
ALTER TABLE payroll_monthly ADD COLUMN IF NOT EXISTS attendance_deduction INTEGER DEFAULT 0;
ALTER TABLE payroll_monthly ADD COLUMN IF NOT EXISTS attendance_detail JSONB DEFAULT '{}';
-- attendance_detail 예: {"결근": {"days": 2, "amount": 200000}, "조퇴": {"count": 1, "amount": 50000}}

-- ══════════════════════════════════════════════════════
--  3. leave_records: leave_type 확장 (결근/조퇴/무급휴가/지각 추가)
-- ══════════════════════════════════════════════════════

-- 기존 CHECK 제약이 있을 수 있으므로 제거 후 재생성
-- leave_type은 TEXT이므로 CHECK 제약이 없으면 그대로 사용 가능
-- 명시적으로 COMMENT만 추가
COMMENT ON COLUMN leave_records.leave_type IS
    '연차, 반차, 병가, 경조, 특별휴가, 결근, 조퇴, 무급휴가, 지각';

-- leave_records에 deduction_amount 컬럼 추가 (급여 차감액 기록용)
ALTER TABLE leave_records ADD COLUMN IF NOT EXISTS deduction_amount INTEGER DEFAULT 0;

-- ══════════════════════════════════════════════════════
--  4. 인덱스
-- ══════════════════════════════════════════════════════

CREATE INDEX IF NOT EXISTS idx_employees_retire_date ON employees(retire_date);
CREATE INDEX IF NOT EXISTS idx_leave_records_type ON leave_records(leave_type);
