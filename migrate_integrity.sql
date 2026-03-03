-- ============================================================
-- 데이터 무결성 보호 계층 (Integrity & Validation Layer)
-- Supabase SQL Editor에서 실행
-- ============================================================

-- 1) integrity_report (정합성 검사 보고서)
CREATE TABLE IF NOT EXISTS integrity_report (
    id              BIGSERIAL PRIMARY KEY,
    check_date      DATE NOT NULL,
    passed          BOOLEAN NOT NULL DEFAULT TRUE,
    critical_count  INT DEFAULT 0,
    warning_count   INT DEFAULT 0,
    info_count      INT DEFAULT 0,
    summary         TEXT,
    details         JSONB,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_integrity_report_date ON integrity_report(check_date);
CREATE INDEX IF NOT EXISTS idx_integrity_report_passed ON integrity_report(passed);

-- 2) import_runs 테이블에 file_hash 인덱스 추가 (중복 파일 감지 고속화)
CREATE INDEX IF NOT EXISTS idx_import_runs_hash ON import_runs(file_hash)
    WHERE file_hash IS NOT NULL;
