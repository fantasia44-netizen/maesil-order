-- ============================================================
-- 데이터 무결성 보호 계층 (Integrity & Validation Layer)
-- Supabase SQL Editor에서 실행
-- ============================================================

-- 1) integrity_report (정합성 검사 결과)
CREATE TABLE IF NOT EXISTS integrity_report (
    id              BIGSERIAL PRIMARY KEY,
    check_date      DATE NOT NULL,
    passed          BOOLEAN NOT NULL DEFAULT TRUE,
    critical_count  INTEGER DEFAULT 0,
    warning_count   INTEGER DEFAULT 0,
    info_count      INTEGER DEFAULT 0,
    summary         TEXT DEFAULT '',
    details         JSONB DEFAULT '[]',
    run_by          TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_integrity_report_date
    ON integrity_report(check_date);
CREATE INDEX IF NOT EXISTS idx_integrity_report_passed
    ON integrity_report(passed);

-- 2) stock_ledger 확장: transfer_id (이동 원자성 보장)
-- 같은 이동 건의 MOVE_OUT + MOVE_IN을 하나로 묶는 ID
ALTER TABLE stock_ledger ADD COLUMN IF NOT EXISTS transfer_id TEXT;
CREATE INDEX IF NOT EXISTS idx_stock_ledger_transfer_id
    ON stock_ledger(transfer_id) WHERE transfer_id IS NOT NULL;

-- 3) import_runs 확장: file_hash 인덱스 (중복 업로드 빠른 감지)
CREATE INDEX IF NOT EXISTS idx_import_runs_file_hash
    ON import_runs(file_hash) WHERE file_hash IS NOT NULL;

-- 4) validation_log (검증 실패 이력 — audit_logs와 별도로 빠른 집계용)
CREATE TABLE IF NOT EXISTS validation_log (
    id              BIGSERIAL PRIMARY KEY,
    action          TEXT NOT NULL,
    error_code      TEXT,
    message         TEXT,
    user_id         TEXT,
    details         JSONB DEFAULT '{}',
    created_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_validation_log_action
    ON validation_log(action);
CREATE INDEX IF NOT EXISTS idx_validation_log_created
    ON validation_log(created_at);
