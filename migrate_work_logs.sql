-- ============================================================
-- work_logs: 직원 작업 이력 테이블
-- 주문만들기, 송장처리, 엑셀 업로드 등 모든 배치 작업 기록
-- ============================================================

CREATE TABLE IF NOT EXISTS work_logs (
    id            BIGSERIAL PRIMARY KEY,
    -- 누가
    user_name     VARCHAR(100) NOT NULL,
    user_role     VARCHAR(50),
    -- 무엇을
    action        VARCHAR(100) NOT NULL,     -- '주문처리', '리얼패킹', '외부일괄', '송장업로드', '엑셀업로드', '패킹완료', '출고처리' 등
    category      VARCHAR(50) DEFAULT '주문', -- '주문', '송장', '패킹', '출고', '재고', '관리' 등
    -- 상세
    channel       VARCHAR(50),               -- 스마트스토어, 쿠팡, 자사몰 등
    target_type   VARCHAR(50),               -- 송장, 리얼패킹, 외부일괄 등
    detail        TEXT,                       -- 사람이 읽을 수 있는 요약
    -- 결과
    result_status VARCHAR(20) DEFAULT 'success', -- success, partial, error
    total_count   INT DEFAULT 0,             -- 처리 대상 전체 건수
    success_count INT DEFAULT 0,             -- 성공 건수
    error_count   INT DEFAULT 0,             -- 실패 건수
    -- 부가 데이터 (JSON)
    meta          JSONB,                     -- 파일명, 매칭율, 송장번호 목록 등 상세 데이터
    -- 시간
    started_at    TIMESTAMPTZ DEFAULT NOW(),
    finished_at   TIMESTAMPTZ,
    duration_ms   INT,                       -- 소요시간(ms)
    -- 추적
    ip_address    VARCHAR(50),
    created_at    TIMESTAMPTZ DEFAULT NOW()
);

-- 인덱스
CREATE INDEX IF NOT EXISTS idx_work_logs_user ON work_logs(user_name);
CREATE INDEX IF NOT EXISTS idx_work_logs_action ON work_logs(action);
CREATE INDEX IF NOT EXISTS idx_work_logs_channel ON work_logs(channel);
CREATE INDEX IF NOT EXISTS idx_work_logs_created ON work_logs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_work_logs_category ON work_logs(category);

-- RLS (Row Level Security)
ALTER TABLE work_logs ENABLE ROW LEVEL SECURITY;
CREATE POLICY work_logs_all ON work_logs FOR ALL USING (true);
