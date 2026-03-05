-- ==========================================================
-- 패킹센터 Phase 2: packing_jobs 테이블 + Storage 설정
-- Supabase SQL Editor에서 실행
-- ==========================================================

-- 1. app_users에 company_name 컬럼 (Phase 1에서 미실행 시)
ALTER TABLE app_users ADD COLUMN IF NOT EXISTS company_name TEXT DEFAULT NULL;

-- 2. packing_jobs 테이블
CREATE TABLE IF NOT EXISTS packing_jobs (
    id                BIGSERIAL PRIMARY KEY,

    -- 작업자
    user_id           BIGINT NOT NULL,
    username          TEXT NOT NULL,
    company_name      TEXT,

    -- 바코드 / 주문 매칭
    scanned_barcode   TEXT NOT NULL,
    channel           TEXT,
    order_no          TEXT,
    product_name      TEXT,
    recipient_name    TEXT,
    order_info        JSONB,

    -- 녹화
    video_path        TEXT,
    video_size_bytes  BIGINT DEFAULT 0,
    video_duration_ms BIGINT DEFAULT 0,

    -- 상태
    status            TEXT DEFAULT 'recording',
    started_at        TIMESTAMPTZ NOT NULL,
    completed_at      TIMESTAMPTZ,
    created_at        TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_packing_jobs_user      ON packing_jobs(user_id);
CREATE INDEX IF NOT EXISTS idx_packing_jobs_barcode   ON packing_jobs(scanned_barcode);
CREATE INDEX IF NOT EXISTS idx_packing_jobs_status    ON packing_jobs(status);
CREATE INDEX IF NOT EXISTS idx_packing_jobs_started   ON packing_jobs(started_at);
CREATE INDEX IF NOT EXISTS idx_packing_jobs_completed ON packing_jobs(completed_at);

-- 3. Storage 버킷 생성
INSERT INTO storage.buckets (id, name, public, file_size_limit, allowed_mime_types)
VALUES (
    'packing-videos',
    'packing-videos',
    false,
    104857600,
    ARRAY['video/webm', 'video/mp4']
)
ON CONFLICT (id) DO NOTHING;

-- 4. Storage RLS 정책
CREATE POLICY "packing_video_insert" ON storage.objects
    FOR INSERT TO anon
    WITH CHECK (bucket_id = 'packing-videos');

CREATE POLICY "packing_video_select" ON storage.objects
    FOR SELECT TO anon
    USING (bucket_id = 'packing-videos');

-- 완료
SELECT 'Packing Phase 2 migration complete' AS result;
