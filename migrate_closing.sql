-- ==========================================================
-- daily_revenue 테이블 마이그레이션 (channel 컬럼 추가)
-- Supabase SQL Editor에서 실행
-- ==========================================================

-- 1. channel 컬럼 추가 (이미 있으면 무시)
ALTER TABLE daily_revenue ADD COLUMN IF NOT EXISTS channel TEXT NOT NULL DEFAULT '';

-- 2. 기존 유니크 제약조건 삭제
ALTER TABLE daily_revenue DROP CONSTRAINT IF EXISTS daily_revenue_revenue_date_product_name_category_key;

-- 3. 새 유니크 제약조건 (channel 포함)
ALTER TABLE daily_revenue ADD CONSTRAINT daily_revenue_date_product_category_channel_key
    UNIQUE(revenue_date, product_name, category, channel);

-- 4. channel 인덱스
CREATE INDEX IF NOT EXISTS idx_daily_revenue_channel ON daily_revenue(channel);


-- ==========================================================
-- daily_closing 테이블 생성 (일일마감)
-- ==========================================================
CREATE TABLE IF NOT EXISTS daily_closing (
    id              BIGSERIAL PRIMARY KEY,
    closing_date    DATE NOT NULL,
    closing_type    TEXT NOT NULL,        -- 'revenue' or 'stock'
    status          TEXT NOT NULL DEFAULT 'open',  -- 'open' or 'closed'
    cutoff_time     TIME NOT NULL DEFAULT '15:05',
    closed_by       TEXT,
    closed_at       TIMESTAMPTZ,
    reopened_by     TEXT,
    reopened_at     TIMESTAMPTZ,
    memo            TEXT DEFAULT '',
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(closing_date, closing_type)
);
CREATE INDEX IF NOT EXISTS idx_daily_closing_date ON daily_closing(closing_date);

-- ==========================================================
-- 완료 확인
-- ==========================================================
SELECT 'Migration complete' AS status;
