-- ==========================================================
-- product_cost_history 테이블 생성 (매입단가 이력 관리)
-- Supabase SQL Editor에서 실행
-- ==========================================================

CREATE TABLE IF NOT EXISTS product_cost_history (
    id                   BIGSERIAL PRIMARY KEY,
    product_name         TEXT NOT NULL,
    old_cost_price       NUMERIC,
    new_cost_price       NUMERIC,
    old_conversion_ratio NUMERIC,
    new_conversion_ratio NUMERIC,
    changed_by           TEXT,
    change_reason        TEXT DEFAULT '',
    effective_date       DATE NOT NULL,
    created_at           TIMESTAMPTZ DEFAULT NOW()
);

-- 인덱스: 품목명 조회용
CREATE INDEX IF NOT EXISTS idx_cost_history_product
    ON product_cost_history(product_name);

-- 인덱스: 날짜 범위 조회용
CREATE INDEX IF NOT EXISTS idx_cost_history_date
    ON product_cost_history(effective_date DESC);

-- 인덱스: 품목 + 날짜 복합 조회용
CREATE INDEX IF NOT EXISTS idx_cost_history_product_date
    ON product_cost_history(product_name, effective_date DESC);
