-- ============================================================
-- Phase 5: 수량 기반 생산계획 엔진 (2026-03)
-- Supabase SQL Editor에서 실행
-- ============================================================

-- 1) product_costs 확장: 생산계획 설정
ALTER TABLE product_costs ADD COLUMN IF NOT EXISTS safety_stock INTEGER DEFAULT 0;
ALTER TABLE product_costs ADD COLUMN IF NOT EXISTS lead_time_days INTEGER DEFAULT 3;
ALTER TABLE product_costs ADD COLUMN IF NOT EXISTS is_production_target BOOLEAN DEFAULT NULL;
-- NULL = 자동 판별 (cost_type='생산' or material_type='완제품' → 대상)
-- TRUE = 강제 포함, FALSE = 강제 제외

-- 1-b) product_costs 확장: 판매분석용 분류
ALTER TABLE product_costs ADD COLUMN IF NOT EXISTS sales_category TEXT DEFAULT '';
-- 큐브, 세트, oem, pack, 해미애찬 등 관리자 엑셀의 '구분'과 동일

-- 2) production_plan (생산계획 결과)
CREATE TABLE IF NOT EXISTS production_plan (
    id                       BIGSERIAL PRIMARY KEY,
    plan_date                DATE NOT NULL,
    product_name             TEXT NOT NULL,
    current_stock            NUMERIC DEFAULT 0,
    avg_daily_sales          NUMERIC DEFAULT 0,
    depletion_days           NUMERIC,
    safety_stock             INTEGER DEFAULT 0,
    lead_time_days           INTEGER DEFAULT 3,
    target_stock             NUMERIC DEFAULT 0,
    recommended_production   NUMERIC DEFAULT 0,
    status                   TEXT DEFAULT '',
    unit                     TEXT DEFAULT '개',
    created_at               TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_production_plan_date
    ON production_plan(plan_date);
CREATE INDEX IF NOT EXISTS idx_production_plan_product
    ON production_plan(product_name);
CREATE INDEX IF NOT EXISTS idx_production_plan_status
    ON production_plan(status);
