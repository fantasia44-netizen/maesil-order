-- 005_daily_revenue_warehouse.sql
-- daily_revenue 테이블에 warehouse 컬럼 추가
-- aggregation.py 통합집계에서 창고별 집계에 사용

ALTER TABLE daily_revenue
    ADD COLUMN IF NOT EXISTS warehouse TEXT NOT NULL DEFAULT '넥스원';
