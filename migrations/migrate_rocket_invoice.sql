-- 로켓매출 송장번호 지원
-- Supabase SQL Editor에서 실행

-- 1. invoice_no 컬럼 추가 (이미 실행됨)
ALTER TABLE daily_revenue ADD COLUMN IF NOT EXISTS invoice_no TEXT DEFAULT '';

-- 2. unique constraint 수정: invoice_no 포함
-- 기존: (revenue_date, product_name, category, channel)
-- 변경: (revenue_date, product_name, category, channel, invoice_no)
ALTER TABLE daily_revenue DROP CONSTRAINT IF EXISTS daily_revenue_unique_key;
ALTER TABLE daily_revenue ADD CONSTRAINT daily_revenue_unique_key
  UNIQUE (revenue_date, product_name, category, channel, invoice_no);
