-- 로켓매출에 송장번호 컬럼 추가
-- Supabase SQL Editor에서 실행

ALTER TABLE daily_revenue ADD COLUMN IF NOT EXISTS invoice_no TEXT DEFAULT '';
