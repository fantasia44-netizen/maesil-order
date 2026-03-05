-- ==========================================================
-- 주문수집일(collection_date) 컬럼 추가
-- Supabase SQL Editor에서 실행
-- ==========================================================

-- order_transactions에 collection_date 컬럼 추가
-- 재고차감/통합집계 기준일로 사용 (매출은 기존 order_date 유지)
ALTER TABLE order_transactions
ADD COLUMN IF NOT EXISTS collection_date DATE;

-- 기존 데이터: collection_date가 NULL인 경우 order_date로 백필 (선택사항)
-- UPDATE order_transactions SET collection_date = order_date WHERE collection_date IS NULL;

-- 인덱스 추가 (통합집계/수불부 조회 성능)
CREATE INDEX IF NOT EXISTS idx_order_transactions_collection_date
ON order_transactions(collection_date);

SELECT 'collection_date migration complete' AS result;
