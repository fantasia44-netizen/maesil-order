-- ============================================================
-- 채널명 리네이밍: 스마트스토어 → 스마트스토어_배마마
--                  해미애찬     → 스마트스토어_해미애찬
-- ============================================================
-- 실행 전 반드시 백업 또는 트랜잭션 내 실행
-- 2026-03-20

BEGIN;

-- 1) order_transactions
UPDATE order_transactions SET channel = '스마트스토어_배마마'   WHERE channel = '스마트스토어';
UPDATE order_transactions SET channel = '스마트스토어_해미애찬' WHERE channel = '해미애찬';

-- 2) api_orders
UPDATE api_orders SET channel = '스마트스토어_배마마'   WHERE channel = '스마트스토어';
UPDATE api_orders SET channel = '스마트스토어_해미애찬' WHERE channel = '해미애찬';

-- 3) order_shipping
UPDATE order_shipping SET channel = '스마트스토어_배마마'   WHERE channel = '스마트스토어';
UPDATE order_shipping SET channel = '스마트스토어_해미애찬' WHERE channel = '해미애찬';

-- 4) daily_revenue
UPDATE daily_revenue SET channel = '스마트스토어_배마마'   WHERE channel = '스마트스토어';
UPDATE daily_revenue SET channel = '스마트스토어_해미애찬' WHERE channel = '해미애찬';

-- 5) marketplace_api_config
UPDATE marketplace_api_config SET channel = '스마트스토어_배마마'   WHERE channel = '스마트스토어';
UPDATE marketplace_api_config SET channel = '스마트스토어_해미애찬' WHERE channel = '해미애찬';

-- 6) api_settlements
UPDATE api_settlements SET channel = '스마트스토어_배마마'   WHERE channel = '스마트스토어';
UPDATE api_settlements SET channel = '스마트스토어_해미애찬' WHERE channel = '해미애찬';

-- 7) api_sync_log
UPDATE api_sync_log SET channel = '스마트스토어_배마마'   WHERE channel = '스마트스토어';
UPDATE api_sync_log SET channel = '스마트스토어_해미애찬' WHERE channel = '해미애찬';

-- 8) channel_costs
UPDATE channel_costs SET channel = '스마트스토어_배마마'   WHERE channel = '스마트스토어';
UPDATE channel_costs SET channel = '스마트스토어_해미애찬' WHERE channel = '해미애찬';

-- 9) marketplace_api_config에 platform 컬럼 추가
ALTER TABLE marketplace_api_config ADD COLUMN IF NOT EXISTS platform TEXT DEFAULT '';
UPDATE marketplace_api_config SET platform = 'naver'   WHERE channel LIKE '스마트스토어%';
UPDATE marketplace_api_config SET platform = 'coupang'  WHERE channel = '쿠팡';
UPDATE marketplace_api_config SET platform = 'cafe24'   WHERE channel = '자사몰';

COMMIT;
