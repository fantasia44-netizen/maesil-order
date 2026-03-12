-- migrate_expense_categories_v2.sql
-- 비용 카테고리 정리: 총무 요청 반영
-- 실행: Supabase SQL Editor에서 실행

-- ============================================
-- 1) 기존 항목 변경
-- ============================================

-- 배송비 → 운반비
UPDATE expense_categories SET name = '운반비' WHERE name = '배송비';

-- expenses 테이블에서도 기존 데이터 카테고리명 변경
UPDATE expenses SET category = '운반비' WHERE category = '배송비';

-- parent 분류 정리 (판관비/제조경비)
UPDATE expense_categories SET parent = '판관비' WHERE name = '인건비';
UPDATE expense_categories SET parent = '판관비' WHERE name = '임차료';
UPDATE expense_categories SET parent = '판관비' WHERE name = '운반비';    -- (구 배송비)
UPDATE expense_categories SET parent = '판관비' WHERE name = '보험료';
UPDATE expense_categories SET parent = '판관비' WHERE name = '기타';

-- ============================================
-- 2) 계산서로 자동 잡히는 항목 → 비활성화
--    (매출원가 COGS에 이미 반영되므로 expenses 중복 불필요)
-- ============================================

UPDATE expense_categories SET is_active = FALSE WHERE name = '수도광열비';  -- 한전 계산서
UPDATE expense_categories SET is_active = FALSE WHERE name = '소모품비';    -- 세금계산서
UPDATE expense_categories SET is_active = FALSE WHERE name = '포장비';      -- 세금계산서
UPDATE expense_categories SET is_active = FALSE WHERE name = '광고선전비';  -- 세금계산서/정산서
UPDATE expense_categories SET is_active = FALSE WHERE name = '감가상각비';  -- 내부 계산, 수동비용 아님

-- ============================================
-- 3) 신규 항목 추가 (계산서 안 들어오는 것들)
-- ============================================

INSERT INTO expense_categories (name, parent, sort_order, is_active) VALUES
    ('세금과공과',   '판관비',   3,  TRUE),   -- 재산세, 자동차세 등 (고지서)
    ('복리후생비',   '판관비',   4,  TRUE),   -- 식대, 경조사 등 (카드/현금)
    ('연구개발비',   '제조경비', 10, TRUE),   -- R&D (제조경비만)
    ('이자비용',     '영업외',   11, TRUE),   -- 은행 이자 (면세, 계산서 미발행)
    ('지급수수료',   '판관비',   7,  TRUE)    -- 세금계산서 외 수수료 (카드결제 등)
ON CONFLICT (name) DO NOTHING;

-- ============================================
-- 정리 후 최종 카테고리 (활성 기준):
-- ============================================
-- [판관비]
--   인건비       (payroll 자동 sync)
--   임차료       (수동, 일부 세금계산서 연결 가능)
--   세금과공과   (신규: 재산세/자동차세 등 고지서)
--   복리후생비   (신규: 식대/경조사 등)
--   보험료       (손해보험 등 면세 → 수동)
--   운반비       (구 배송비 → 명칭변경)
--   지급수수료   (신규: 카드수수료 등)
--   기타(잡비)
--
-- [제조경비]
--   연구개발비   (신규)
--
-- [영업외]
--   이자비용     (신규: 은행 이자)
-- ============================================
