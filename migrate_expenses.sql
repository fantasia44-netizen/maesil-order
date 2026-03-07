-- migrate_expenses.sql
-- 간접비(비용) 관리 테이블
-- 실행: Supabase SQL Editor에서 실행

-- 1) 비용 테이블
CREATE TABLE IF NOT EXISTS expenses (
    id BIGSERIAL PRIMARY KEY,
    expense_date DATE NOT NULL,
    expense_month TEXT NOT NULL,          -- '2026-03' 형식
    category TEXT NOT NULL,               -- 인건비, 임차료, 수도광열비 등
    subcategory TEXT DEFAULT '',           -- 세부 분류
    amount NUMERIC NOT NULL DEFAULT 0,
    is_recurring BOOLEAN DEFAULT FALSE,   -- 반복 비용 여부
    tax_invoice_id TEXT,                  -- 세금계산서 연결 (autotool_accounting)
    memo TEXT DEFAULT '',
    registered_by TEXT,                   -- 등록자 username
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_expenses_month ON expenses(expense_month);
CREATE INDEX IF NOT EXISTS idx_expenses_category ON expenses(category);

-- 2) 비용 카테고리 마스터
CREATE TABLE IF NOT EXISTS expense_categories (
    id BIGSERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    parent TEXT DEFAULT '',               -- 상위 분류 (판관비, 제조원가 등)
    sort_order INTEGER DEFAULT 999,
    is_active BOOLEAN DEFAULT TRUE
);

-- 3) 기본 카테고리 데이터
INSERT INTO expense_categories (name, parent, sort_order) VALUES
    ('인건비',     '판관비',   1),
    ('임차료',     '판관비',   2),
    ('수도광열비', '판관비',   3),
    ('소모품비',   '판관비',   4),
    ('포장비',     '제조원가', 5),
    ('배송비',     '판관비',   6),
    ('광고선전비', '판관비',   7),
    ('보험료',     '판관비',   8),
    ('감가상각비', '판관비',   9),
    ('기타',       '판관비',   99)
ON CONFLICT (name) DO NOTHING;
