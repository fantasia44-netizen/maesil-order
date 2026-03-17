-- ============================================================
-- 전표 시스템 마이그레이션 (Week 1: 회계 엔진)
-- 실행: Supabase SQL Editor
-- ============================================================

-- 1. account_codes 식품업 특화 계정 추가
-- ─────────────────────────────────────
INSERT INTO account_codes (code, name, category, sort_order) VALUES
    ('105', '상품재고', '자산', 105),
    ('106', '원재료', '자산', 106),
    ('109', '선수금', '부채', 109),
    ('404', '매출반품', '수익', 404),
    ('502', '매출원가', '비용', 502),
    ('513', '포장비', '비용', 513),
    ('514', '폐기손실', '비용', 514)
ON CONFLICT (code) DO NOTHING;


-- 2. journal_entries (전표 헤더)
-- ─────────────────────────────
CREATE TABLE IF NOT EXISTS journal_entries (
    id              BIGSERIAL PRIMARY KEY,
    journal_date    DATE NOT NULL,
    journal_type    TEXT NOT NULL,           -- sales_invoice, purchase_invoice, receipt, payment, platform_sales, platform_fee, settlement_receipt, payroll, manual
    description     TEXT DEFAULT '',
    total_debit     BIGINT NOT NULL DEFAULT 0,
    total_credit    BIGINT NOT NULL DEFAULT 0,
    status          TEXT DEFAULT 'posted',   -- draft, posted, reversed
    ref_type        TEXT,                    -- tax_invoice, bank_transaction, payment_match, platform_settlement, payroll
    ref_id          BIGINT,                  -- 참조 테이블 ID
    reversed_by     BIGINT,                  -- 역분개 전표 ID
    created_by      TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_je_date ON journal_entries(journal_date);
CREATE INDEX IF NOT EXISTS idx_je_type ON journal_entries(journal_type);
CREATE INDEX IF NOT EXISTS idx_je_status ON journal_entries(status);
CREATE INDEX IF NOT EXISTS idx_je_ref ON journal_entries(ref_type, ref_id);


-- 3. journal_lines (전표 라인 — 복식기입)
-- ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS journal_lines (
    id                  BIGSERIAL PRIMARY KEY,
    journal_entry_id    BIGINT NOT NULL REFERENCES journal_entries(id) ON DELETE CASCADE,
    line_no             INTEGER NOT NULL DEFAULT 1,
    account_code        TEXT NOT NULL,       -- account_codes.code 참조
    account_name        TEXT DEFAULT '',
    debit_amount        BIGINT NOT NULL DEFAULT 0,
    credit_amount       BIGINT NOT NULL DEFAULT 0,
    description         TEXT DEFAULT '',
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_jl_entry ON journal_lines(journal_entry_id);
CREATE INDEX IF NOT EXISTS idx_jl_account ON journal_lines(account_code);


-- 4. event_account_mapping (이벤트→계정 자동 매핑)
-- ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS event_account_mapping (
    id                      BIGSERIAL PRIMARY KEY,
    event_type              TEXT NOT NULL UNIQUE,
    debit_account           TEXT NOT NULL,
    credit_account          TEXT NOT NULL,
    description_template    TEXT DEFAULT '',
    is_active               BOOLEAN DEFAULT TRUE
);

-- 초기 매핑 데이터
INSERT INTO event_account_mapping (event_type, debit_account, credit_account, description_template) VALUES
    ('sales_invoice',       '108', '401', '매출 세금계산서 발행'),
    ('purchase_invoice',    '501', '201', '매입 세금계산서 등록'),
    ('receipt',             '102', '108', '매출대금 입금 (매칭)'),
    ('payment',             '201', '102', '매입대금 지급 (매칭)'),
    ('platform_sales',      '110', '403', '플랫폼 정산 매출'),
    ('platform_fee',        '512', '110', '플랫폼 수수료'),
    ('settlement_receipt',  '102', '110', '정산금 입금'),
    ('payroll',             '502', '202', '급여 전표')
ON CONFLICT (event_type) DO NOTHING;


-- 5. RLS 정책 (Supabase anon key 접근 허용)
-- ──────────────────────────────────────────
ALTER TABLE journal_entries ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow all for journal_entries" ON journal_entries FOR ALL USING (true) WITH CHECK (true);

ALTER TABLE journal_lines ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow all for journal_lines" ON journal_lines FOR ALL USING (true) WITH CHECK (true);

ALTER TABLE event_account_mapping ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow all for event_account_mapping" ON event_account_mapping FOR ALL USING (true) WITH CHECK (true);
