-- 카드 이용내역 테이블
CREATE TABLE IF NOT EXISTS card_transactions (
    id BIGSERIAL PRIMARY KEY,
    bank_account_id BIGINT REFERENCES bank_accounts(id) ON DELETE CASCADE,
    approval_date DATE NOT NULL,          -- 이용일자
    approval_time TEXT DEFAULT '',         -- 이용시간
    approval_no TEXT DEFAULT '',           -- 승인번호
    merchant_name TEXT DEFAULT '',         -- 가맹점명
    amount BIGINT DEFAULT 0,              -- 이용금액
    card_type TEXT DEFAULT '',             -- 카드구분 (신용/체크)
    installment TEXT DEFAULT '일시불',     -- 할부개월
    is_cancelled BOOLEAN DEFAULT FALSE,   -- 취소여부
    category TEXT DEFAULT '미분류',        -- 비용분류
    description TEXT DEFAULT '',           -- 비고
    matched_journal_id BIGINT,            -- 매칭된 전표 ID
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(bank_account_id, approval_date, approval_no, amount)
);

-- 인덱스
CREATE INDEX IF NOT EXISTS idx_card_tx_date ON card_transactions(approval_date);
CREATE INDEX IF NOT EXISTS idx_card_tx_account ON card_transactions(bank_account_id);
CREATE INDEX IF NOT EXISTS idx_card_tx_category ON card_transactions(category);

-- RLS
ALTER TABLE card_transactions ENABLE ROW LEVEL SECURITY;
CREATE POLICY "card_transactions_all" ON card_transactions FOR ALL USING (true);
