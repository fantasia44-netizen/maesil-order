-- ============================================================
-- AutoTool 회계 ERP 마이그레이션
-- Supabase SQL Editor에서 실행
-- ============================================================

-- 1. CODEF 연결 정보
CREATE TABLE IF NOT EXISTS codef_connections (
    id              BIGSERIAL PRIMARY KEY,
    connected_id    TEXT NOT NULL UNIQUE,
    organization    TEXT NOT NULL,              -- 기관코드
    login_type      TEXT DEFAULT '1',           -- 0=인증서, 1=ID/PW
    status          TEXT DEFAULT 'active',      -- active/expired/error
    memo            TEXT DEFAULT '',
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- 2. 은행 계좌
CREATE TABLE IF NOT EXISTS bank_accounts (
    id              BIGSERIAL PRIMARY KEY,
    connected_id    TEXT NOT NULL,
    bank_code       TEXT NOT NULL,              -- 기관코드 (0004=KB 등)
    bank_name       TEXT NOT NULL,
    account_number  TEXT NOT NULL,
    account_holder  TEXT DEFAULT '',
    account_type    TEXT DEFAULT 'checking',    -- checking/savings/loan
    is_active       BOOLEAN DEFAULT TRUE,
    last_synced_at  TIMESTAMPTZ,
    last_synced_date TEXT,                      -- 마지막 동기화 거래일자
    memo            TEXT DEFAULT '',
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(bank_code, account_number)
);
CREATE INDEX IF NOT EXISTS idx_bank_accounts_connected ON bank_accounts(connected_id);

-- 3. 은행 거래내역
CREATE TABLE IF NOT EXISTS bank_transactions (
    id                  BIGSERIAL PRIMARY KEY,
    bank_account_id     BIGINT REFERENCES bank_accounts(id) ON DELETE CASCADE,
    transaction_date    DATE NOT NULL,
    transaction_time    TEXT DEFAULT '',
    transaction_type    TEXT NOT NULL,              -- 입금/출금
    amount              BIGINT NOT NULL DEFAULT 0,
    balance             BIGINT DEFAULT 0,
    counterpart_name    TEXT DEFAULT '',            -- 거래상대 (적요)
    counterpart_account TEXT DEFAULT '',
    description         TEXT DEFAULT '',
    category            TEXT DEFAULT '',            -- 수동 분류 (급여/원재료/매출입금/정산금 등)
    -- ── 매칭 필드 ──
    matched_invoice_id  BIGINT,                    -- 매칭된 세금계산서 ID
    matched_settlement_id BIGINT,                  -- [확장] 매칭된 플랫폼 정산 ID
    -- ── 원본 식별 ──
    codef_transaction_id TEXT DEFAULT '',
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(bank_account_id, transaction_date, transaction_time, amount, counterpart_name)
);
CREATE INDEX IF NOT EXISTS idx_bank_tx_date ON bank_transactions(transaction_date);
CREATE INDEX IF NOT EXISTS idx_bank_tx_account ON bank_transactions(bank_account_id);
CREATE INDEX IF NOT EXISTS idx_bank_tx_type ON bank_transactions(transaction_type);
CREATE INDEX IF NOT EXISTS idx_bank_tx_category ON bank_transactions(category);

-- 4. 세금계산서
CREATE TABLE IF NOT EXISTS tax_invoices (
    id                  BIGSERIAL PRIMARY KEY,
    direction           TEXT NOT NULL,              -- sales(매출) / purchase(매입)
    invoice_number      TEXT DEFAULT '',            -- 국세청 승인번호
    mgt_key             TEXT DEFAULT '',            -- 팝빌 관리번호
    write_date          DATE NOT NULL,
    issue_date          DATE,
    tax_type            TEXT DEFAULT '과세',
    charge_direction    TEXT DEFAULT '정과금',
    issue_type          TEXT DEFAULT '정발행',
    purpose_type        TEXT DEFAULT '영수',
    -- 공급자
    supplier_corp_num   TEXT NOT NULL,
    supplier_corp_name  TEXT DEFAULT '',
    supplier_ceo_name   TEXT DEFAULT '',
    supplier_addr       TEXT DEFAULT '',
    supplier_biz_type   TEXT DEFAULT '',
    supplier_biz_class  TEXT DEFAULT '',
    supplier_email      TEXT DEFAULT '',
    -- 공급받는자
    buyer_corp_num      TEXT NOT NULL,
    buyer_corp_name     TEXT DEFAULT '',
    buyer_ceo_name      TEXT DEFAULT '',
    buyer_addr          TEXT DEFAULT '',
    buyer_biz_type      TEXT DEFAULT '',
    buyer_biz_class     TEXT DEFAULT '',
    buyer_email         TEXT DEFAULT '',
    -- 금액
    supply_cost_total   BIGINT NOT NULL DEFAULT 0,
    tax_total           BIGINT NOT NULL DEFAULT 0,
    total_amount        BIGINT NOT NULL DEFAULT 0,
    -- 품목 상세 (JSON)
    items               JSONB DEFAULT '[]'::JSONB,
    -- 상태
    status              TEXT DEFAULT 'draft',       -- draft/issued/sent/cancelled
    popbill_nts_result  TEXT DEFAULT '',
    -- 매칭
    matched_transaction_id BIGINT,                  -- 매칭된 은행거래 ID
    partner_id          BIGINT,                     -- 거래처 FK (nullable)
    -- 메타
    memo                TEXT DEFAULT '',
    registered_by       TEXT DEFAULT '',
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_tax_inv_direction ON tax_invoices(direction);
CREATE INDEX IF NOT EXISTS idx_tax_inv_write_date ON tax_invoices(write_date);
CREATE INDEX IF NOT EXISTS idx_tax_inv_supplier ON tax_invoices(supplier_corp_num);
CREATE INDEX IF NOT EXISTS idx_tax_inv_buyer ON tax_invoices(buyer_corp_num);
CREATE INDEX IF NOT EXISTS idx_tax_inv_status ON tax_invoices(status);
CREATE INDEX IF NOT EXISTS idx_tax_inv_mgt_key ON tax_invoices(mgt_key);

-- 5. 매출-입금 매칭
CREATE TABLE IF NOT EXISTS payment_matches (
    id                  BIGSERIAL PRIMARY KEY,
    tax_invoice_id      BIGINT REFERENCES tax_invoices(id) ON DELETE SET NULL,
    bank_transaction_id BIGINT REFERENCES bank_transactions(id) ON DELETE SET NULL,
    settlement_id       BIGINT,                    -- [확장] 플랫폼 정산 FK
    match_type          TEXT NOT NULL DEFAULT 'manual',  -- manual/auto/platform
    match_status        TEXT NOT NULL DEFAULT 'matched', -- matched/partial/unmatched
    matched_amount      BIGINT NOT NULL DEFAULT 0,
    invoice_amount      BIGINT DEFAULT 0,
    transaction_amount  BIGINT DEFAULT 0,
    difference          BIGINT DEFAULT 0,
    partner_name        TEXT DEFAULT '',
    memo                TEXT DEFAULT '',
    matched_by          TEXT DEFAULT '',
    matched_at          TIMESTAMPTZ DEFAULT NOW(),
    created_at          TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_pm_invoice ON payment_matches(tax_invoice_id);
CREATE INDEX IF NOT EXISTS idx_pm_tx ON payment_matches(bank_transaction_id);
CREATE INDEX IF NOT EXISTS idx_pm_status ON payment_matches(match_status);

-- 6. 계정과목 코드
CREATE TABLE IF NOT EXISTS account_codes (
    id              BIGSERIAL PRIMARY KEY,
    code            TEXT NOT NULL UNIQUE,
    name            TEXT NOT NULL,
    category        TEXT NOT NULL,              -- 자산/부채/자본/수익/비용
    parent_code     TEXT,
    is_active       BOOLEAN DEFAULT TRUE,
    sort_order      INTEGER DEFAULT 999,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO account_codes (code, name, category, sort_order) VALUES
    ('101', '현금', '자산', 1),
    ('102', '보통예금', '자산', 2),
    ('108', '매출채권', '자산', 3),
    ('110', '미수금', '자산', 4),
    ('120', '선급금', '자산', 5),
    ('201', '매입채무', '부채', 10),
    ('202', '미지급금', '부채', 11),
    ('203', '예수금', '부채', 12),
    ('204', '부가세예수금', '부채', 13),
    ('205', '부가세대급금', '자산', 6),
    ('401', '매출', '수익', 20),
    ('402', '매출할인', '수익', 21),
    ('403', '플랫폼매출', '수익', 22),
    ('501', '매입', '비용', 30),
    ('502', '급여', '비용', 31),
    ('503', '임차료', '비용', 32),
    ('504', '통신비', '비용', 33),
    ('505', '수도광열비', '비용', 34),
    ('506', '소모품비', '비용', 35),
    ('507', '배송비', '비용', 36),
    ('508', '광고선전비', '비용', 37),
    ('509', '접대비', '비용', 38),
    ('510', '감가상각비', '비용', 39),
    ('511', '판매수수료', '비용', 40),
    ('512', '플랫폼수수료', '비용', 41),
    ('599', '기타비용', '비용', 50)
ON CONFLICT (code) DO NOTHING;

-- ============================================================
-- 7. [확장 준비] 플랫폼 정산 테이블
--    Phase 2에서 온라인 플랫폼 API 연동 시 사용
-- ============================================================
CREATE TABLE IF NOT EXISTS platform_settlements (
    id                  BIGSERIAL PRIMARY KEY,
    channel             TEXT NOT NULL,              -- smartstore/coupang/oasis/11st/kakao 등
    settlement_date     DATE NOT NULL,              -- 정산일
    settlement_period_from DATE,                    -- 정산 대상기간 시작
    settlement_period_to   DATE,                    -- 정산 대상기간 종료
    -- 금액 상세
    gross_sales         BIGINT DEFAULT 0,           -- 총 주문금액 (결제금액)
    platform_fee        BIGINT DEFAULT 0,           -- 플랫폼 수수료
    delivery_fee        BIGINT DEFAULT 0,           -- 배송비 (플랫폼 부담분)
    promotion_discount  BIGINT DEFAULT 0,           -- 프로모션/쿠폰 할인
    returns_refunds     BIGINT DEFAULT 0,           -- 반품/환불
    adjustments         BIGINT DEFAULT 0,           -- 기타 조정
    net_settlement      BIGINT DEFAULT 0,           -- 실 정산금액
    -- 매칭
    matched_transaction_id BIGINT,                  -- 매칭된 은행입금 ID
    match_status        TEXT DEFAULT 'pending',     -- pending/matched/partial/discrepancy
    -- 상세 내역 (JSON)
    order_details       JSONB DEFAULT '[]'::JSONB,  -- [{order_id, amount, fee, ...}]
    fee_details         JSONB DEFAULT '{}'::JSONB,  -- {sales_fee, fulfillment_fee, ad_fee, ...}
    -- 메타
    api_reference       TEXT DEFAULT '',             -- 플랫폼 API 원본 ID
    memo                TEXT DEFAULT '',
    synced_at           TIMESTAMPTZ,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(channel, settlement_date, api_reference)
);
CREATE INDEX IF NOT EXISTS idx_ps_channel ON platform_settlements(channel);
CREATE INDEX IF NOT EXISTS idx_ps_date ON platform_settlements(settlement_date);
CREATE INDEX IF NOT EXISTS idx_ps_match ON platform_settlements(match_status);

-- 8. [확장 준비] 플랫폼 수수료 설정
CREATE TABLE IF NOT EXISTS platform_fee_config (
    id              BIGSERIAL PRIMARY KEY,
    channel         TEXT NOT NULL,
    fee_type        TEXT NOT NULL,              -- sales_commission/fulfillment/ad/coupon 등
    fee_name        TEXT NOT NULL,
    rate            NUMERIC(8,4) DEFAULT 0,     -- 수수료율 (%)
    fixed_amount    BIGINT DEFAULT 0,           -- 고정 수수료
    effective_from  DATE,
    effective_to    DATE,
    memo            TEXT DEFAULT '',
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(channel, fee_type, effective_from)
);

-- 기본 수수료 설정 (대략적, 실제는 계약에 따라 다름)
INSERT INTO platform_fee_config (channel, fee_type, fee_name, rate) VALUES
    ('smartstore', 'sales_commission', '판매 수수료', 5.5),
    ('coupang', 'sales_commission', '판매 수수료', 10.8),
    ('oasis', 'sales_commission', '판매 수수료', 20.0),
    ('11st', 'sales_commission', '판매 수수료', 13.0),
    ('kakao', 'sales_commission', '판매 수수료', 12.0),
    ('auction', 'sales_commission', '판매 수수료', 12.0),
    ('gmarket', 'sales_commission', '판매 수수료', 12.0)
ON CONFLICT DO NOTHING;
