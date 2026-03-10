-- migrate_marketplace.sql
-- 마켓플레이스 API 연동 테이블 (네이버/쿠팡/Cafe24)

-- 1. marketplace_api_config — API 인증 정보
CREATE TABLE IF NOT EXISTS marketplace_api_config (
    id                    BIGSERIAL PRIMARY KEY,
    channel               TEXT NOT NULL UNIQUE,
    client_id             TEXT DEFAULT '',
    client_secret         TEXT DEFAULT '',
    access_token          TEXT DEFAULT '',
    refresh_token         TEXT DEFAULT '',
    token_expires_at      TIMESTAMPTZ,
    vendor_id             TEXT DEFAULT '',
    mall_id               TEXT DEFAULT '',
    is_active             BOOLEAN DEFAULT FALSE,
    last_synced_at        TIMESTAMPTZ,
    sync_interval_minutes INTEGER DEFAULT 60,
    extra_config          JSONB DEFAULT '{}',
    created_at            TIMESTAMPTZ DEFAULT NOW(),
    updated_at            TIMESTAMPTZ DEFAULT NOW()
);

-- 2. api_sync_log — 동기화 실행 이력
CREATE TABLE IF NOT EXISTS api_sync_log (
    id                BIGSERIAL PRIMARY KEY,
    channel           TEXT NOT NULL,
    sync_type         TEXT NOT NULL,
    status            TEXT NOT NULL DEFAULT 'running',
    started_at        TIMESTAMPTZ DEFAULT NOW(),
    finished_at       TIMESTAMPTZ,
    records_fetched   INTEGER DEFAULT 0,
    records_new       INTEGER DEFAULT 0,
    records_updated   INTEGER DEFAULT 0,
    date_from         DATE,
    date_to           DATE,
    error_message     TEXT,
    error_detail      JSONB,
    triggered_by      TEXT DEFAULT 'system',
    created_at        TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_api_sync_log_channel ON api_sync_log(channel);
CREATE INDEX IF NOT EXISTS idx_api_sync_log_started ON api_sync_log(started_at DESC);

-- 3. api_orders — API 주문 데이터 (교차검증용)
CREATE TABLE IF NOT EXISTS api_orders (
    id                             BIGSERIAL PRIMARY KEY,
    channel                        TEXT NOT NULL,
    api_order_id                   TEXT NOT NULL,
    api_line_id                    TEXT DEFAULT '',
    order_date                     DATE NOT NULL,
    product_name                   TEXT DEFAULT '',
    option_name                    TEXT DEFAULT '',
    qty                            INTEGER DEFAULT 0,
    unit_price                     INTEGER DEFAULT 0,
    total_amount                   INTEGER DEFAULT 0,
    discount_amount                INTEGER DEFAULT 0,
    settlement_amount              INTEGER DEFAULT 0,
    commission                     INTEGER DEFAULT 0,
    shipping_fee                   INTEGER DEFAULT 0,
    fee_detail                     JSONB DEFAULT '{}',
    order_status                   TEXT DEFAULT '',
    matched_order_transaction_id   BIGINT,
    match_status                   TEXT DEFAULT 'pending',
    match_detail                   JSONB DEFAULT '{}',
    raw_data                       JSONB DEFAULT '{}',
    raw_hash                       TEXT DEFAULT '',
    synced_at                      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(channel, api_order_id, api_line_id)
);
CREATE INDEX IF NOT EXISTS idx_api_orders_channel_date ON api_orders(channel, order_date);
CREATE INDEX IF NOT EXISTS idx_api_orders_match ON api_orders(match_status);
CREATE INDEX IF NOT EXISTS idx_api_orders_api_order ON api_orders(api_order_id);

-- 4. api_settlements — API 정산 데이터
CREATE TABLE IF NOT EXISTS api_settlements (
    id                                BIGSERIAL PRIMARY KEY,
    channel                           TEXT NOT NULL,
    settlement_date                   DATE NOT NULL,
    settlement_id                     TEXT DEFAULT '',
    gross_sales                       INTEGER DEFAULT 0,
    total_commission                  INTEGER DEFAULT 0,
    shipping_fee_income               INTEGER DEFAULT 0,
    shipping_fee_cost                 INTEGER DEFAULT 0,
    coupon_discount                   INTEGER DEFAULT 0,
    point_discount                    INTEGER DEFAULT 0,
    other_deductions                  INTEGER DEFAULT 0,
    net_settlement                    INTEGER DEFAULT 0,
    fee_breakdown                     JSONB DEFAULT '{}',
    matched_platform_settlement_id    BIGINT,
    match_status                      TEXT DEFAULT 'pending',
    amount_diff                       INTEGER DEFAULT 0,
    raw_data                          JSONB DEFAULT '{}',
    synced_at                         TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(channel, settlement_date, settlement_id)
);
CREATE INDEX IF NOT EXISTS idx_api_settlements_channel_date ON api_settlements(channel, settlement_date);

-- RLS 정책 (Supabase)
ALTER TABLE marketplace_api_config ENABLE ROW LEVEL SECURITY;
ALTER TABLE api_sync_log ENABLE ROW LEVEL SECURITY;
ALTER TABLE api_orders ENABLE ROW LEVEL SECURITY;
ALTER TABLE api_settlements ENABLE ROW LEVEL SECURITY;

CREATE POLICY "service_role_all_marketplace_api_config" ON marketplace_api_config
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_all_api_sync_log" ON api_sync_log
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_all_api_orders" ON api_orders
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_all_api_settlements" ON api_settlements
    FOR ALL USING (true) WITH CHECK (true);
