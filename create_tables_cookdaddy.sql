-- ============================================================
-- 쿡대디 Supabase 테이블 생성 SQL (전체)
-- Supabase 대시보드 > SQL Editor에서 실행
-- URL: https://supabase.com/dashboard/project/yfktbbszelpzydrktmuo/sql
-- ============================================================

-- ============================================================
-- 1. 기본 테이블 (앱 운영 필수)
-- ============================================================

-- 1-1) app_users (사용자)
CREATE TABLE IF NOT EXISTS app_users (
    id                BIGSERIAL PRIMARY KEY,
    username          TEXT UNIQUE NOT NULL,
    name              TEXT DEFAULT '',
    password_hash     TEXT NOT NULL,
    role              TEXT NOT NULL DEFAULT 'sales',
    is_active_user    BOOLEAN DEFAULT TRUE,
    is_approved       BOOLEAN DEFAULT FALSE,
    failed_login_count INTEGER DEFAULT 0,
    locked_until      TIMESTAMPTZ,
    last_login        TIMESTAMPTZ,
    password_changed_at TEXT,
    created_at        TIMESTAMPTZ DEFAULT NOW(),
    updated_at        TIMESTAMPTZ DEFAULT NOW()
);

-- 1-2) audit_logs (감사 로그)
CREATE TABLE IF NOT EXISTS audit_logs (
    id            BIGSERIAL PRIMARY KEY,
    action        TEXT NOT NULL,
    user_id       INTEGER,
    user_name     TEXT,
    target        TEXT,
    detail        TEXT,
    ip_address    TEXT,
    old_value     JSONB,
    new_value     JSONB,
    is_reverted   BOOLEAN DEFAULT FALSE,
    reverted_by   INTEGER,
    reverted_at   TIMESTAMPTZ,
    created_at    TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_audit_logs_action ON audit_logs(action);
CREATE INDEX IF NOT EXISTS idx_audit_logs_user ON audit_logs(user_id);

-- 1-3) role_permissions (부서별 메뉴 접근 권한)
CREATE TABLE IF NOT EXISTS role_permissions (
    id          BIGSERIAL PRIMARY KEY,
    role        TEXT NOT NULL,
    page_key    TEXT NOT NULL,
    is_allowed  BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(role, page_key)
);
CREATE INDEX IF NOT EXISTS idx_role_perm_role ON role_permissions(role);

-- ============================================================
-- 2. 재고 관련 테이블
-- ============================================================

-- 2-1) stock_ledger (재고 대장)
CREATE TABLE IF NOT EXISTS stock_ledger (
    id               BIGSERIAL PRIMARY KEY,
    transaction_date DATE NOT NULL,
    product_name     TEXT NOT NULL,
    category         TEXT DEFAULT '',
    type             TEXT NOT NULL,
    location         TEXT DEFAULT '',
    qty              NUMERIC NOT NULL DEFAULT 0,
    unit             TEXT DEFAULT '개',
    is_deleted       BOOLEAN DEFAULT FALSE,
    deleted_at       TIMESTAMPTZ,
    deleted_by       TEXT,
    batch_id         TEXT,
    food_type        TEXT DEFAULT '',
    event_uid        TEXT,
    ref_event_uid    TEXT,
    transfer_id      TEXT,
    created_at       TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_stock_ledger_batch ON stock_ledger(batch_id);
CREATE INDEX IF NOT EXISTS idx_stock_ledger_food_type ON stock_ledger(food_type);
CREATE UNIQUE INDEX IF NOT EXISTS idx_stock_ledger_event_uid
    ON stock_ledger(event_uid) WHERE event_uid IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_stock_ledger_ref_event ON stock_ledger(ref_event_uid)
    WHERE ref_event_uid IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_stock_ledger_transfer_id ON stock_ledger(transfer_id)
    WHERE transfer_id IS NOT NULL;

-- ============================================================
-- 3. 마스터 데이터 테이블
-- ============================================================

-- 3-1) master_products (품목마스터)
CREATE TABLE IF NOT EXISTS master_products (
    id            BIGSERIAL PRIMARY KEY,
    product_name  TEXT UNIQUE NOT NULL,
    line_code     TEXT DEFAULT '0',
    sort_order    INTEGER DEFAULT 999,
    barcode       TEXT DEFAULT '',
    updated_at    TIMESTAMPTZ DEFAULT NOW()
);

-- 3-2) option_master (주문옵션 매칭용)
CREATE TABLE IF NOT EXISTS option_master (
    id              BIGSERIAL PRIMARY KEY,
    original_name   TEXT,
    product_name    TEXT,
    line_code       TEXT,
    sort_order      NUMERIC,
    barcode         TEXT,
    match_key       TEXT UNIQUE,
    last_matched_at TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- 3-3) master_prices (가격표)
CREATE TABLE IF NOT EXISTS master_prices (
    id            BIGSERIAL PRIMARY KEY,
    product_name  TEXT UNIQUE NOT NULL,
    sku           TEXT DEFAULT '',
    naver_price   INTEGER DEFAULT 0,
    coupang_price INTEGER DEFAULT 0,
    rocket_price  INTEGER DEFAULT 0,
    jasa_price    INTEGER DEFAULT 0,
    updated_at    TIMESTAMPTZ DEFAULT NOW()
);

-- 3-4) bom_master (세트옵션 BOM)
CREATE TABLE IF NOT EXISTS bom_master (
    id            BIGSERIAL PRIMARY KEY,
    channel       TEXT NOT NULL,
    set_name      TEXT NOT NULL,
    components    TEXT NOT NULL,
    updated_at    TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(channel, set_name)
);

-- 3-5) product_costs (품목별 단가)
CREATE TABLE IF NOT EXISTS product_costs (
    id                 BIGSERIAL PRIMARY KEY,
    product_name       TEXT UNIQUE NOT NULL,
    cost_price         NUMERIC NOT NULL DEFAULT 0,
    unit               TEXT DEFAULT '',
    memo               TEXT DEFAULT '',
    weight             NUMERIC DEFAULT 0,
    weight_unit        TEXT DEFAULT 'g',
    cost_type          TEXT DEFAULT '매입',
    material_type      TEXT DEFAULT '원료',
    purchase_unit      TEXT DEFAULT '',
    standard_unit      TEXT DEFAULT '',
    conversion_ratio   NUMERIC DEFAULT 1,
    food_type          TEXT DEFAULT '',
    is_stock_managed   BOOLEAN DEFAULT TRUE,
    safety_stock       INTEGER DEFAULT 0,
    lead_time_days     INTEGER DEFAULT 3,
    is_production_target BOOLEAN DEFAULT NULL,
    sales_category     TEXT DEFAULT '',
    updated_at         TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_product_costs_name ON product_costs(product_name);

-- 3-6) channel_costs (채널별 비용)
CREATE TABLE IF NOT EXISTS channel_costs (
    id           BIGSERIAL PRIMARY KEY,
    channel      TEXT UNIQUE NOT NULL,
    fee_rate     NUMERIC DEFAULT 0,
    shipping     NUMERIC DEFAULT 0,
    packaging    NUMERIC DEFAULT 0,
    other_cost   NUMERIC DEFAULT 0,
    memo         TEXT DEFAULT '',
    updated_at   TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- 4. 매출 관련 테이블
-- ============================================================

-- 4-1) daily_revenue (일일매출)
CREATE TABLE IF NOT EXISTS daily_revenue (
    id            BIGSERIAL PRIMARY KEY,
    revenue_date  DATE NOT NULL,
    product_name  TEXT NOT NULL,
    category      TEXT NOT NULL,
    channel       TEXT NOT NULL DEFAULT '',
    qty           INTEGER NOT NULL DEFAULT 0,
    unit_price    INTEGER NOT NULL DEFAULT 0,
    revenue       INTEGER NOT NULL DEFAULT 0,
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(revenue_date, product_name, category, channel)
);
CREATE INDEX IF NOT EXISTS idx_daily_revenue_date ON daily_revenue(revenue_date);
CREATE INDEX IF NOT EXISTS idx_daily_revenue_product ON daily_revenue(product_name);
CREATE INDEX IF NOT EXISTS idx_daily_revenue_channel ON daily_revenue(channel);

-- 4-2) daily_closing (일일마감)
CREATE TABLE IF NOT EXISTS daily_closing (
    id              BIGSERIAL PRIMARY KEY,
    closing_date    DATE NOT NULL,
    closing_type    TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'open',
    cutoff_time     TIME NOT NULL DEFAULT '15:05',
    closed_by       TEXT,
    closed_at       TIMESTAMPTZ,
    reopened_by     TEXT,
    reopened_at     TIMESTAMPTZ,
    memo            TEXT DEFAULT '',
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(closing_date, closing_type)
);
CREATE INDEX IF NOT EXISTS idx_daily_closing_date ON daily_closing(closing_date);

-- ============================================================
-- 5. 거래처 관련 테이블
-- ============================================================

-- 5-1) business_partners (거래처)
CREATE TABLE IF NOT EXISTS business_partners (
    id                BIGSERIAL PRIMARY KEY,
    partner_name      TEXT NOT NULL,
    business_number   TEXT DEFAULT '',
    representative    TEXT DEFAULT '',
    contact_person    TEXT DEFAULT '',
    phone             TEXT DEFAULT '',
    fax               TEXT DEFAULT '',
    email             TEXT DEFAULT '',
    address           TEXT DEFAULT '',
    business_type     TEXT DEFAULT '',
    business_item     TEXT DEFAULT '',
    memo              TEXT DEFAULT '',
    created_at        TIMESTAMPTZ DEFAULT NOW(),
    updated_at        TIMESTAMPTZ DEFAULT NOW()
);

-- 5-2) my_business (내 사업장)
CREATE TABLE IF NOT EXISTS my_business (
    id                BIGSERIAL PRIMARY KEY,
    business_name     TEXT NOT NULL,
    business_number   TEXT DEFAULT '',
    representative    TEXT DEFAULT '',
    address           TEXT DEFAULT '',
    phone             TEXT DEFAULT '',
    fax               TEXT DEFAULT '',
    business_type     TEXT DEFAULT '',
    business_item     TEXT DEFAULT '',
    is_default        BOOLEAN DEFAULT FALSE,
    created_at        TIMESTAMPTZ DEFAULT NOW(),
    updated_at        TIMESTAMPTZ DEFAULT NOW()
);

-- 5-3) manual_trades (수동 거래)
CREATE TABLE IF NOT EXISTS manual_trades (
    id              BIGSERIAL PRIMARY KEY,
    trade_date      DATE NOT NULL,
    partner_id      INTEGER,
    partner_name    TEXT NOT NULL,
    my_biz_id       INTEGER,
    my_biz_name     TEXT DEFAULT '',
    product_name    TEXT NOT NULL,
    qty             NUMERIC NOT NULL DEFAULT 0,
    unit            TEXT DEFAULT '개',
    unit_price      NUMERIC DEFAULT 0,
    supply_amount   NUMERIC DEFAULT 0,
    vat             NUMERIC DEFAULT 0,
    total_amount    NUMERIC DEFAULT 0,
    location        TEXT DEFAULT '',
    memo            TEXT DEFAULT '',
    is_outbound_done BOOLEAN DEFAULT FALSE,
    registered_by   TEXT DEFAULT '',
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- 5-4) purchase_orders (발주서 이력)
CREATE TABLE IF NOT EXISTS purchase_orders (
    id               BIGSERIAL PRIMARY KEY,
    order_date       DATE NOT NULL,
    partner_id       INTEGER,
    partner_name     TEXT NOT NULL,
    my_biz_name      TEXT,
    request_date     DATE,
    delivery_note    TEXT,
    order_manager    TEXT,
    invoice_manager  TEXT,
    manager_contact  TEXT,
    caution_text     TEXT,
    items            JSONB NOT NULL DEFAULT '[]',
    item_count       INTEGER DEFAULT 0,
    registered_by    TEXT,
    created_at       TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_purchase_orders_date ON purchase_orders(order_date);
CREATE INDEX IF NOT EXISTS idx_purchase_orders_partner ON purchase_orders(partner_name);

-- ============================================================
-- 6. 행사/쿠폰 테이블
-- ============================================================

-- 6-1) promotions (행사)
CREATE TABLE IF NOT EXISTS promotions (
    id            BIGSERIAL PRIMARY KEY,
    name          TEXT NOT NULL DEFAULT '',
    product_name  TEXT NOT NULL,
    category      TEXT NOT NULL,
    start_date    DATE NOT NULL,
    end_date      DATE NOT NULL,
    promo_price   INTEGER NOT NULL DEFAULT 0,
    memo          TEXT DEFAULT '',
    is_active     BOOLEAN NOT NULL DEFAULT TRUE,
    created_by    TEXT,
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    updated_at    TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_promotions_product ON promotions(product_name);
CREATE INDEX IF NOT EXISTS idx_promotions_dates ON promotions(start_date, end_date);

-- 6-2) coupons (쿠폰)
CREATE TABLE IF NOT EXISTS coupons (
    id              BIGSERIAL PRIMARY KEY,
    name            TEXT NOT NULL DEFAULT '',
    product_name    TEXT NOT NULL,
    category        TEXT NOT NULL,
    start_date      DATE NOT NULL,
    end_date        DATE NOT NULL,
    discount_type   TEXT NOT NULL DEFAULT '금액',
    discount_value  NUMERIC NOT NULL DEFAULT 0,
    memo            TEXT DEFAULT '',
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_by      TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_coupons_product ON coupons(product_name);
CREATE INDEX IF NOT EXISTS idx_coupons_dates ON coupons(start_date, end_date);

-- ============================================================
-- 7. 주문 파이프라인 테이블
-- ============================================================

-- 7-1) import_runs (업로드 감사)
CREATE TABLE IF NOT EXISTS import_runs (
    id            BIGSERIAL PRIMARY KEY,
    channel       TEXT NOT NULL,
    filename      TEXT,
    file_hash     TEXT,
    uploaded_by   TEXT,
    total_rows    INT DEFAULT 0,
    success_count INT DEFAULT 0,
    changed_count INT DEFAULT 0,
    fail_count    INT DEFAULT 0,
    error_summary JSONB,
    status        TEXT DEFAULT 'processing',
    created_at    TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_import_runs_channel ON import_runs(channel);
CREATE INDEX IF NOT EXISTS idx_import_runs_created ON import_runs(created_at);
CREATE INDEX IF NOT EXISTS idx_import_runs_file_hash ON import_runs(file_hash)
    WHERE file_hash IS NOT NULL;

-- 7-2) order_transactions (주문 거래 데이터)
CREATE TABLE IF NOT EXISTS order_transactions (
    id               BIGSERIAL PRIMARY KEY,
    import_run_id    BIGINT REFERENCES import_runs(id),
    channel          TEXT NOT NULL,
    order_date       DATE NOT NULL,
    order_datetime   TEXT,
    order_no         TEXT NOT NULL,
    line_no          INT NOT NULL DEFAULT 1,
    original_option  TEXT,
    original_product TEXT,
    raw_data         JSONB,
    raw_hash         TEXT,
    parser_version   TEXT DEFAULT '1.0',
    product_name     TEXT,
    barcode          TEXT,
    line_code        INT,
    sort_order       INT,
    qty              INT NOT NULL DEFAULT 1,
    unit_price       NUMERIC DEFAULT 0,
    total_amount     NUMERIC DEFAULT 0,
    discount_amount  NUMERIC DEFAULT 0,
    settlement       NUMERIC DEFAULT 0,
    commission       NUMERIC DEFAULT 0,
    shipping_fee     NUMERIC DEFAULT 0,
    status           TEXT DEFAULT '정상',
    status_reason    TEXT,
    status_changed_at TIMESTAMPTZ,
    is_outbound_done BOOLEAN DEFAULT FALSE,
    outbound_date    DATE,
    revenue_category TEXT,
    processed_at     TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(channel, order_no, line_no)
);
CREATE INDEX IF NOT EXISTS idx_ot_order_date ON order_transactions(order_date);
CREATE INDEX IF NOT EXISTS idx_ot_channel ON order_transactions(channel);
CREATE INDEX IF NOT EXISTS idx_ot_product ON order_transactions(product_name);
CREATE INDEX IF NOT EXISTS idx_ot_status ON order_transactions(status);
CREATE INDEX IF NOT EXISTS idx_ot_import_run ON order_transactions(import_run_id);
CREATE INDEX IF NOT EXISTS idx_ot_outbound_done ON order_transactions(is_outbound_done);

-- 7-3) order_shipping (배송정보 - 개인정보)
CREATE TABLE IF NOT EXISTS order_shipping (
    id               BIGSERIAL PRIMARY KEY,
    channel          TEXT NOT NULL,
    order_no         TEXT NOT NULL,
    name             TEXT,
    phone            TEXT,
    phone2           TEXT,
    address          TEXT,
    memo             TEXT,
    invoice_no       TEXT,
    courier          TEXT,
    shipping_status  TEXT DEFAULT '대기',
    created_at       TIMESTAMPTZ DEFAULT NOW(),
    expires_at       TIMESTAMPTZ,
    is_anonymized    BOOLEAN DEFAULT FALSE,
    anonymized_at    TIMESTAMPTZ,
    UNIQUE(channel, order_no)
);
CREATE INDEX IF NOT EXISTS idx_os_expires ON order_shipping(expires_at);
CREATE INDEX IF NOT EXISTS idx_os_status ON order_shipping(shipping_status);

-- 7-4) order_change_log (주문 변경 이력)
CREATE TABLE IF NOT EXISTS order_change_log (
    id                    BIGSERIAL PRIMARY KEY,
    order_transaction_id  BIGINT REFERENCES order_transactions(id),
    import_run_id         BIGINT,
    channel               TEXT,
    order_no              TEXT,
    field_name            TEXT,
    before_value          TEXT,
    after_value           TEXT,
    change_type           TEXT,
    change_reason         TEXT,
    changed_by            TEXT,
    changed_at            TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_ocl_order ON order_change_log(order_transaction_id);
CREATE INDEX IF NOT EXISTS idx_ocl_changed ON order_change_log(changed_at);

-- ============================================================
-- 8. 무결성 / 검증 테이블
-- ============================================================

-- 8-1) integrity_report (정합성 검사 보고서)
CREATE TABLE IF NOT EXISTS integrity_report (
    id              BIGSERIAL PRIMARY KEY,
    check_date      DATE NOT NULL,
    passed          BOOLEAN NOT NULL DEFAULT TRUE,
    critical_count  INTEGER DEFAULT 0,
    warning_count   INTEGER DEFAULT 0,
    info_count      INTEGER DEFAULT 0,
    summary         TEXT DEFAULT '',
    details         JSONB DEFAULT '[]',
    run_by          TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_integrity_report_date ON integrity_report(check_date);

-- 8-2) validation_log (검증 실패 이력)
CREATE TABLE IF NOT EXISTS validation_log (
    id              BIGSERIAL PRIMARY KEY,
    action          TEXT NOT NULL,
    error_code      TEXT,
    message         TEXT,
    user_id         TEXT,
    details         JSONB DEFAULT '{}',
    created_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_validation_log_action ON validation_log(action);
CREATE INDEX IF NOT EXISTS idx_validation_log_created ON validation_log(created_at);

-- ============================================================
-- 9. 생산계획 테이블
-- ============================================================

CREATE TABLE IF NOT EXISTS production_plan (
    id                       BIGSERIAL PRIMARY KEY,
    plan_date                DATE NOT NULL,
    product_name             TEXT NOT NULL,
    current_stock            NUMERIC DEFAULT 0,
    avg_daily_sales          NUMERIC DEFAULT 0,
    depletion_days           NUMERIC,
    safety_stock             INTEGER DEFAULT 0,
    lead_time_days           INTEGER DEFAULT 3,
    target_stock             NUMERIC DEFAULT 0,
    recommended_production   NUMERIC DEFAULT 0,
    status                   TEXT DEFAULT '',
    unit                     TEXT DEFAULT '개',
    created_at               TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_production_plan_date ON production_plan(plan_date);
CREATE INDEX IF NOT EXISTS idx_production_plan_product ON production_plan(product_name);
CREATE INDEX IF NOT EXISTS idx_production_plan_status ON production_plan(status);


-- ============================================================
-- 10. RPC 함수 (주문 배치 upsert + 주문 수정/취소)
-- ============================================================

-- 10-1) rpc_upsert_order_batch
CREATE OR REPLACE FUNCTION rpc_upsert_order_batch(
    p_import_run_id BIGINT,
    p_orders JSONB
)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    v_order        JSONB;
    v_txn          JSONB;
    v_ship         JSONB;
    v_existing     RECORD;
    v_inserted     INT := 0;
    v_updated      INT := 0;
    v_skipped      INT := 0;
    v_failed       INT := 0;
    v_errors       JSONB := '[]'::JSONB;
    v_idx          INT := 0;
    v_txn_id       BIGINT;
    v_field        TEXT;
    v_old_val      TEXT;
    v_new_val      TEXT;
    v_fields       TEXT[] := ARRAY[
        'order_date', 'order_datetime', 'original_option', 'original_product',
        'product_name', 'barcode', 'line_code', 'sort_order',
        'qty', 'unit_price', 'total_amount', 'discount_amount',
        'settlement', 'commission', 'shipping_fee'
    ];
BEGIN
    FOR v_order IN SELECT * FROM jsonb_array_elements(p_orders)
    LOOP
        v_idx := v_idx + 1;
        v_txn  := v_order->'transaction';
        v_ship := v_order->'shipping';

        BEGIN
            SELECT id, raw_hash, status INTO v_existing
            FROM order_transactions
            WHERE channel  = v_txn->>'channel'
              AND order_no = v_txn->>'order_no'
              AND line_no  = COALESCE((v_txn->>'line_no')::INT, 1);

            IF v_existing IS NOT NULL THEN
                IF v_existing.status IN ('취소', '환불') THEN
                    v_skipped := v_skipped + 1;
                    CONTINUE;
                END IF;
                IF v_existing.raw_hash IS NOT NULL
                   AND v_existing.raw_hash = v_txn->>'raw_hash' THEN
                    v_skipped := v_skipped + 1;
                    CONTINUE;
                END IF;

                v_txn_id := v_existing.id;

                FOREACH v_field IN ARRAY v_fields
                LOOP
                    SELECT
                        CASE v_field
                            WHEN 'order_date'        THEN ot.order_date::TEXT
                            WHEN 'original_option'   THEN ot.original_option
                            WHEN 'original_product'  THEN ot.original_product
                            WHEN 'product_name'      THEN ot.product_name
                            WHEN 'barcode'           THEN ot.barcode
                            WHEN 'line_code'         THEN ot.line_code::TEXT
                            WHEN 'sort_order'        THEN ot.sort_order::TEXT
                            WHEN 'qty'               THEN ot.qty::TEXT
                            WHEN 'unit_price'        THEN ot.unit_price::TEXT
                            WHEN 'total_amount'      THEN ot.total_amount::TEXT
                            WHEN 'discount_amount'   THEN ot.discount_amount::TEXT
                            WHEN 'settlement'        THEN ot.settlement::TEXT
                            WHEN 'commission'        THEN ot.commission::TEXT
                            WHEN 'shipping_fee'      THEN ot.shipping_fee::TEXT
                            WHEN 'order_datetime'    THEN ot.order_datetime
                        END
                    INTO v_old_val
                    FROM order_transactions ot
                    WHERE ot.id = v_txn_id;

                    v_new_val := v_txn->>v_field;

                    IF v_old_val IS DISTINCT FROM v_new_val THEN
                        INSERT INTO order_change_log (
                            order_transaction_id, import_run_id, channel, order_no,
                            field_name, before_value, after_value,
                            change_type, changed_by
                        ) VALUES (
                            v_txn_id, p_import_run_id,
                            v_txn->>'channel', v_txn->>'order_no',
                            v_field, v_old_val, v_new_val,
                            'upsert_변경', 'system'
                        );
                    END IF;
                END LOOP;

                UPDATE order_transactions SET
                    import_run_id    = p_import_run_id,
                    order_date       = (v_txn->>'order_date')::DATE,
                    order_datetime   = v_txn->>'order_datetime',
                    original_option  = v_txn->>'original_option',
                    original_product = v_txn->>'original_product',
                    raw_data         = (v_txn->'raw_data'),
                    raw_hash         = v_txn->>'raw_hash',
                    parser_version   = COALESCE(v_txn->>'parser_version', '1.0'),
                    product_name     = v_txn->>'product_name',
                    barcode          = v_txn->>'barcode',
                    line_code        = (v_txn->>'line_code')::INT,
                    sort_order       = (v_txn->>'sort_order')::INT,
                    qty              = COALESCE((v_txn->>'qty')::INT, 1),
                    unit_price       = COALESCE((v_txn->>'unit_price')::NUMERIC, 0),
                    total_amount     = COALESCE((v_txn->>'total_amount')::NUMERIC, 0),
                    discount_amount  = COALESCE((v_txn->>'discount_amount')::NUMERIC, 0),
                    settlement       = COALESCE((v_txn->>'settlement')::NUMERIC, 0),
                    commission       = COALESCE((v_txn->>'commission')::NUMERIC, 0),
                    shipping_fee     = COALESCE((v_txn->>'shipping_fee')::NUMERIC, 0),
                    processed_at     = now()
                WHERE id = v_txn_id;

                v_updated := v_updated + 1;
            ELSE
                INSERT INTO order_transactions (
                    import_run_id, channel, order_date, order_datetime, order_no, line_no,
                    original_option, original_product,
                    raw_data, raw_hash, parser_version,
                    product_name, barcode, line_code, sort_order,
                    qty, unit_price, total_amount, discount_amount,
                    settlement, commission, shipping_fee
                ) VALUES (
                    p_import_run_id,
                    v_txn->>'channel',
                    (v_txn->>'order_date')::DATE,
                    v_txn->>'order_datetime',
                    v_txn->>'order_no',
                    COALESCE((v_txn->>'line_no')::INT, 1),
                    v_txn->>'original_option',
                    v_txn->>'original_product',
                    (v_txn->'raw_data'),
                    v_txn->>'raw_hash',
                    COALESCE(v_txn->>'parser_version', '1.0'),
                    v_txn->>'product_name',
                    v_txn->>'barcode',
                    (v_txn->>'line_code')::INT,
                    (v_txn->>'sort_order')::INT,
                    COALESCE((v_txn->>'qty')::INT, 1),
                    COALESCE((v_txn->>'unit_price')::NUMERIC, 0),
                    COALESCE((v_txn->>'total_amount')::NUMERIC, 0),
                    COALESCE((v_txn->>'discount_amount')::NUMERIC, 0),
                    COALESCE((v_txn->>'settlement')::NUMERIC, 0),
                    COALESCE((v_txn->>'commission')::NUMERIC, 0),
                    COALESCE((v_txn->>'shipping_fee')::NUMERIC, 0)
                );
                v_inserted := v_inserted + 1;
            END IF;

            IF v_ship IS NOT NULL AND v_ship->>'name' IS NOT NULL THEN
                INSERT INTO order_shipping (
                    channel, order_no,
                    name, phone, phone2, address, memo,
                    expires_at
                ) VALUES (
                    v_txn->>'channel',
                    v_txn->>'order_no',
                    v_ship->>'name',
                    v_ship->>'phone',
                    v_ship->>'phone2',
                    v_ship->>'address',
                    v_ship->>'memo',
                    now() + INTERVAL '6 months'
                )
                ON CONFLICT (channel, order_no) DO UPDATE SET
                    name    = EXCLUDED.name,
                    phone   = EXCLUDED.phone,
                    phone2  = EXCLUDED.phone2,
                    address = EXCLUDED.address,
                    memo    = EXCLUDED.memo;
            END IF;

        EXCEPTION WHEN OTHERS THEN
            v_failed := v_failed + 1;
            v_errors := v_errors || jsonb_build_object(
                'row', v_idx,
                'order_no', v_txn->>'order_no',
                'error', SQLERRM
            );
        END;
    END LOOP;

    UPDATE import_runs SET
        success_count = v_inserted + v_updated,
        changed_count = v_updated,
        fail_count    = v_failed,
        error_summary = CASE WHEN v_failed > 0 THEN v_errors ELSE NULL END,
        status        = CASE
                          WHEN v_failed = 0 THEN 'completed'
                          WHEN v_inserted + v_updated > 0 THEN 'partial'
                          ELSE 'failed'
                        END
    WHERE id = p_import_run_id;

    RETURN jsonb_build_object(
        'inserted', v_inserted,
        'updated',  v_updated,
        'skipped',  v_skipped,
        'failed',   v_failed,
        'errors',   v_errors
    );
END;
$$;


-- 10-2) rpc_cancel_or_edit_order
CREATE OR REPLACE FUNCTION rpc_cancel_or_edit_order(
    p_order_id   BIGINT,
    p_change_type TEXT,
    p_payload    JSONB,
    p_reason     TEXT,
    p_user       TEXT
)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    v_existing   RECORD;
    v_field      TEXT;
    v_old_val    TEXT;
    v_new_val    TEXT;
BEGIN
    SELECT * INTO v_existing
    FROM order_transactions
    WHERE id = p_order_id;

    IF v_existing IS NULL THEN
        RETURN jsonb_build_object('success', false, 'error', '주문을 찾을 수 없습니다');
    END IF;

    IF v_existing.status IN ('취소', '환불') AND p_change_type IN ('취소', '환불') THEN
        RETURN jsonb_build_object('success', false, 'error', '이미 ' || v_existing.status || ' 처리된 주문입니다');
    END IF;

    IF p_change_type = '취소' OR p_change_type = '환불' THEN
        INSERT INTO order_change_log (
            order_transaction_id, channel, order_no,
            field_name, before_value, after_value,
            change_type, change_reason, changed_by
        ) VALUES (
            p_order_id, v_existing.channel, v_existing.order_no,
            'status', v_existing.status, p_change_type,
            p_change_type, p_reason, p_user
        );

        UPDATE order_transactions SET
            status            = p_change_type,
            status_reason     = p_reason,
            status_changed_at = now()
        WHERE id = p_order_id;

        UPDATE order_shipping SET
            shipping_status = '취소'
        WHERE channel  = v_existing.channel
          AND order_no = v_existing.order_no;

    ELSIF p_change_type = '수정' THEN
        FOR v_field, v_new_val IN
            SELECT key, value#>>'{}'
            FROM jsonb_each(p_payload)
        LOOP
            v_old_val := CASE v_field
                WHEN 'qty'             THEN v_existing.qty::TEXT
                WHEN 'unit_price'      THEN v_existing.unit_price::TEXT
                WHEN 'total_amount'    THEN v_existing.total_amount::TEXT
                WHEN 'discount_amount' THEN v_existing.discount_amount::TEXT
                WHEN 'product_name'    THEN v_existing.product_name
                WHEN 'order_date'      THEN v_existing.order_date::TEXT
                ELSE NULL
            END;

            IF v_old_val IS DISTINCT FROM v_new_val THEN
                INSERT INTO order_change_log (
                    order_transaction_id, channel, order_no,
                    field_name, before_value, after_value,
                    change_type, change_reason, changed_by
                ) VALUES (
                    p_order_id, v_existing.channel, v_existing.order_no,
                    v_field, v_old_val, v_new_val,
                    '수정', p_reason, p_user
                );
            END IF;
        END LOOP;

        UPDATE order_transactions SET
            qty             = COALESCE((p_payload->>'qty')::INT, qty),
            unit_price      = COALESCE((p_payload->>'unit_price')::NUMERIC, unit_price),
            total_amount    = COALESCE((p_payload->>'total_amount')::NUMERIC, total_amount),
            discount_amount = COALESCE((p_payload->>'discount_amount')::NUMERIC, discount_amount),
            product_name    = COALESCE(p_payload->>'product_name', product_name),
            order_date      = COALESCE((p_payload->>'order_date')::DATE, order_date),
            status_changed_at = now()
        WHERE id = p_order_id;

    ELSE
        RETURN jsonb_build_object('success', false, 'error', '올바르지 않은 change_type: ' || p_change_type);
    END IF;

    RETURN jsonb_build_object('success', true, 'change_type', p_change_type, 'order_id', p_order_id);
END;
$$;


-- 10-3) ensure_stock_ledger_columns (컬럼 마이그레이션용)
CREATE OR REPLACE FUNCTION ensure_stock_ledger_columns()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    -- 이 함수는 앱 시작 시 호출되어 stock_ledger에 필요한 컬럼이 있는지 확인합니다
    -- CREATE TABLE IF NOT EXISTS로 이미 모든 컬럼이 생성되므로 별도 작업 불필요
    RETURN;
END;
$$;


-- ============================================================
-- 11. 권한 부여 (anon/authenticated)
-- ============================================================
GRANT EXECUTE ON FUNCTION rpc_upsert_order_batch(BIGINT, JSONB) TO anon, authenticated;
GRANT EXECUTE ON FUNCTION rpc_cancel_or_edit_order(BIGINT, TEXT, JSONB, TEXT, TEXT) TO anon, authenticated;
GRANT EXECUTE ON FUNCTION ensure_stock_ledger_columns() TO anon, authenticated;


-- ============================================================
-- 12. RLS 비활성화 (anon key CRUD 허용)
-- ============================================================
ALTER TABLE app_users DISABLE ROW LEVEL SECURITY;
ALTER TABLE audit_logs DISABLE ROW LEVEL SECURITY;
ALTER TABLE role_permissions DISABLE ROW LEVEL SECURITY;
ALTER TABLE stock_ledger DISABLE ROW LEVEL SECURITY;
ALTER TABLE master_products DISABLE ROW LEVEL SECURITY;
ALTER TABLE option_master DISABLE ROW LEVEL SECURITY;
ALTER TABLE master_prices DISABLE ROW LEVEL SECURITY;
ALTER TABLE bom_master DISABLE ROW LEVEL SECURITY;
ALTER TABLE product_costs DISABLE ROW LEVEL SECURITY;
ALTER TABLE channel_costs DISABLE ROW LEVEL SECURITY;
ALTER TABLE daily_revenue DISABLE ROW LEVEL SECURITY;
ALTER TABLE daily_closing DISABLE ROW LEVEL SECURITY;
ALTER TABLE business_partners DISABLE ROW LEVEL SECURITY;
ALTER TABLE my_business DISABLE ROW LEVEL SECURITY;
ALTER TABLE manual_trades DISABLE ROW LEVEL SECURITY;
ALTER TABLE purchase_orders DISABLE ROW LEVEL SECURITY;
ALTER TABLE promotions DISABLE ROW LEVEL SECURITY;
ALTER TABLE coupons DISABLE ROW LEVEL SECURITY;
ALTER TABLE import_runs DISABLE ROW LEVEL SECURITY;
ALTER TABLE order_transactions DISABLE ROW LEVEL SECURITY;
ALTER TABLE order_shipping DISABLE ROW LEVEL SECURITY;
ALTER TABLE order_change_log DISABLE ROW LEVEL SECURITY;
ALTER TABLE integrity_report DISABLE ROW LEVEL SECURITY;
ALTER TABLE validation_log DISABLE ROW LEVEL SECURITY;
ALTER TABLE production_plan DISABLE ROW LEVEL SECURITY;

-- ============================================================
-- 완료!
-- ============================================================
SELECT '쿡대디 테이블 생성 완료 (25개 테이블 + 3개 RPC 함수)' AS status;
