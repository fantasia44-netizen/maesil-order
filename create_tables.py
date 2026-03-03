"""Supabase 테이블 생성 스크립트 (1회성)
daily_revenue, product_master, price_master, bom_master 테이블을 생성합니다.
이미 존재하는 테이블은 건너뜁니다.

사용법: python create_tables.py
"""
from supabase import create_client

SUPABASE_URL = "https://pbocckpuiyzijspqpvqz.supabase.co"
SUPABASE_KEY = "sb_publishable_5TAy2FEAWeRmRCbOz6S14g_x4a8aOYI"

db = create_client(SUPABASE_URL, SUPABASE_KEY)

# 테이블 존재 확인 함수
def table_exists(table_name):
    try:
        db.table(table_name).select("id").limit(1).execute()
        return True
    except:
        return False

# 테이블별 테스트 삽입으로 자동 생성 (Supabase는 REST API로 CREATE TABLE 불가)
# → Supabase 대시보드 SQL Editor에서 실행해야 합니다.

SQL = """
-- 1) daily_revenue (일일매출) — channel 컬럼 추가
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

-- 1-b) daily_closing (일일마감)
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

-- 2) product_master (옵션마스터)
CREATE TABLE IF NOT EXISTS product_master (
    id            BIGSERIAL PRIMARY KEY,
    product_name  TEXT UNIQUE NOT NULL,
    line_code     TEXT DEFAULT '0',
    sort_order    INTEGER DEFAULT 999,
    barcode       TEXT DEFAULT '',
    updated_at    TIMESTAMPTZ DEFAULT NOW()
);

-- 3) price_master (가격표)
CREATE TABLE IF NOT EXISTS price_master (
    id            BIGSERIAL PRIMARY KEY,
    product_name  TEXT UNIQUE NOT NULL,
    sku           TEXT DEFAULT '',
    naver_price   INTEGER DEFAULT 0,
    coupang_price INTEGER DEFAULT 0,
    rocket_price  INTEGER DEFAULT 0,
    updated_at    TIMESTAMPTZ DEFAULT NOW()
);

-- 4) bom_master (세트옵션 BOM)
CREATE TABLE IF NOT EXISTS bom_master (
    id            BIGSERIAL PRIMARY KEY,
    channel       TEXT NOT NULL,
    set_name      TEXT NOT NULL,
    components    TEXT NOT NULL,
    updated_at    TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(channel, set_name)
);

-- 5) product_costs (품목별 매입단가)
CREATE TABLE IF NOT EXISTS product_costs (
    id            BIGSERIAL PRIMARY KEY,
    product_name  TEXT UNIQUE NOT NULL,
    cost_price    NUMERIC NOT NULL DEFAULT 0,
    unit          TEXT DEFAULT '',
    memo          TEXT DEFAULT '',
    updated_at    TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_product_costs_name ON product_costs(product_name);

-- 6) channel_costs (채널별 비용: 수수료/배송비/포장비/기타)
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

-- 7) product_costs 확장: 중량 + 유형 컬럼 추가
ALTER TABLE product_costs ADD COLUMN IF NOT EXISTS weight NUMERIC DEFAULT 0;
ALTER TABLE product_costs ADD COLUMN IF NOT EXISTS weight_unit TEXT DEFAULT 'g';
ALTER TABLE product_costs ADD COLUMN IF NOT EXISTS cost_type TEXT DEFAULT '매입';
-- cost_type: '매입' = 원재료(구매품), '생산' = 완제품(생산품)

-- 8) product_costs 확장: 종류(material_type) 컬럼 추가
ALTER TABLE product_costs ADD COLUMN IF NOT EXISTS material_type TEXT DEFAULT '원료';
-- material_type: '원료', '부재료', '반제품', '완제품', '포장재'

-- 마이그레이션: 기존 생산 유형은 완제품으로 전환
UPDATE product_costs SET material_type = '완제품' WHERE cost_type = '생산' AND material_type = '원료';

-- 8-1) product_costs 확장: 매입단위/사용단위/변환비율 (Phase 1 원부자재 일원화)
ALTER TABLE product_costs ADD COLUMN IF NOT EXISTS purchase_unit TEXT DEFAULT '';
ALTER TABLE product_costs ADD COLUMN IF NOT EXISTS standard_unit TEXT DEFAULT '';
ALTER TABLE product_costs ADD COLUMN IF NOT EXISTS conversion_ratio NUMERIC DEFAULT 1;
-- purchase_unit: 매입단위 (박스, 포대, kg 등)
-- standard_unit: 사용단위 (g, 개, kg 등)
-- conversion_ratio: 1 매입단위 = X 사용단위 (예: 1박스=10,000g → 10000)

-- 9) stock_ledger 확장: batch_id 컬럼 추가 (생산 배치 연결용)
-- PRODUCTION과 PROD_OUT을 정확히 연결하여 수율 계산 정확도 향상
ALTER TABLE stock_ledger ADD COLUMN IF NOT EXISTS batch_id TEXT;
CREATE INDEX IF NOT EXISTS idx_stock_ledger_batch ON stock_ledger(batch_id);

-- 10) audit_logs 확장: 상세 변경 이력 + 롤백 지원
ALTER TABLE audit_logs ADD COLUMN IF NOT EXISTS user_name TEXT;
ALTER TABLE audit_logs ADD COLUMN IF NOT EXISTS old_value JSONB;
ALTER TABLE audit_logs ADD COLUMN IF NOT EXISTS new_value JSONB;
ALTER TABLE audit_logs ADD COLUMN IF NOT EXISTS is_reverted BOOLEAN DEFAULT FALSE;
ALTER TABLE audit_logs ADD COLUMN IF NOT EXISTS reverted_by INTEGER;
ALTER TABLE audit_logs ADD COLUMN IF NOT EXISTS reverted_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS idx_audit_logs_action ON audit_logs(action);
CREATE INDEX IF NOT EXISTS idx_audit_logs_user ON audit_logs(user_id);

-- 11) stock_ledger 소프트 삭제 지원 (삭제 방지 + 복원 가능)
ALTER TABLE stock_ledger ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN DEFAULT FALSE;
ALTER TABLE stock_ledger ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;
ALTER TABLE stock_ledger ADD COLUMN IF NOT EXISTS deleted_by TEXT;

-- 12) purchase_orders (발주서 이력)
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

-- business_partners: 담당자 컬럼 추가 (대표자 = representative, 담당자 = contact_person)
ALTER TABLE business_partners ADD COLUMN IF NOT EXISTS contact_person TEXT;

-- 13) role_permissions (부서별 메뉴 접근 권한)
CREATE TABLE IF NOT EXISTS role_permissions (
    id          BIGSERIAL PRIMARY KEY,
    role        TEXT NOT NULL,
    page_key    TEXT NOT NULL,
    is_allowed  BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(role, page_key)
);
CREATE INDEX IF NOT EXISTS idx_role_perm_role ON role_permissions(role);

-- 14) promotions (행사등록: 품목+채널+기간 → 판매가 조정)
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

-- 15) coupons (쿠폰등록: 품목+채널+기간 → 할인 적용)
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

-- 16) product_costs 확장: 식품유형 (food_type) 컬럼 추가
ALTER TABLE product_costs ADD COLUMN IF NOT EXISTS food_type TEXT DEFAULT '';
-- food_type: '농산물', '수산물', '축산물', '' (미지정)

-- 17) stock_ledger 확장: 식품유형 (food_type) 컬럼 추가
ALTER TABLE stock_ledger ADD COLUMN IF NOT EXISTS food_type TEXT DEFAULT '';
CREATE INDEX IF NOT EXISTS idx_stock_ledger_food_type ON stock_ledger(food_type);

-- 18) stock_ledger qty 소수점 지원 (kg 단위 소수점 수량)
ALTER TABLE stock_ledger ALTER COLUMN qty TYPE NUMERIC USING qty::NUMERIC;

-- ============================================================
-- Phase 1: 주문 수집 파이프라인 테이블 (2026-03)
-- ============================================================

-- 19) import_runs (업로드 감사/증거)
CREATE TABLE IF NOT EXISTS import_runs (
    id            BIGSERIAL PRIMARY KEY,
    channel       TEXT NOT NULL,
    filename      TEXT,
    file_hash     TEXT,                        -- SHA256 (중복 파일 감지)
    uploaded_by   TEXT,
    total_rows    INT DEFAULT 0,
    success_count INT DEFAULT 0,
    changed_count INT DEFAULT 0,               -- upsert 시 변경된 건수
    fail_count    INT DEFAULT 0,
    error_summary JSONB,                       -- [{row: 5, error: '옵션매칭실패'}, ...]
    status        TEXT DEFAULT 'processing',   -- processing/completed/partial/failed
    created_at    TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_import_runs_channel ON import_runs(channel);
CREATE INDEX IF NOT EXISTS idx_import_runs_created ON import_runs(created_at);

-- 20) order_transactions (거래 데이터 - 영구)
CREATE TABLE IF NOT EXISTS order_transactions (
    id               BIGSERIAL PRIMARY KEY,
    import_run_id    BIGINT REFERENCES import_runs(id),
    channel          TEXT NOT NULL,
    order_date       DATE NOT NULL,
    order_no         TEXT NOT NULL,
    line_no          INT NOT NULL DEFAULT 1,

    -- 원본 보존
    original_option  TEXT,
    original_product TEXT,
    raw_data         JSONB,                    -- 해당 행 전체 원본
    raw_hash         TEXT,                     -- 행 데이터 해시 (변경 감지)
    parser_version   TEXT DEFAULT '1.0',

    -- 매칭 결과
    product_name     TEXT,
    barcode          TEXT,
    line_code        INT,
    sort_order       INT,

    -- 금액
    qty              INT NOT NULL DEFAULT 1,
    unit_price       NUMERIC DEFAULT 0,
    total_amount     NUMERIC DEFAULT 0,
    discount_amount  NUMERIC DEFAULT 0,
    settlement       NUMERIC DEFAULT 0,
    commission       NUMERIC DEFAULT 0,

    -- 상태
    status           TEXT DEFAULT '정상',      -- 정상/취소/환불
    status_reason    TEXT,
    status_changed_at TIMESTAMPTZ,

    processed_at     TIMESTAMPTZ DEFAULT now(),

    UNIQUE(channel, order_no, line_no)
);
CREATE INDEX IF NOT EXISTS idx_ot_order_date ON order_transactions(order_date);
CREATE INDEX IF NOT EXISTS idx_ot_channel ON order_transactions(channel);
CREATE INDEX IF NOT EXISTS idx_ot_product ON order_transactions(product_name);
CREATE INDEX IF NOT EXISTS idx_ot_status ON order_transactions(status);
CREATE INDEX IF NOT EXISTS idx_ot_import_run ON order_transactions(import_run_id);

-- 21) order_shipping (개인정보 - 6개월 후 익명화)
CREATE TABLE IF NOT EXISTS order_shipping (
    id               BIGSERIAL PRIMARY KEY,
    channel          TEXT NOT NULL,
    order_no         TEXT NOT NULL,

    -- PII
    name             TEXT,
    phone            TEXT,
    phone2           TEXT,
    address          TEXT,
    memo             TEXT,

    -- 배송 처리
    invoice_no       TEXT,
    courier          TEXT,
    shipping_status  TEXT DEFAULT '대기',       -- 대기/발송/완료/취소

    -- 보관 정책
    created_at       TIMESTAMPTZ DEFAULT now(),
    expires_at       TIMESTAMPTZ,              -- created_at + 6개월
    is_anonymized    BOOLEAN DEFAULT false,
    anonymized_at    TIMESTAMPTZ,

    UNIQUE(channel, order_no)
);
CREATE INDEX IF NOT EXISTS idx_os_expires ON order_shipping(expires_at);
CREATE INDEX IF NOT EXISTS idx_os_status ON order_shipping(shipping_status);

-- 22) order_change_log (변경 이력)
CREATE TABLE IF NOT EXISTS order_change_log (
    id                    BIGSERIAL PRIMARY KEY,
    order_transaction_id  BIGINT REFERENCES order_transactions(id),
    import_run_id         BIGINT,              -- upsert 변경 시 어느 업로드에서 발생
    channel               TEXT,
    order_no              TEXT,
    field_name            TEXT,
    before_value          TEXT,
    after_value           TEXT,
    change_type           TEXT,                -- upsert_변경/수정/취소/환불
    change_reason         TEXT,
    changed_by            TEXT,
    changed_at            TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_ocl_order ON order_change_log(order_transaction_id);
CREATE INDEX IF NOT EXISTS idx_ocl_changed ON order_change_log(changed_at);

-- ============================================================
-- Phase 2: order_transactions 확장 (출고/매출 자동처리용)
-- ============================================================
ALTER TABLE order_transactions ADD COLUMN IF NOT EXISTS
    is_outbound_done BOOLEAN DEFAULT false;
ALTER TABLE order_transactions ADD COLUMN IF NOT EXISTS
    outbound_date DATE;
ALTER TABLE order_transactions ADD COLUMN IF NOT EXISTS
    revenue_category TEXT;

CREATE INDEX IF NOT EXISTS idx_ot_outbound_done ON order_transactions(is_outbound_done);

-- ============================================================
-- Phase 3: stock_ledger 중복 방지 + 역분개 추적 (2026-03)
-- ============================================================

-- event_uid: 재고 이벤트 고유 식별자 (중복 차감 방지)
-- 형식: "SO:{order_transaction_id}:{product_name}" 또는 "IN:{날짜}:{product}"
ALTER TABLE stock_ledger ADD COLUMN IF NOT EXISTS event_uid TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS idx_stock_ledger_event_uid
    ON stock_ledger(event_uid) WHERE event_uid IS NOT NULL;

-- ref_event_uid: 역분개 시 원본 이벤트 참조
-- SALES_RETURN의 ref_event_uid → 원본 SALES_OUT의 event_uid
ALTER TABLE stock_ledger ADD COLUMN IF NOT EXISTS ref_event_uid TEXT;
CREATE INDEX IF NOT EXISTS idx_stock_ledger_ref_event ON stock_ledger(ref_event_uid)
    WHERE ref_event_uid IS NOT NULL;

-- ============================================================
-- Phase 3b: order_transactions 추가 컬럼 (2026-03)
-- ============================================================

-- order_datetime: 원본 주문일시 보존 (시간 포함, 채널별 주문일시/결제일시)
ALTER TABLE order_transactions ADD COLUMN IF NOT EXISTS
    order_datetime TEXT;

-- shipping_fee: 배송비 (주문서 원본 금액)
ALTER TABLE order_transactions ADD COLUMN IF NOT EXISTS
    shipping_fee NUMERIC DEFAULT 0;

-- ============================================================
-- Phase 4: 데이터 무결성 보호 계층 (2026-03)
-- ============================================================

-- 23) integrity_report (정합성 검사 결과)
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
CREATE INDEX IF NOT EXISTS idx_integrity_report_date
    ON integrity_report(check_date);

-- 24) stock_ledger 확장: transfer_id (이동 원자성 추적)
ALTER TABLE stock_ledger ADD COLUMN IF NOT EXISTS transfer_id TEXT;
CREATE INDEX IF NOT EXISTS idx_stock_ledger_transfer_id
    ON stock_ledger(transfer_id) WHERE transfer_id IS NOT NULL;

-- 25) import_runs file_hash 빠른 검색
CREATE INDEX IF NOT EXISTS idx_import_runs_file_hash
    ON import_runs(file_hash) WHERE file_hash IS NOT NULL;

-- 26) validation_log (검증 실패 이력)
CREATE TABLE IF NOT EXISTS validation_log (
    id              BIGSERIAL PRIMARY KEY,
    action          TEXT NOT NULL,
    error_code      TEXT,
    message         TEXT,
    user_id         TEXT,
    details         JSONB DEFAULT '{}',
    created_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_validation_log_action
    ON validation_log(action);
CREATE INDEX IF NOT EXISTS idx_validation_log_created
    ON validation_log(created_at);

-- ============================================================
-- Phase 5: 수량 기반 생산계획 엔진 (2026-03)
-- ============================================================

-- 27) product_costs 확장: 생산계획 설정
ALTER TABLE product_costs ADD COLUMN IF NOT EXISTS safety_stock INTEGER DEFAULT 0;
ALTER TABLE product_costs ADD COLUMN IF NOT EXISTS lead_time_days INTEGER DEFAULT 3;
ALTER TABLE product_costs ADD COLUMN IF NOT EXISTS is_production_target BOOLEAN DEFAULT NULL;
-- NULL = 자동 판별 (cost_type='생산' or material_type='완제품' → 대상)
-- TRUE = 강제 포함, FALSE = 강제 제외

-- 28) production_plan (생산계획 결과)
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
CREATE INDEX IF NOT EXISTS idx_production_plan_date
    ON production_plan(plan_date);
CREATE INDEX IF NOT EXISTS idx_production_plan_product
    ON production_plan(product_name);
CREATE INDEX IF NOT EXISTS idx_production_plan_status
    ON production_plan(status);
"""

print("=" * 60)
print("Supabase 테이블 생성 SQL")
print("=" * 60)
print()
print("아래 SQL을 Supabase 대시보드 > SQL Editor에서 실행하세요:")
print("URL: https://supabase.com/dashboard/project/pbocckpuiyzijspqpvqz/sql")
print()
print(SQL)
print("=" * 60)

# 현재 테이블 상태 확인
print("\n📊 현재 테이블 상태:")
for t in ["daily_revenue", "daily_closing", "product_master", "price_master", "bom_master", "product_costs", "channel_costs",
          "import_runs", "order_transactions", "order_shipping", "order_change_log"]:
    exists = table_exists(t)
    status = "✅ 존재" if exists else "❌ 없음 (생성 필요)"
    print(f"  {t}: {status}")
