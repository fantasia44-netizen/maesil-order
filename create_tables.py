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
-- 1) daily_revenue (일일매출)
CREATE TABLE IF NOT EXISTS daily_revenue (
    id            BIGSERIAL PRIMARY KEY,
    revenue_date  DATE NOT NULL,
    product_name  TEXT NOT NULL,
    category      TEXT NOT NULL,
    qty           INTEGER NOT NULL DEFAULT 0,
    unit_price    INTEGER NOT NULL DEFAULT 0,
    revenue       INTEGER NOT NULL DEFAULT 0,
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(revenue_date, product_name, category)
);
CREATE INDEX IF NOT EXISTS idx_daily_revenue_date ON daily_revenue(revenue_date);
CREATE INDEX IF NOT EXISTS idx_daily_revenue_product ON daily_revenue(product_name);

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
for t in ["daily_revenue", "product_master", "price_master", "bom_master", "product_costs", "channel_costs"]:
    exists = table_exists(t)
    status = "✅ 존재" if exists else "❌ 없음 (생성 필요)"
    print(f"  {t}: {status}")
