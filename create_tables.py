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

-- 7) product_costs 확장: 중량 컬럼 추가
ALTER TABLE product_costs ADD COLUMN IF NOT EXISTS weight NUMERIC DEFAULT 0;
ALTER TABLE product_costs ADD COLUMN IF NOT EXISTS weight_unit TEXT DEFAULT 'g';
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
