-- ============================================================
-- 상품 통합 마스터 테이블 (products)
-- product_costs + master_prices → 단일 테이블
-- Supabase SQL Editor에서 실행
-- ============================================================

-- 1. products 테이블 생성
CREATE TABLE IF NOT EXISTS products (
    id BIGSERIAL PRIMARY KEY,
    product_name TEXT NOT NULL UNIQUE,
    barcode TEXT DEFAULT '',
    sku TEXT DEFAULT '',

    -- 분류
    material_type TEXT DEFAULT '완제품',
    food_type TEXT DEFAULT '',
    storage_method TEXT DEFAULT '',

    -- 원가
    cost_price NUMERIC DEFAULT 0,
    cost_type TEXT DEFAULT '매입',

    -- 판매가
    naver_price NUMERIC DEFAULT 0,
    coupang_price NUMERIC DEFAULT 0,
    rocket_price NUMERIC DEFAULT 0,
    self_mall_price NUMERIC DEFAULT 0,

    -- 규격
    unit TEXT DEFAULT '개',
    weight NUMERIC DEFAULT 0,
    weight_unit TEXT DEFAULT 'g',
    purchase_unit TEXT DEFAULT '',
    standard_unit TEXT DEFAULT '',
    conversion_ratio NUMERIC DEFAULT 1,

    -- 관리
    is_active BOOLEAN DEFAULT TRUE,
    memo TEXT DEFAULT '',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- 2. 데이터 병합: product_costs + master_prices → products
INSERT INTO products (
    product_name, barcode, sku,
    material_type, food_type, storage_method,
    cost_price, cost_type,
    naver_price, coupang_price, rocket_price, self_mall_price,
    unit, weight, weight_unit, purchase_unit, standard_unit, conversion_ratio,
    memo
)
SELECT
    pc.product_name,
    COALESCE(pc.barcode, ''),
    COALESCE(mp."SKU", ''),
    COALESCE(pc.material_type, '완제품'),
    COALESCE(pc.food_type, ''),
    COALESCE(pc.storage_method, ''),
    COALESCE(pc.cost_price, 0),
    COALESCE(pc.cost_type, '매입'),
    COALESCE(mp."네이버판매가", 0),
    COALESCE(mp."쿠팡판매가", 0),
    COALESCE(mp."로켓판매가", 0),
    COALESCE(mp."자사몰판매가", 0),
    COALESCE(pc.unit, '개'),
    COALESCE(pc.weight, 0),
    COALESCE(pc.weight_unit, 'g'),
    COALESCE(pc.purchase_unit, ''),
    COALESCE(pc.standard_unit, ''),
    COALESCE(pc.conversion_ratio, 1),
    COALESCE(pc.memo, '')
FROM product_costs pc
LEFT JOIN master_prices mp ON pc.product_name = mp."품목명"
WHERE pc.is_deleted IS NOT TRUE
ON CONFLICT (product_name) DO NOTHING;

-- master_prices에만 있는 상품 (product_costs에 없는 것)
INSERT INTO products (product_name, sku, naver_price, coupang_price, rocket_price, self_mall_price)
SELECT
    mp."품목명",
    COALESCE(mp."SKU", ''),
    COALESCE(mp."네이버판매가", 0),
    COALESCE(mp."쿠팡판매가", 0),
    COALESCE(mp."로켓판매가", 0),
    COALESCE(mp."자사몰판매가", 0)
FROM master_prices mp
WHERE NOT EXISTS (SELECT 1 FROM products p WHERE p.product_name = mp."품목명")
ON CONFLICT (product_name) DO NOTHING;

-- 3. 인덱스
CREATE INDEX IF NOT EXISTS idx_products_material_type ON products(material_type);
CREATE INDEX IF NOT EXISTS idx_products_is_active ON products(is_active);
