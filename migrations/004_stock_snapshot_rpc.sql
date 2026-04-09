-- ═══════════════════════════════════════════════════════════
-- 004: 재고 현황 스냅샷 SQL RPC — Phase 3 (autotool)
--
-- 대상: services/stock_service.py query_stock_snapshot
--       stock_ledger 전체 풀스캔 → pandas DataFrame groupby
--       수만 행 메모리 피크. SaaS 확장 차단.
--
-- 전략: (product_name, location, category, storage_method, unit)
--       기준 SUM(qty) 을 SQL에서 집계.
--       split_manufacture/split_expiry/split_lot_number 별도 파라미터 지원.
--       category/storage_method 빈값 상속은 SQL window function으로 처리.
--
-- 신규 RPC:
--   get_stock_snapshot_agg(date_to, split_mode) → [stock rows]
--     split_mode: 'none' | 'expiry' | 'manufacture' | 'lot_number'
-- ═══════════════════════════════════════════════════════════

DROP FUNCTION IF EXISTS get_stock_snapshot_agg(DATE, TEXT);

CREATE OR REPLACE FUNCTION get_stock_snapshot_agg(
    p_date_to DATE,
    p_split_mode TEXT DEFAULT 'none'
) RETURNS TABLE (
    product_name TEXT,
    location TEXT,
    category TEXT,
    storage_method TEXT,
    unit TEXT,
    qty NUMERIC,
    expiry_date TEXT,
    manufacture_date TEXT,
    lot_number TEXT,
    grade TEXT
)
LANGUAGE sql STABLE SECURITY DEFINER SET statement_timeout = '15s'
AS $$
    WITH filt AS (
        SELECT
            REPLACE(COALESCE(product_name, ''), ' ', '') AS product_name,
            COALESCE(location, '') AS location,
            COALESCE(category, '') AS category,
            COALESCE(storage_method, '') AS storage_method,
            COALESCE(unit, '개') AS unit,
            COALESCE(qty, 0)::NUMERIC AS qty,
            COALESCE(expiry_date::TEXT, '') AS expiry_date,
            COALESCE(manufacture_date::TEXT, '') AS manufacture_date,
            COALESCE(lot_number, '') AS lot_number,
            COALESCE(grade, '') AS grade
        FROM stock_ledger
        WHERE status = 'active'
          AND transaction_date <= p_date_to
          AND COALESCE(product_name, '') <> ''
    ),
    -- 같은 (product_name, location) 내에서 category/storage_method 최빈값 상속
    cat_fill AS (
        SELECT
            product_name, location,
            (ARRAY_AGG(category ORDER BY CASE WHEN category <> '' THEN 0 ELSE 1 END))[1] AS filled_category,
            (ARRAY_AGG(storage_method ORDER BY CASE WHEN storage_method <> '' THEN 0 ELSE 1 END))[1] AS filled_storage
        FROM filt
        GROUP BY product_name, location
    ),
    enriched AS (
        SELECT
            f.product_name,
            f.location,
            COALESCE(NULLIF(f.category, ''), cf.filled_category, '') AS category,
            COALESCE(NULLIF(f.storage_method, ''), cf.filled_storage, '') AS storage_method,
            f.unit,
            f.qty,
            f.expiry_date,
            f.manufacture_date,
            f.lot_number,
            f.grade
        FROM filt f
        LEFT JOIN cat_fill cf
          ON cf.product_name = f.product_name AND cf.location = f.location
    )
    SELECT
        product_name,
        location,
        category,
        storage_method,
        unit,
        SUM(qty)::NUMERIC AS qty,
        CASE WHEN p_split_mode = 'expiry' THEN expiry_date ELSE '' END AS expiry_date,
        CASE WHEN p_split_mode = 'manufacture' THEN manufacture_date ELSE '' END AS manufacture_date,
        CASE WHEN p_split_mode = 'lot_number' THEN lot_number ELSE '' END AS lot_number,
        CASE WHEN p_split_mode = 'lot_number' THEN grade ELSE '' END AS grade
    FROM enriched
    GROUP BY
        product_name, location, category, storage_method, unit,
        CASE WHEN p_split_mode = 'expiry' THEN expiry_date ELSE '' END,
        CASE WHEN p_split_mode = 'manufacture' THEN manufacture_date ELSE '' END,
        CASE WHEN p_split_mode = 'lot_number' THEN lot_number ELSE '' END,
        CASE WHEN p_split_mode = 'lot_number' THEN grade ELSE '' END
    HAVING SUM(qty) <> 0
    ORDER BY product_name, location
    LIMIT 5000;
$$;

GRANT EXECUTE ON FUNCTION get_stock_snapshot_agg(DATE, TEXT)
    TO authenticated, service_role, anon;


-- ─────────────────────────────────────────────
-- 이력 조회 (history view) — 타입 필터 SQL 이관
-- ─────────────────────────────────────────────
DROP FUNCTION IF EXISTS get_stock_history_view(DATE, DATE, TEXT[], TEXT, TEXT, TEXT, TEXT, INTEGER);

CREATE OR REPLACE FUNCTION get_stock_history_view(
    p_date_to DATE,
    p_date_from DATE DEFAULT NULL,
    p_types TEXT[] DEFAULT NULL,
    p_location TEXT DEFAULT NULL,
    p_category TEXT DEFAULT NULL,
    p_storage_method TEXT DEFAULT NULL,
    p_search TEXT DEFAULT NULL,
    p_limit INTEGER DEFAULT 5000
) RETURNS TABLE (
    transaction_date TEXT,
    type TEXT,
    product_name TEXT,
    qty NUMERIC,
    unit TEXT,
    location TEXT,
    category TEXT,
    expiry_date TEXT,
    storage_method TEXT,
    lot_number TEXT,
    grade TEXT
)
LANGUAGE sql STABLE SECURITY DEFINER SET statement_timeout = '15s'
AS $$
    SELECT
        transaction_date::TEXT,
        type::TEXT,
        COALESCE(product_name, '')::TEXT AS product_name,
        COALESCE(qty, 0)::NUMERIC AS qty,
        COALESCE(unit, '개')::TEXT AS unit,
        COALESCE(location, '')::TEXT AS location,
        COALESCE(category, '')::TEXT AS category,
        COALESCE(expiry_date::TEXT, '') AS expiry_date,
        COALESCE(storage_method, '')::TEXT AS storage_method,
        COALESCE(lot_number, '')::TEXT AS lot_number,
        COALESCE(grade, '')::TEXT AS grade
    FROM stock_ledger
    WHERE status = 'active'
      AND transaction_date <= p_date_to
      AND (p_date_from IS NULL OR transaction_date >= p_date_from)
      AND COALESCE(qty, 0) <> 0
      AND (p_types IS NULL OR type = ANY(p_types))
      AND (p_location IS NULL OR p_location = '전체' OR location = p_location)
      AND (p_category IS NULL OR p_category = '전체' OR category = p_category)
      AND (p_storage_method IS NULL OR p_storage_method = '전체' OR storage_method = p_storage_method)
      AND (p_search IS NULL OR product_name ILIKE '%' || p_search || '%')
    ORDER BY transaction_date DESC, id DESC
    LIMIT LEAST(GREATEST(p_limit, 1), 10000);
$$;

GRANT EXECUTE ON FUNCTION get_stock_history_view(DATE, DATE, TEXT[], TEXT, TEXT, TEXT, TEXT, INTEGER)
    TO authenticated, service_role, anon;
