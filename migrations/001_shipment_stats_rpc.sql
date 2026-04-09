-- ═══════════════════════════════════════════════════════════
-- 001: 출고 통계 SQL RPC — Phase 2-3 (autotool)
--
-- 대상: services/shipment_stats_service.py
--       stock_ledger SALES_OUT 풀스캔 + Python 8회 dict 집계.
--
-- 신규 RPC:
--   get_shipment_stats_agg — summary + daily/monthly + location/category
--                            + daily_location + monthly_location + top_products
-- ═══════════════════════════════════════════════════════════

DROP FUNCTION IF EXISTS get_shipment_stats_agg(DATE, DATE, TEXT);

CREATE OR REPLACE FUNCTION get_shipment_stats_agg(
    p_date_from DATE DEFAULT NULL,
    p_date_to DATE DEFAULT NULL,
    p_location TEXT DEFAULT NULL
) RETURNS JSONB
LANGUAGE plpgsql STABLE SECURITY DEFINER SET statement_timeout = '15s'
AS $$
DECLARE
    v_summary JSONB;
    v_daily JSONB;
    v_monthly JSONB;
    v_loc JSONB;
    v_cat JSONB;
    v_daily_loc JSONB;
    v_monthly_loc JSONB;
    v_top JSONB;
    v_loc_totals JSONB;
    v_locations TEXT[];
    v_grand_total BIGINT;
BEGIN
    -- 모든 집계의 베이스 CTE는 PL/pgSQL에서 재사용 불가 → 임시 테이블
    CREATE TEMP TABLE IF NOT EXISTS _tmp_ship ON COMMIT DROP AS
    SELECT
        COALESCE(product_name,'') AS product_name,
        COALESCE(location,'기타') AS location,
        COALESCE(category,'기타') AS category,
        COALESCE(transaction_date::TEXT,'') AS transaction_date,
        ABS(COALESCE(qty,0))::BIGINT AS qty
    FROM stock_ledger
    WHERE status = 'active'
      AND type = 'SALES_OUT'
      AND (p_date_from IS NULL OR transaction_date >= p_date_from)
      AND (p_date_to IS NULL OR transaction_date <= p_date_to)
      AND (p_location IS NULL OR p_location = '전체' OR location = p_location);

    -- 1) summary
    WITH s AS (
        SELECT
            COALESCE(SUM(qty),0)::BIGINT AS total_qty,
            COUNT(*)::BIGINT AS total_count,
            COUNT(DISTINCT NULLIF(product_name,''))::BIGINT AS total_items,
            COUNT(DISTINCT NULLIF(transaction_date,''))::BIGINT AS days
        FROM _tmp_ship
    )
    SELECT jsonb_build_object(
        'total_qty', total_qty,
        'total_count', total_count,
        'total_items', total_items,
        'days', GREATEST(days, 1),
        'daily_avg', CASE WHEN days > 0
                          THEN ROUND(total_qty::NUMERIC / days, 1)
                          ELSE 0 END
    ) INTO v_summary FROM s;

    -- 2) daily_totals
    SELECT COALESCE(jsonb_agg(jsonb_build_object('date', d, 'total', total) ORDER BY d), '[]'::jsonb)
    INTO v_daily FROM (
        SELECT transaction_date AS d, SUM(qty)::BIGINT AS total
        FROM _tmp_ship WHERE transaction_date <> ''
        GROUP BY transaction_date
    ) x;

    -- 3) monthly_totals
    SELECT COALESCE(jsonb_agg(jsonb_build_object('month', m, 'total', total) ORDER BY m), '[]'::jsonb)
    INTO v_monthly FROM (
        SELECT LEFT(transaction_date, 7) AS m, SUM(qty)::BIGINT AS total
        FROM _tmp_ship WHERE LENGTH(transaction_date) >= 7
        GROUP BY LEFT(transaction_date, 7)
    ) x;

    -- 4) location_breakdown
    SELECT COALESCE(jsonb_agg(jsonb_build_object('location', location, 'total', total) ORDER BY total DESC), '[]'::jsonb)
    INTO v_loc FROM (
        SELECT location, SUM(qty)::BIGINT AS total
        FROM _tmp_ship GROUP BY location
    ) x;

    -- 5) category_breakdown
    SELECT COALESCE(jsonb_agg(jsonb_build_object('category', category, 'total', total) ORDER BY total DESC), '[]'::jsonb)
    INTO v_cat FROM (
        SELECT category, SUM(qty)::BIGINT AS total
        FROM _tmp_ship GROUP BY category
    ) x;

    -- 창고 순서 (위치 총계 내림차순)
    SELECT ARRAY_AGG(location ORDER BY total DESC),
           jsonb_object_agg(location, total),
           COALESCE(SUM(total), 0)::BIGINT
    INTO v_locations, v_loc_totals, v_grand_total
    FROM (
        SELECT location, SUM(qty)::BIGINT AS total
        FROM _tmp_ship GROUP BY location
    ) x;

    -- 6) daily_location_totals
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'date', d,
        'locations', loc_data,
        'total', total
    ) ORDER BY d), '[]'::jsonb)
    INTO v_daily_loc FROM (
        SELECT
            transaction_date AS d,
            jsonb_object_agg(location, sum_qty) AS loc_data,
            SUM(sum_qty)::BIGINT AS total
        FROM (
            SELECT transaction_date, location, SUM(qty)::BIGINT AS sum_qty
            FROM _tmp_ship WHERE transaction_date <> ''
            GROUP BY transaction_date, location
        ) y
        GROUP BY transaction_date
    ) x;

    -- 7) monthly_location_totals
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'month', m,
        'locations', loc_data,
        'total', total
    ) ORDER BY m), '[]'::jsonb)
    INTO v_monthly_loc FROM (
        SELECT
            LEFT(transaction_date, 7) AS m,
            jsonb_object_agg(location, sum_qty) AS loc_data,
            SUM(sum_qty)::BIGINT AS total
        FROM (
            SELECT LEFT(transaction_date, 7) AS mm, transaction_date, location,
                   SUM(qty)::BIGINT AS sum_qty
            FROM _tmp_ship WHERE LENGTH(transaction_date) >= 7
            GROUP BY LEFT(transaction_date, 7), transaction_date, location
        ) y
        GROUP BY LEFT(transaction_date, 7)
    ) x;

    -- 8) top_products (limit 15)
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'name', product_name,
        'qty', qty,
        'category', category
    ) ORDER BY qty DESC), '[]'::jsonb)
    INTO v_top FROM (
        SELECT
            product_name,
            SUM(qty)::BIGINT AS qty,
            (ARRAY_AGG(category))[1] AS category
        FROM _tmp_ship
        WHERE product_name <> ''
        GROUP BY product_name
        ORDER BY SUM(qty) DESC
        LIMIT 15
    ) x;

    DROP TABLE IF EXISTS _tmp_ship;

    RETURN jsonb_build_object(
        'summary', v_summary,
        'daily_totals', v_daily,
        'monthly_totals', v_monthly,
        'location_breakdown', v_loc,
        'category_breakdown', v_cat,
        'daily_location_totals', jsonb_build_object(
            'locations', COALESCE(to_jsonb(v_locations), '[]'::jsonb),
            'rows', v_daily_loc,
            'totals', COALESCE(v_loc_totals, '{}'::jsonb),
            'grand_total', v_grand_total
        ),
        'monthly_location_totals', jsonb_build_object(
            'locations', COALESCE(to_jsonb(v_locations), '[]'::jsonb),
            'rows', v_monthly_loc,
            'totals', COALESCE(v_loc_totals, '{}'::jsonb),
            'grand_total', v_grand_total
        ),
        'top_products', v_top
    );
END;
$$;

GRANT EXECUTE ON FUNCTION get_shipment_stats_agg(DATE, DATE, TEXT)
    TO authenticated, service_role, anon;
