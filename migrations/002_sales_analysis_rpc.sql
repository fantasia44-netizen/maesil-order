-- ═══════════════════════════════════════════════════════════
-- 002: 월간 판매분석 SQL RPC — Phase 2-3 (autotool)
--
-- 대상: services/sales_analysis_service.py
--       _fetch_month_sales × 2개월 = order_transactions + daily_revenue
--       (로켓) 각각 풀스캔 + defaultdict 집계.
--
-- 신규 RPC:
--   get_monthly_sales_agg(year, month) — 해당 월 품목별 판매 집계
--     DB_CUTOFF_DATE 이전: daily_revenue 전체 (거래처매출 제외)
--     이후: order_transactions + daily_revenue(로켓만)
-- ═══════════════════════════════════════════════════════════

DROP FUNCTION IF EXISTS get_monthly_sales_agg(INTEGER, INTEGER, DATE);

CREATE OR REPLACE FUNCTION get_monthly_sales_agg(
    p_year INTEGER,
    p_month INTEGER,
    p_cutoff DATE DEFAULT '2026-01-01'
) RETURNS JSONB
LANGUAGE sql STABLE SECURITY DEFINER SET statement_timeout = '10s'
AS $$
    WITH month_range AS (
        SELECT
            MAKE_DATE(p_year, p_month, 1) AS first_day,
            (MAKE_DATE(p_year, p_month, 1) + INTERVAL '1 month' - INTERVAL '1 day')::DATE AS last_day
    ),
    -- 1) order_transactions (cutoff 이후 구간)
    ot_agg AS (
        SELECT
            TRIM(COALESCE(product_name, '')) AS product_name,
            COALESCE(SUM(qty), 0)::BIGINT AS total_qty,
            COALESCE(SUM(total_amount), 0)::BIGINT AS total_amount
        FROM order_transactions
        WHERE status = '정상'
          AND order_date BETWEEN
              GREATEST((SELECT first_day FROM month_range), p_cutoff)
              AND (SELECT last_day FROM month_range)
          AND TRIM(COALESCE(product_name, '')) <> ''
        GROUP BY TRIM(COALESCE(product_name, ''))
    ),
    -- 2) daily_revenue (cutoff 이후: 로켓만)
    dr_rocket_agg AS (
        SELECT
            TRIM(COALESCE(product_name, '')) AS product_name,
            COALESCE(SUM(qty), 0)::BIGINT AS total_qty,
            COALESCE(SUM(revenue), 0)::BIGINT AS total_amount
        FROM daily_revenue
        WHERE (is_deleted IS NULL OR is_deleted = FALSE)
          AND category = '로켓'
          AND revenue_date BETWEEN
              GREATEST((SELECT first_day FROM month_range), p_cutoff)
              AND (SELECT last_day FROM month_range)
          AND TRIM(COALESCE(product_name, '')) <> ''
        GROUP BY TRIM(COALESCE(product_name, ''))
    ),
    -- 3) daily_revenue (cutoff 이전: 거래처매출 제외 전체)
    dr_legacy_agg AS (
        SELECT
            TRIM(COALESCE(product_name, '')) AS product_name,
            COALESCE(SUM(qty), 0)::BIGINT AS total_qty,
            COALESCE(SUM(revenue), 0)::BIGINT AS total_amount
        FROM daily_revenue
        WHERE (is_deleted IS NULL OR is_deleted = FALSE)
          AND COALESCE(category, '') <> '거래처매출'
          AND revenue_date BETWEEN
              (SELECT first_day FROM month_range)
              AND LEAST((SELECT last_day FROM month_range), (p_cutoff - INTERVAL '1 day')::DATE)
          AND TRIM(COALESCE(product_name, '')) <> ''
        GROUP BY TRIM(COALESCE(product_name, ''))
    ),
    merged AS (
        SELECT product_name, total_qty, total_amount FROM ot_agg
        UNION ALL
        SELECT product_name, total_qty, total_amount FROM dr_rocket_agg
        UNION ALL
        SELECT product_name, total_qty, total_amount FROM dr_legacy_agg
    ),
    final AS (
        SELECT
            product_name,
            SUM(total_qty)::BIGINT AS total_qty,
            SUM(total_amount)::BIGINT AS total_amount
        FROM merged
        GROUP BY product_name
    )
    SELECT COALESCE(
        jsonb_object_agg(
            product_name,
            jsonb_build_object('total_qty', total_qty, 'total_amount', total_amount)
        ),
        '{}'::jsonb
    )
    FROM final;
$$;

GRANT EXECUTE ON FUNCTION get_monthly_sales_agg(INTEGER, INTEGER, DATE)
    TO authenticated, service_role, anon;
