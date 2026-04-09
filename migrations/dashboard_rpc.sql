-- ═══════════════════════════════════════════════════════════
-- 대시보드 Phase 2-0 SQL RPC — autotool
--
-- 배경: 대시보드가 6개 병렬 쿼리 + 5분 TTL 캐시로 감춰져 있지만
--       캐시 미스 시 4개 테이블 raw 풀스캔 발생. SaaS 동시 접속 OOM 위험.
--
-- 신규 RPC (5종):
--   get_dashboard_outbound_summary  (pending/done 카운트)
--   get_dashboard_revenue_trend     (N일 매출 추이)
--   get_dashboard_orders_by_channel (채널별 주문 통계)
--   get_dashboard_top_products      (매출 TOP N 상품)
--   get_dashboard_stock_by_location (창고별 재고 품목 수)
-- ═══════════════════════════════════════════════════════════

-- ─────────────────────────────────────────────
-- 1) 출고 처리 현황 (pending/done)
-- ─────────────────────────────────────────────
DROP FUNCTION IF EXISTS get_dashboard_outbound_summary(DATE, DATE);

CREATE OR REPLACE FUNCTION get_dashboard_outbound_summary(
    p_date_from DATE,
    p_date_to DATE DEFAULT NULL
) RETURNS JSONB
LANGUAGE sql STABLE SECURITY DEFINER SET statement_timeout = '5s'
AS $$
    SELECT jsonb_build_object(
        'pending', COALESCE(SUM(CASE WHEN is_outbound_done = FALSE THEN 1 ELSE 0 END),0)::BIGINT,
        'done', COALESCE(SUM(CASE WHEN is_outbound_done = TRUE THEN 1 ELSE 0 END),0)::BIGINT
    )
    FROM order_transactions
    WHERE status = '정상'
      AND order_date >= p_date_from
      AND (p_date_to IS NULL OR order_date <= p_date_to);
$$;

GRANT EXECUTE ON FUNCTION get_dashboard_outbound_summary(DATE, DATE)
    TO authenticated, service_role, anon;


-- ─────────────────────────────────────────────
-- 2) 매출 추이 (N일)
--    order_transactions (cutoff 이후) + daily_revenue (cutoff 이전) 합산
-- ─────────────────────────────────────────────
DROP FUNCTION IF EXISTS get_dashboard_revenue_trend(INTEGER, DATE);

CREATE OR REPLACE FUNCTION get_dashboard_revenue_trend(
    p_days INTEGER DEFAULT 7,
    p_cutoff DATE DEFAULT '2026-01-01'
) RETURNS JSONB
LANGUAGE sql STABLE SECURITY DEFINER SET statement_timeout = '5s'
AS $$
    WITH date_range AS (
        SELECT (CURRENT_DATE - (p_days - 1) * INTERVAL '1 day')::DATE AS df,
               CURRENT_DATE AS dt
    ),
    ot_agg AS (
        SELECT
            order_date::DATE AS d,
            COALESCE(SUM(total_amount),0)::BIGINT AS total,
            COALESCE(SUM(settlement),0)::BIGINT AS settlement
        FROM order_transactions
        WHERE status = '정상'
          AND order_date BETWEEN GREATEST((SELECT df FROM date_range), p_cutoff) AND (SELECT dt FROM date_range)
        GROUP BY order_date
    ),
    dr_agg AS (
        SELECT
            revenue_date::DATE AS d,
            COALESCE(SUM(revenue),0)::BIGINT AS total
        FROM daily_revenue
        WHERE (is_deleted IS NULL OR is_deleted = FALSE)
          AND COALESCE(category,'') NOT IN ('거래처매출','로켓')
          AND revenue_date BETWEEN (SELECT df FROM date_range)
                               AND LEAST((SELECT dt FROM date_range), (p_cutoff - INTERVAL '1 day')::DATE)
        GROUP BY revenue_date
    ),
    merged AS (
        SELECT d, total, settlement FROM ot_agg
        UNION ALL
        SELECT d, total, 0::BIGINT AS settlement FROM dr_agg
    ),
    final AS (
        SELECT d, SUM(total)::BIGINT AS total, SUM(settlement)::BIGINT AS settlement
        FROM merged
        GROUP BY d
        ORDER BY d
    )
    SELECT COALESCE(
        jsonb_agg(jsonb_build_object(
            'date', to_char(d, 'YYYY-MM-DD'),
            'total', total,
            'settlement', settlement
        ) ORDER BY d),
        '[]'::jsonb
    )
    FROM final;
$$;

GRANT EXECUTE ON FUNCTION get_dashboard_revenue_trend(INTEGER, DATE)
    TO authenticated, service_role, anon;


-- ─────────────────────────────────────────────
-- 3) 채널별 주문 통계
-- ─────────────────────────────────────────────
DROP FUNCTION IF EXISTS get_dashboard_orders_by_channel(DATE, DATE);

CREATE OR REPLACE FUNCTION get_dashboard_orders_by_channel(
    p_date_from DATE DEFAULT NULL,
    p_date_to DATE DEFAULT NULL
) RETURNS JSONB
LANGUAGE sql STABLE SECURITY DEFINER SET statement_timeout = '5s'
AS $$
    WITH agg AS (
        SELECT
            COALESCE(channel, '기타') AS channel,
            COUNT(*)::BIGINT AS cnt,
            COALESCE(SUM(qty),0)::BIGINT AS qty,
            COALESCE(SUM(total_amount),0)::BIGINT AS amount
        FROM order_transactions
        WHERE status = '정상'
          AND (p_date_from IS NULL OR order_date >= p_date_from)
          AND (p_date_to IS NULL OR order_date <= p_date_to)
        GROUP BY COALESCE(channel, '기타')
    )
    SELECT COALESCE(
        jsonb_agg(jsonb_build_object(
            'channel', channel,
            'count', cnt,
            'qty', qty,
            'amount', amount
        ) ORDER BY cnt DESC),
        '[]'::jsonb
    )
    FROM agg;
$$;

GRANT EXECUTE ON FUNCTION get_dashboard_orders_by_channel(DATE, DATE)
    TO authenticated, service_role, anon;


-- ─────────────────────────────────────────────
-- 4) TOP N 매출 상품
-- ─────────────────────────────────────────────
DROP FUNCTION IF EXISTS get_dashboard_top_products(INTEGER, INTEGER);

CREATE OR REPLACE FUNCTION get_dashboard_top_products(
    p_days INTEGER DEFAULT 30,
    p_limit INTEGER DEFAULT 10
) RETURNS JSONB
LANGUAGE sql STABLE SECURITY DEFINER SET statement_timeout = '5s'
AS $$
    WITH agg AS (
        SELECT
            REPLACE(COALESCE(product_name,''), ' ', '') AS product_name,
            COALESCE(SUM(qty),0)::BIGINT AS qty,
            COALESCE(SUM(total_amount),0)::BIGINT AS revenue,
            COALESCE(SUM(settlement),0)::BIGINT AS settlement
        FROM order_transactions
        WHERE status = '정상'
          AND order_date >= (CURRENT_DATE - (p_days - 1) * INTERVAL '1 day')::DATE
          AND COALESCE(product_name,'') <> ''
        GROUP BY REPLACE(COALESCE(product_name,''), ' ', '')
        ORDER BY revenue DESC
        LIMIT LEAST(GREATEST(p_limit, 1), 100)
    )
    SELECT COALESCE(
        jsonb_agg(jsonb_build_object(
            'product_name', product_name,
            'qty', qty,
            'revenue', revenue,
            'settlement', settlement
        ) ORDER BY revenue DESC),
        '[]'::jsonb
    )
    FROM agg;
$$;

GRANT EXECUTE ON FUNCTION get_dashboard_top_products(INTEGER, INTEGER)
    TO authenticated, service_role, anon;


-- ─────────────────────────────────────────────
-- 5) 창고별 재고 요약 (N일치 stock_ledger 집계)
-- ─────────────────────────────────────────────
DROP FUNCTION IF EXISTS get_dashboard_stock_by_location(INTEGER);

CREATE OR REPLACE FUNCTION get_dashboard_stock_by_location(
    p_days INTEGER DEFAULT 90
) RETURNS JSONB
LANGUAGE sql STABLE SECURITY DEFINER SET statement_timeout = '10s'
AS $$
    WITH stock_agg AS (
        SELECT
            COALESCE(product_name,'') AS product_name,
            COALESCE(location,'') AS location,
            SUM(COALESCE(qty,0))::BIGINT AS total_qty
        FROM stock_ledger
        WHERE transaction_date >= (CURRENT_DATE - (p_days - 1) * INTERVAL '1 day')::DATE
          AND transaction_date <= CURRENT_DATE
        GROUP BY COALESCE(product_name,''), COALESCE(location,'')
        HAVING SUM(COALESCE(qty,0)) > 0
    ),
    by_loc AS (
        SELECT
            location,
            COUNT(*)::BIGINT AS product_count,
            SUM(total_qty)::BIGINT AS total_qty
        FROM stock_agg
        WHERE product_name <> ''
        GROUP BY location
    )
    SELECT COALESCE(
        jsonb_agg(jsonb_build_object(
            'location', location,
            'product_count', product_count,
            'total_qty', total_qty
        ) ORDER BY product_count DESC),
        '[]'::jsonb
    )
    FROM by_loc;
$$;

GRANT EXECUTE ON FUNCTION get_dashboard_stock_by_location(INTEGER)
    TO authenticated, service_role, anon;
