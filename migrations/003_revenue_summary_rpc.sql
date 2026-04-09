-- ═══════════════════════════════════════════════════════════
-- 003: 매출 관리 요약 SQL RPC — Phase 2-3 (autotool)
--
-- 대상: blueprints/revenue.py /revenue 목록 페이지
--       db.query_revenue()가 order_transactions + daily_revenue 풀스캔 +
--       주 단위 청크 반복 → 90일치 수천~수만 행 메모리 적재.
--
-- 전략: 총계는 SQL 집계, 목록 rows는 limit 2000으로 제한 (+ 기본 30일).
--
-- 신규 RPC:
--   get_revenue_summary_agg — 총 매출/정산/수수료 합계 (SQL aggregation only)
-- ═══════════════════════════════════════════════════════════

DROP FUNCTION IF EXISTS get_revenue_summary_agg(DATE, DATE, TEXT);

CREATE OR REPLACE FUNCTION get_revenue_summary_agg(
    p_date_from DATE,
    p_date_to DATE,
    p_category TEXT DEFAULT NULL
) RETURNS JSONB
LANGUAGE sql STABLE SECURITY DEFINER SET statement_timeout = '10s'
AS $$
    WITH ot_agg AS (
        SELECT
            COALESCE(SUM(total_amount), 0)::BIGINT AS revenue,
            COALESCE(SUM(settlement), 0)::BIGINT AS settlement,
            COALESCE(SUM(commission), 0)::BIGINT AS commission,
            COUNT(*)::BIGINT AS cnt
        FROM order_transactions
        WHERE status = '정상'
          AND order_date BETWEEN p_date_from AND p_date_to
          AND (p_category IS NULL OR p_category = '전체' OR category = p_category)
    ),
    dr_agg AS (
        SELECT
            COALESCE(SUM(revenue), 0)::BIGINT AS revenue,
            COALESCE(SUM(settlement), 0)::BIGINT AS settlement,
            COALESCE(SUM(commission), 0)::BIGINT AS commission,
            COUNT(*)::BIGINT AS cnt
        FROM daily_revenue
        WHERE (is_deleted IS NULL OR is_deleted = FALSE)
          AND revenue_date BETWEEN p_date_from AND p_date_to
          AND (p_category IS NULL OR p_category = '전체' OR category = p_category)
    ),
    by_channel AS (
        SELECT jsonb_object_agg(channel, total) AS data
        FROM (
            SELECT COALESCE(channel, '기타') AS channel,
                   SUM(total_amount)::BIGINT AS total
            FROM order_transactions
            WHERE status = '정상'
              AND order_date BETWEEN p_date_from AND p_date_to
              AND (p_category IS NULL OR p_category = '전체' OR category = p_category)
            GROUP BY COALESCE(channel, '기타')
        ) x
    ),
    by_category AS (
        SELECT jsonb_object_agg(category, total) AS data
        FROM (
            SELECT COALESCE(category, '기타') AS category,
                   SUM(total_amount)::BIGINT AS total
            FROM order_transactions
            WHERE status = '정상'
              AND order_date BETWEEN p_date_from AND p_date_to
            GROUP BY COALESCE(category, '기타')
        ) x
    )
    SELECT jsonb_build_object(
        'total_revenue', (ot.revenue + dr.revenue),
        'total_settlement', (ot.settlement + dr.settlement),
        'total_commission', (ot.commission + dr.commission),
        'total_count', (ot.cnt + dr.cnt),
        'by_channel', COALESCE(bc.data, '{}'::jsonb),
        'by_category', COALESCE(bg.data, '{}'::jsonb)
    )
    FROM ot_agg ot
    CROSS JOIN dr_agg dr
    CROSS JOIN by_channel bc
    CROSS JOIN by_category bg;
$$;

GRANT EXECUTE ON FUNCTION get_revenue_summary_agg(DATE, DATE, TEXT)
    TO authenticated, service_role, anon;
