-- 006_fix_aggregation_summary_rpc.sql
-- get_aggregation_summary RPC 수정
-- daily_revenue에 channel/amount 컬럼 없음 → 제거
-- revenue 컬럼만 사용

CREATE OR REPLACE FUNCTION get_aggregation_summary(
    p_date_from DATE,
    p_date_to DATE
) RETURNS JSONB
LANGUAGE plpgsql STABLE SECURITY DEFINER SET statement_timeout = '10s'
AS $$
DECLARE
    v_outbound   JSONB;
    v_inbound    BIGINT;
    v_production BIGINT;
    v_revenue    JSONB;
BEGIN
    -- 출고 (SALES_OUT + ETC_OUT + ADJUST)
    WITH filt AS (
        SELECT
            COALESCE(product_name,'') AS product_name,
            COALESCE(location,'기타') AS location,
            ABS(COALESCE(qty,0))::BIGINT AS qty
        FROM stock_ledger
        WHERE status = 'active'
          AND transaction_date BETWEEN p_date_from AND p_date_to
          AND type IN ('SALES_OUT','ETC_OUT','ADJUST')
    ),
    totals AS (
        SELECT
            COUNT(*)::BIGINT AS cnt,
            COUNT(DISTINCT product_name)::BIGINT AS items,
            COALESCE(SUM(qty),0)::BIGINT AS total_qty
        FROM filt
    ),
    by_loc AS (
        SELECT jsonb_object_agg(location, cnt) AS locations
        FROM (SELECT location, COUNT(*)::BIGINT AS cnt
              FROM filt GROUP BY location) x
    )
    SELECT jsonb_build_object(
        'count', t.cnt,
        'items', t.items,
        'qty', t.total_qty,
        'locations', COALESCE(b.locations,'{}'::jsonb)
    )
    INTO v_outbound
    FROM totals t CROSS JOIN by_loc b;

    -- 입고 카운트
    SELECT COUNT(*)::BIGINT INTO v_inbound
    FROM stock_ledger
    WHERE status = 'active'
      AND transaction_date BETWEEN p_date_from AND p_date_to
      AND type = 'INBOUND';

    -- 생산 카운트
    SELECT COUNT(*)::BIGINT INTO v_production
    FROM stock_ledger
    WHERE status = 'active'
      AND transaction_date BETWEEN p_date_from AND p_date_to
      AND type IN ('PRODUCTION','PROD_OUT');

    -- 매출 집계 (daily_revenue: revenue_date/product_name/category/qty/revenue)
    -- channel/amount 컬럼 없음 → category별 집계만
    WITH filt AS (
        SELECT
            COALESCE(category,'기타') AS category,
            COALESCE(revenue, 0)::NUMERIC AS rev
        FROM daily_revenue
        WHERE (is_deleted IS NULL OR is_deleted = FALSE)
          AND revenue_date BETWEEN p_date_from AND p_date_to
    ),
    totals AS (
        SELECT
            COUNT(*)::BIGINT AS cnt,
            COALESCE(SUM(rev),0)::BIGINT AS total_rev
        FROM filt
    ),
    by_cat AS (
        SELECT jsonb_object_agg(category, sum_rev) AS by_category
        FROM (SELECT category, SUM(rev)::BIGINT AS sum_rev
              FROM filt GROUP BY category) x
    )
    SELECT jsonb_build_object(
        'count', t.cnt,
        'total', t.total_rev,
        'by_category', COALESCE(bc.by_category,'{}'::jsonb),
        'by_channel', '{}'::jsonb   -- daily_revenue에 channel 없음
    )
    INTO v_revenue
    FROM totals t CROSS JOIN by_cat bc;

    RETURN jsonb_build_object(
        'outbound', v_outbound,
        'inbound_count', v_inbound,
        'production_count', v_production,
        'revenue', v_revenue
    );
END;
$$;

GRANT EXECUTE ON FUNCTION get_aggregation_summary(DATE, DATE)
    TO authenticated, service_role, anon;
