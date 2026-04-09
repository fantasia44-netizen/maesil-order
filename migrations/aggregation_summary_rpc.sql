-- ═══════════════════════════════════════════════════════════
-- 집계 API Phase 1 SQL 이관 (autotool aggregation)
--
-- 대상: aggregation_bp /api/summary, /api/channel-orders
--       → 4+ raw 페이지네이션 루프 + 3+ defaultdict 중첩 해소
--
-- 신규 RPC:
--   get_aggregation_summary — 출고/입고/생산/매출 카운터 전부 JSONB
--   get_channel_orders_agg  — 일자×그룹 주문수량 피벗 JSONB
-- ═══════════════════════════════════════════════════════════

-- ─────────────────────────────────────────────
-- 1) 집계 요약 — /api/summary 전용
-- ─────────────────────────────────────────────
DROP FUNCTION IF EXISTS get_aggregation_summary(DATE, DATE);

CREATE OR REPLACE FUNCTION get_aggregation_summary(
    p_date_from DATE,
    p_date_to DATE
) RETURNS JSONB
LANGUAGE plpgsql STABLE SECURITY DEFINER SET statement_timeout = '10s'
AS $$
DECLARE
    v_outbound JSONB;
    v_inbound BIGINT;
    v_production BIGINT;
    v_revenue JSONB;
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

    -- 생산 카운트 (PRODUCTION + PROD_OUT)
    SELECT COUNT(*)::BIGINT INTO v_production
    FROM stock_ledger
    WHERE status = 'active'
      AND transaction_date BETWEEN p_date_from AND p_date_to
      AND type IN ('PRODUCTION','PROD_OUT');

    -- 매출 (daily_revenue 기반 단순 집계 — category/channel 합산)
    WITH filt AS (
        SELECT
            COALESCE(category,'기타') AS category,
            NULLIF(COALESCE(channel,''),'') AS channel,
            COALESCE(revenue, amount, 0)::NUMERIC AS rev
        FROM daily_revenue
        WHERE revenue_date BETWEEN p_date_from AND p_date_to
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
    ),
    by_ch AS (
        SELECT jsonb_object_agg(channel, sum_rev) AS by_channel
        FROM (SELECT channel, SUM(rev)::BIGINT AS sum_rev
              FROM filt WHERE channel IS NOT NULL GROUP BY channel) x
    )
    SELECT jsonb_build_object(
        'count', t.cnt,
        'total', t.total_rev,
        'by_category', COALESCE(bc.by_category,'{}'::jsonb),
        'by_channel', COALESCE(bh.by_channel,'{}'::jsonb)
    )
    INTO v_revenue
    FROM totals t CROSS JOIN by_cat bc CROSS JOIN by_ch bh;

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


-- ─────────────────────────────────────────────
-- 2) 채널별 일자별 주문수량 집계 — /api/channel-orders 전용
--    order_transactions(collection_date/order_date fallback) + daily_revenue +
--    stock_ledger(ETC_OUT/ADJUST) 통합
-- ─────────────────────────────────────────────
DROP FUNCTION IF EXISTS get_channel_orders_agg(DATE, DATE);

CREATE OR REPLACE FUNCTION get_channel_orders_agg(
    p_date_from DATE,
    p_date_to DATE
) RETURNS JSONB
LANGUAGE plpgsql STABLE SECURITY DEFINER SET statement_timeout = '15s'
AS $$
DECLARE
    v_rows JSONB;
BEGIN
    WITH ot_resolved AS (
        -- collection_date 우선, 없으면 order_date
        SELECT
            COALESCE(collection_date, order_date)::DATE AS d,
            channel,
            COALESCE(qty,0)::BIGINT AS qty
        FROM order_transactions
        WHERE status = '정상'
          AND COALESCE(collection_date, order_date) BETWEEN p_date_from AND p_date_to
    ),
    ot_grouped AS (
        SELECT
            d,
            CASE
                WHEN channel IN ('N배송_수동','N배송') THEN 'N배송'
                WHEN channel = '쿠팡' THEN '쿠팡매출'
                ELSE '일반매출'
            END AS grp,
            SUM(qty)::BIGINT AS qty
        FROM ot_resolved
        WHERE d IS NOT NULL
        GROUP BY d, grp
    ),
    dr_grouped AS (
        SELECT
            revenue_date::DATE AS d,
            CASE WHEN category = '로켓' THEN '로켓' ELSE '거래처매출' END AS grp,
            SUM(COALESCE(qty,0))::BIGINT AS qty
        FROM daily_revenue
        WHERE category IN ('거래처매출','로켓')
          AND revenue_date BETWEEN p_date_from AND p_date_to
        GROUP BY revenue_date, grp
    ),
    sl_grouped AS (
        SELECT
            transaction_date::DATE AS d,
            '기타출고'::TEXT AS grp,
            SUM(ABS(COALESCE(qty,0)))::BIGINT AS qty
        FROM stock_ledger
        WHERE status = 'active'
          AND type IN ('ETC_OUT','ADJUST')
          AND transaction_date BETWEEN p_date_from AND p_date_to
        GROUP BY transaction_date
    ),
    all_union AS (
        SELECT d, grp, qty FROM ot_grouped
        UNION ALL
        SELECT d, grp, qty FROM dr_grouped
        UNION ALL
        SELECT d, grp, qty FROM sl_grouped
    ),
    agg AS (
        SELECT d, grp, SUM(qty)::BIGINT AS qty
        FROM all_union
        GROUP BY d, grp
    ),
    per_date AS (
        SELECT
            d,
            jsonb_object_agg(grp, qty) AS groups
        FROM agg
        GROUP BY d
    )
    SELECT COALESCE(
        jsonb_agg(jsonb_build_object('date', to_char(d,'YYYY-MM-DD'), 'groups', groups)
                  ORDER BY d),
        '[]'::jsonb)
    INTO v_rows
    FROM per_date;

    RETURN jsonb_build_object('rows', v_rows);
END;
$$;

GRANT EXECUTE ON FUNCTION get_channel_orders_agg(DATE, DATE)
    TO authenticated, service_role, anon;
