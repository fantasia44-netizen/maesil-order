-- ═══════════════════════════════════════════════════════════
-- 손익표 월별 집계 RPC — Phase 1 OOM 차단 (autotool)
--
-- 배경: pnl_service.calculate_monthly_pnl 이 테이블 4개(api_settlements,
--       tax_invoices sales/purchase, expenses)를 Python에서 풀스캔 +
--       4개 defaultdict 중첩. calculate_pnl_trend은 이걸 6개월×4테이블
--       반복 호출 → 누적 24,000+ 행 메모리 피크.
--
-- 해결: 월별 1회 RPC 호출로 모든 집계를 JSONB로 반환.
--       Python 서비스는 JSONB 파싱 + 카테고리 매핑만 수행.
-- ═══════════════════════════════════════════════════════════

DROP FUNCTION IF EXISTS get_pnl_monthly_agg(DATE, DATE, TEXT);

CREATE OR REPLACE FUNCTION get_pnl_monthly_agg(
    p_date_from DATE,
    p_date_to DATE,
    p_year_month TEXT
) RETURNS JSONB
LANGUAGE plpgsql STABLE SECURITY DEFINER SET statement_timeout = '10s'
AS $$
DECLARE
    v_settle_prefixes TEXT[] := ARRAY[
        'nsettle_', 'wsettle_', 'rocket_', '11settle_',
        'tsettle_', 'osettle_', 'auction_', 'gmarket_'
    ];
    v_platform_buyers TEXT[] := ARRAY['쿠팡(주)', '쿠팡주식회사'];

    v_online JSONB;
    v_ad JSONB;
    v_b2b JSONB;
    v_purchase JSONB;
    v_expenses JSONB;
BEGIN
    -- ────────────────────────────────────────
    -- 1) 온라인 매출 (api_settlements, 정산서 prefix만)
    -- ────────────────────────────────────────
    WITH filt AS (
        SELECT channel,
               COALESCE(gross_sales,0)::BIGINT AS gross,
               COALESCE(total_commission,0)::BIGINT AS comm
        FROM api_settlements
        WHERE settlement_date BETWEEN p_date_from AND p_date_to
          AND settlement_id IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM unnest(v_settle_prefixes) p
              WHERE settlement_id LIKE p || '%'
          )
    ),
    totals AS (
        SELECT
            COALESCE(SUM(gross),0)::BIGINT AS online_total,
            COALESCE(SUM(comm),0)::BIGINT AS online_commission
        FROM filt
    ),
    by_ch AS (
        SELECT
            jsonb_object_agg(channel, sum_gross) AS by_channel,
            jsonb_object_agg(channel, sum_comm) AS comm_by_channel
        FROM (
            SELECT COALESCE(channel,'기타') AS channel,
                   SUM(gross)::BIGINT AS sum_gross,
                   SUM(comm)::BIGINT AS sum_comm
            FROM filt
            GROUP BY COALESCE(channel,'기타')
        ) x
    )
    SELECT jsonb_build_object(
        'online_total', t.online_total,
        'online_commission', t.online_commission,
        'by_channel', COALESCE(b.by_channel, '{}'::jsonb),
        'commission_by_channel', COALESCE(b.comm_by_channel, '{}'::jsonb)
    )
    INTO v_online
    FROM totals t CROSS JOIN by_ch b;

    -- ────────────────────────────────────────
    -- 2) 광고비 (api_settlements, settlement_id LIKE 'ad_cost_%')
    -- ────────────────────────────────────────
    WITH ad AS (
        SELECT COALESCE(channel,'기타') AS channel,
               COALESCE(other_deductions,0)::BIGINT AS ad_cost
        FROM api_settlements
        WHERE settlement_date BETWEEN p_date_from AND p_date_to
          AND settlement_id LIKE 'ad_cost_%'
    ),
    totals AS (
        SELECT COALESCE(SUM(ad_cost),0)::BIGINT AS total_ad
        FROM ad
    ),
    by_ch AS (
        SELECT jsonb_object_agg(channel, sum_ad) AS by_channel
        FROM (
            SELECT channel, SUM(ad_cost)::BIGINT AS sum_ad
            FROM ad GROUP BY channel
        ) x
    )
    SELECT jsonb_build_object(
        'total_ad_cost', t.total_ad,
        'by_channel', COALESCE(b.by_channel, '{}'::jsonb)
    )
    INTO v_ad
    FROM totals t CROSS JOIN by_ch b;

    -- ────────────────────────────────────────
    -- 3) 거래처 매출 (tax_invoices direction='sales', 플랫폼 제외)
    -- ────────────────────────────────────────
    WITH filt AS (
        SELECT
            COALESCE(buyer_corp_name, '기타') AS vendor,
            COALESCE(supply_cost_total, supply_amount, 0)::BIGINT AS amt
        FROM tax_invoices
        WHERE direction = 'sales'
          AND (is_deleted IS NULL OR is_deleted = FALSE)
          AND COALESCE(status,'') <> 'cancelled'
          AND write_date BETWEEN p_date_from AND p_date_to
          AND COALESCE(buyer_corp_name, '기타') <> ALL(v_platform_buyers)
    ),
    totals AS (
        SELECT COALESCE(SUM(amt),0)::BIGINT AS b2b_total FROM filt
    ),
    by_v AS (
        SELECT jsonb_object_agg(vendor, sum_amt) AS by_vendor
        FROM (
            SELECT vendor, SUM(amt)::BIGINT AS sum_amt
            FROM filt GROUP BY vendor
        ) x
    )
    SELECT jsonb_build_object(
        'b2b_total', t.b2b_total,
        'by_vendor', COALESCE(v.by_vendor, '{}'::jsonb)
    )
    INTO v_b2b
    FROM totals t CROSS JOIN by_v v;

    -- ────────────────────────────────────────
    -- 4) 매입 (tax_invoices direction='purchase')
    -- ────────────────────────────────────────
    WITH filt AS (
        SELECT
            COALESCE(supplier_corp_name, '기타') AS vendor,
            COALESCE(supply_cost_total, supply_amount, 0)::BIGINT AS amt
        FROM tax_invoices
        WHERE direction = 'purchase'
          AND (is_deleted IS NULL OR is_deleted = FALSE)
          AND COALESCE(status,'') <> 'cancelled'
          AND write_date BETWEEN p_date_from AND p_date_to
    ),
    totals AS (
        SELECT COALESCE(SUM(amt),0)::BIGINT AS purchase_total FROM filt
    ),
    by_v AS (
        SELECT jsonb_object_agg(vendor, sum_amt) AS by_vendor
        FROM (
            SELECT vendor, SUM(amt)::BIGINT AS sum_amt
            FROM filt GROUP BY vendor
        ) x
    )
    SELECT jsonb_build_object(
        'purchase_total', t.purchase_total,
        'by_vendor', COALESCE(v.by_vendor, '{}'::jsonb)
    )
    INTO v_purchase
    FROM totals t CROSS JOIN by_v v;

    -- ────────────────────────────────────────
    -- 5) 판관비 (expenses, year_month 기준)
    -- ────────────────────────────────────────
    WITH filt AS (
        SELECT COALESCE(category,'기타') AS category,
               COALESCE(amount,0)::NUMERIC AS amt
        FROM expenses
        WHERE (is_deleted IS NULL OR is_deleted = FALSE)
          AND (expense_month = p_year_month
               OR expense_date BETWEEN p_date_from AND p_date_to)
    ),
    by_cat AS (
        SELECT jsonb_object_agg(category, sum_amt) AS by_category
        FROM (
            SELECT category, SUM(amt)::NUMERIC AS sum_amt
            FROM filt GROUP BY category
        ) x
    )
    SELECT jsonb_build_object(
        'by_category', COALESCE(by_cat.by_category, '{}'::jsonb)
    )
    INTO v_expenses
    FROM by_cat;

    -- ────────────────────────────────────────
    -- 결과 병합
    -- ────────────────────────────────────────
    RETURN jsonb_build_object(
        'revenue', v_online,
        'ad_cost', v_ad,
        'b2b', v_b2b,
        'purchase', v_purchase,
        'expenses', v_expenses
    );
END;
$$;

GRANT EXECUTE ON FUNCTION get_pnl_monthly_agg(DATE, DATE, TEXT)
    TO authenticated, service_role, anon;
