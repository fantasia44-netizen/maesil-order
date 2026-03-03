-- ============================================================
-- Phase 1: 주문 수집 RPC 함수 (Supabase SQL Editor에서 실행)
-- ============================================================

-- ============================================================
-- 1) rpc_upsert_order_batch
--    주문 배치 upsert (트랜잭션 보호)
--    - 새 주문 → INSERT
--    - 동일 주문, raw_hash 동일 → SKIP
--    - 동일 주문, raw_hash 다름 → UPDATE + change_log
--    - status='취소'/'환불' → SKIP (수동 처리 보호)
-- ============================================================
CREATE OR REPLACE FUNCTION rpc_upsert_order_batch(
    p_import_run_id BIGINT,
    p_orders JSONB  -- [{transaction: {...}, shipping: {...}}, ...]
)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    v_order        JSONB;
    v_txn          JSONB;
    v_ship         JSONB;
    v_existing     RECORD;
    v_inserted     INT := 0;
    v_updated      INT := 0;
    v_skipped      INT := 0;
    v_failed       INT := 0;
    v_errors       JSONB := '[]'::JSONB;
    v_idx          INT := 0;
    v_txn_id       BIGINT;
    v_field        TEXT;
    v_old_val      TEXT;
    v_new_val      TEXT;
    v_fields       TEXT[] := ARRAY[
        'order_date', 'order_datetime', 'original_option', 'original_product',
        'product_name', 'barcode', 'line_code', 'sort_order',
        'qty', 'unit_price', 'total_amount', 'discount_amount',
        'settlement', 'commission', 'shipping_fee'
    ];
BEGIN
    FOR v_order IN SELECT * FROM jsonb_array_elements(p_orders)
    LOOP
        v_idx := v_idx + 1;
        v_txn  := v_order->'transaction';
        v_ship := v_order->'shipping';

        BEGIN
            -- 기존 주문 조회
            SELECT id, raw_hash, status
            INTO v_existing
            FROM order_transactions
            WHERE channel  = v_txn->>'channel'
              AND order_no = v_txn->>'order_no'
              AND line_no  = COALESCE((v_txn->>'line_no')::INT, 1);

            IF v_existing IS NOT NULL THEN
                -- 취소/환불 주문은 보호 (SKIP)
                IF v_existing.status IN ('취소', '환불') THEN
                    v_skipped := v_skipped + 1;
                    CONTINUE;
                END IF;

                -- raw_hash 동일 → 변경 없음 (SKIP)
                IF v_existing.raw_hash IS NOT NULL
                   AND v_existing.raw_hash = v_txn->>'raw_hash' THEN
                    v_skipped := v_skipped + 1;
                    CONTINUE;
                END IF;

                -- raw_hash 다름 → UPDATE + change_log
                v_txn_id := v_existing.id;

                -- 변경된 필드 기록
                FOREACH v_field IN ARRAY v_fields
                LOOP
                    SELECT
                        CASE v_field
                            WHEN 'order_date'        THEN ot.order_date::TEXT
                            WHEN 'original_option'   THEN ot.original_option
                            WHEN 'original_product'  THEN ot.original_product
                            WHEN 'product_name'      THEN ot.product_name
                            WHEN 'barcode'           THEN ot.barcode
                            WHEN 'line_code'         THEN ot.line_code::TEXT
                            WHEN 'sort_order'        THEN ot.sort_order::TEXT
                            WHEN 'qty'               THEN ot.qty::TEXT
                            WHEN 'unit_price'        THEN ot.unit_price::TEXT
                            WHEN 'total_amount'      THEN ot.total_amount::TEXT
                            WHEN 'discount_amount'   THEN ot.discount_amount::TEXT
                            WHEN 'settlement'        THEN ot.settlement::TEXT
                            WHEN 'commission'        THEN ot.commission::TEXT
                        WHEN 'shipping_fee'      THEN ot.shipping_fee::TEXT
                        WHEN 'order_datetime'    THEN ot.order_datetime
                        END
                    INTO v_old_val
                    FROM order_transactions ot
                    WHERE ot.id = v_txn_id;

                    v_new_val := v_txn->>v_field;

                    IF v_old_val IS DISTINCT FROM v_new_val THEN
                        INSERT INTO order_change_log (
                            order_transaction_id, import_run_id, channel, order_no,
                            field_name, before_value, after_value,
                            change_type, changed_by
                        ) VALUES (
                            v_txn_id, p_import_run_id,
                            v_txn->>'channel', v_txn->>'order_no',
                            v_field, v_old_val, v_new_val,
                            'upsert_변경', 'system'
                        );
                    END IF;
                END LOOP;

                -- order_transactions UPDATE
                UPDATE order_transactions SET
                    import_run_id    = p_import_run_id,
                    order_date       = (v_txn->>'order_date')::DATE,
                    order_datetime   = v_txn->>'order_datetime',
                    original_option  = v_txn->>'original_option',
                    original_product = v_txn->>'original_product',
                    raw_data         = (v_txn->'raw_data'),
                    raw_hash         = v_txn->>'raw_hash',
                    parser_version   = COALESCE(v_txn->>'parser_version', '1.0'),
                    product_name     = v_txn->>'product_name',
                    barcode          = v_txn->>'barcode',
                    line_code        = (v_txn->>'line_code')::INT,
                    sort_order       = (v_txn->>'sort_order')::INT,
                    qty              = COALESCE((v_txn->>'qty')::INT, 1),
                    unit_price       = COALESCE((v_txn->>'unit_price')::NUMERIC, 0),
                    total_amount     = COALESCE((v_txn->>'total_amount')::NUMERIC, 0),
                    discount_amount  = COALESCE((v_txn->>'discount_amount')::NUMERIC, 0),
                    settlement       = COALESCE((v_txn->>'settlement')::NUMERIC, 0),
                    commission       = COALESCE((v_txn->>'commission')::NUMERIC, 0),
                    shipping_fee     = COALESCE((v_txn->>'shipping_fee')::NUMERIC, 0),
                    processed_at     = now()
                WHERE id = v_txn_id;

                v_updated := v_updated + 1;
            ELSE
                -- 새 주문 INSERT
                INSERT INTO order_transactions (
                    import_run_id, channel, order_date, order_datetime, order_no, line_no,
                    original_option, original_product,
                    raw_data, raw_hash, parser_version,
                    product_name, barcode, line_code, sort_order,
                    qty, unit_price, total_amount, discount_amount,
                    settlement, commission, shipping_fee
                ) VALUES (
                    p_import_run_id,
                    v_txn->>'channel',
                    (v_txn->>'order_date')::DATE,
                    v_txn->>'order_datetime',
                    v_txn->>'order_no',
                    COALESCE((v_txn->>'line_no')::INT, 1),
                    v_txn->>'original_option',
                    v_txn->>'original_product',
                    (v_txn->'raw_data'),
                    v_txn->>'raw_hash',
                    COALESCE(v_txn->>'parser_version', '1.0'),
                    v_txn->>'product_name',
                    v_txn->>'barcode',
                    (v_txn->>'line_code')::INT,
                    (v_txn->>'sort_order')::INT,
                    COALESCE((v_txn->>'qty')::INT, 1),
                    COALESCE((v_txn->>'unit_price')::NUMERIC, 0),
                    COALESCE((v_txn->>'total_amount')::NUMERIC, 0),
                    COALESCE((v_txn->>'discount_amount')::NUMERIC, 0),
                    COALESCE((v_txn->>'settlement')::NUMERIC, 0),
                    COALESCE((v_txn->>'commission')::NUMERIC, 0),
                    COALESCE((v_txn->>'shipping_fee')::NUMERIC, 0)
                );

                v_inserted := v_inserted + 1;
            END IF;

            -- shipping upsert (PII 분리 저장)
            IF v_ship IS NOT NULL AND v_ship->>'name' IS NOT NULL THEN
                INSERT INTO order_shipping (
                    channel, order_no,
                    name, phone, phone2, address, memo,
                    expires_at
                ) VALUES (
                    v_txn->>'channel',
                    v_txn->>'order_no',
                    v_ship->>'name',
                    v_ship->>'phone',
                    v_ship->>'phone2',
                    v_ship->>'address',
                    v_ship->>'memo',
                    now() + INTERVAL '6 months'
                )
                ON CONFLICT (channel, order_no) DO UPDATE SET
                    name    = EXCLUDED.name,
                    phone   = EXCLUDED.phone,
                    phone2  = EXCLUDED.phone2,
                    address = EXCLUDED.address,
                    memo    = EXCLUDED.memo;
            END IF;

        EXCEPTION WHEN OTHERS THEN
            v_failed := v_failed + 1;
            v_errors := v_errors || jsonb_build_object(
                'row', v_idx,
                'order_no', v_txn->>'order_no',
                'error', SQLERRM
            );
        END;
    END LOOP;

    -- import_runs 결과 갱신
    UPDATE import_runs SET
        success_count = v_inserted + v_updated,
        changed_count = v_updated,
        fail_count    = v_failed,
        error_summary = CASE WHEN v_failed > 0 THEN v_errors ELSE NULL END,
        status        = CASE
                          WHEN v_failed = 0 THEN 'completed'
                          WHEN v_inserted + v_updated > 0 THEN 'partial'
                          ELSE 'failed'
                        END
    WHERE id = p_import_run_id;

    RETURN jsonb_build_object(
        'inserted', v_inserted,
        'updated',  v_updated,
        'skipped',  v_skipped,
        'failed',   v_failed,
        'errors',   v_errors
    );
END;
$$;


-- ============================================================
-- 2) rpc_cancel_or_edit_order
--    주문 수정/취소/환불 (RPC only)
-- ============================================================
CREATE OR REPLACE FUNCTION rpc_cancel_or_edit_order(
    p_order_id   BIGINT,
    p_change_type TEXT,   -- '수정'/'취소'/'환불'
    p_payload    JSONB,   -- 수정 시: {"qty": 3, "unit_price": 15000, ...}
    p_reason     TEXT,
    p_user       TEXT
)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    v_existing   RECORD;
    v_field      TEXT;
    v_old_val    TEXT;
    v_new_val    TEXT;
BEGIN
    -- 기존 주문 조회
    SELECT * INTO v_existing
    FROM order_transactions
    WHERE id = p_order_id;

    IF v_existing IS NULL THEN
        RETURN jsonb_build_object('success', false, 'error', '주문을 찾을 수 없습니다');
    END IF;

    -- 이미 취소/환불된 주문은 재변경 불가
    IF v_existing.status IN ('취소', '환불') AND p_change_type IN ('취소', '환불') THEN
        RETURN jsonb_build_object('success', false, 'error', '이미 ' || v_existing.status || ' 처리된 주문입니다');
    END IF;

    IF p_change_type = '취소' OR p_change_type = '환불' THEN
        -- 취소/환불 처리
        INSERT INTO order_change_log (
            order_transaction_id, channel, order_no,
            field_name, before_value, after_value,
            change_type, change_reason, changed_by
        ) VALUES (
            p_order_id, v_existing.channel, v_existing.order_no,
            'status', v_existing.status, p_change_type,
            p_change_type, p_reason, p_user
        );

        UPDATE order_transactions SET
            status            = p_change_type,
            status_reason     = p_reason,
            status_changed_at = now()
        WHERE id = p_order_id;

        -- shipping도 취소 상태로
        UPDATE order_shipping SET
            shipping_status = '취소'
        WHERE channel  = v_existing.channel
          AND order_no = v_existing.order_no;

    ELSIF p_change_type = '수정' THEN
        -- 수정 처리: payload의 각 필드별 change_log
        FOR v_field, v_new_val IN
            SELECT key, value#>>'{}'
            FROM jsonb_each(p_payload)
        LOOP
            v_old_val := CASE v_field
                WHEN 'qty'             THEN v_existing.qty::TEXT
                WHEN 'unit_price'      THEN v_existing.unit_price::TEXT
                WHEN 'total_amount'    THEN v_existing.total_amount::TEXT
                WHEN 'discount_amount' THEN v_existing.discount_amount::TEXT
                WHEN 'product_name'    THEN v_existing.product_name
                WHEN 'order_date'      THEN v_existing.order_date::TEXT
                ELSE NULL
            END;

            IF v_old_val IS DISTINCT FROM v_new_val THEN
                INSERT INTO order_change_log (
                    order_transaction_id, channel, order_no,
                    field_name, before_value, after_value,
                    change_type, change_reason, changed_by
                ) VALUES (
                    p_order_id, v_existing.channel, v_existing.order_no,
                    v_field, v_old_val, v_new_val,
                    '수정', p_reason, p_user
                );
            END IF;
        END LOOP;

        -- 실제 필드 업데이트
        UPDATE order_transactions SET
            qty             = COALESCE((p_payload->>'qty')::INT, qty),
            unit_price      = COALESCE((p_payload->>'unit_price')::NUMERIC, unit_price),
            total_amount    = COALESCE((p_payload->>'total_amount')::NUMERIC, total_amount),
            discount_amount = COALESCE((p_payload->>'discount_amount')::NUMERIC, discount_amount),
            product_name    = COALESCE(p_payload->>'product_name', product_name),
            order_date      = COALESCE((p_payload->>'order_date')::DATE, order_date),
            status_changed_at = now()
        WHERE id = p_order_id;

    ELSE
        RETURN jsonb_build_object('success', false, 'error', '올바르지 않은 change_type: ' || p_change_type);
    END IF;

    RETURN jsonb_build_object('success', true, 'change_type', p_change_type, 'order_id', p_order_id);
END;
$$;


-- ============================================================
-- 3) 권한 부여 (anon/authenticated 역할)
--    Supabase는 기본적으로 anon key로 호출하므로 EXECUTE 권한 필요
-- ============================================================
GRANT EXECUTE ON FUNCTION rpc_upsert_order_batch(BIGINT, JSONB) TO anon, authenticated;
GRANT EXECUTE ON FUNCTION rpc_cancel_or_edit_order(BIGINT, TEXT, JSONB, TEXT, TEXT) TO anon, authenticated;


-- ============================================================
-- 4) RLS 비활성화 (anon key INSERT/UPDATE 허용)
--    Supabase는 새 테이블에 RLS 자동 활성화 → anon key 차단됨
-- ============================================================
ALTER TABLE import_runs DISABLE ROW LEVEL SECURITY;
ALTER TABLE order_transactions DISABLE ROW LEVEL SECURITY;
ALTER TABLE order_shipping DISABLE ROW LEVEL SECURITY;
ALTER TABLE order_change_log DISABLE ROW LEVEL SECURITY;
