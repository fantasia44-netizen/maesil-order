-- 배송 상태 추적용 컬럼 추가
-- 2026-03-23: 운송상태 자동 추적 기능

ALTER TABLE order_shipping
  ADD COLUMN IF NOT EXISTS delivery_status TEXT,
  ADD COLUMN IF NOT EXISTS delivery_status_raw TEXT,
  ADD COLUMN IF NOT EXISTS delivery_status_updated_at TIMESTAMPTZ;

-- 인덱스: 추적 대상 조회 최적화
CREATE INDEX IF NOT EXISTS idx_order_shipping_delivery_tracking
  ON order_shipping (shipping_status, delivery_status)
  WHERE shipping_status = '발송'
    AND (delivery_status IS NULL OR delivery_status NOT IN ('배송완료', '구매확정'));

COMMENT ON COLUMN order_shipping.delivery_status IS '정규화 배송상태: 발송대기/발송완료/배송중/배송완료/구매확정';
COMMENT ON COLUMN order_shipping.delivery_status_raw IS '마켓플레이스 원본 상태값';
COMMENT ON COLUMN order_shipping.delivery_status_updated_at IS '마지막 상태 폴링 시각';
