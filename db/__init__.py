"""
db/ — 도메인별 DB Repository 패키지.

기존 db_supabase.py(5,230줄/288메서드)를 도메인별로 점진 분리.
하위호환: db_supabase.SupabaseDB는 그대로 유지, 새 repo는 mixin으로 결합.

분리 순서 (AI 코드리뷰 권고):
  1. shipping_repo — 송장/배송 (16메서드)
  2. orders_repo — 주문/import_run (16메서드)
  3. inventory_repo — 재고/수불장 (15메서드)
  4. finance_repo — 매출/비용/은행/세금 (37메서드)
  5. marketplace_repo — API설정/동기화 (12메서드)
  6. auth_repo — 사용자/권한 (14메서드)
  7. hr_repo — 인사/급여 (19메서드)
  8. product_repo — 상품/옵션/마스터 (33메서드)
  9. packing_repo — 패킹 (7메서드)
  10. settlement_repo — 정산 (6메서드)

사용법:
  기존: from db_supabase import SupabaseDB → 그대로 동작
  신규: from db.shipping_repo import ShippingRepo → 도메인별 직접 사용 가능
"""
