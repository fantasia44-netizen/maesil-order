# AutoTool 프로젝트 메모리

## 프로젝트 개요
- **이유식 재료 주문처리 자동화 시스템** (Danny님 운영)
- Flask 웹앱 + Supabase DB + 로컬 Python 스크립트
- 경로: `C:\autotool`

## 아키텍처
- **웹앱**: Flask (blueprints 구조), Flask-Login, CSRF, Jinja2 templates
- **DB**: Supabase (db_supabase.py, 96KB+)
- **Python 실행**: `"C:\Program Files\PyManager\python.exe"`
- **배포**: Render (프로덕션), HTTPS 강제, 리버스 프록시
- **core/**: 데이터 무결성 보호 계층 (ValidationEngine + IntegrityMonitor)

## 역할 (config.py ROLES)
- admin(100), ceo(90), manager(80), sales(50), logistics(50), production(50), general(50)
- CEO: 모바일 대시보드 전용 (ceo_dashboard.html)

## 주요 모듈 (blueprints/ — 29개)
- dashboard, master, inbound, outbound, production, repack, transfer
- stock, ledger, revenue, trade, aggregation, set_assembly
- adjustment, etc_outbound, history, promotions, price_mgmt
- bom_cost, yield_mgmt, base_data, mobile, orders, admin (admin.py)
- **closing** — 일일마감
- **shipment** — 출고관리
- **integrity** — 정합성 검사 (무결성 계층)
- **planning** — 생산계획 + 판매분석

## 주요 서비스 (services/ — 24개)
- ledger_service, revenue_service, transfer_service, validation
- inbound_service, outbound_service, production_service
- set_assembly_service, bom_cost_service, yield_service
- aggregator, etc_outbound_service, excel_io, adjustment_service
- channel_config, order_processor, order_to_stock_service
- **dashboard_service** — KPI/대시보드 데이터
- **planning_service** — 수량 기반 생산계획 엔진
- **sales_analysis_service** — 월간 판매분석
- **stock_service, repack_service** — 재고/소분 서비스
- **tz_utils** — KST 시간대 유틸리티

## core/ (데이터 무결성 계층)
- **validation_engine.py** — 1차 실시간 검증 (트랜잭션 전)
- **integrity_monitor.py** — 2차 사후 감시 (정합성 점검)

## 리포트 (reports/)
- invoice_report, snapshot_report, production_daily, repack_daily
- inbound_daily, ledger_report, purchase_order_report, pdf_common

## check_db_v12/ (로컬 데스크탑 앱)
- Tkinter 기반 앱 (로컬 DB 체크 도구)
- 자체 models, db_supabase, reports 포함

## 주문처리 스크립트
- order_tool 시리즈 (order_tool.py ~ order_tool_final.py)
- 집계프로그램 (집계프로그램2_0.py, 집계프로그램3_0.py)
- 옵션 매칭: 학습→정확→퍼지 3단계

## 인증/보안
- Flask-Login, CSRF 보호, 세션 비활동 타임아웃
- 권한 테이블 (PAGE_REGISTRY 기반, 37개 메뉴)
- auth.py 별도 모듈
- 데이터 무결성 보호 계층 (core/)

## 지원 채널 (8개)
스마트스토어, 쿠팡, 옥션/G마켓, 자사몰, 오아시스, 11번가, 카카오, 해미애찬
- [채널별 컬럼 매핑](channel_column_mapping.md)

## 매출 구조
- daily_revenue 사전계산 제거 → order_transactions 실시간 집계 방식
- FIFO lot_number/grade 추적 (입고·생산·창고이동)

## Phase 1: 주문 수집 파이프라인 (완료/운영중)
- [계획서](phase1_plan.md) | [요구사항](phase1_order_pipeline.md)
- 주요 파일: create_tables.py, create_rpc_functions.sql
- services/channel_config.py, services/order_processor.py
- blueprints/orders.py, templates/orders/*

## 회계/재무 (finance/)
- **PNL(손익표)**: pnl_service.py — 매출→매출원가→판관비/제조경비→영업외→당기순이익
- **매출원가**: 세금계산서/계산서 기반 자동 집계 (거래처별 by_vendor)
- **비용 카테고리**: expense_categories 테이블 (판관비/제조경비/영업외 분류)
  - 수동입력: 인건비, 임차료, 세금과공과, 복리후생비, 운반비, 보험료, 연구개발비, 이자비용, 지급수수료, 기타
  - 계산서 자동: 수도광열비, 소모품비, 포장비, 광고선전비 → 비활성화 (COGS에서 잡힘)
  - 감가상각비 → 비활성화 (별도 계산)
- **채널 라벨**: config.py CHANNEL_LABELS (스마트스토어→배마마/해미애찬 분리)
- **급여**: hr_service.py — 4대보험, 6단계 세율, 일할계산, 근태차감
  - payroll → expenses → P&L 자동 sync (sync_payroll_to_expenses)

## 은행/정산
- bank_transactions, bank_matching (미지급금 수동매칭)
- 은행 엑셀 업로드 기능

## SQL 마이그레이션 파일
- migrate_closing.sql — 일일마감 테이블
- migrate_integrity.sql — 무결성 관련 테이블
- migrate_planning.sql — 생산계획 관련 테이블
- migrate_hr_v3.sql — 급여 일할계산/근태차감 확장
- migrate_expense_categories_v2.sql — 비용 카테고리 정리

## 마켓플레이스 자동 송장등록 (2026-03-12 구현)
- **3개 채널**: 쿠팡(HMAC-SHA256), 네이버(OAuth2), Cafe24(OAuth2 refresh)
- **파일**: services/marketplace/{coupang,naver,cafe24}_client.py → `register_invoice()`
- **오케스트레이션**: services/marketplace_sync_service.py → `push_invoices()`
- **라우트**: blueprints/marketplace.py → POST /marketplace/push-invoices
- **택배사 코드**: config.py COURIER_CODES (CJ대한통운만, 채널별 코드 상이)
- **DB**: 기존 order_shipping + api_orders 테이블 재사용, 신규 SQL 불필요

## 중요 버그 수정 이력 (2026-03-12)
- **페이지네이션 ORDER BY 누락**: _paginate_query 사용하는 14곳에 .order("id") 추가
  - ORDER BY 없이 OFFSET 페이지네이션 → 행 중복/누락 → 수불장 수치 매번 다름
- **로켓배송 재고차감일**: collection_date → order_date → today 3단계 fallback (4곳)
  - order_to_stock_service.py 내 모든 non-N배송 stk_date 할당
- **품목명 공백 정규화**: db_supabase.py `_normalize_product_names()` INSERT 시 적용
  - stock_service.py query_all_stock_data에서 조회 시에도 공백 제거
- **로그인 속도 최적화**: 백그라운드 스레드(auth.py), TTL 캐시(db_supabase.py), 세션 캐시(app.py)

## 상세 문서
- [프로젝트 구조 상세](project_structure.md)
- [Phase 1 계획서](phase1_plan.md)
- [채널별 컬럼 매핑](channel_column_mapping.md)
