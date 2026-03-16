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

## 주요 모듈 (blueprints/ — 34개+)
- dashboard, master, inbound, outbound, production, repack, transfer
- stock, ledger, revenue, trade, aggregation, set_assembly
- adjustment, etc_outbound, history, promotions, price_mgmt
- bom_cost, yield_mgmt, base_data, mobile, orders, admin (admin.py)
- **closing** — 일일마감
- **shipment** — 출고관리
- **integrity** — 정합성 검사 (무결성 계층)
- **planning** — 생산계획 + 판매분석
- **accounting** — 회계 대시보드, 매출-입금 매칭
- **bank** — 은행/카드 계좌 연결, 거래내역, 엑셀 업로드
- **tax_invoice** — 세금계산서 (홈택스 엑셀 업/다운로드)
- **journal** — 분개장 (복식부기)
- **marketplace** — 마켓플레이스 송장등록/정산/검증
- **packing** — 택배 송장 (CJ대한통운 연동)

## 주요 서비스 (services/ — 35개+)
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
- **codef_service** — CODEF 은행/카드 API (인증서 로그인 포함)
- **popbill_service** — 팝빌 세금계산서 API (보류중)
- **bank_service / bank_excel_service** — 은행 거래내역 + 엑셀 파서
- **card_service** — 카드 거래내역 동기화
- **tax_invoice_service** — 세금계산서 (홈택스 파서)
- **matching_service** — 매출-입금-정산-매입 자동매칭
- **journal_service** — 분개장 복식부기
- **settlement_service** — 채널별 정산
- **marketplace_sync_service** — 마켓플레이스 송장 push
- **marketplace_validation_service** — 마켓플레이스 데이터 검증
- **courier/cj_client** — CJ대한통운 택배 연동

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

## 은행/카드/정산 (2026-03-13 최신)
- **CODEF 연동**: 은행 계좌 + 카드 거래내역 자동 동기화
  - 모드: sandbox(테스트) / demo(실은행,무료) / product(실은행,유료)
  - 환경변수: CODEF_MODE, CODEF_DEMO_CLIENT_ID/SECRET, CODEF_PUBLIC_KEY
  - **공인인증서 로그인 지원**: loginType='0', derFile + keyFile (Base64) + 비밀번호(RSA)
  - ID/PW 로그인: loginType='1'
  - 인증서 파일 위치: `C:\Users\{사용자}\AppData\LocalLow\NPKI\` (signCert.der + signPri.key)
- **은행 엑셀 업로드**: bank_excel_service.py — 국민/신한/우리/농협 등 은행별 파서
- **카드 거래내역**: card_service.py — CODEF 카드사 연동 + 분류
- **매칭 엔진**: matching_service.py — 매출-입금, 정산-입금, 매입-출금 자동매칭
- bank_transactions, bank_matching, card_transactions 테이블

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

## 치명적 사고 이력 — 재발 방지 필수

### 1. 새 파일 커밋 누락 사고 (2026-03-13~16, option_matcher.py)
- `order_processor.py`에서 매칭 로직을 `option_matcher.py`로 분리 후 **새 파일을 git add 안 함**
- 로컬은 정상, Render 배포 → `No module named` → 온라인주문처리 3일 장애
- 다른 PC에서 복구 시 원본 로직 모르고 단순화 → 옵션매칭 전면 깨짐
- **규칙**: 커밋 전 `git status`로 untracked 새 파일 확인, import와 새 파일은 반드시 같은 커밋, 복구 시 `git show 커밋^` 으로 원본 로직 확인

### 2. RPC 동시 수정 누락 (2026-03-05, collection_date)
- order_transactions에 컬럼 추가 후 Supabase RPC 미수정 → collection_date 누락
- **규칙**: 스키마 변경 시 관련 RPC 함수도 반드시 동시 수정

## 중요 버그 수정 이력 (2026-03-12)
- **페이지네이션 ORDER BY 누락**: _paginate_query 사용하는 14곳에 .order("id") 추가
  - ORDER BY 없이 OFFSET 페이지네이션 → 행 중복/누락 → 수불장 수치 매번 다름
- **로켓배송 재고차감일**: collection_date → order_date → today 3단계 fallback (4곳)
  - order_to_stock_service.py 내 모든 non-N배송 stk_date 할당
- **품목명 공백 정규화**: db_supabase.py `_normalize_product_names()` INSERT 시 적용
  - stock_service.py query_all_stock_data에서 조회 시에도 공백 제거
- **로그인 속도 최적화**: 백그라운드 스레드(auth.py), TTL 캐시(db_supabase.py), 세션 캐시(app.py)

## 세금계산서 (2026-03-13 최신)
- **팝빌 → 홈택스 엑셀 전환**: 팝빌 비용 문제로 보류, 홈택스 엑셀 업/다운로드 방식
- **파서**: tax_invoice_service.py `parse_hometax_excel()` — 헤더 자동 탐지 + 면세/과세 판별
- **배치 중복 체크**: `query_existing_invoice_numbers()` → 승인번호 기준
- **취소 로직**: 팝빌 취소 실패해도 DB 취소 처리 (데모 데이터 등)
- **테스트 데이터 삭제**: `/delete-test-data` — draft + cancelled 건 일괄 삭제
- 팝빌 코드 주석 보존 → 추후 복원 가능

## 분개장 (journal) — 2026-03-13 추가
- blueprints/journal.py, services/journal_service.py
- 복식부기 분개 입력/조회, 시산표

## 레포 구조 (2PC/3PC 공유)
- **autotool** (Private): `C:\autotool_git\autotool\` → Render 배포 (메인)
- **autotool_accounting** (Public): `C:\autotool_git\` → 회계 전용 개발
- 회계 코드는 autotool에 합쳐져 있음. **양쪽 수정 시 동기화 필요**
- `.claude/MEMORY.md`를 git으로 3대 PC 공유

## 중요 Jinja2/Python 주의사항
- `dict.items`는 Jinja2에서 dict의 `.items()` 메서드로 해석됨 → `dict['items']`로 접근
- Python 같은 클래스 내 동일 이름 메서드 → 나중 정의가 덮어씀 (중복 주의)

## 3PL — PackFlow SaaS (2026-03-15 신규)
- **경로**: autotool_accounting 레포 `3pl/` 폴더
- **별도 Flask 앱** (Supabase, Multi-tenant operator_id 기반)
- **구조**: blueprints(api/client/operator/packing), repositories(10개), services(7개)
- **과금/정산**: 7개 카테고리(입고/출고/보관/택배/부자재/반품/부가서비스), 21개 프리셋
- **출고관리**: 일반출고, 반품출고, 창고이동 (shipment_type)
- **migrations**: 7개 SQL (001~007)
- **버그점검 완료**: 반품출고 재고 미반영 수정 (log_movement + adjust_stock)
- **상세**: `3pl/docs/architecture_review_20260315.md`

## 상세 문서
- [프로젝트 구조 상세](project_structure.md)
- [Phase 1 계획서](phase1_plan.md)
- [채널별 컬럼 매핑](channel_column_mapping.md)
