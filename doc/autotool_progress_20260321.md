# AutoTool 통합시스템 — 상세 진행도 보고서

> **작성일**: 2026-03-21
> **시스템**: 쿡대디 이유식 통합 ERP/WMS
> **스택**: Flask 3.1 + Supabase + Bootstrap 5 + Render 배포
> **레포**: `fantasia44-netizen/autotool` (Private)

---

## 1. 전체 규모 요약

| 항목 | 수량 | 비고 |
|------|------|------|
| Python 파일 | 111개 | blueprints+services+core+reports+root |
| 총 Python LOC | ~46,700줄 | |
| HTML 템플릿 | 88개 | ~21,000줄 |
| SQL 마이그레이션 | 20개 | |
| pip 패키지 | 18개 | |
| 라우트(URL) | 337개 | |
| DB 메서드 | 255개 | db_supabase.py 단일 파일 |
| 테스트 파일 | 0개 | 자동화 테스트 없음 |
| TODO/FIXME | 0개 | |

---

## 2. 모듈별 완성도

### 2-1. 블루프린트 계층 (38개, 14,286줄)

| 모듈 | 파일 | 줄수 | 라우트 | 완성도 | 상태 |
|------|------|------|--------|--------|------|
| 마켓플레이스 | marketplace.py | 2,654 | 28 | 95% | 쿠팡/네이버/카페24 연동 완료 |
| 주문관리 | orders.py | 1,624 | 34 | 90% | 엑셀+API 수집, 재고차감 |
| 주문API연동 | orders_api.py | 317 | 4 | 85% | API 주문수집+롤백 신규 |
| 인사/급여 | hr.py | 1,024 | 33 | 90% | 4대보험, 세율, 근태 |
| 집계 | aggregation.py | 1,294 | 6 | 90% | 채널별 매출집계 |
| 패킹센터 | packing.py | 954 | 20 | 85% | 택배연동+영상녹화 |
| 출고관리 | outbound.py | 978 | 13 | 90% | 일반/반품/이동 출고 |
| 거래처 | trade.py | 922 | 22 | 90% | 매입/발주/납품 |
| 재고조정 | adjustment.py | 677 | 12 | 85% | 일반조정+일괄조정 |
| 회계 | accounting.py | 569 | 19 | 85% | 매출-입금 매칭 |
| 기준정보 | master.py | 577 | 15 | 90% | 품목/거래처/창고 |
| 은행/카드 | bank.py | 486 | 15 | 80% | CODEF+엑셀업로드 |
| 생산계획 | planning.py | 457 | 11 | 80% | 수량기반 계획 |
| BOM원가 | bom_cost.py | 371 | 10 | 80% | 원가계산 |
| 재무 | finance.py | 359 | 13 | 80% | P&L, 비용관리 |
| 세금계산서 | tax_invoice.py | 323 | 8 | 80% | 홈택스 엑셀 파서 |
| 소분 | repack.py | 362 | 10 | 85% | 소분작업 |
| 수불장 | ledger.py | 349 | 3 | 90% | 입출고 원장 |
| 생산 | production.py | 311 | 9 | 85% | 생산실행 |
| 매출 | revenue.py | 241 | 5 | 85% | 매출통계 강화 |
| 세트조립 | set_assembly.py | 259 | 5 | 80% | 세트품목 |
| 프로모션 | promotions.py | 243 | 9 | 75% | 증정/할인 |
| 이력조회 | history.py | 216 | 4 | 80% | 변경이력 |
| 기타출고 | etc_outbound.py | 208 | 4 | 80% | 샘플/폐기 |
| 모바일 | mobile.py | 204 | 6 | 70% | CEO 대시보드 |
| 출고관리 | shipment.py | 188 | 3 | 80% | 출고통계 |
| 재고이동 | transfer.py | 185 | 5 | 80% | 창고간 이동 |
| 일일마감 | closing.py | 170 | 4 | 75% | 마감집계 |
| 분개장 | journal.py | 153 | 5 | 75% | 복식부기 |
| 가격관리 | price_mgmt.py | 143 | 4 | 75% | 단가이력 |
| 기초데이터 | base_data.py | 130 | 3 | 70% | 코드관리 |
| 정합성 | integrity.py | 80 | 4 | 70% | 무결성 검사 |
| 대조 | reconciliation.py | 77 | 4 | 60% | 대사 기능 |
| 수율관리 | yield_mgmt.py | 73 | 3 | 75% | 생산수율 |
| 대시보드 | dashboard.py | 69 | 4 | 80% | 서비스 위임 |
| 재고현황 | stock.py | 254 | 3 | 85% | 재고조회 |
| 관리자 | admin.py (root) | 752 | — | 85% | 사용자/권한 |

### 2-2. 서비스 계층 (45개, 19,395줄)

| 규모 | 파일명 | 줄수 | 핵심 기능 |
|------|--------|------|-----------|
| **대형 (500+줄)** | | | |
| | order_processor.py | 1,238 | 주문→재고 파이프라인 |
| | order_to_stock_service.py | 897 | 재고차감 엔진 |
| | pnl_service.py | 709 | 손익계산서 |
| | repack_service.py | 659 | 소분관리 |
| | aggregator.py | 659 | 매출집계 |
| | financial_report_service.py | 627 | 재무보고 |
| | stock_service.py | 621 | 재고관리 핵심 |
| | outbound_service.py | 603 | 출고처리 |
| | tax_invoice_service.py | 584 | 세금계산서 |
| | matching_service.py | 572 | 매출-입금 매칭 |
| | channel_config.py | 559 | 채널 레지스트리 |
| | marketplace_sync_service.py | 559 | 마켓 동기화 |
| | revenue_service.py | 556 | 매출통계 |
| | production_service.py | 512 | 생산관리 |
| | yield_service.py | 502 | 수율관리 |
| | popbill_service.py | 496 | 팝빌 API |
| **중형 (200~499줄)** | | | |
| | marketplace_validation_service.py | 449 | 데이터 검증 |
| | journal_service.py | 409 | 분개장 |
| | planning_service.py | 394 | 생산계획 |
| | report_service.py | 369 | 리포트 |
| | bank_excel_service.py | 367 | 은행엑셀 파서 |
| | set_assembly_service.py | 339 | 세트조립 |
| | sales_analysis_service.py | 338 | 판매분석 |
| | bom_cost_service.py | 324 | BOM원가 |
| | excel_io.py | 300 | 엑셀 입출력 |
| | codef_service.py | 316 | CODEF API |
| | settlement_service.py | 282 | 정산처리 |
| | ledger_service.py | 279 | 수불장 |
| | reconciliation_service.py | 261 | 대사 |
| | shipment_stats_service.py | 207 | 출고통계 |
| | actual_cost_service.py | 206 | 실제원가 |
| | bank_service.py | 200 | 은행서비스 |
| **소형 (<200줄)** | | | |
| | etc_outbound_service.py | 179 | 기타출고 |
| | card_service.py | 171 | 카드거래 |
| | transfer_service.py | 161 | 창고이동 |
| | dashboard_service.py | 156 | KPI 대시보드 |
| | option_matcher.py | 148 | 옵션매칭 |
| | adjustment_service.py | 140 | 재고조정 |
| | api_order_converter.py | 122 | API주문 변환 |
| | hr_service.py | 662 | 인사/급여 |
| | validation.py | 94 | 입력검증 |
| | storage_helper.py | 93 | 파일저장 |
| | inbound_service.py | 81 | 입고처리 |
| | tz_utils.py | 29 | 시간대 유틸 |

### 2-3. 마켓플레이스 클라이언트 (6개, 1,996줄)

| 파일 | 줄수 | 채널 | 인증방식 |
|------|------|------|----------|
| naver_client.py | 538 | 네이버 스마트스토어 | OAuth2 |
| cafe24_client.py | 530 | 카페24 자사몰 | OAuth2 refresh |
| coupang_client.py | 481 | 쿠팡 | HMAC-SHA256 |
| naver_ad_client.py | 249 | 네이버 검색광고 | API키+서명 |
| base_client.py | 130 | 공통 베이스 | — |
| __init__.py | 68 | 팩토리 | — |

### 2-4. 리포트 (10개, 1,923줄)

| 파일 | 줄수 | 출력형태 |
|------|------|----------|
| ledger_report.py | 546 | PDF |
| payroll_report.py | 293 | PDF |
| pdf_common.py | 227 | 공통유틸 |
| purchase_order_report.py | 223 | PDF |
| invoice_report.py | 197 | PDF |
| snapshot_report.py | 116 | PDF |
| repack_daily.py | 130 | PDF |
| production_daily.py | 108 | PDF |
| inbound_daily.py | 83 | PDF |

### 2-5. 핵심 인프라

| 파일 | 줄수 | 역할 |
|------|------|------|
| db_supabase.py | 5,051 | DB 계층 (255개 메서드, 재시도/캐시/풀링) |
| app.py | 502 | Flask 팩토리 + 보안헤더 + 에러핸들러 |
| auth.py | 398 | 인증/RBAC/속도제한/세션관리 |
| models.py | 204 | 데이터 모델 |
| config.py | 174 | 환경별 설정 |
| core/integrity_monitor.py | 518 | 정합성 감시 |
| core/validation_engine.py | 515 | 실시간 검증 |

---

## 3. 기능 영역별 완성도

| 영역 | 완성도 | 핵심 구현 | 미완성/보류 |
|------|--------|-----------|-------------|
| **주문처리** | 90% | 엑셀수집, API수집, 옵션매칭, 재고차감 | 11번가/G마켓 API |
| **재고관리** | 90% | FIFO, lot추적, 수불장, 정합성검사 | 자동재주문점 |
| **생산관리** | 80% | 생산계획, 실행, 수율, BOM원가 | MES연동(설계완료) |
| **출고/배송** | 85% | 일반/반품/이동, CJ택배연동 | 다중택배사 |
| **마켓연동** | 90% | 쿠팡/네이버/카페24 송장+정산 | 추가채널 |
| **회계/재무** | 80% | P&L, 분개장, 매칭엔진, 비용관리 | 총계정원장 자동화 |
| **은행/카드** | 80% | CODEF 연동, 엑셀업로드, 카드동기화 | 실시간 알림 |
| **세금계산서** | 80% | 홈택스 엑셀 파서, 배치중복체크 | 팝빌 실서비스(보류) |
| **인사/급여** | 90% | 4대보험, 6단계세율, 일할계산, 근태 | 연말정산 |
| **리포트** | 75% | PDF 9종 (수불장/급여/발주/일일) | 이메일발송, 예약 |
| **대시보드** | 80% | KPI 카드, CEO모바일, 매출차트 | 실시간 갱신 |
| **보안/인증** | 90% | bcrypt, RBAC, CSRF, 속도제한, 감사로그 | 2FA |
| **배포/운영** | 80% | Docker, Render, Gunicorn, 보안헤더 | CI/CD, 모니터링 |
| **테스트** | 0% | — | 전체 미구현 |

---

## 4. 최근 1주 변경사항 (2026-03-15 ~ 03-21)

| 커밋 | 내용 | 영향 |
|------|------|------|
| 43ed3c0 | API 연동 상태 뱃지 실시간 업데이트 | 주문관리 UI |
| 4c7cf87 | Render bcrypt 누락 수정 | 네이버 API 인증 |
| 65ba8a9 | 채널명 하드코딩 전면 제거 | 템플릿+서비스 전체 |
| 181c048 | 채널 레지스트리 범용화 + API 미매칭 자동 롤백 | channel_config.py |
| c5dc7d5 | CSRF 토큰 단일 변수 통합 | JS 에러 방지 |
| e23ba0c | **주문관리 > API연동 탭 신규** | orders_api.py 신규 |
| 174ceb7 | Supabase Storage 한글 파일명 수정 | 파일업로드 |
| 91c44c5 | 세션 만료 시 fetch JSON 파싱 에러 방지 | 전역 안정성 |

---

## 5. 종합 평가

### 강점
- 337개 라우트, 255개 DB 메서드 — **실운영 가능한 규모**
- 마켓플레이스 3사 완전 연동 (주문수집→송장→정산)
- 데이터 무결성 보호 2중 계층 (validation_engine + integrity_monitor)
- 채널 레지스트리 범용화로 신규 채널 추가 용이

### 리스크
- **자동화 테스트 0건** — 회귀 버그 위험
- **CI/CD 없음** — 수동 배포 의존
- **단일 Gunicorn 워커** — 동시접속 한계
- **모니터링/알림 없음** — 장애 감지 지연

### 종합 완성도: **80%** (실운영 중, 확장 전 보강 필요)
