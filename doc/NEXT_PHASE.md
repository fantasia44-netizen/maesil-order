# 다음 작업 — AutoTool

> 마지막 업데이트: 2026-03-28
> 마지막 커밋: 226b0b1 (2026-03-27)

## 현재 상태
- 주문관리 검색/대시보드 버그 수정 완료 (Render 배포됨)
- 기타출고 버그 5건 수정 완료
- 송장관리 CJ API 운영승인 대기 중

## 우선순위

### 🔴 HIGH — 운영 안정화

**1. CJ택배 운영 승인 + 실운영 전환**
- 개발서버 테스트 완료, 운영 승인 대기
- 문서: `doc/CJ_API_운영승인요청서.docx`
- .env: CJ_USE_PROD=true로 전환 예정
- 고객번호 A(30494329, lc=5) / B(B133030971, lc=0~4)

**2. 자동주문수집 검증**
- 네이버/쿠팡/카페24 API 연동 코드 있음 (blueprints/orders_api.py)
- 실운영 검증 필요 (배마마 계정)

**3. 옵션매칭 안정화**
- option_matcher.py 3단계 (학습→정확→퍼지) 구현됨
- 실데이터 검증 필요

### 🟡 MEDIUM

**4. 축산물 MES 모듈 (쿡대디용)**
- DB 테이블 생성 → 이력번호 API → 묶음번호 → 수율관리
- 설계서: `doc/축산물_MES_설계서.md`

**5. 매출분석 캐시**
- 현재 매번 DB 쿼리 → 캐시 추가

### 🟢 LOW — 3PL (autotool_accounting/3pl)

**6. 과금 엔진 UI**
- 020 SQL 마이그레이션 미실행 (cookdaddy DB)
- 요금 템플릿 UI + 손익 분석 대시보드

**7. 3PL API 주문수집**
- autotool marketplace 코드 이식
- 화주별 API 키 세팅 구조

**8. 3PL 현장 화면 개선**
- 피킹 자동화, 반품관리, No-Click UI
- 설계서: `3pl/doc/Phase4_현장화면분할_설계서.md`

## 작업 순서 요약
```
1. CJ 운영승인 → 송장 실운영
2. 자동주문수집 실검증
3. 축산물 MES Phase 1~2
4. 3PL 과금 UI + 020 SQL
5. 3PL API 주문수집 이식
```

## 주의사항
- db_supabase.py 5,230줄 — repo 분리 진행 중 (12개 repo, shim 전환 예정)
- 커밋 시 새 파일 git add 확인 (option_matcher 사고 재발 방지)
- push는 사용자 명령 후에만 (Render 자동배포)
- DB 스키마 변경 시 Supabase RPC 동시 수정
