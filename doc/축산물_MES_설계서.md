# [통합툴 확장] 축산물 생산·이력 관리 시스템(MES) 구축 계획서

> **작성 목적**: 기존 통합툴(ERP/WMS) 내에 식육포장처리 및 축산물 가공 업체를 위한 전용 생산관리 모듈 신규 구축
> **핵심 과제**: 외부 정부 API 연동을 통한 이력번호 자동 조회, 묶음번호 생성, 법적 의무 전산 신고 자동화
> **대상**: 쿡대디 통합툴 (autotool)
> **AI 리뷰**: Gemini 작성 → API 실존 검증 완료

---

## 1. 외부 API 실존 검증 결과

### 1-1. 국내산 축산물통합이력정보 API ✅ 실존 확인

| 항목 | 내용 |
|------|------|
| **제공기관** | 축산물품질평가원 (EKAPE) |
| **공공데이터포털** | [data.go.kr/data/15058923](https://www.data.go.kr/data/15058923/openapi.do) |
| **엔드포인트** | `http://data.ekape.or.kr/openapi-data/service/user/animalTrace/traceNoSearch` |
| **인증** | 공공데이터포털 API 키 (자동승인) |
| **호출 제한** | 개발 10,000건/일, 운영 활용사례 등록 시 증가 |
| **입력** | 소/돼지 이력번호 12자리 또는 묶음번호 15자리 |
| **출력** | 품종, 성별, 출생일, 도축장명, 도축일, 등급, 부위, 유통정보 등 |
| **구현 가능성** | ✅ **즉시 가능** — 자동승인, REST API, 무료 |

### 1-2. 쇠고기이력정보 API ✅ 실존 확인

| 항목 | 내용 |
|------|------|
| **공공데이터포털** | [data.go.kr/data/15056898](https://www.data.go.kr/data/15056898/openapi.do) |
| **용도** | 소 개체별 상세 이력 (통합이력 API와 병행 사용 가능) |
| **구현 가능성** | ✅ **즉시 가능** |

### 1-3. 수입축산물 이력정보 API ✅ 실존 확인

| 항목 | 내용 |
|------|------|
| **제공기관** | 농림축산검역본부 (MeatWatch) |
| **공공데이터포털** | [data.go.kr/data/15118023](https://www.data.go.kr/data/15118023/openapi.do) |
| **MeatWatch 포털** | [meatwatch.go.kr](https://www.meatwatch.go.kr/cs/wsm/selectWsrvIntrcnHtml.do) |
| **연동 방식** | REST, Java API, Web Service, JSP-XML |
| **입력** | 수입유통식별번호 12자리, 선하증권번호 등 |
| **출력** | 원산지, 도축장, 가공장, 수출/수입업체, 도축일, 유통기한 등 |
| **신청 절차** | 회원가입 → 오픈서비스 신청 → 관리자 승인 (1~2일) |
| **구현 가능성** | ✅ **가능** — 승인 절차 필요 |

### 1-4. MeatWatch 연계 오픈서비스 (거래내역/생산 입력) ✅ 실존 확인

| 항목 | 내용 |
|------|------|
| **용도** | 수입축산물 거래내역 + 제품생산내역 **입력(Push)** |
| **연동 방식** | REST, Java API, Web Service |
| **신청** | 기업전용 오픈서비스 별도 신청 → 키 발급 |
| **구현 가능성** | ✅ **가능** — 기업 신청 필요 |

### 1-5. 축산물품질평가원 전산신고 API ⚠️ 확인 필요

| 항목 | 내용 |
|------|------|
| **용도** | 국내산 이력제 전산신고 (입고/포장처리/출고) 자동화 |
| **mtrace.go.kr** | [mtrace.go.kr](https://mtrace.go.kr/) — 현재 웹 포털로만 신고 가능 |
| **API 존재** | ⚠️ **공개 API 미확인** — B2B 별도 협의 필요할 수 있음 |
| **대안** | 웹 스크래핑 또는 엑셀 자동 생성 → 수동 업로드 |
| **구현 가능성** | ⚠️ **직접 API 불가 시 엑셀 자동화로 우회** |

---

## 2. 실제 구현 가능한 범위 정리

```
즉시 구현 가능 (API 자동승인):
  ✅ 국내산 이력번호 → 도축정보/등급 자동 조회
  ✅ 쇠고기 개체별 상세 이력 조회

승인 후 구현 가능 (1~2일):
  ✅ 수입축산물 이력정보 조회
  ✅ 수입축산물 거래내역/생산내역 입력 (MeatWatch 연계)

협의 필요:
  ⚠️ 국내산 전산신고 API (mtrace.go.kr)
  → 대안: 신고 양식 엑셀 자동 생성 후 포털에 업로드
```

---

## 3. 축산물 생산관리 핵심 워크플로우

### Step 1. 원료육 입고 (Inbound)

```
현장:  바코드 스캔 (이력번호 12자리)
        ↓
시스템: 공공데이터 API 호출 → 도축일/등급/부위 자동 채움
        ↓
       실측 중량 입력 (저울 연동 또는 수동)
        ↓
       raw_meats 테이블 저장 (상태: 보관중)
        ↓
       [전산신고] 가능 시 API 자동, 불가 시 엑셀 생성
```

### Step 2. 생산 작업지시 + 수율 관리

```
현장:  작업지시서 확인 (예: 삼겹살 500g × 100팩)
        ↓
       창고에서 원료육 스캔 → 가공장 투입
        ↓
시스템: 원료육 상태 '보관중' → '생산투입'
        ↓
       투입 중량 합계 자동 집계
        ↓
       생산 완료 후:
       수율(%) = 산출 중량 / 투입 중량 × 100
       감량(kg) = 투입 - 산출 (뼈/지방/수분 등)
```

### Step 3. 묶음번호(Lot) 생성 ★ 핵심

```
시스템: 15자리 묶음번호 자동 생성
       예: L260319001 (L + YYMMDD + 일련번호)
        ↓
       lot_trace_mapping: 어떤 원료육이 얼마나 투입됐는지 N:M 기록
        ↓
       라벨 프린터 → 묶음번호 + 유통기한 + 보관방법 바코드 출력
        ↓
       [전산신고] 포장처리 신고 (API 또는 엑셀)
```

### Step 4. 완제품 출고

```
현장:  묶음번호 바코드 스캔 → 출고 확정
        ↓
시스템: stock_ledger 재고 차감
        ↓
       과금 트리거 (출고비 + 택배비)
        ↓
       [전산신고] 전출(양도) 신고
        ↓
       이력 역추적: 완제품 바코드 → 묶음번호 → 원료육 이력번호
```

---

## 4. DB 스키마

### 4-1. `raw_meats` (원료육 재고)

```sql
CREATE TABLE raw_meats (
    id BIGSERIAL PRIMARY KEY,
    trace_no TEXT NOT NULL,              -- 이력번호 12자리
    origin_type TEXT DEFAULT '국내',      -- 국내/수입
    part_name TEXT NOT NULL,             -- 부위명 (삼겹살, 목심 등)
    grade TEXT,                          -- 등급 (1+, 1, 2 등)
    breed TEXT,                          -- 품종 (한우, 한돈 등)
    gender TEXT,                         -- 성별 (암/수/거세)
    slaughter_date DATE,                 -- 도축일자
    slaughter_house TEXT,                -- 도축장명
    inbound_date DATE NOT NULL,          -- 입고일자
    inbound_weight NUMERIC NOT NULL,     -- 입고 중량 (kg)
    current_weight NUMERIC NOT NULL,     -- 현재 남은 중량
    location TEXT,                       -- 보관 위치
    storage_temp TEXT DEFAULT '냉장',     -- 냉장/냉동
    expiry_date DATE,                    -- 유통기한
    supplier TEXT,                       -- 공급업체명
    status TEXT DEFAULT '보관중',         -- 보관중/생산투입/소진
    api_trace_data JSONB DEFAULT '{}',   -- API 원본 응답 보관
    api_report_status TEXT DEFAULT 'pending',
    memo TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_raw_meats_trace ON raw_meats(trace_no);
CREATE INDEX idx_raw_meats_status ON raw_meats(status);
```

### 4-2. `production_lots` (생산 묶음)

```sql
CREATE TABLE production_lots (
    id BIGSERIAL PRIMARY KEY,
    lot_no TEXT UNIQUE NOT NULL,          -- 묶음번호 15자리
    product_name TEXT NOT NULL,           -- 완제품명
    production_date DATE NOT NULL,
    expiry_date DATE,
    pack_weight NUMERIC,                  -- 개별 포장 중량 (g)
    pack_count INT DEFAULT 0,             -- 포장 수량
    total_weight NUMERIC NOT NULL,        -- 총 산출 중량 (kg)
    input_weight NUMERIC DEFAULT 0,       -- 총 투입 중량 (kg)
    yield_rate NUMERIC DEFAULT 0,         -- 수율 (%)
    loss_weight NUMERIC DEFAULT 0,        -- 감량 (kg)
    status TEXT DEFAULT '생산완료',
    api_report_status TEXT DEFAULT 'pending',
    memo TEXT,
    created_by TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

### 4-3. `lot_trace_mapping` (이력 추적 매핑) ★

```sql
CREATE TABLE lot_trace_mapping (
    id BIGSERIAL PRIMARY KEY,
    lot_id BIGINT NOT NULL REFERENCES production_lots(id),
    raw_meat_id BIGINT NOT NULL REFERENCES raw_meats(id),
    used_weight NUMERIC NOT NULL,         -- 사용된 원료 중량 (kg)
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_ltm_lot ON lot_trace_mapping(lot_id);
CREATE INDEX idx_ltm_raw ON lot_trace_mapping(raw_meat_id);
```

---

## 5. 통합툴 연결점

```
기존 통합툴                    축산물 MES
───────────                  ───────────
stock_ledger          ←───── raw_meats 입고 시 재고 연동
product_costs         ←───── production_lots 완제품 마스터 연결
생산관리 (production) ←───── 작업지시/수율 연동
출고관리 (outbound)   ←───── 완제품 출고 시 과금 트리거
입고관리 (inbound)    ←───── 원료육 입고 시 stock_ledger 연동
```

---

## 6. 구현 우선순위

### Phase 1: DB + 기본 CRUD (즉시)
- 3개 테이블 생성
- 원료육 입고/조회 UI
- 묶음번호 생성 + 매핑 UI

### Phase 2: 공공데이터 API 연동 (즉시)
- API 키 발급 (자동승인)
- 이력번호 스캔 → 도축정보 자동 조회
- raw_meats에 api_trace_data 저장

### Phase 3: 수율 관리 + 라벨
- 투입 → 산출 수율 자동 계산
- 라벨 프린터 연동 (묶음번호 바코드)

### Phase 4: 전산신고 (협의 후)
- mtrace.go.kr 신고 자동화 (API 가능 시)
- 불가 시 엑셀 양식 자동 생성 → 다운로드
- MeatWatch 연계 (수입육)

---

## 7. 영업 포인트

1. **"매일 밤 엑셀 신고 → 자동화"** — 이력제 수기 등록 100% 대체
2. **"입고 스캔 1초 → 등급/도축일 자동"** — 타이핑 실수 = 과태료 방지
3. **"완벽한 이력 역추적"** — 완제품 바코드 → 어떤 도축장 언제 고기인지 1초

---

Sources:
- [축산물통합이력정보 API](https://www.data.go.kr/data/15058923/openapi.do)
- [쇠고기이력정보 API](https://www.data.go.kr/data/15056898/openapi.do)
- [수입축산물이력정보 API](https://www.data.go.kr/data/15118023/openapi.do)
- [MeatWatch 오픈서비스](https://www.meatwatch.go.kr/cs/wsm/selectWsrvIntrcnHtml.do)
- [축산물이력제 포털](https://mtrace.go.kr/)
