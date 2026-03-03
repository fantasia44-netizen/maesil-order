"""
채널별 주문서 컬럼 자동 인식 엔진 (Phase 1)

- 인덱스 하드코딩 제거 → 컬럼명 키워드 매칭
- 새 채널 추가 = 키워드 목록만 등록
- 채널 자동 감지 (컬럼 시그니처 기반)
"""
import unicodedata


# ============================================================
# 채널별 컬럼 키워드 매핑
# ============================================================
# _common: 모든 채널에서 공통으로 시도하는 키워드
# 채널별: 해당 채널 고유 키워드 (우선 적용) + 메타 설정
#
# 메타 키 (_로 시작):
#   _signature  : 채널 자동 감지용 컬럼명 (이 컬럼이 있으면 해당 채널)
#   _header_row : 헤더 행 번호 (기본 0)
#   _encrypted  : 암호화 여부
#   _password   : 복호화 비밀번호
#   _csv        : CSV 파일 여부
# ============================================================

CHANNEL_COLUMN_MAP = {
    "_common": {
        "order_no":   ["주문번호", "상품주문번호"],
        "order_group": ["주문번호"],   # 배송비 등 주문 단위 필드 그룹핑용
        "order_date": ["주문일시", "주문일", "결제일", "결제일시", "발주일"],
        "product":    ["상품명", "등록상품명", "주문상품명"],
        "option":     ["옵션정보", "옵션", "등록옵션명"],
        "qty":        ["수량", "구매수", "구매수(수량)"],
        "name":       ["수취인명", "수취인", "수취인이름", "수령인", "수령인명"],
        "phone":      ["수취인연락처1", "수취인전화번호", "수령인 휴대폰", "휴대폰번호", "수령지전화"],
        "phone2":     ["수취인연락처2", "수령인 전화번호", "전화번호"],
        "address":    ["기본배송지", "수취인 주소", "주소", "주소(도로명)"],
        "address2":   ["상세배송지"],
        "memo":       ["배송메세지", "배송메시지", "배송시 요구사항", "비고"],
        "status":     ["주문상태", "주문상태명"],
        # 금액 관련
        "unit_price": ["판매단가", "판매가", "옵션판매가(판매단가)"],
        "total":      ["결제금액", "판매금액", "주문금액"],
        "discount":   ["할인금액"],
        "settlement": ["정산예정금액", "정산기준금액"],
        "commission": ["서비스이용료"],
        "shipping_fee": ["배송비", "배송비 합계", "배송비 금액"],
    },

    "스마트스토어": {
        "_signature":  ["상품주문번호"],
        "_encrypted":  True,
        "_password":   "1111",
        # 채널 고유 키워드 (공통보다 우선)
        "order_no":    ["상품주문번호"],
        "order_group": ["주문번호"],       # 배송비 그룹핑용 (상품주문번호 ≠ 주문번호)
        "order_date":  ["주문일시", "결제일"],
        "option":      ["옵션정보"],
        "status":      ["주문상태"],
        "address":     ["기본배송지"],
        "address2":    ["상세배송지"],
        "phone":       ["수취인연락처1"],
        "phone2":      ["수취인연락처2"],
        "memo":        ["배송메세지"],
        "unit_price":  ["옵션가격", "상품가격"],
        "total":       ["최종 상품별 총 주문금액"],
        "discount":    ["최종 상품별 할인액"],
        "settlement":  ["정산예정금액"],
        "commission":  ["네이버페이 주문관리 수수료"],
        "commission2": ["매출연동 수수료"],
        "seller_discount": ["판매자 부담 할인액"],
        "n_ship":      ["배송속성"],
        "item_price":  ["상품가격"],
        "shipping_fee": ["배송비 합계"],
    },

    "쿠팡": {
        "_signature":  ["묶음배송번호"],
        "order_no":    ["주문번호"],
        "order_date":  ["주문일"],
        "product":     ["등록상품명", "노출상품명"],
        "option":      ["등록옵션명", "노출옵션명"],
        "qty":         ["구매수(수량)", "구매수"],
        "name":        ["수취인이름", "수취인"],
        "phone":       ["수취인전화번호"],
        "address":     ["수취인 주소"],
        "memo":        ["배송메세지"],
        "unit_price":  ["옵션판매가(판매단가)", "옵션판매가"],
        "total":       ["결제액"],
        "shipping_fee": ["배송비"],
    },

    "옥션/G마켓": {
        "_signature":  ["판매아이디"],
        "order_no":    ["주문번호"],
        "order_date":  ["주문일"],
        "name":        ["수령인명", "수령인"],
        "phone":       ["수령인 휴대폰"],
        "phone2":      ["수령인 전화번호"],
        "address":     ["주소"],
        "memo":        ["배송시 요구사항"],
        "unit_price":  ["판매단가"],
        "total":       ["판매금액"],
        "settlement":  ["정산예정금액"],
        "shipping_fee": ["배송비 금액"],
        "commission":  ["서비스이용료"],
        "seller_coupon":  ["판매자쿠폰할인"],
        "buyer_coupon":   ["구매쿠폰적용금액"],
    },

    "자사몰": {
        "_signature":  ["쇼핑몰번호"],
        "_csv":        True,
        "order_no":    ["주문번호"],
        "order_date":  ["발주일"],
        "product":     ["주문상품명"],
        "name":        ["수령인"],
        "phone":       ["핸드폰"],
        "phone2":      ["수령지전화"],
        "address":     ["주소"],
        "memo":        ["비고"],
        "unit_price":  ["판매가", "상품판매가"],
        "total":       ["결제금액", "총결제금액", "주문금액", "총주문금액", "상품금액"],
        "option_price": ["옵션추가 가격"],
        "shipping_fee": ["배송비"],
    },

    "오아시스": {
        "_signature":  ["배송매장"],
        "order_no":    ["주문번호"],
        "order_date":  ["주문일"],
        "name":        ["수령인"],
        "phone":       ["수령인 전화번호"],
        "address":     ["주소(도로명)"],
        "memo":        ["배송메시지"],
        "unit_price":  ["상품가", "판매가"],
        "total":       ["결제금액"],
        "item_price":  ["상품가"],
        "item_discount": ["상품할인가"],
        "discount":    ["할인금액"],
        "coupon":      ["쿠폰 적용 금액"],
        "points":      ["사용적립금"],
        "shipping_fee": ["배송비"],
    },

    "11번가": {
        "_signature":  ["주문순번"],
        "_header_row": 2,
        "order_no":    ["주문번호"],
        "order_date":  ["결제일시", "주문일시"],
        "name":        ["수취인"],
        "phone":       ["휴대폰번호"],
        "phone2":      ["전화번호"],
        "address":     ["주소"],
        "memo":        ["배송메시지"],
        "unit_price":  ["판매단가"],
        "total":       ["주문금액"],
        "option_price": ["옵션가"],
        "seller_discount":  ["판매자기본할인금액"],
        "extra_discount":   ["판매자 추가할인금액"],
        "commission":  ["서비스이용료"],
        "settlement":  ["정산예정금액"],
        "shipping_fee": ["배송비"],
    },

    "카카오": {
        "_signature":  ["정산기준금액"],
        "order_no":    ["주문번호"],
        "order_date":  ["주문일"],
        "status":      ["주문상태"],
        "item_price":  ["상품금액"],
        "option_price": ["옵션금액"],
        "seller_discount":  ["판매자할인금액"],
        "seller_coupon":    ["판매자쿠폰할인금액"],
        "settlement":  ["정산기준금액"],
        "commission":  ["기본수수료"],
        "courier":     ["택배사"],
        "invoice_no":  ["송장번호"],
        "shipping_fee": ["배송비"],
    },

    "해미애찬": {
        # 스마트스토어와 동일 포맷 (암호화 엑셀, 동일 컬럼)
        # _signature 없음 → 자동감지 불가, 수동 선택 전용
        "_encrypted":  True,
        "_password":   "1111",
        "_simple_invoice": True,   # 송장 생성 시 단(段) 구분 없이 품목명만 표시
        # 채널 고유 키워드 (스마트스토어와 동일)
        "order_no":    ["상품주문번호"],
        "order_group": ["주문번호"],       # 배송비 그룹핑용
        "order_date":  ["주문일시", "결제일"],
        "option":      ["옵션정보"],
        "status":      ["주문상태"],
        "address":     ["기본배송지"],
        "address2":    ["상세배송지"],
        "phone":       ["수취인연락처1"],
        "phone2":      ["수취인연락처2"],
        "memo":        ["배송메세지"],
        "unit_price":  ["옵션가격", "상품가격"],
        "total":       ["최종 상품별 총 주문금액"],
        "discount":    ["최종 상품별 할인액"],
        "settlement":  ["정산예정금액"],
        "commission":  ["네이버페이 주문관리 수수료"],
        "n_ship":      ["배송속성"],
        "shipping_fee": ["배송비 합계"],
    },
}

# 금액 관련 필드 목록 (order_transactions에 저장할 때 사용)
MONEY_FIELDS = [
    "unit_price", "total", "discount", "settlement", "commission",
    "item_price", "option_price", "shipping_fee",
    "seller_discount", "extra_discount",
    "commission2", "seller_coupon", "buyer_coupon",
    "coupon", "points", "item_discount",
]


def _normalize(text):
    """컬럼명 정규화: NFC + 공백 제거 + 대문자"""
    return unicodedata.normalize('NFC', str(text)).replace(" ", "").upper()


def find_column(df_columns, keywords):
    """
    DataFrame 컬럼 리스트에서 키워드와 매칭되는 컬럼명 반환.
    1차: 정확 매칭 (정규화 후)
    2차: 부분 매칭 (키워드가 컬럼명에 포함)
    """
    normalized_map = {_normalize(c): c for c in df_columns}

    for kw in keywords:
        kw_norm = _normalize(kw)
        if kw_norm in normalized_map:
            return normalized_map[kw_norm]

    # 부분 매칭
    for kw in keywords:
        kw_norm = _normalize(kw)
        for norm_key, orig in normalized_map.items():
            if kw_norm in norm_key:
                return orig

    return None


def find_column_index(df, keywords):
    """
    DataFrame에서 키워드와 매칭되는 컬럼의 인덱스 반환.
    매칭 실패 시 None.
    """
    col_name = find_column(df.columns, keywords)
    if col_name is not None:
        return list(df.columns).index(col_name)
    return None


def detect_channel(df):
    """
    DataFrame의 컬럼명으로 채널을 자동 감지.
    _signature 키워드가 모두 포함된 채널 반환.
    """
    cols_upper = set(_normalize(c) for c in df.columns)

    for ch_name, ch_conf in CHANNEL_COLUMN_MAP.items():
        if ch_name.startswith('_'):
            continue
        sigs = ch_conf.get('_signature', [])
        if not sigs:
            continue
        if all(
            any(_normalize(s) in c for c in cols_upper)
            for s in sigs
        ):
            return ch_name

    return None


def get_channel_config(channel):
    """채널 설정 반환 (메타 키 포함)"""
    return CHANNEL_COLUMN_MAP.get(channel, {})


def get_header_row(channel):
    """채널별 헤더 행 반환 (기본 0)"""
    conf = get_channel_config(channel)
    return conf.get('_header_row', 0)


def is_encrypted(channel):
    """채널 파일이 암호화되어 있는지"""
    conf = get_channel_config(channel)
    return conf.get('_encrypted', False)


def get_password(channel):
    """채널 파일 복호화 비밀번호"""
    conf = get_channel_config(channel)
    return conf.get('_password', '')


def is_csv(channel):
    """채널이 CSV 형식인지"""
    conf = get_channel_config(channel)
    return conf.get('_csv', False)


def build_column_map(df, channel):
    """
    주어진 채널과 DataFrame에 대해 전체 컬럼 매핑 빌드.

    반환: {
        'order_no': column_index_or_None,
        'order_date': column_index_or_None,
        'product': ...,
        'option': ...,
        ...
    }

    채널 고유 키워드 → _common 키워드 순서로 탐색.
    """
    common = CHANNEL_COLUMN_MAP.get('_common', {})
    ch_conf = CHANNEL_COLUMN_MAP.get(channel, {})

    # 매핑할 필드 목록 수집 (메타 키 제외)
    all_fields = set()
    for k in common:
        all_fields.add(k)
    for k in ch_conf:
        if not k.startswith('_'):
            all_fields.add(k)

    col_map = {}
    for field in all_fields:
        # 채널 고유 키워드 우선
        ch_keywords = ch_conf.get(field, [])
        common_keywords = common.get(field, [])
        combined = ch_keywords + [k for k in common_keywords if k not in ch_keywords]

        if combined:
            idx = find_column_index(df, combined)
            col_map[field] = idx
        else:
            col_map[field] = None

    return col_map


def validate_required_columns(col_map, channel):
    """
    필수 컬럼 검증.
    반환: (is_valid, missing_fields_list)
    """
    # 기본 필수 필드
    required = ['order_no', 'product', 'qty']

    # 카카오는 배송 정보 없음
    if channel != '카카오':
        required.extend(['name', 'phone', 'address'])

    missing = []
    for field in required:
        if col_map.get(field) is None:
            missing.append(field)

    return (len(missing) == 0, missing)


# 필드명 → 한글 라벨 (에러 메시지용)
FIELD_LABELS = {
    'order_no':    '주문번호',
    'order_date':  '주문일',
    'product':     '상품명',
    'option':      '옵션',
    'qty':         '수량',
    'name':        '수취인명',
    'phone':       '연락처',
    'phone2':      '연락처2',
    'address':     '주소',
    'address2':    '상세주소',
    'memo':        '배송메모',
    'status':      '주문상태',
    'unit_price':  '판매단가',
    'total':       '주문금액',
    'discount':    '할인액',
    'settlement':  '정산금액',
    'commission':  '수수료',
}


def get_field_label(field):
    """필드 키 → 한글 라벨"""
    return FIELD_LABELS.get(field, field)


# ============================================================
# Phase 2: 채널 → 매출유형(revenue_category) 매핑
# ============================================================
# order_transactions 자동 처리 시 channel → daily_revenue.category 결정
CHANNEL_REVENUE_MAP = {
    "스마트스토어": "일반매출",
    "자사몰":       "자사몰매출",
    "옥션/G마켓":   "일반매출",
    "오아시스":     "일반매출",
    "11번가":       "일반매출",
    "카카오":       "일반매출",
    "해미애찬":     "일반매출",
    "쿠팡":         "쿠팡매출",
    "N배송_수동":   "N배송(용인)",
    "N배송":        "N배송(용인)",
}

# DB 채널명 → 표시용 채널명 정규화
# (order_transactions.channel 값이 다양한 형태로 저장될 수 있으므로 통일)
CHANNEL_DISPLAY_MAP = {
    "N배송_수동":   "N배송",
    "N배송(용인)":  "N배송",
    "카카오쇼핑":   "카카오",
    "해미예찬":     "해미애찬",
}


def normalize_channel_display(ch):
    """DB 채널명을 표시용으로 정규화."""
    if not ch or ch in ('None', 'none', 'null'):
        return '기타'
    return CHANNEL_DISPLAY_MAP.get(ch, ch)

# 단순 송장 채널 (합포장 시 단(段) 구분 없이 품목명만 표시)
SIMPLE_INVOICE_CHANNELS = {"해미애찬"}

# 매출 유형 → 가격표 컬럼 매핑 (resolve_unit_price에서도 사용)
CATEGORY_PRICE_COL = {
    "일반매출":     "네이버판매가",
    "자사몰매출":   "자사몰판매가",
    "쿠팡매출":     "쿠팡판매가",
    "로켓":         "로켓판매가",
    "N배송":        "네이버판매가",
    "N배송(용인)":  "네이버판매가",
}

# 매출 계산 대상 카테고리 (이 리스트에 없으면 매출 기록 안 함)
REVENUE_CATEGORIES = list(CATEGORY_PRICE_COL.keys())

# daily_revenue 전용 카테고리 (order_transactions에 없고 daily_revenue에서만 관리)
# query_revenue() 합산 시 이 카테고리만 daily_revenue에서 가져옴
DAILY_REVENUE_ONLY_CATEGORIES = {"거래처매출", "로켓"}
