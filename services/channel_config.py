"""
채널별 주문서 컬럼 자동 인식 엔진 (Phase 1)

- 인덱스 하드코딩 제거 → 컬럼명 키워드 매칭
- 새 채널 추가 = 키워드 목록만 등록
- 채널 자동 감지 (컬럼 시그니처 기반)
- 플랫폼 기반 확장: 같은 플랫폼(네이버 등) 채널은 컬럼 템플릿 공유
"""
import unicodedata


# ============================================================
# 플랫폼 매핑 — 채널명 → 플랫폼 키
# ============================================================
# 새 네이버 채널 추가 시 여기에 한 줄만 추가하면 됨.
# DB(marketplace_api_config.platform)에도 저장 가능하며, 이 정적 맵이 폴백.
PLATFORM_MAP = {
    # 현행 채널명
    "스마트스토어_배마마":   "naver",
    "스마트스토어_해미애찬": "naver",
    "쿠팡":                 "coupang",
    "자사몰":               "cafe24",
    "옥션/G마켓":           "auction",
    "오아시스":             "oasis",
    "11번가":               "11st",
    "카카오":               "kakao",
    "N배송":                "naver",
    "N배송_수동":           "naver",
    # 레거시 호환 (DB 마이그레이션 전)
    "스마트스토어":         "naver",
    "해미애찬":             "naver",
}

# 플랫폼별 CHANNEL_COLUMN_MAP 템플릿 키
_PLATFORM_TEMPLATE = {
    "naver":    "naver_smartstore",
    "coupang":  "쿠팡",
    "cafe24":   "자사몰",
    "auction":  "옥션/G마켓",
    "oasis":    "오아시스",
    "11st":     "11번가",
    "kakao":    "카카오",
}


def get_platform(channel):
    """채널명 → 플랫폼 키 반환. '스마트스토어_' 접두사는 자동으로 naver."""
    p = PLATFORM_MAP.get(channel)
    if p:
        return p
    if channel and channel.startswith("스마트스토어_"):
        return "naver"
    return channel


def is_naver(channel):
    """네이버(스마트스토어) 플랫폼인지 판별."""
    return get_platform(channel) == "naver"


def has_n_delivery(channel):
    """N배송 필터링이 필요한 채널인지 (네이버 + simple_invoice가 아닌 채널)."""
    return is_naver(channel) and not is_simple_invoice(channel)


def is_simple_invoice(channel):
    """단순 송장 채널 (합포장 시 단 구분 없이 품목명만 표시)."""
    conf = CHANNEL_COLUMN_MAP.get(channel, {})
    if conf.get('_simple_invoice'):
        return True
    # 레거시 세트 폴백
    return channel in _SIMPLE_INVOICE_LEGACY


def get_column_template(channel):
    """채널에 대응하는 CHANNEL_COLUMN_MAP 키 반환.
    채널 자체가 MAP에 있으면 그대로, 없으면 플랫폼 템플릿으로 폴백."""
    if channel in CHANNEL_COLUMN_MAP:
        return channel
    platform = get_platform(channel)
    return _PLATFORM_TEMPLATE.get(platform, channel)


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
#   _simple_invoice : 단순 송장 모드
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

    # 네이버 스마트스토어 공통 템플릿 (모든 네이버 채널이 상속)
    "naver_smartstore": {
        "_signature":  ["상품주문번호"],
        "_encrypted":  True,
        "_password":   "1111",
        "order_no":    ["상품주문번호"],
        "order_group": ["주문번호"],
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
        "option":      ["옵션정보"],
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

    # 스마트스토어_해미애찬: 네이버 템플릿 상속 + 단순송장 오버라이드
    "스마트스토어_해미애찬": {
        "_simple_invoice": True,
        # 컬럼 키워드는 naver_smartstore 템플릿에서 상속 (get_column_template)
    },
}

# 레거시 채널명 → 현행 채널명 매핑 (DB 마이그레이션 후에도 코드 호환용)
LEGACY_CHANNEL_ALIAS = {
    "스마트스토어": "스마트스토어_배마마",
    "해미애찬":     "스마트스토어_해미애찬",
}


def resolve_channel(channel):
    """레거시 채널명을 현행 채널명으로 변환. 이미 현행이면 그대로."""
    return LEGACY_CHANNEL_ALIAS.get(channel, channel)

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
    naver_smartstore 템플릿 매칭 시 '스마트스토어_배마마' 반환 (기본 네이버 채널).
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
            # 네이버 템플릿 매칭 → 기본 네이버 채널 반환
            if ch_name == 'naver_smartstore':
                return '스마트스토어_배마마'
            return ch_name

    return None


def get_channel_config(channel):
    """채널 설정 반환 (메타 키 포함). 플랫폼 템플릿 폴백 지원."""
    conf = CHANNEL_COLUMN_MAP.get(channel)
    if conf:
        return conf
    # 플랫폼 템플릿 폴백
    tmpl_key = get_column_template(channel)
    return CHANNEL_COLUMN_MAP.get(tmpl_key, {})


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
    # 채널 자체 설정 + 플랫폼 템플릿 병합
    tmpl_key = get_column_template(channel)
    tmpl_conf = CHANNEL_COLUMN_MAP.get(tmpl_key, {})
    ch_own = CHANNEL_COLUMN_MAP.get(channel, {})
    # 채널 고유 설정이 템플릿을 오버라이드
    ch_conf = {**tmpl_conf, **ch_own} if ch_own is not tmpl_conf else tmpl_conf

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
    if get_platform(channel) != 'kakao':
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
    "스마트스토어_배마마":   "일반매출",
    "스마트스토어_해미애찬": "일반매출",
    "자사몰":               "자사몰매출",
    "옥션/G마켓":           "일반매출",
    "오아시스":             "일반매출",
    "11번가":               "일반매출",
    "카카오":               "일반매출",
    "쿠팡":                 "쿠팡매출",
    "N배송_수동":           "N배송",
    "N배송":                "N배송",
    # 레거시 호환
    "스마트스토어":         "일반매출",
    "해미애찬":             "일반매출",
}


def get_revenue_category(channel):
    """채널 → 매출 카테고리. 미등록이면 네이버 계열은 일반매출, 그 외 채널명 반환."""
    cat = CHANNEL_REVENUE_MAP.get(channel)
    if cat:
        return cat
    if is_naver(channel):
        return "일반매출"
    return channel

# DB 채널명 → 표시용 채널명 정규화
# (order_transactions.channel 값이 다양한 형태로 저장될 수 있으므로 통일)
CHANNEL_DISPLAY_MAP = {
    "N배송_수동":   "N배송",
    "N배송(용인)":  "N배송",
    "카카오쇼핑":   "카카오",
    # 레거시 정규화
    "해미예찬":     "스마트스토어_해미애찬",
    "해미애찬":     "스마트스토어_해미애찬",
    "스마트스토어": "스마트스토어_배마마",
}


def normalize_channel_display(ch):
    """DB 채널명을 표시용으로 정규화."""
    if not ch or ch in ('None', 'none', 'null'):
        return '기타'
    return CHANNEL_DISPLAY_MAP.get(ch, ch)

# 단순 송장 채널 — is_simple_invoice() 함수 사용 권장
_SIMPLE_INVOICE_LEGACY = {"해미애찬", "스마트스토어_해미애찬"}
SIMPLE_INVOICE_CHANNELS = _SIMPLE_INVOICE_LEGACY  # 하위호환

# 매출 유형 → 가격표 컬럼 매핑 (resolve_unit_price에서도 사용)
CATEGORY_PRICE_COL = {
    "일반매출":     "네이버판매가",
    "자사몰매출":   "자사몰판매가",
    "쿠팡매출":     "쿠팡판매가",
    "로켓":         "로켓판매가",
    "N배송":        "네이버판매가",
}

# 매출 계산 대상 카테고리 (이 리스트에 없으면 매출 기록 안 함)
REVENUE_CATEGORIES = list(CATEGORY_PRICE_COL.keys())

# daily_revenue 전용 카테고리 (order_transactions에 없고 daily_revenue에서만 관리)
# query_revenue() 합산 시 이 카테고리만 daily_revenue에서 가져옴
DAILY_REVENUE_ONLY_CATEGORIES = {"거래처매출", "로켓"}

# DB 전환 기준일 (이 날짜 이전의 daily_revenue는 모든 카테고리 조회)
DB_CUTOFF_DATE = "2026-03-01"

# 레거시 daily_revenue.category → 현재 채널명 매핑
LEGACY_CATEGORY_TO_CHANNEL = {
    "일반매출":     "스마트스토어_배마마",
    "쿠팡매출":     "쿠팡",
    "자사몰매출":   "자사몰",
    "N배송":        "N배송",
}
