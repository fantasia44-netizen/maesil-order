"""
services/option_matcher.py
옵션마스터 매칭 공통 모듈

OrderProcessor, marketplace_validation_service, marketplace blueprint에서
공통으로 사용하는 옵션 매칭 유틸리티.

Key 규칙: 원문명.replace(" ", "").upper()
"""


def build_match_key(mode: str, product_name: str, option_name: str) -> str:
    """채널별 매칭 키 생성.

    스마트스토어/해미애찬: 상품명 + 옵션명 결합
    쿠팡/그 외: 옵션명만 사용 (옵션명이 비어있으면 상품명 fallback)

    Returns:
        정규화 전 원문 키 (공백 포함) — match_option에서 정규화함
    """
    prod = str(product_name or '').strip()
    opt = str(option_name or '').strip()

    if mode in ('스마트스토어', '해미애찬'):
        # 스마트스토어: "상품명 옵션명" 결합
        if prod and opt:
            return f"{prod} {opt}"
        return prod or opt
    else:
        # 쿠팡/자사몰/옥션G마켓/오아시스/11번가/카카오 등
        # 옵션명이 주 키, 없으면 상품명
        return opt if opt else prod


def _normalize(key: str) -> str:
    """공백 제거 + 대문자 정규화 (Key 비교 기준)."""
    return str(key or '').replace(' ', '').upper()


def prepare_opt_list(opt_list: list) -> None:
    """옵션 리스트에 정규화 Key를 미리 계산해 주입 (성능 최적화).

    opt_list 각 항목에 'Key' 필드가 없으면 '원문명' 기준으로 생성.
    in-place 수정.
    """
    for o in opt_list:
        if 'Key' not in o or not o['Key']:
            o['Key'] = _normalize(str(o.get('원문명', '')))


def match_option(key: str, opt_list: list) -> dict | None:
    """정규화된 Key로 옵션마스터에서 일치 항목 탐색.

    Args:
        key: build_match_key()가 반환한 원문 키
        opt_list: prepare_opt_list() 처리된 옵션마스터 리스트

    Returns:
        일치 항목 dict (Key, 품목명, 바코드, 라인코드, 출력순서 포함) or None
    """
    if not key:
        return None
    normalized = _normalize(key)
    for o in opt_list:
        o_key = o.get('Key') or _normalize(str(o.get('원문명', '')))
        if o_key == normalized:
            return o
    return None


def check_option_registration(orders: list, channel: str, opt_list: list) -> dict:
    """주문 목록에 대해 옵션마스터 등록 여부를 일괄 검사.

    Args:
        orders: [{'product_name': ..., 'option_name': ...}, ...] 형태
        channel: 채널명 (build_match_key mode로 사용)
        opt_list: prepare_opt_list() 처리된 옵션마스터 리스트

    Returns:
        {
            'registered': int,          # 매칭 성공 건수
            'unregistered': int,        # 미매칭 건수
            'unregistered_items': list, # 미매칭 원문 키 목록 (중복 제거)
        }
    """
    registered = 0
    unregistered_set = []  # 순서 유지 중복제거용

    for o in orders:
        prod = str(o.get('product_name') or '').strip()
        opt = str(o.get('option_name') or '').strip()
        key = build_match_key(channel, prod, opt)
        match = match_option(key, opt_list)
        if match:
            registered += 1
        else:
            if key and key not in unregistered_set:
                unregistered_set.append(key)

    return {
        'registered': registered,
        'unregistered': len(orders) - registered,
        'unregistered_items': unregistered_set,
    }
