"""
services/option_matcher.py
옵션마스터 매칭 공통 모듈

OrderProcessor, marketplace_validation_service, marketplace blueprint에서
공통으로 사용하는 옵션 매칭 유틸리티.

Key 규칙: 원문명.replace(" ", "").upper()
"""

# "옵션 없음" 판별용 키워드 — 이 값이 option에 있으면 상품명으로 폴백
_NO_OPT = {'단일상품', '옵션없음', '옵션 없음', '기본', '해당없음',
            '없음', '-', 'noption', 'none', 'n/a', '상품정보참조'}


def _is_no_option(val):
    """option 값이 실질적으로 '없음'인지 판별"""
    if not val:
        return True
    v = val.strip()
    if not v:
        return True
    return v in _NO_OPT or any(nk in v for nk in ('단일상품',))


def build_match_key(mode: str, product_name: str, option_name: str) -> str:
    """채널별 매칭 키 생성.

    쿠팡: 옵션 없으면 상품명만, 있으면 상품명+옵션 결합
    옥션/G마켓: 옵션에서 '/' 앞부분 사용, 없으면 상품명
    스마트스토어/자사몰/오아시스/11번가/카카오 등: 옵션 유효하면 옵션, 아니면 상품명

    Returns:
        정규화 전 원문 키 (공백 포함) — match_option에서 정규화함
    """
    prod = str(product_name or '').strip()
    opt = str(option_name or '').strip()

    if mode == "쿠팡":
        # 쿠팡: 단일상품/빈옵션 → 상품명만, 아니면 상품명+옵션
        return prod if _is_no_option(opt) else prod + opt
    elif mode == "옥션/G마켓":
        # 옥션/G마켓: 옵션에서 '/' 앞부분 사용, 없으면 상품명
        return opt.split('/')[0].strip() if opt and not _is_no_option(opt) else prod
    else:
        # 스마트스토어/자사몰/오아시스/11번가/카카오/해미애찬 등
        # 옵션이 유효하면 옵션 사용, 단일상품/빈값이면 상품명 폴백
        return opt if opt and not _is_no_option(opt) else prod


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

    1차: 정확 매칭 (우선)
    2차: 부분 매칭 (가장 긴 Key 우선 — Key 최소 4자 + 품목명 비어있지 않은 항목만)

    Args:
        key: build_match_key()가 반환한 원문 키
        opt_list: prepare_opt_list() 처리된 옵션마스터 리스트

    Returns:
        일치 항목 dict (Key, 품목명, 바코드, 라인코드, 출력순서 포함) or None
    """
    if not key:
        return None
    normalized = _normalize(key)

    # 1차: 정확 매칭
    for o in opt_list:
        o_key = o.get('Key') or _normalize(str(o.get('원문명', '')))
        if o_key == normalized:
            return o

    # 2차: 부분 매칭 (가장 긴 Key 우선 → 오트밀가루 > 오트밀)
    candidates = [o for o in opt_list
                  if len(o.get('Key', '') or '') >= 4
                  and o.get('품목명', '').strip()
                  and (o.get('Key') or '') in normalized]
    if candidates:
        return max(candidates, key=lambda o: len(o.get('Key', '') or ''))

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
