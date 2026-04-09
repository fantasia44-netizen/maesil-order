"""품목명 정규화 — 전사 단일 규칙 (Single Source of Truth).

모든 product_name 의 저장/매칭/groupby/dict 키 생성은 이 모듈의 canonical() 을 통과해야 한다.

규칙 (심플):
  1. 모든 종류의 공백 제거 (일반/전각/NBSP/narrow NBSP/FIGURE SPACE/탭/개행)
  2. strip

의도적으로 포함하지 않은 것:
  - NFKC 정규화 (전각→반각, 한자 호환 변환) — 재고 매칭에 불필요
  - 대소문자 통일 — 옵션매칭 전용
  - 구분자(,/;) 통일 — 옵션매칭 전용

예:
  canonical("중기이유식 세트")    → "중기이유식세트"
  canonical("중기이유식　세트")    → "중기이유식세트" (전각 공백)
  canonical("  고구마&사과 퓨레 ") → "고구마&사과퓨레"
"""

# 제거 대상 공백 문자 집합
_WHITESPACE_CHARS = (
    ' ',        # 일반 공백
    '\u3000',   # 전각 공백 (IDEOGRAPHIC SPACE)
    '\u00a0',   # NBSP
    '\u202f',   # NARROW NO-BREAK SPACE
    '\u2007',   # FIGURE SPACE
    '\t',       # 탭
    '\r',       # CR
    '\n',       # LF
)


def canonical(name) -> str:
    """품목명 정규화 (전사 표준).

    Args:
        name: 입력 문자열 (None/빈값 허용)

    Returns:
        정규화된 문자열. 빈 입력은 '' 반환.
    """
    if not name:
        return ''
    s = str(name)
    for ch in _WHITESPACE_CHARS:
        if ch in s:
            s = s.replace(ch, '')
    return s.strip()


def canonical_or(name, fallback=''):
    """canonical() 이 빈 문자열이면 fallback 반환."""
    result = canonical(name)
    return result if result else fallback


# ── 호환성 재수출 (기존 normalize_product_name 사용처에서 import 가능) ──
normalize_product_name = canonical
_norm = canonical
