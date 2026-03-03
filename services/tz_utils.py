"""
tz_utils.py — KST 시간대 유틸리티.

Render(UTC 서버)에서 한국 시간(KST = UTC+9) 기준으로 동작하기 위한 공통 헬퍼.
모든 날짜/시간 관련 로직에서 datetime.now() 대신 now_kst()를 사용해야 합니다.
"""
from datetime import datetime, timedelta

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")


def now_kst():
    """KST 기준 현재 시각 반환."""
    return datetime.now(KST)


def today_kst():
    """KST 기준 오늘 날짜 문자열 (YYYY-MM-DD)."""
    return now_kst().strftime('%Y-%m-%d')


def days_ago_kst(days):
    """KST 기준 N일 전 날짜 문자열 (YYYY-MM-DD)."""
    return (now_kst() - timedelta(days=days)).strftime('%Y-%m-%d')
