"""
db/base.py — DB Repository 공통 베이스.

db_supabase.py의 공통 유틸을 추출하여 모든 repo가 상속.
"""


class BaseRepo:
    """도메인 Repository 공통 베이스.

    Args:
        client: Supabase client 인스턴스 (supabase.create_client() 결과)
    """

    def __init__(self, client):
        self.client = client

    def _safe_execute(self, operation_name: str, func, *args, **kwargs):
        """공통 에러 핸들러 — 실패 시 None 반환 + 로그.

        Returns:
            성공: func 결과
            실패: default (기본 None)
        """
        default = kwargs.pop('_default', None)
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f"[DB] {operation_name} error: {e}")
            return default
