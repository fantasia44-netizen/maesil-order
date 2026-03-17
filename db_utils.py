"""Thread-safe DB access helper for multi-tenant Flask app.

app.db는 프로세스 전역 변수라 gthread(멀티스레드) 환경에서
다른 사업자 DB로 바뀔 수 있음 → g.db (요청별 격리) 사용.
"""
from flask import g


def get_db():
    """Return the current request's DB instance (set by before_request)."""
    return g.db
