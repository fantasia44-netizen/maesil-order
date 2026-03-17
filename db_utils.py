"""Thread-safe DB access helper for multi-tenant Flask app.

app.db는 프로세스 전역 변수라 gthread(멀티스레드) 환경에서
다른 사업자 DB로 바뀔 수 있음 → g.db (요청별 격리) 사용.
"""
from flask import g


def get_db():
    """Return the current request's DB instance (set by before_request)."""
    db = g.get('db')
    if db is None:
        # before_request가 실행되지 않았거나 db_pool 초기화 실패 시 fallback
        from flask import current_app
        db = getattr(current_app, 'db', None)
        if db is None:
            raise RuntimeError('DB 연결을 사용할 수 없습니다. 잠시 후 다시 시도하세요.')
    return db
