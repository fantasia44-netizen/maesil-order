"""
conftest.py — pytest 공통 fixture.

실제 DB 연결 + Flask 앱 컨텍스트 제공.
스테이징 데이터가 아닌 실 DB 읽기 전용 테스트 위주.
"""
import os
import sys
import pytest

# 프로젝트 루트를 path에 추가
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
os.chdir(os.path.join(os.path.dirname(__file__), '..'))

from dotenv import load_dotenv
load_dotenv()


@pytest.fixture(scope='session')
def app():
    """Flask 앱 인스턴스."""
    from app import create_app
    app = create_app()
    app.config['TESTING'] = True
    return app


@pytest.fixture(scope='session')
def db(app):
    """SupabaseDB 인스턴스."""
    with app.app_context():
        from db_utils import get_db
        yield get_db()


@pytest.fixture(scope='session')
def supabase_client():
    """Raw Supabase client (직접 쿼리용)."""
    from supabase import create_client
    url = os.getenv('SUPABASE_URL')
    key = os.getenv('SUPABASE_KEY')
    return create_client(url, key)
