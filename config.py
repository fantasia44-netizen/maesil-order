import os
from datetime import timedelta

basedir = os.path.abspath(os.path.dirname(__file__))

SUPABASE_URL = "https://pbocckpuiyzijspqpvqz.supabase.co"
SUPABASE_KEY = "sb_publishable_5TAy2FEAWeRmRCbOz6S14g_x4a8aOYI"


class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY') or os.urandom(32).hex()

    # Supabase
    SUPABASE_URL = os.environ.get('SUPABASE_URL') or SUPABASE_URL
    SUPABASE_KEY = os.environ.get('SUPABASE_KEY') or SUPABASE_KEY

    # 세션 보안
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = 'Lax'
    PERMANENT_SESSION_LIFETIME = timedelta(hours=8)

    # 로그인 시도 제한
    LOGIN_MAX_ATTEMPTS = 5
    LOGIN_LOCKOUT_MINUTES = 15

    # 파일 업로드
    UPLOAD_FOLDER = os.path.join(basedir, 'uploads')
    OUTPUT_FOLDER = os.path.join(basedir, 'output')
    MAX_CONTENT_LENGTH = 50 * 1024 * 1024  # 50MB

    # 팀/권한 정의
    ROLES = {
        'admin': {'name': '관리자', 'level': 100},
        'manager': {'name': '책임자', 'level': 80},
        'sales': {'name': '영업팀', 'level': 50},
        'logistics': {'name': '물류팀', 'level': 50},
        'production': {'name': '생산팀', 'level': 50},
    }


class ProductionConfig(Config):
    SESSION_COOKIE_SECURE = True


class DevelopmentConfig(Config):
    DEBUG = True
