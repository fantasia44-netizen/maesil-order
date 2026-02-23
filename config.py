import os
from datetime import timedelta

basedir = os.path.abspath(os.path.dirname(__file__))

SUPABASE_URL = "https://pbocckpuiyzijspqpvqz.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBib2Nja3B1aXl6aWpzcHFwdnF6Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzA3MDI0NjgsImV4cCI6MjA4NjI3ODQ2OH0.-oh6BjjSaOOSEavwK3xbvX5AkYPLJUp9VuGbcLWuFHc"


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

    # IP 기반 로그인 시도 제한 (무차별 대입 방지)
    IP_RATE_LIMIT_ATTEMPTS = 20        # IP당 최대 시도 횟수
    IP_RATE_LIMIT_WINDOW = 900         # 제한 윈도우 (초) = 15분
    IP_RATE_LIMIT_BLOCK_DURATION = 1800  # 차단 시간 (초) = 30분

    # 세션 비활동 타임아웃 (분)
    SESSION_INACTIVITY_TIMEOUT = 60    # 60분 비활동 시 자동 로그아웃

    # 파일 업로드
    UPLOAD_FOLDER = os.path.join(basedir, 'uploads')
    OUTPUT_FOLDER = os.path.join(basedir, 'output')
    MAX_CONTENT_LENGTH = 50 * 1024 * 1024  # 50MB

    # 팀/권한 정의
    ROLES = {
        'admin': {'name': '관리자', 'level': 100},
        'manager': {'name': '총괄책임자', 'level': 80},
        'sales': {'name': '영업부', 'level': 50},
        'logistics': {'name': '물류팀', 'level': 50},
        'production': {'name': '생산부', 'level': 50},
        'general': {'name': '총무부', 'level': 50},
    }


class ProductionConfig(Config):
    """외부 접속 시 사용 (NAS 배포용)"""
    DEBUG = False

    # HTTPS 강제 (리버스프록시 뒤에서)
    SESSION_COOKIE_SECURE = True
    SESSION_COOKIE_SAMESITE = 'Lax'
    PREFERRED_URL_SCHEME = 'https'

    # 더 짧은 세션 타임아웃
    PERMANENT_SESSION_LIFETIME = timedelta(hours=4)
    SESSION_INACTIVITY_TIMEOUT = 30    # 30분

    # 더 엄격한 로그인 제한
    LOGIN_MAX_ATTEMPTS = 3
    IP_RATE_LIMIT_ATTEMPTS = 10


class DevelopmentConfig(Config):
    DEBUG = True
    SESSION_COOKIE_SECURE = False      # 로컬 HTTP 개발용
