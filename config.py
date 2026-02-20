import os
from datetime import timedelta

basedir = os.path.abspath(os.path.dirname(__file__))


class Config:
    # 보안 키 (배포 시 환경변수로 반드시 변경)
    SECRET_KEY = os.environ.get('SECRET_KEY') or os.urandom(32).hex()

    # DB
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL') or \
        'sqlite:///' + os.path.join(basedir, 'instance', 'app.db')
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # 세션 보안
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = 'Lax'
    PERMANENT_SESSION_LIFETIME = timedelta(hours=8)

    # 로그인 시도 제한
    LOGIN_MAX_ATTEMPTS = 5
    LOGIN_LOCKOUT_MINUTES = 15

    # 파일 업로드
    UPLOAD_FOLDER = os.path.join(basedir, 'uploads')
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
    SESSION_COOKIE_SECURE = True  # HTTPS only


class DevelopmentConfig(Config):
    DEBUG = True
