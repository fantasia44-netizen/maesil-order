import os
from datetime import timedelta
from dotenv import load_dotenv

basedir = os.path.abspath(os.path.dirname(__file__))
load_dotenv(os.path.join(basedir, '.env'))

SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')

# 쿡대디 Supabase (별도 프로젝트)
COOKDADDY_SUPABASE_URL = os.environ.get('COOKDADDY_SUPABASE_URL')
COOKDADDY_SUPABASE_KEY = os.environ.get('COOKDADDY_SUPABASE_KEY')

# 사업자 설정
DEFAULT_BUSINESS = 'baemama'
BUSINESSES = {
    'baemama': {
        'name': '배마마',
        'supabase_url': SUPABASE_URL,
        'supabase_key': SUPABASE_KEY,
        'color': '#2c3e50',
        'icon': 'bi-gear-wide-connected',
        'exclude_pages': [],
    },
    'cookdaddy': {
        'name': '쿡대디',
        'supabase_url': os.environ.get('COOKDADDY_SUPABASE_URL') or COOKDADDY_SUPABASE_URL,
        'supabase_key': os.environ.get('COOKDADDY_SUPABASE_KEY') or COOKDADDY_SUPABASE_KEY,
        'color': '#c0392b',
        'icon': 'bi-fire',
        'exclude_pages': [
            'orders', 'order_manage', 'n_delivery', 'rocket_manual',
            'aggregation', 'shipment', 'promotions',
            # 회계 ERP (배마마 전용)
            'bank_transactions', 'tax_invoices', 'ar_management',
            'ap_management', 'settlements', 'accounting_reports',
        ],
    },
}


class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY') or os.urandom(32).hex()

    # Supabase
    SUPABASE_URL = os.environ.get('SUPABASE_URL') or SUPABASE_URL
    SUPABASE_KEY = os.environ.get('SUPABASE_KEY') or SUPABASE_KEY

    # 세션 보안
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = 'Lax'
    SESSION_COOKIE_SECURE = False
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

    # 일일 마감 설정
    DAILY_CUTOFF_TIME = '15:05'   # 기본 매출마감·재고출고마감 시각 (오후 3시 5분)

    # 파일 업로드
    UPLOAD_FOLDER = os.path.join(basedir, 'uploads')
    OUTPUT_FOLDER = os.path.join(basedir, 'output')
    MAX_CONTENT_LENGTH = 100 * 1024 * 1024  # 100MB (패킹 영상 업로드)

    # 패킹센터 영상 설정
    PACKING_VIDEO_MAX_BYTES = 100 * 1024 * 1024   # 100MB
    PACKING_VIDEO_BITRATE = 1_000_000              # 1 Mbps

    # ── CODEF (코드에프) 설정 ──
    CODEF_DEMO_CLIENT_ID = os.environ.get('CODEF_DEMO_CLIENT_ID', '')
    CODEF_DEMO_CLIENT_SECRET = os.environ.get('CODEF_DEMO_CLIENT_SECRET', '')
    CODEF_CLIENT_ID = os.environ.get('CODEF_CLIENT_ID', '')
    CODEF_CLIENT_SECRET = os.environ.get('CODEF_CLIENT_SECRET', '')
    CODEF_PUBLIC_KEY = os.environ.get('CODEF_PUBLIC_KEY', '')
    CODEF_IS_TEST = os.environ.get('CODEF_IS_TEST', 'true').lower() == 'true'
    CODEF_MODE = os.environ.get('CODEF_MODE', 'sandbox')  # sandbox/demo/product

    # ── Popbill (팝빌) 설정 ──
    POPBILL_LINK_ID = os.environ.get('POPBILL_LINK_ID', 'TESTER')
    POPBILL_SECRET_KEY = os.environ.get('POPBILL_SECRET_KEY', '')
    POPBILL_IS_TEST = os.environ.get('POPBILL_IS_TEST', 'true').lower() == 'true'
    POPBILL_IP_RESTRICT = False   # Render 유동 IP 대응
    POPBILL_CORP_NUM = os.environ.get('POPBILL_CORP_NUM', '')  # 우리 사업자번호

    # 팀/권한 정의
    ROLES = {
        'admin': {'name': '관리자', 'level': 100},
        'ceo': {'name': '대표', 'level': 90},
        'manager': {'name': '총괄책임자', 'level': 80},
        'sales': {'name': '영업부', 'level': 50},
        'logistics': {'name': '물류팀', 'level': 50},
        'production': {'name': '생산부', 'level': 50},
        'general': {'name': '총무부', 'level': 50},
        'packing': {'name': '위탁업체', 'level': 10},
    }


class ProductionConfig(Config):
    """외부 접속 시 사용 (NAS 배포용)"""
    DEBUG = False

    # HTTPS 강제 (리버스프록시 뒤에서)
    SESSION_COOKIE_SECURE = True
    SESSION_COOKIE_SAMESITE = 'Lax'
    PREFERRED_URL_SCHEME = 'https'

    # 세션 타임아웃
    PERMANENT_SESSION_LIFETIME = timedelta(hours=8)
    SESSION_INACTIVITY_TIMEOUT = 120    # 120분 (2시간) 비활동 시 자동 로그아웃

    # 더 엄격한 로그인 제한
    LOGIN_MAX_ATTEMPTS = 3
    IP_RATE_LIMIT_ATTEMPTS = 10


class DevelopmentConfig(Config):
    DEBUG = True
    SESSION_COOKIE_SECURE = False      # 로컬 HTTP 개발용
