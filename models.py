"""
models.py — Supabase 기반 User/AuditLog 모델 (Flask-Login 호환)
SQLAlchemy 제거, Supabase 직접 쿼리
"""
from datetime import datetime, timezone, timedelta
from flask_login import UserMixin
from werkzeug.security import generate_password_hash, check_password_hash


# ── check_db_v12 상수 (기존 그대로) ──

VERSION = "v12"
VERSION_DATE = "2026-02-20"

INV_TYPE_LABELS = {
    "INBOUND": "입고", "PRODUCTION": "생산", "PROD_OUT": "생산출고",
    "SALES_OUT": "판매출고", "SALES_RETURN": "판매반품",
    "MOVE_OUT": "이동출고", "MOVE_IN": "이동입고",
    "INIT": "기초재고", "REPACK_OUT": "소분투입", "REPACK_IN": "소분산출",
    "SET_OUT": "세트투입", "SET_IN": "세트산출",
    "ETC_OUT": "기타출고",
    "ETC_IN": "기타입고",
    "ADJUST": "재고조정",
}

ETC_OUT_REASONS = ["무상출고", "실험사용", "샘플", "폐기", "클레임", "오배송", "기타"]

FOOD_TYPES = ["농산물", "수산물", "축산물"]

LEDGER_CATEGORY_MAP = {
    "제품수불부": ["제품", "완제품"],
    "반제품수불부": ["반제품"],
    "원료수불부": ["원료", "원재료"],
    "부자재수불부": ["부자재"],
}

REVENUE_CATEGORIES = ["일반매출", "자사몰매출", "쿠팡매출", "로켓", "N배송", "N배송(용인)", "거래처매출"]
APPROVAL_LABELS = ["담당자", "과장", "본부장", "상무", "대표"]
TEMPLATE_OPTIONS = [
    "재고현황", "제품수불부", "반제품수불부", "원료수불부",
    "부자재수불부", "생산일지", "소분작업일지",
]

CHANGE_LOG = [
    "v12.2: 부서별 권한 설정 + 행사/쿠폰 관리",
    "v12.1: 거래 관리 모듈 — 거래처 등록, 수동 거래등록(재고연동), 거래명세서 PDF, 내 사업장 관리",
    "v12: 소분(리패킹) 탭 추가",
    "v11: HACCP 템플릿 기반 PDF",
]

# ── 메뉴 페이지 레지스트리 (동적 권한 관리용) ──
# (page_key, name, icon, url, default_roles)
PAGE_REGISTRY = [
    ('dashboard',      '대시보드',       'bi-house',              '/',                    ['admin','ceo','manager','sales','logistics','production','general']),
    ('stock',          '재고 현황',      'bi-box',                '/stock',               ['admin','ceo','manager','sales','logistics','production','general']),
    ('orders',         '온라인주문처리',  'bi-cart',               '/orders',              ['admin','manager','sales']),
    ('order_manage',   '주문 관리',      'bi-clipboard-data',     '/orders/manage',       ['admin','manager','sales']),
    ('n_delivery',     'N배송 수동입력',  'bi-pencil-square',      '/orders/n-delivery',   ['admin','manager','sales']),
    ('aggregation',    '통합 집계',      'bi-calculator',         '/aggregation',         ['admin','manager','sales']),
    ('shipment',       '출고 관리',      'bi-box-arrow-right',    '/shipment',            ['admin','ceo','manager','sales','logistics','general']),
    ('closing',        '일일마감',       'bi-calendar-check',     '/closing',             ['admin','manager','sales','logistics']),
    ('price',          '판매관리',       'bi-tags',               '/price',               ['admin','manager','sales','general']),
    ('trade',          '거래처 관리',    'bi-building',           '/trade',               ['admin','ceo','manager','sales','general']),
    ('outbound',       '거래처주문처리',  'bi-truck',              '/outbound',            ['admin','ceo','manager','sales','general']),
    ('purchase_order', '발주서 관리',    'bi-file-earmark-text',  '/trade/purchase-order', ['admin','manager','sales','general']),
    ('revenue',        '매출 관리',      'bi-currency-won',       '/revenue',             ['admin','ceo','manager','sales','general']),
    ('promotions',     '행사/쿠폰',     'bi-megaphone',          '/promotions',          ['admin','manager','sales','general']),
    ('inbound',        '입고 관리',      'bi-box-arrow-in-down',  '/inbound',             ['admin','manager','logistics','production']),
    ('production',     '생산 관리',      'bi-gear',               '/production',          ['admin','manager','logistics','production']),
    ('adjustment',     '재고 조정',      'bi-pencil-square',      '/adjustment',          ['admin','manager','production','logistics','general']),
    ('set_assembly',   '세트작업',       'bi-boxes',              '/set-assembly',        ['admin','manager','sales','logistics','production','general']),
    ('transfer',       '창고 이동',      'bi-arrow-left-right',   '/transfer',            ['admin','manager','logistics','general']),
    ('repack',         '소분 관리',      'bi-scissors',           '/repack',              ['admin','manager','production']),
    ('etc_outbound',   '기타출고',       'bi-box-arrow-right',    '/etc-outbound',        ['admin','manager','sales','logistics','production','general']),
    ('ledger',         '수불장',         'bi-journal-text',       '/ledger',              ['admin','manager','logistics','production','general']),
    ('history',        '이력 관리',      'bi-clock-history',      '/history',             ['admin','manager','logistics','production','general']),
    ('bom_cost',       'BOM 원가',       'bi-piggy-bank',         '/bom-cost',            ['admin','manager']),
    ('yield_mgmt',     '수율 관리',      'bi-graph-up',           '/yield',               ['admin','manager','production']),
    ('planning',       '생산계획',       'bi-clipboard-data',     '/planning',            ['admin','ceo','manager','production']),
    ('base_data',      '기초 데이터',    'bi-hdd',                '/base-data',           ['admin','manager']),
    ('master',         '마스터 관리',    'bi-database',           '/master',              ['admin']),
    ('admin_users',    '사용자 관리',    'bi-people',             '/admin/users',         ['admin']),
    ('admin_perms',    '권한 설정',      'bi-shield-lock',        '/admin/permissions',   ['admin']),
    ('admin_logs',     '감사 로그',      'bi-shield-check',       '/admin/logs',          ['admin']),
    ('integrity',      '정합성 검사',    'bi-clipboard2-check',   '/integrity',           ['admin','manager']),
]


# ── Supabase 기반 User 클래스 (Flask-Login 호환) ──

class User(UserMixin):
    """Supabase app_users 테이블과 매핑되는 User 클래스"""

    @staticmethod
    def _parse_dt(val):
        """ISO datetime 문자열을 datetime 객체로 변환. None/실패 시 None."""
        if val is None:
            return None
        if isinstance(val, str):
            try:
                return datetime.fromisoformat(val.replace('Z', '+00:00'))
            except (ValueError, TypeError):
                return None
        return val

    def __init__(self, data=None):
        if data is None:
            data = {}
        self.id = data.get('id')
        self.username = data.get('username', '')
        self.name = data.get('name', '')
        self.password_hash = data.get('password_hash', '')
        self.role = data.get('role', 'sales')
        self.is_active_user = data.get('is_active_user', True)
        self.is_approved = data.get('is_approved', False)
        self.failed_login_count = data.get('failed_login_count', 0)
        self.locked_until = self._parse_dt(data.get('locked_until'))
        self.last_login = self._parse_dt(data.get('last_login'))
        self.password_changed_at = data.get('password_changed_at')
        self.created_at = self._parse_dt(data.get('created_at'))
        self.updated_at = self._parse_dt(data.get('updated_at'))

    def get_id(self):
        return str(self.id)

    @property
    def is_active(self):
        return self.is_active_user and self.is_approved

    def set_password(self, password):
        self.password_hash = generate_password_hash(password, method='scrypt')
        self.password_changed_at = datetime.now(timezone.utc).isoformat()

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

    def is_locked(self):
        if self.locked_until and isinstance(self.locked_until, datetime):
            return self.locked_until > datetime.now(timezone.utc)
        return False

    @property
    def role_name(self):
        from config import Config
        return Config.ROLES.get(self.role, {}).get('name', self.role)

    @property
    def role_level(self):
        from config import Config
        return Config.ROLES.get(self.role, {}).get('level', 0)

    def has_permission(self, required_level):
        return self.role_level >= required_level

    def can_view_all(self):
        return self.role_level >= 80

    def is_admin(self):
        return self.role == 'admin'
