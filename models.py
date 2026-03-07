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

REVENUE_CATEGORIES = ["일반매출", "자사몰매출", "쿠팡매출", "로켓", "N배송", "거래처매출"]
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
# (page_key, name, icon, url, default_roles, group)
# group: None=최상단 고정, 그 외 그룹명 문자열

MENU_GROUPS = ['주문·판매', '거래처', '재고·물류', '생산', '매출·분석', '관리']

PAGE_REGISTRY = [
    # ── 홈 (최상단 고정) ──
    ('dashboard',      '대시보드',       'bi-house',              '/',                    ['admin','ceo','manager','sales','logistics','production','general'], None),

    # ── 주문·판매 ──
    ('orders',         '온라인주문처리',  'bi-cart',               '/orders',              ['admin','manager','sales'],                                        '주문·판매'),
    ('order_manage',   '주문 관리',      'bi-clipboard-data',     '/orders/manage',       ['admin','manager','sales'],                                        '주문·판매'),
    ('n_delivery',     'N배송 수동입력',  'bi-pencil-square',      '/orders/n-delivery',   ['admin','manager','sales'],                                        '주문·판매'),
    ('rocket_manual',  '로켓매출 수동입력','bi-rocket-takeoff',     '/orders/rocket-manual', ['admin','manager','sales'],                                        '주문·판매'),
    ('import_runs',    '업로드이력 관리', 'bi-arrow-counterclockwise', '/orders/import-runs', ['admin','manager'],                                                '주문·판매'),
    ('aggregation',    '통합 집계',      'bi-calculator',         '/aggregation',         ['admin','manager','sales'],                                        '주문·판매'),
    ('shipment',       '출고 관리',      'bi-box-arrow-right',    '/shipment',            ['admin','ceo','manager','sales','logistics','general'],             '주문·판매'),
    ('price',          '판매가관리',     'bi-tags',               '/price',               ['admin','manager','sales','general'],                               '주문·판매'),
    ('promotions',     '행사/쿠폰',     'bi-megaphone',          '/promotions',          ['admin','manager','sales','general'],                               '주문·판매'),

    # ── 거래처 ──
    ('trade',          '거래처 관리',    'bi-building',           '/trade',               ['admin','ceo','manager','sales','general'],                         '거래처'),
    ('outbound',       '거래처주문처리',  'bi-truck',              '/outbound',            ['admin','ceo','manager','sales','general'],                         '거래처'),
    ('purchase_order', '발주서 관리',    'bi-file-earmark-text',  '/trade/purchase-order', ['admin','manager','sales','general'],                              '거래처'),

    # ── 재고·물류 ──
    ('stock',          '재고 현황',      'bi-box',                '/stock',               ['admin','ceo','manager','sales','logistics','production','general'], '재고·물류'),
    ('inbound',        '입고 관리',      'bi-box-arrow-in-down',  '/inbound',             ['admin','manager','logistics','production'],                        '재고·물류'),
    ('adjustment',     '재고 조정',      'bi-pencil-square',      '/adjustment',          ['admin','manager','production','logistics','general'],              '재고·물류'),
    ('transfer',       '창고 이동',      'bi-arrow-left-right',   '/transfer',            ['admin','manager','logistics','general'],                           '재고·물류'),
    ('etc_outbound',   '기타출고',       'bi-box-arrow-right',    '/etc-outbound',        ['admin','manager','sales','logistics','production','general'],      '재고·물류'),
    ('ledger',         '수불장',         'bi-journal-text',       '/ledger',              ['admin','manager','logistics','production','general'],              '재고·물류'),

    # ── 생산 ──
    ('production',     '생산 관리',      'bi-gear',               '/production',          ['admin','manager','logistics','production'],                        '생산'),
    ('planning',       '생산계획',       'bi-clipboard-data',     '/planning',            ['admin','ceo','manager','production'],                              '생산'),
    ('repack',         '소분 관리',      'bi-scissors',           '/repack',              ['admin','manager','production'],                                    '생산'),
    ('set_assembly',   '세트작업',       'bi-boxes',              '/set-assembly',        ['admin','manager','sales','logistics','production','general'],      '생산'),
    ('bom_cost',       'BOM 원가',       'bi-piggy-bank',         '/bom-cost',            ['admin','manager'],                                                '생산'),
    ('yield_mgmt',     '수율 관리',      'bi-graph-up',           '/yield',               ['admin','manager','production'],                                    '생산'),

    # ── 매출·분석 ──
    ('revenue',        '매출 관리',      'bi-currency-won',       '/revenue',             ['admin','ceo','manager','sales','general'],                         '매출·분석'),
    ('expenses',       '비용 관리',      'bi-cash-stack',         '/finance/expenses',    ['admin','manager'],                                                '매출·분석'),
    ('pnl',            '관리 손익표',    'bi-clipboard-data',     '/finance/pnl',         ['admin','ceo','manager'],                                          '매출·분석'),
    ('finance_dashboard','재무현황',     'bi-bar-chart-line',     '/finance/dashboard',   ['admin','ceo','manager'],                                          '매출·분석'),
    ('sales_analysis', '판매분석',       'bi-graph-up-arrow',     '/planning/sales',      ['admin','ceo','manager','production'],                              '매출·분석'),
    ('closing',        '일일마감',       'bi-calendar-check',     '/closing',             ['admin','manager','sales','logistics'],                             '매출·분석'),
    ('history',        '이력 관리',      'bi-clock-history',      '/history',             ['admin','manager','logistics','production','general'],              '매출·분석'),

    # ── 관리 ──
    ('employees',      '직원 관리',      'bi-person-badge',       '/hr/employees',        ['admin'],                                                          '관리'),
    ('payroll',        '급여 관리',      'bi-wallet2',            '/hr/payroll',          ['admin'],                                                          '관리'),
    ('leave',          '연차 관리',      'bi-calendar-event',     '/hr/leave',            ['admin','manager'],                                                '관리'),
    ('base_data',      '기초 데이터',    'bi-hdd',                '/base-data',           ['admin','manager'],                                                '관리'),
    ('master',         '마스터 관리',    'bi-database',           '/master',              ['admin'],                                                          '관리'),
    ('admin_users',    '사용자 관리',    'bi-people',             '/admin/users',         ['admin'],                                                          '관리'),
    ('admin_perms',    '권한 설정',      'bi-shield-lock',        '/admin/permissions',   ['admin'],                                                          '관리'),
    ('admin_logs',     '감사 로그',      'bi-shield-check',       '/admin/logs',          ['admin'],                                                          '관리'),
    ('integrity',      '정합성 검사',    'bi-clipboard2-check',   '/integrity',           ['admin','manager'],                                                '관리'),
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
        self.company_name = data.get('company_name', '')
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
