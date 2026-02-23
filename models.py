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
    "SALES_OUT": "판매출고", "MOVE_OUT": "이동출고", "MOVE_IN": "이동입고",
    "INIT": "기초재고", "REPACK_OUT": "소분투입", "REPACK_IN": "소분산출",
    "SET_OUT": "세트투입", "SET_IN": "세트산출",
    "ETC_OUT": "기타출고",
    "ETC_IN": "기타입고",
}

ETC_OUT_REASONS = ["무상출고", "실험사용", "샘플", "폐기", "클레임", "오배송", "기타"]

LEDGER_CATEGORY_MAP = {
    "제품수불부": ["제품", "완제품"],
    "반제품수불부": ["반제품"],
    "원료수불부": ["원료", "원재료"],
    "부자재수불부": ["부자재"],
}

REVENUE_CATEGORIES = ["일반매출", "쿠팡매출", "로켓", "N배송(용인)", "거래처매출"]
APPROVAL_LABELS = ["담당자", "과장", "본부장", "상무", "대표"]
TEMPLATE_OPTIONS = [
    "재고현황", "제품수불부", "반제품수불부", "원료수불부",
    "부자재수불부", "생산일지", "소분작업일지",
]

CHANGE_LOG = [
    "v12.1: 거래 관리 모듈 — 거래처 등록, 수동 거래등록(재고연동), 거래명세서 PDF, 내 사업장 관리",
    "v12: 소분(리패킹) 탭 추가",
    "v11: HACCP 템플릿 기반 PDF",
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
