from datetime import datetime, timezone
from flask_sqlalchemy import SQLAlchemy
from flask_login import UserMixin
from werkzeug.security import generate_password_hash, check_password_hash

db = SQLAlchemy()


class User(UserMixin, db.Model):
    __tablename__ = 'users'

    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False, index=True)
    name = db.Column(db.String(100), nullable=False)  # 실명
    password_hash = db.Column(db.String(256), nullable=False)
    role = db.Column(db.String(20), nullable=False, default='sales')
    is_active_user = db.Column(db.Boolean, default=True)
    is_approved = db.Column(db.Boolean, default=False)  # 관리자 승인 필요

    # 보안
    failed_login_count = db.Column(db.Integer, default=0)
    locked_until = db.Column(db.DateTime, nullable=True)
    last_login = db.Column(db.DateTime, nullable=True)
    password_changed_at = db.Column(db.DateTime, nullable=True)

    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc),
                           onupdate=lambda: datetime.now(timezone.utc))

    # 관계
    logs = db.relationship('AuditLog', backref='user', lazy='dynamic')

    def set_password(self, password):
        self.password_hash = generate_password_hash(password, method='scrypt')
        self.password_changed_at = datetime.now(timezone.utc)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

    def is_locked(self):
        if self.locked_until and self.locked_until > datetime.now(timezone.utc):
            return True
        if self.locked_until and self.locked_until <= datetime.now(timezone.utc):
            self.failed_login_count = 0
            self.locked_until = None
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
        """책임자 이상: 모든 데이터 조회 가능"""
        return self.role_level >= 80

    def is_admin(self):
        return self.role == 'admin'


class AuditLog(db.Model):
    """감사 로그 - 주요 작업 기록"""
    __tablename__ = 'audit_logs'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=True)
    action = db.Column(db.String(50), nullable=False)  # login, logout, create, update, delete
    target = db.Column(db.String(200), nullable=True)   # 대상
    detail = db.Column(db.Text, nullable=True)
    ip_address = db.Column(db.String(45), nullable=True)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))


# ============================================================
# 아래는 나중에 회사에서 check DB 기능 통합 시 사용할 테이블 예시
# 필요에 따라 수정/추가하세요
# ============================================================

class Customer(db.Model):
    """거래처 관리"""
    __tablename__ = 'customers'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(200), nullable=False)
    code = db.Column(db.String(50), unique=True, nullable=True)
    contact = db.Column(db.String(100), nullable=True)
    phone = db.Column(db.String(20), nullable=True)
    address = db.Column(db.String(300), nullable=True)
    memo = db.Column(db.Text, nullable=True)
    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc),
                           onupdate=lambda: datetime.now(timezone.utc))
