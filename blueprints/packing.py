"""
blueprints/packing.py — 패킹센터 (위탁업체 전용)
별도 로그인/회원가입 + 전용 GUI
"""
import time
from functools import wraps
from datetime import datetime, timezone

from flask import (
    Blueprint, render_template, redirect, url_for,
    flash, request, current_app, session, jsonify,
)
from flask_login import login_user, logout_user, login_required, current_user
from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField
from wtforms.validators import DataRequired, Length, EqualTo, Regexp

from models import User

packing_bp = Blueprint('packing', __name__, url_prefix='/packing')


# ── Forms ──

class PackingLoginForm(FlaskForm):
    username = StringField('아이디', validators=[DataRequired(), Length(max=80)])
    password = PasswordField('비밀번호', validators=[DataRequired()])


class PackingRegisterForm(FlaskForm):
    username = StringField('아이디', validators=[
        DataRequired(), Length(min=4, max=80),
        Regexp(r'^[a-zA-Z0-9_]+$', message='영문, 숫자, 밑줄만 사용 가능합니다.')
    ])
    name = StringField('이름', validators=[DataRequired(), Length(max=100)])
    company_name = StringField('업체명', validators=[DataRequired(), Length(max=200)])
    password = PasswordField('비밀번호', validators=[
        DataRequired(), Length(min=8, message='비밀번호는 8자 이상이어야 합니다.')
    ])
    password2 = PasswordField('비밀번호 확인', validators=[
        DataRequired(), EqualTo('password', message='비밀번호가 일치하지 않습니다.')
    ])


class PackingChangePasswordForm(FlaskForm):
    current_password = PasswordField('현재 비밀번호', validators=[DataRequired()])
    new_password = PasswordField('새 비밀번호', validators=[
        DataRequired(), Length(min=8)
    ])
    new_password2 = PasswordField('새 비밀번호 확인', validators=[
        DataRequired(), EqualTo('new_password', message='비밀번호가 일치하지 않습니다.')
    ])


# ── Decorator ──

def packing_required(f):
    """패킹센터 접근 제어 (packing + 운영자 허용)"""
    @wraps(f)
    @login_required
    def wrapped(*args, **kwargs):
        # packing 역할 + 관리자/책임자는 패킹센터 접근 가능
        if current_user.role not in ('packing', 'admin', 'manager'):
            flash('패킹센터 접근 권한이 없습니다.', 'danger')
            return redirect(url_for('main.dashboard'))
        return f(*args, **kwargs)
    return wrapped


# ── Helper: auth.py 함수 재사용 ──

def _packing_log_action(action, target=None, detail=None, user_id=None):
    """감사 로그 기록"""
    from auth import _log_action
    _log_action(action, target=target, detail=detail, user_id=user_id)


# ── Routes ──

@packing_bp.route('/login', methods=['GET', 'POST'])
def packing_login():
    """패킹센터 전용 로그인"""
    if current_user.is_authenticated:
        if current_user.role in ('packing', 'admin', 'manager'):
            return redirect(url_for('packing.index'))
        return redirect(url_for('main.dashboard'))

    form = PackingLoginForm()

    if form.validate_on_submit():
        from auth import _get_client_ip, _check_ip_rate_limit, _record_ip_attempt
        client_ip = _get_client_ip()

        # IP 차단 확인
        blocked_seconds = _check_ip_rate_limit(client_ip)
        if blocked_seconds > 0:
            minutes = blocked_seconds // 60 + 1
            flash(f'너무 많은 로그인 시도. {minutes}분 후 다시 시도하세요.', 'danger')
            return render_template('packing/login.html', form=form)

        row = current_app.db.query_user_by_username(form.username.data)
        user = User(row) if row else None

        # 계정 잠금 확인
        if user and user.is_locked():
            flash('계정이 잠겼습니다. 잠시 후 다시 시도해주세요.', 'danger')
            return render_template('packing/login.html', form=form)

        if user and user.check_password(form.password.data):
            # 패킹 + 운영자(admin/manager) 허용
            if user.role not in ('packing', 'admin', 'manager'):
                flash('패킹센터 접근 권한이 없습니다.', 'danger')
                return render_template('packing/login.html', form=form)

            if not user.is_active_user:
                flash('비활성화된 계정입니다. 관리자에게 문의하세요.', 'danger')
                return render_template('packing/login.html', form=form)

            if not user.is_approved:
                flash('관리자 승인 대기 중입니다.', 'warning')
                return render_template('packing/login.html', form=form)

            # 로그인 성공
            current_app.db.update_user(user.id, {
                'failed_login_count': 0,
                'locked_until': None,
                'last_login': datetime.now(timezone.utc).isoformat(),
            })

            login_user(user, remember=False)
            session.permanent = True
            session['_last_active'] = time.time()
            _packing_log_action('packing_login', target=user.username,
                                detail=f'IP: {client_ip}')

            return redirect(url_for('packing.index'))
        else:
            # 로그인 실패
            _record_ip_attempt(client_ip)
            if user:
                new_count = user.failed_login_count + 1
                update_data = {'failed_login_count': new_count}
                max_attempts = current_app.config.get('LOGIN_MAX_ATTEMPTS', 5)
                if new_count >= max_attempts:
                    from datetime import timedelta
                    lockout = current_app.config.get('LOGIN_LOCKOUT_MINUTES', 15)
                    update_data['locked_until'] = (
                        datetime.now(timezone.utc) + timedelta(minutes=lockout)
                    ).isoformat()
                    flash(f'로그인 {max_attempts}회 실패. {lockout}분간 잠금됩니다.', 'danger')
                current_app.db.update_user(user.id, update_data)
            flash('아이디 또는 비밀번호가 올바르지 않습니다.', 'danger')

    return render_template('packing/login.html', form=form)


@packing_bp.route('/register', methods=['GET', 'POST'])
def packing_register():
    """패킹센터 위탁업체 회원가입"""
    if current_user.is_authenticated:
        if current_user.role == 'packing':
            return redirect(url_for('packing.index'))
        return redirect(url_for('main.dashboard'))

    form = PackingRegisterForm()
    if form.validate_on_submit():
        existing = current_app.db.query_user_by_username(form.username.data)
        if existing:
            flash('이미 사용 중인 아이디입니다.', 'danger')
            return render_template('packing/register.html', form=form)

        temp_user = User()
        temp_user.set_password(form.password.data)

        current_app.db.insert_user({
            'username': form.username.data,
            'name': form.name.data,
            'company_name': form.company_name.data,
            'password_hash': temp_user.password_hash,
            'role': 'packing',
            'is_approved': False,
            'is_active_user': True,
        })

        created = current_app.db.query_user_by_username(form.username.data)
        created_id = created['id'] if created else None
        _packing_log_action('packing_register', target=form.username.data,
                            user_id=created_id,
                            detail=f'업체: {form.company_name.data}')

        flash('가입 신청이 완료되었습니다. 관리자 승인 후 이용 가능합니다.', 'success')
        return redirect(url_for('packing.packing_login'))

    return render_template('packing/register.html', form=form)


@packing_bp.route('/logout')
@login_required
def packing_logout():
    """패킹센터 로그아웃"""
    is_operator = current_user.role in ('admin', 'manager')
    _packing_log_action('packing_logout', target=current_user.username)
    if is_operator:
        # 운영자는 로그아웃하지 않고 통합시스템으로 복귀
        flash('통합시스템으로 돌아갑니다.', 'info')
        return redirect(url_for('main.dashboard'))
    logout_user()
    session.clear()
    flash('로그아웃 되었습니다.', 'info')
    return redirect(url_for('packing.packing_login'))


@packing_bp.route('/')
@packing_required
def index():
    """패킹센터 홈"""
    return render_template('packing/index.html')


@packing_bp.route('/change-password', methods=['GET', 'POST'])
@packing_required
def change_password():
    """패킹센터 비밀번호 변경"""
    form = PackingChangePasswordForm()
    if form.validate_on_submit():
        if not current_user.check_password(form.current_password.data):
            flash('현재 비밀번호가 올바르지 않습니다.', 'danger')
            return render_template('packing/change_password.html', form=form)

        current_user.set_password(form.new_password.data)
        current_app.db.update_user(current_user.id, {
            'password_hash': current_user.password_hash,
            'password_changed_at': current_user.password_changed_at,
        })
        _packing_log_action('packing_change_password', target=current_user.username)
        flash('비밀번호가 변경되었습니다.', 'success')
        return redirect(url_for('packing.index'))

    return render_template('packing/change_password.html', form=form)
