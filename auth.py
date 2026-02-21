from datetime import datetime, timedelta, timezone
from functools import wraps

from flask import Blueprint, render_template, redirect, url_for, flash, request, current_app
from flask_login import login_user, logout_user, login_required, current_user
from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, SelectField
from wtforms.validators import DataRequired, Length, EqualTo, Regexp

from models import User

auth_bp = Blueprint('auth', __name__)


# ── Forms ──

class LoginForm(FlaskForm):
    username = StringField('아이디', validators=[DataRequired(), Length(max=80)])
    password = PasswordField('비밀번호', validators=[DataRequired()])


class RegisterForm(FlaskForm):
    username = StringField('아이디', validators=[
        DataRequired(), Length(min=4, max=80),
        Regexp(r'^[a-zA-Z0-9_]+$', message='영문, 숫자, 밑줄만 사용 가능합니다.')
    ])
    name = StringField('이름', validators=[DataRequired(), Length(max=100)])
    password = PasswordField('비밀번호', validators=[
        DataRequired(), Length(min=8, message='비밀번호는 8자 이상이어야 합니다.')
    ])
    password2 = PasswordField('비밀번호 확인', validators=[
        DataRequired(), EqualTo('password', message='비밀번호가 일치하지 않습니다.')
    ])
    role = SelectField('소속', choices=[
        ('sales', '영업팀'),
        ('logistics', '물류팀'),
        ('production', '생산팀'),
    ])


class ChangePasswordForm(FlaskForm):
    current_password = PasswordField('현재 비밀번호', validators=[DataRequired()])
    new_password = PasswordField('새 비밀번호', validators=[
        DataRequired(), Length(min=8)
    ])
    new_password2 = PasswordField('새 비밀번호 확인', validators=[
        DataRequired(), EqualTo('new_password', message='비밀번호가 일치하지 않습니다.')
    ])


# ── 권한 데코레이터 ──

def role_required(*roles):
    """특정 역할만 접근 허용"""
    def decorator(f):
        @wraps(f)
        @login_required
        def wrapped(*args, **kwargs):
            if current_user.role not in roles:
                flash('접근 권한이 없습니다.', 'danger')
                return redirect(url_for('main.dashboard'))
            return f(*args, **kwargs)
        return wrapped
    return decorator


def level_required(min_level):
    """특정 레벨 이상만 접근 허용"""
    def decorator(f):
        @wraps(f)
        @login_required
        def wrapped(*args, **kwargs):
            if current_user.role_level < min_level:
                flash('접근 권한이 없습니다.', 'danger')
                return redirect(url_for('main.dashboard'))
            return f(*args, **kwargs)
        return wrapped
    return decorator


def _log_action(action, target=None, detail=None, user_id=None):
    current_app.db.insert_audit_log({
        'user_id': user_id or (current_user.id if current_user.is_authenticated else None),
        'action': action,
        'target': target,
        'detail': detail,
        'ip_address': request.remote_addr,
    })


# ── Routes ──

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('main.dashboard'))

    form = LoginForm()
    if form.validate_on_submit():
        row = current_app.db.query_user_by_username(form.username.data)
        user = User(row) if row else None

        # 계정 잠금 확인
        if user and user.is_locked():
            flash('계정이 잠겼습니다. 잠시 후 다시 시도해주세요.', 'danger')
            return render_template('login.html', form=form)

        if user and user.check_password(form.password.data):
            if not user.is_active_user:
                flash('비활성화된 계정입니다. 관리자에게 문의하세요.', 'danger')
                return render_template('login.html', form=form)

            if not user.is_approved:
                flash('관리자 승인 대기 중입니다.', 'warning')
                return render_template('login.html', form=form)

            # 로그인 성공
            current_app.db.update_user(user.id, {
                'failed_login_count': 0,
                'locked_until': None,
                'last_login': datetime.now(timezone.utc).isoformat(),
            })

            login_user(user, remember=False)
            _log_action('login', target=user.username)

            next_page = request.args.get('next')
            if next_page and not next_page.startswith('/'):
                next_page = None
            return redirect(next_page or url_for('main.dashboard'))
        else:
            # 로그인 실패
            if user:
                new_count = user.failed_login_count + 1
                update_data = {'failed_login_count': new_count}
                max_attempts = current_app.config.get('LOGIN_MAX_ATTEMPTS', 5)
                if new_count >= max_attempts:
                    lockout = current_app.config.get('LOGIN_LOCKOUT_MINUTES', 15)
                    update_data['locked_until'] = (
                        datetime.now(timezone.utc) + timedelta(minutes=lockout)
                    ).isoformat()
                    flash(f'로그인 {max_attempts}회 실패. {lockout}분간 잠금됩니다.', 'danger')
                current_app.db.update_user(user.id, update_data)
                _log_action('login_failed', target=form.username.data, user_id=user.id)
            flash('아이디 또는 비밀번호가 올바르지 않습니다.', 'danger')

    return render_template('login.html', form=form)


@auth_bp.route('/register', methods=['GET', 'POST'])
def register():
    if current_user.is_authenticated:
        return redirect(url_for('main.dashboard'))

    form = RegisterForm()
    if form.validate_on_submit():
        existing = current_app.db.query_user_by_username(form.username.data)
        if existing:
            flash('이미 사용 중인 아이디입니다.', 'danger')
            return render_template('register.html', form=form)

        # 비밀번호 해시 생성
        temp_user = User()
        temp_user.set_password(form.password.data)

        current_app.db.insert_user({
            'username': form.username.data,
            'name': form.name.data,
            'password_hash': temp_user.password_hash,
            'role': form.role.data,
            'is_approved': False,
            'is_active_user': True,
        })

        # 새로 생성된 사용자 조회하여 audit log 에 user_id 기록
        created = current_app.db.query_user_by_username(form.username.data)
        created_id = created['id'] if created else None

        _log_action('register', target=form.username.data, user_id=created_id)
        flash('회원가입이 완료되었습니다. 관리자 승인 후 이용 가능합니다.', 'success')
        return redirect(url_for('auth.login'))

    return render_template('register.html', form=form)


@auth_bp.route('/logout')
@login_required
def logout():
    _log_action('logout', target=current_user.username)
    logout_user()
    flash('로그아웃 되었습니다.', 'info')
    return redirect(url_for('auth.login'))


@auth_bp.route('/change-password', methods=['GET', 'POST'])
@login_required
def change_password():
    form = ChangePasswordForm()
    if form.validate_on_submit():
        if not current_user.check_password(form.current_password.data):
            flash('현재 비밀번호가 올바르지 않습니다.', 'danger')
            return render_template('change_password.html', form=form)

        current_user.set_password(form.new_password.data)
        current_app.db.update_user(current_user.id, {
            'password_hash': current_user.password_hash,
            'password_changed_at': current_user.password_changed_at,
        })
        _log_action('change_password', target=current_user.username)
        flash('비밀번호가 변경되었습니다.', 'success')
        return redirect(url_for('main.dashboard'))

    return render_template('change_password.html', form=form)
