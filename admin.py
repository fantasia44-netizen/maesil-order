from flask import (
    Blueprint, render_template, redirect, url_for,
    flash, request, abort, current_app,
)
from flask_login import login_required, current_user
from flask_wtf import FlaskForm
from wtforms import StringField, SelectField, BooleanField
from wtforms.validators import DataRequired, Length

from models import User
from auth import role_required, _log_action

admin_bp = Blueprint('admin', __name__, url_prefix='/admin')


# ── Pagination helper (replaces Flask-SQLAlchemy paginate) ──

class Pagination:
    def __init__(self, items, page, per_page, total):
        self.items = items
        self.page = page
        self.per_page = per_page
        self.total = total
        self.pages = (total + per_page - 1) // per_page if total > 0 else 0
        self.has_prev = page > 1
        self.has_next = page < self.pages
        self.prev_num = page - 1
        self.next_num = page + 1

    def iter_pages(self, left_edge=1, right_edge=1, left_current=2, right_current=2):
        """Yield page numbers for pagination widget, with None as gap marker."""
        last = 0
        for num in range(1, self.pages + 1):
            if (
                num <= left_edge
                or (self.page - left_current <= num <= self.page + right_current)
                or num > self.pages - right_edge
            ):
                if last + 1 != num:
                    yield None
                yield num
                last = num


# ── AuditLogItem wrapper (dict → attribute access for templates) ──

class AuditLogItem:
    """Wraps an audit-log dict so Jinja templates can use dot notation."""

    def __init__(self, data: dict):
        self._data = data

    def __getattr__(self, name):
        try:
            return self._data[name]
        except KeyError:
            raise AttributeError(name)

    # keep dict-style access working too
    def get(self, key, default=None):
        return self._data.get(key, default)


class _UserStub:
    """Minimal object exposing .name so the template can do log.user.name."""
    def __init__(self, name):
        self.name = name


# ── Form ──

class UserEditForm(FlaskForm):
    name = StringField('이름', validators=[DataRequired(), Length(max=100)])
    role = SelectField('소속/권한', choices=[
        ('admin', '관리자'),
        ('manager', '책임자'),
        ('sales', '영업팀'),
        ('logistics', '물류팀'),
        ('production', '생산팀'),
    ])
    is_active_user = BooleanField('활성 상태')
    is_approved = BooleanField('승인 상태')


# ── Helper ──

def _get_user_or_404(user_id):
    """Fetch a user via Supabase; abort 404 if not found. Returns User object."""
    row = current_app.db.query_user_by_id(user_id)
    if row is None:
        abort(404)
    return User(row)


def _parse_datetime(value):
    """Best-effort parse of an ISO datetime string into a datetime object."""
    if value is None:
        return None
    if isinstance(value, str):
        from datetime import datetime
        try:
            return datetime.fromisoformat(value.replace('Z', '+00:00'))
        except (ValueError, TypeError):
            return None
    return value


# ── Routes ──

@admin_bp.route('/users')
@role_required('admin')
def user_list():
    raw_users = current_app.db.query_all_users()
    # Wrap dicts in User objects for attribute access in templates
    users = [User(row) for row in raw_users]
    # Sort by created_at descending (newest first)
    users.sort(key=lambda u: u.created_at or '', reverse=True)
    pending_count = current_app.db.count_pending_users()
    return render_template('admin/user_list.html', users=users, pending_count=pending_count)


@admin_bp.route('/users/<int:user_id>', methods=['GET', 'POST'])
@role_required('admin')
def user_edit(user_id):
    user = _get_user_or_404(user_id)
    form = UserEditForm()

    if request.method == 'GET':
        # Populate form with current user data
        form.name.data = user.name
        form.role.data = user.role
        form.is_active_user.data = user.is_active_user
        form.is_approved.data = user.is_approved

    if form.validate_on_submit():
        old_role = user.role
        new_data = {
            'name': form.name.data,
            'role': form.role.data,
            'is_active_user': form.is_active_user.data,
            'is_approved': form.is_approved.data,
        }
        current_app.db.update_user(user_id, new_data)

        detail = f'역할: {old_role} → {form.role.data}' if old_role != form.role.data else None
        _log_action('user_update', target=user.username, detail=detail)
        flash(f'{form.name.data} 정보가 수정되었습니다.', 'success')
        return redirect(url_for('admin.user_list'))

    return render_template('admin/user_edit.html', form=form, user=user)


@admin_bp.route('/users/<int:user_id>/approve', methods=['POST'])
@role_required('admin')
def user_approve(user_id):
    user = _get_user_or_404(user_id)
    current_app.db.update_user(user_id, {'is_approved': True})
    _log_action('user_approve', target=user.username)
    flash(f'{user.name} 승인 완료.', 'success')
    return redirect(url_for('admin.user_list'))


@admin_bp.route('/users/<int:user_id>/toggle-active', methods=['POST'])
@role_required('admin')
def user_toggle_active(user_id):
    user = _get_user_or_404(user_id)
    if user.id == current_user.id:
        flash('자기 자신을 비활성화할 수 없습니다.', 'danger')
        return redirect(url_for('admin.user_list'))

    new_status = not user.is_active_user
    current_app.db.update_user(user_id, {'is_active_user': new_status})
    status = '활성화' if new_status else '비활성화'
    _log_action('user_toggle', target=user.username, detail=status)
    flash(f'{user.name} 계정이 {status} 되었습니다.', 'success')
    return redirect(url_for('admin.user_list'))


@admin_bp.route('/users/<int:user_id>/reset-password', methods=['POST'])
@role_required('admin')
def user_reset_password(user_id):
    user = _get_user_or_404(user_id)
    temp_password = 'change1234!'
    user.set_password(temp_password)
    current_app.db.update_user(user_id, {
        'password_hash': user.password_hash,
        'password_changed_at': user.password_changed_at,
        'failed_login_count': 0,
        'locked_until': None,
    })
    _log_action('password_reset', target=user.username)
    flash(f'{user.name} 비밀번호가 초기화되었습니다. 임시 비밀번호: {temp_password}', 'warning')
    return redirect(url_for('admin.user_list'))


@admin_bp.route('/users/<int:user_id>/unlock', methods=['POST'])
@role_required('admin')
def user_unlock(user_id):
    user = _get_user_or_404(user_id)
    current_app.db.update_user(user_id, {
        'failed_login_count': 0,
        'locked_until': None,
    })
    _log_action('user_unlock', target=user.username)
    flash(f'{user.name} 계정 잠금이 해제되었습니다.', 'success')
    return redirect(url_for('admin.user_list'))


@admin_bp.route('/logs')
@role_required('admin')
def audit_logs():
    page = request.args.get('page', 1, type=int)
    per_page = 50
    items, total = current_app.db.query_audit_logs(page, per_page)

    # Wrap each dict so templates can use dot notation (log.action, log.user.name, etc.)
    wrapped = []
    for item in items:
        # Ensure created_at is a datetime object for strftime in template
        item['created_at'] = _parse_datetime(item.get('created_at'))
        # Attach a .user stub with .name
        user_name = item.pop('user_name', None) or '-'
        item['user'] = _UserStub(user_name)
        wrapped.append(AuditLogItem(item))

    logs = Pagination(wrapped, page, per_page, total)
    return render_template('admin/audit_logs.html', logs=logs)
