from flask import Blueprint, render_template, redirect, url_for, flash, request
from flask_login import login_required, current_user
from flask_wtf import FlaskForm
from wtforms import StringField, SelectField, BooleanField
from wtforms.validators import DataRequired, Length

from models import db, User, AuditLog
from auth import role_required, _log_action
from config import Config

admin_bp = Blueprint('admin', __name__, url_prefix='/admin')


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


@admin_bp.route('/users')
@role_required('admin')
def user_list():
    users = User.query.order_by(User.created_at.desc()).all()
    pending_count = User.query.filter_by(is_approved=False).count()
    return render_template('admin/user_list.html', users=users, pending_count=pending_count)


@admin_bp.route('/users/<int:user_id>', methods=['GET', 'POST'])
@role_required('admin')
def user_edit(user_id):
    user = User.query.get_or_404(user_id)
    form = UserEditForm(obj=user)

    if form.validate_on_submit():
        old_role = user.role
        user.name = form.name.data
        user.role = form.role.data
        user.is_active_user = form.is_active_user.data
        user.is_approved = form.is_approved.data
        db.session.commit()

        detail = f'역할: {old_role} → {user.role}' if old_role != user.role else None
        _log_action('user_update', target=user.username, detail=detail)
        flash(f'{user.name} 정보가 수정되었습니다.', 'success')
        return redirect(url_for('admin.user_list'))

    return render_template('admin/user_edit.html', form=form, user=user)


@admin_bp.route('/users/<int:user_id>/approve', methods=['POST'])
@role_required('admin')
def user_approve(user_id):
    user = User.query.get_or_404(user_id)
    user.is_approved = True
    db.session.commit()
    _log_action('user_approve', target=user.username)
    flash(f'{user.name} 승인 완료.', 'success')
    return redirect(url_for('admin.user_list'))


@admin_bp.route('/users/<int:user_id>/toggle-active', methods=['POST'])
@role_required('admin')
def user_toggle_active(user_id):
    user = User.query.get_or_404(user_id)
    if user.id == current_user.id:
        flash('자기 자신을 비활성화할 수 없습니다.', 'danger')
        return redirect(url_for('admin.user_list'))

    user.is_active_user = not user.is_active_user
    db.session.commit()
    status = '활성화' if user.is_active_user else '비활성화'
    _log_action('user_toggle', target=user.username, detail=status)
    flash(f'{user.name} 계정이 {status} 되었습니다.', 'success')
    return redirect(url_for('admin.user_list'))


@admin_bp.route('/users/<int:user_id>/reset-password', methods=['POST'])
@role_required('admin')
def user_reset_password(user_id):
    user = User.query.get_or_404(user_id)
    temp_password = 'change1234!'
    user.set_password(temp_password)
    user.failed_login_count = 0
    user.locked_until = None
    db.session.commit()
    _log_action('password_reset', target=user.username)
    flash(f'{user.name} 비밀번호가 초기화되었습니다. 임시 비밀번호: {temp_password}', 'warning')
    return redirect(url_for('admin.user_list'))


@admin_bp.route('/users/<int:user_id>/unlock', methods=['POST'])
@role_required('admin')
def user_unlock(user_id):
    user = User.query.get_or_404(user_id)
    user.failed_login_count = 0
    user.locked_until = None
    db.session.commit()
    _log_action('user_unlock', target=user.username)
    flash(f'{user.name} 계정 잠금이 해제되었습니다.', 'success')
    return redirect(url_for('admin.user_list'))


@admin_bp.route('/logs')
@role_required('admin')
def audit_logs():
    page = request.args.get('page', 1, type=int)
    logs = AuditLog.query.order_by(AuditLog.created_at.desc()).paginate(
        page=page, per_page=50, error_out=False
    )
    return render_template('admin/audit_logs.html', logs=logs)
