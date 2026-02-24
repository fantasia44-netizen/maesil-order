import json
from datetime import datetime, timezone

from flask import (
    Blueprint, render_template, redirect, url_for,
    flash, request, abort, current_app, jsonify,
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
    action_filter = request.args.get('action', '').strip()
    user_filter = request.args.get('user', '').strip()
    date_from = request.args.get('date_from', '').strip()
    date_to = request.args.get('date_to', '').strip()

    items, total = current_app.db.query_audit_logs(
        page, per_page,
        action_filter=action_filter or None,
        user_filter=user_filter or None,
        date_from=date_from or None,
        date_to=date_to or None,
    )

    # Wrap each dict so templates can use dot notation (log.action, log.user.name, etc.)
    wrapped = []
    for item in items:
        # Ensure created_at is a datetime object for strftime in template
        item['created_at'] = _parse_datetime(item.get('created_at'))
        # Attach a .user stub with .name
        user_name = item.pop('user_name', None) or '-'
        item['user'] = _UserStub(user_name)
        # old_value/new_value가 JSON 문자열이면 dict로 파싱
        for key in ('old_value', 'new_value'):
            val = item.get(key)
            if isinstance(val, str):
                try:
                    item[key] = json.loads(val)
                except (json.JSONDecodeError, TypeError):
                    pass
        wrapped.append(AuditLogItem(item))

    logs = Pagination(wrapped, page, per_page, total)
    return render_template('admin/audit_logs.html', logs=logs,
                           action_filter=action_filter,
                           user_filter=user_filter,
                           date_from=date_from, date_to=date_to)


# ── 롤백 지원 액션 목록 ──
_REVERTABLE_ACTIONS = {
    'update_product_cost', 'delete_product_cost',
    'update_channel_cost', 'delete_channel_cost',
    'edit_stock_ledger', 'delete_stock_ledger',
    'update_price', 'batch_update_price',
}


@admin_bp.route('/logs/<int:log_id>/revert', methods=['POST'])
@role_required('admin')
def revert_audit_log(log_id):
    """감사 로그 기반 롤백 — old_value를 복원"""
    db = current_app.db

    log_entry = db.query_audit_log_by_id(log_id)
    if not log_entry:
        return jsonify({'error': '로그를 찾을 수 없습니다.'}), 404

    action = log_entry.get('action', '')
    old_value = log_entry.get('old_value')
    is_reverted = log_entry.get('is_reverted', False)

    if is_reverted:
        return jsonify({'error': '이미 되돌린 작업입니다.'}), 400

    if action not in _REVERTABLE_ACTIONS:
        return jsonify({'error': f'{action}은(는) 되돌리기를 지원하지 않습니다.'}), 400

    if not old_value:
        return jsonify({'error': '이전 데이터(old_value)가 없어 되돌릴 수 없습니다.'}), 400

    # JSON string → dict 변환
    if isinstance(old_value, str):
        try:
            old_value = json.loads(old_value)
        except (json.JSONDecodeError, TypeError):
            return jsonify({'error': 'old_value 파싱 오류'}), 400

    try:
        target = log_entry.get('target', '')

        # ── 액션별 롤백 수행 ──
        if action == 'update_product_cost':
            # old_value: {cost_price, unit, memo, weight, weight_unit, cost_type,
            #             material_type, purchase_unit, standard_unit, conversion_ratio}
            db.upsert_product_cost(
                product_name=target,
                cost_price=old_value.get('cost_price', 0),
                unit=old_value.get('unit', ''),
                memo=old_value.get('memo', ''),
                weight=old_value.get('weight', 0),
                weight_unit=old_value.get('weight_unit', 'g'),
                cost_type=old_value.get('cost_type', '매입'),
                material_type=old_value.get('material_type', '원료'),
                purchase_unit=old_value.get('purchase_unit', ''),
                standard_unit=old_value.get('standard_unit', ''),
                conversion_ratio=old_value.get('conversion_ratio', 1),
            )

        elif action == 'delete_product_cost':
            # old_value: 삭제 전 전체 데이터
            db.upsert_product_cost(
                product_name=target,
                cost_price=old_value.get('cost_price', 0),
                unit=old_value.get('unit', ''),
                memo=old_value.get('memo', ''),
                weight=old_value.get('weight', 0),
                weight_unit=old_value.get('weight_unit', 'g'),
                cost_type=old_value.get('cost_type', '매입'),
                material_type=old_value.get('material_type', '원료'),
                purchase_unit=old_value.get('purchase_unit', ''),
                standard_unit=old_value.get('standard_unit', ''),
                conversion_ratio=old_value.get('conversion_ratio', 1),
            )

        elif action == 'update_channel_cost':
            db.upsert_channel_cost(
                channel=target,
                fee_rate=old_value.get('fee_rate', 0),
                shipping=old_value.get('shipping', 0),
                packaging=old_value.get('packaging', 0),
                other_cost=old_value.get('other_cost', 0),
                memo=old_value.get('memo', ''),
            )

        elif action == 'delete_channel_cost':
            db.upsert_channel_cost(
                channel=target,
                fee_rate=old_value.get('fee_rate', 0),
                shipping=old_value.get('shipping', 0),
                packaging=old_value.get('packaging', 0),
                other_cost=old_value.get('other_cost', 0),
                memo=old_value.get('memo', ''),
            )

        elif action == 'edit_stock_ledger':
            # old_value: 수정 전 필드들
            row_id = int(target)
            db.update_stock_ledger(row_id, old_value)

        elif action == 'delete_stock_ledger':
            # old_value: 삭제 전 전체 레코드 → 재삽입
            restore_data = {k: v for k, v in old_value.items()
                           if k not in ('id', 'created_at', 'is_deleted', 'deleted_at', 'deleted_by')}
            if restore_data.get('product_name'):
                db.insert_stock_ledger([restore_data])

        else:
            return jsonify({'error': f'{action} 롤백 미구현'}), 400

        # 롤백 완료 표시
        db.update_audit_log(log_id, {
            'is_reverted': True,
            'reverted_by': current_user.id,
            'reverted_at': datetime.now(timezone.utc).isoformat(),
        })

        # 롤백 자체도 감사 로그 기록
        _log_action('revert_action', target=str(log_id),
                     detail=f'작업 되돌리기: {action} → {target}')

        return jsonify({'success': True, 'message': f'{action} 작업이 되돌려졌습니다.'})

    except Exception as e:
        return jsonify({'error': f'롤백 중 오류: {str(e)}'}), 500
