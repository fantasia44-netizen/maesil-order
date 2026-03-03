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

from models import User, PAGE_REGISTRY
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

# ── 소속(역할) 목록 헬퍼 (admin 제외) ──
def _editable_roles():
    """Config.ROLES에서 admin 제외 역할 목록 반환."""
    from config import Config
    return [(k, v['name']) for k, v in Config.ROLES.items() if k != 'admin']


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
    'delete_revenue', 'delete_purchase_order',
    'delete_trade', 'delete_partner', 'delete_business',
    'update_partner',
    # 재고원장 기반 (생산/입고/조정/소분/세트)
    'delete_production', 'update_production',
    'delete_inbound', 'update_inbound',
    'delete_adjustment', 'update_adjustment',
    'delete_repack', 'update_repack',
    'delete_set_assembly',
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

        elif action == 'delete_revenue':
            # old_value: daily_revenue 레코드 → 재삽입
            restore_data = {k: v for k, v in old_value.items()
                           if k not in ('id', 'created_at')}
            if restore_data.get('product_name'):
                db.client.table("daily_revenue").insert(restore_data).execute()

        elif action == 'delete_purchase_order':
            # old_value: purchase_orders 레코드 → 재삽입
            restore_data = {k: v for k, v in old_value.items()
                           if k not in ('id', 'created_at')}
            if restore_data:
                db.client.table("purchase_orders").insert(restore_data).execute()

        elif action == 'delete_trade':
            # old_value: manual_trades 레코드 → 재삽입
            restore_data = {k: v for k, v in old_value.items()
                           if k not in ('id', 'created_at')}
            if restore_data:
                db.client.table("manual_trades").insert(restore_data).execute()

        elif action == 'delete_partner':
            # old_value: business_partners 레코드 → 재삽입
            restore_data = {k: v for k, v in old_value.items()
                           if k not in ('id', 'created_at')}
            if restore_data.get('partner_name'):
                db.insert_partner(restore_data)

        elif action == 'delete_business':
            # old_value: my_business 레코드 → 재삽입
            restore_data = {k: v for k, v in old_value.items()
                           if k not in ('id', 'created_at')}
            if restore_data:
                db.client.table("my_business").insert(restore_data).execute()

        elif action == 'update_partner':
            # old_value: 수정 전 파트너 필드들
            partner_id = int(target)
            update_fields = {k: v for k, v in old_value.items()
                            if k not in ('id', 'created_at')}
            if update_fields:
                db.update_partner(partner_id, update_fields)

        # ── 재고원장 삭제 복원 (생산/입고/조정/소분) ──
        elif action in ('delete_production', 'delete_inbound',
                        'delete_adjustment', 'delete_repack'):
            # old_value: stock_ledger 단일 레코드 → 재삽입
            restore_data = {k: v for k, v in old_value.items()
                           if k not in ('id', 'created_at', 'is_deleted',
                                        'deleted_at', 'deleted_by')}
            if restore_data.get('product_name'):
                db.insert_stock_ledger([restore_data])

        # ── 재고원장 수정 복원 (생산/입고/조정/소분) ──
        elif action in ('update_production', 'update_inbound',
                        'update_adjustment', 'update_repack'):
            # old_value: 수정 전 전체 레코드 → 원래 값으로 복원
            row_id = int(target)
            restore_fields = {k: v for k, v in old_value.items()
                             if k not in ('id', 'created_at', 'is_deleted',
                                          'deleted_at', 'deleted_by')}
            if restore_fields:
                db.update_stock_ledger(row_id, restore_fields)

        # ── 세트작업 삭제 복원 (다건) ──
        elif action == 'delete_set_assembly':
            # old_value: stock_ledger 레코드 리스트 → 전부 재삽입
            if isinstance(old_value, list):
                for rec in old_value:
                    restore_data = {k: v for k, v in rec.items()
                                   if k not in ('id', 'created_at', 'is_deleted',
                                                'deleted_at', 'deleted_by')}
                    if restore_data.get('product_name'):
                        db.insert_stock_ledger([restore_data])
            elif isinstance(old_value, dict):
                restore_data = {k: v for k, v in old_value.items()
                               if k not in ('id', 'created_at', 'is_deleted',
                                            'deleted_at', 'deleted_by')}
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


# ══════════════════════════════════════════════════════════════
# 부서별 권한 설정
# ══════════════════════════════════════════════════════════════

@admin_bp.route('/permissions')
@role_required('admin')
def permissions():
    """권한 설정 매트릭스 페이지."""
    from config import Config
    db = current_app.db

    # DB에서 현재 권한 조회
    perms = db.query_role_permissions(use_cache=False)

    # 역할 목록 (admin 포함 전체)
    roles = [(k, v['name']) for k, v in Config.ROLES.items()]

    return render_template(
        'admin/permissions.html',
        page_registry=PAGE_REGISTRY,
        roles=roles,
        perms=perms,
    )


@admin_bp.route('/permissions/save', methods=['POST'])
@role_required('admin')
def permissions_save():
    """권한 저장 (AJAX). 역할별 {page_key: bool} 일괄 upsert."""
    from config import Config
    db = current_app.db

    try:
        data = request.get_json(force=True)
        if not data:
            return jsonify({'error': '데이터가 없습니다.'}), 400

        # data 형태: {role: {page_key: bool, ...}, ...}
        page_keys = {pk for pk, *_ in PAGE_REGISTRY}

        for role, page_perms in data.items():
            if role not in Config.ROLES:
                continue
            # admin 역할은 모두 True 강제
            if role == 'admin':
                page_perms = {pk: True for pk in page_keys}
            else:
                # 유효한 page_key만 필터
                page_perms = {pk: bool(v) for pk, v in page_perms.items()
                              if pk in page_keys}
            db.upsert_role_permissions(role, page_perms)

        _log_action('update_permissions', detail='부서별 권한 설정 변경')

        return jsonify({'success': True, 'message': '권한이 저장되었습니다.'})

    except Exception as e:
        return jsonify({'error': f'저장 중 오류: {str(e)}'}), 500


@admin_bp.route('/permissions/reset', methods=['POST'])
@role_required('admin')
def permissions_reset():
    """권한 기본값으로 초기화 (AJAX)."""
    from config import Config
    db = current_app.db

    try:
        # 기존 데이터 삭제 후 seed
        db.client.table("role_permissions").delete().neq("id", 0).execute()
        db._invalidate_perm_cache()

        # 기본값 다시 생성
        from datetime import datetime as _dt, timezone as _tz
        now_str = _dt.now(_tz.utc).isoformat()
        payload = []
        for page_key, name, icon, url, default_roles in PAGE_REGISTRY:
            for role in Config.ROLES.keys():
                payload.append({
                    'role': role,
                    'page_key': page_key,
                    'is_allowed': role in default_roles,
                    'updated_at': now_str,
                })
        for i in range(0, len(payload), 500):
            db.client.table("role_permissions").upsert(
                payload[i:i+500], on_conflict="role,page_key"
            ).execute()
        db._invalidate_perm_cache()

        _log_action('reset_permissions', detail='부서별 권한 기본값 초기화')

        return jsonify({'success': True, 'message': '기본값으로 초기화되었습니다.'})

    except Exception as e:
        return jsonify({'error': f'초기화 중 오류: {str(e)}'}), 500


# ================================================================
# 개인정보 익명화 (6개월 경과 배송정보)
# ================================================================

@admin_bp.route('/anonymize-shipping', methods=['POST'])
@role_required('admin')
def anonymize_shipping():
    """만료된 배송 개인정보 익명화 실행"""
    try:
        count = current_app.db.anonymize_expired_shipping()
        _log_action('anonymize_shipping', detail=f'{count}건 익명화 처리')
        return jsonify({'success': True, 'count': count, 'message': f'{count}건 익명화 완료'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
