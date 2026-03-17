"""journal.py -- 전표 관리 Blueprint."""
from flask import Blueprint, render_template, request, current_app, flash, redirect, url_for, jsonify
from flask_login import login_required, current_user
from auth import role_required, _log_action
from services.tz_utils import today_kst, days_ago_kst
from db_utils import get_db

journal_bp = Blueprint('journal', __name__, url_prefix='/journal')


@journal_bp.route('/')
@role_required('admin', 'ceo', 'manager', 'general')
def index():
    """전표 목록."""
    from services.journal_service import get_journals

    date_from = request.args.get('date_from', days_ago_kst(30))
    date_to = request.args.get('date_to', today_kst())
    journal_type = request.args.get('type', '')
    status = request.args.get('status', '')

    entries = get_journals(
        get_db(),
        date_from=date_from,
        date_to=date_to,
        journal_type=journal_type or None,
        status=status or None,
    )

    return render_template('journal/index.html',
                           entries=entries,
                           date_from=date_from,
                           date_to=date_to,
                           selected_type=journal_type,
                           selected_status=status)


@journal_bp.route('/<int:entry_id>')
@role_required('admin', 'ceo', 'manager', 'general')
def detail(entry_id):
    """전표 상세."""
    from services.journal_service import get_journal_detail

    result = get_journal_detail(get_db(), entry_id)
    if not result:
        flash('전표를 찾을 수 없습니다.', 'warning')
        return redirect(url_for('journal.index'))

    return render_template('journal/detail.html',
                           entry=result['entry'],
                           lines=result['lines'])


@journal_bp.route('/manual', methods=['POST'])
@role_required('admin')
def manual_entry():
    """수동 전표 생성."""
    from services.journal_service import create_journal

    journal_date = request.form.get('journal_date', today_kst())
    description = request.form.get('description', '')

    # 라인 파싱 (account_code[], debit[], credit[], line_desc[])
    codes = request.form.getlist('account_code[]')
    debits = request.form.getlist('debit[]')
    credits = request.form.getlist('credit[]')
    descs = request.form.getlist('line_desc[]')

    if not codes:
        flash('전표 라인을 입력하세요.', 'danger')
        return redirect(url_for('journal.index'))

    lines = []
    for i, code in enumerate(codes):
        if not code:
            continue
        debit = int(debits[i].replace(',', '') or '0') if i < len(debits) else 0
        credit = int(credits[i].replace(',', '') or '0') if i < len(credits) else 0
        if debit == 0 and credit == 0:
            continue
        lines.append({
            'account_code': code,
            'account_name': '',
            'debit': debit,
            'credit': credit,
            'description': descs[i] if i < len(descs) else '',
        })

    if not lines:
        flash('유효한 전표 라인이 없습니다.', 'danger')
        return redirect(url_for('journal.index'))

    try:
        entry_id = create_journal(
            get_db(),
            journal_date=journal_date,
            journal_type='manual',
            lines=lines,
            description=description,
            created_by=current_user.username,
        )
        _log_action('create_journal', detail=f'수동 전표 생성 ID={entry_id}')
        flash(f'전표가 생성되었습니다. (ID: {entry_id})', 'success')
        return redirect(url_for('journal.detail', entry_id=entry_id))
    except ValueError as e:
        flash(f'전표 생성 오류: {e}', 'danger')
    except Exception as e:
        flash(f'전표 생성 실패: {e}', 'danger')

    return redirect(url_for('journal.index'))


@journal_bp.route('/<int:entry_id>/reverse', methods=['POST'])
@role_required('admin')
def reverse(entry_id):
    """역분개."""
    from services.journal_service import reverse_journal

    try:
        rev_id = reverse_journal(get_db(), entry_id,
                                  reversed_by=current_user.username)
        _log_action('reverse_journal',
                    detail=f'전표 {entry_id} 역분개 → {rev_id}')
        flash(f'역분개 완료 (새 전표 ID: {rev_id})', 'success')
        return redirect(url_for('journal.detail', entry_id=rev_id))
    except ValueError as e:
        flash(f'역분개 오류: {e}', 'danger')
    except Exception as e:
        flash(f'역분개 실패: {e}', 'danger')

    return redirect(url_for('journal.detail', entry_id=entry_id))


@journal_bp.route('/trial-balance')
@role_required('admin', 'ceo', 'manager', 'general')
def trial_balance():
    """시산표."""
    from services.journal_service import get_trial_balance

    date_from = request.args.get('date_from', today_kst()[:7] + '-01')
    date_to = request.args.get('date_to', today_kst())

    accounts = get_trial_balance(get_db(), date_from=date_from, date_to=date_to)

    total_debit = sum(a['total_debit'] for a in accounts)
    total_credit = sum(a['total_credit'] for a in accounts)

    return render_template('journal/trial_balance.html',
                           accounts=accounts,
                           total_debit=total_debit,
                           total_credit=total_credit,
                           date_from=date_from,
                           date_to=date_to)
