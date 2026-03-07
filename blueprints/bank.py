"""bank.py -- 은행 거래내역 관리 Blueprint."""
from flask import Blueprint, render_template, request, current_app, flash, redirect, url_for, jsonify
from flask_login import login_required, current_user
from auth import role_required, _log_action
from services.tz_utils import today_kst, days_ago_kst

bank_bp = Blueprint('bank', __name__, url_prefix='/bank')


@bank_bp.route('/')
@role_required('admin', 'ceo', 'manager', 'general')
def index():
    """은행 계좌 목록 + 연결 관리"""
    db = current_app.db
    accounts = db.query_bank_accounts()
    connections = db.query_codef_connections()
    from services.codef_service import BANK_CODES
    return render_template('bank/index.html',
                           accounts=accounts,
                           connections=connections,
                           bank_codes=BANK_CODES)


@bank_bp.route('/connect', methods=['POST'])
@role_required('admin', 'manager')
def connect():
    """CODEF 은행 계좌 연결"""
    bank_code = request.form.get('bank_code', '')
    login_id = request.form.get('login_id', '')
    login_pw = request.form.get('login_pw', '')
    login_type = request.form.get('login_type', '1')

    if not bank_code or not login_id or not login_pw:
        flash('은행, 아이디, 비밀번호를 모두 입력하세요.', 'danger')
        return redirect(url_for('bank.index'))

    try:
        codef = current_app.codef
        connected_id = codef.create_connected_id(bank_code, login_type, login_id, login_pw)

        # DB에 연결 정보 저장
        current_app.db.insert_codef_connection({
            'connected_id': connected_id,
            'organization': bank_code,
            'login_type': login_type,
        })

        # 보유계좌 조회 후 자동 등록
        accounts = codef.get_account_list(connected_id, bank_code)
        bank_name = codef.get_bank_name(bank_code)

        for acc in accounts:
            try:
                current_app.db.insert_bank_account({
                    'connected_id': connected_id,
                    'bank_code': bank_code,
                    'bank_name': bank_name,
                    'account_number': acc.get('resAccount', acc.get('account', '')),
                    'account_holder': acc.get('resAccountName', acc.get('accountName', '')),
                })
            except Exception:
                pass  # 중복 계좌 스킵

        _log_action('connect_bank',
                    detail=f'{bank_name} {len(accounts)}개 계좌 연결')
        flash(f'{bank_name} {len(accounts)}개 계좌 연결 완료', 'success')
    except Exception as e:
        flash(f'계좌 연결 오류: {e}', 'danger')

    return redirect(url_for('bank.index'))


@bank_bp.route('/transactions')
@role_required('admin', 'ceo', 'manager', 'general')
def transactions():
    """거래내역 조회"""
    db = current_app.db
    date_from = request.args.get('date_from', days_ago_kst(30))
    date_to = request.args.get('date_to', today_kst())
    account_id = request.args.get('account_id', '')
    tx_type = request.args.get('type', '전체')

    accounts = db.query_bank_accounts()
    txns = db.query_bank_transactions(
        date_from=date_from, date_to=date_to,
        bank_account_id=int(account_id) if account_id else None,
        transaction_type=tx_type if tx_type != '전체' else None,
    )

    from services.bank_service import get_transaction_summary
    summary = get_transaction_summary(
        db, date_from=date_from, date_to=date_to,
        bank_account_id=int(account_id) if account_id else None,
    )

    return render_template('bank/transactions.html',
                           transactions=txns, accounts=accounts,
                           summary=summary,
                           date_from=date_from, date_to=date_to,
                           account_id=account_id, tx_type=tx_type)


@bank_bp.route('/sync/<int:account_id>', methods=['POST'])
@role_required('admin', 'manager')
def sync_account(account_id):
    """계좌 거래내역 동기화"""
    try:
        from services.bank_service import sync_bank_transactions
        result = sync_bank_transactions(current_app.db, current_app.codef, account_id)
        _log_action('sync_bank',
                    detail=f'계좌 {account_id}: 신규 {result["new_count"]}건')
        flash(f'동기화 완료: 신규 {result["new_count"]}건', 'success')
    except Exception as e:
        flash(f'동기화 오류: {e}', 'danger')
    return redirect(url_for('bank.transactions'))


@bank_bp.route('/sync-all', methods=['POST'])
@role_required('admin', 'manager')
def sync_all():
    """전체 계좌 일괄 동기화"""
    try:
        from services.bank_service import sync_all_accounts
        results = sync_all_accounts(current_app.db, current_app.codef)
        total_new = sum(r.get('new_count', 0) for r in results)
        _log_action('sync_bank_all',
                    detail=f'{len(results)}개 계좌, 신규 {total_new}건')
        flash(f'전체 동기화 완료: {len(results)}개 계좌, 신규 {total_new}건', 'success')
    except Exception as e:
        flash(f'전체 동기화 오류: {e}', 'danger')
    return redirect(url_for('bank.transactions'))


@bank_bp.route('/api/transaction/<int:tx_id>/category', methods=['PUT'])
@role_required('admin', 'manager', 'general')
def update_category(tx_id):
    """거래 분류(카테고리) 수정 API"""
    data = request.get_json()
    category = data.get('category', '')
    try:
        current_app.db.update_bank_transaction(tx_id, {'category': category})
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bank_bp.route('/api/summary')
@role_required('admin', 'ceo', 'manager', 'general')
def api_summary():
    """거래 요약 JSON (대시보드용)"""
    date_from = request.args.get('date_from', days_ago_kst(30))
    date_to = request.args.get('date_to', today_kst())
    from services.bank_service import get_transaction_summary
    summary = get_transaction_summary(current_app.db, date_from=date_from, date_to=date_to)
    return jsonify(summary)


@bank_bp.route('/accounts/delete/<int:account_id>', methods=['POST'])
@role_required('admin')
def delete_account(account_id):
    """계좌 삭제"""
    try:
        current_app.db.delete_bank_account(account_id)
        _log_action('delete_bank_account',
                    detail=f'계좌 {account_id} 삭제')
        flash('계좌가 삭제되었습니다.', 'success')
    except Exception as e:
        flash(f'삭제 오류: {e}', 'danger')
    return redirect(url_for('bank.index'))
