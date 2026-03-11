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
    from services.codef_service import BANK_CODES, CARD_CODES
    return render_template('bank/index.html',
                           accounts=accounts,
                           connections=connections,
                           bank_codes=BANK_CODES,
                           card_codes=CARD_CODES)


@bank_bp.route('/connect', methods=['POST'])
@role_required('admin', 'manager')
def connect():
    """CODEF 은행/카드 연결"""
    import base64
    connect_type = request.form.get('connect_type', 'bank')  # bank or card
    org_code = request.form.get('bank_code', '') or request.form.get('card_code', '')
    login_type = request.form.get('login_type', '1')
    client_type = request.form.get('client_type', 'P')
    login_id = request.form.get('login_id', '')
    login_pw = request.form.get('login_pw', '')
    business_type = 'CD' if connect_type == 'card' else 'BK'

    if not org_code:
        flash('은행/카드사를 선택하세요.', 'danger')
        return redirect(url_for('bank.index'))

    cert_der_b64 = ''
    cert_key_b64 = ''

    if login_type == '0':
        # 공인인증서 모드
        cert_pw = request.form.get('cert_pw', '')
        cert_der = request.files.get('cert_der')
        cert_key = request.files.get('cert_key')
        if not cert_der or not cert_key or not cert_pw:
            flash('인증서 파일(.der, .key)과 비밀번호를 모두 입력하세요.', 'danger')
            return redirect(url_for('bank.index'))
        cert_der_b64 = base64.b64encode(cert_der.read()).decode('utf-8')
        cert_key_b64 = base64.b64encode(cert_key.read()).decode('utf-8')
        login_pw = cert_pw  # 인증서 비밀번호
    else:
        # ID/PW 모드
        if not login_id or not login_pw:
            flash('아이디와 비밀번호를 모두 입력하세요.', 'danger')
            return redirect(url_for('bank.index'))

    try:
        codef = current_app.codef
        connected_id = codef.create_connected_id(
            org_code, login_type, login_id, login_pw,
            client_type=client_type,
            business_type=business_type,
            cert_der_base64=cert_der_b64, cert_key_base64=cert_key_b64,
        )

        # DB에 연결 정보 저장
        current_app.db.insert_codef_connection({
            'connected_id': connected_id,
            'organization': org_code,
            'login_type': login_type,
        })

        org_name = codef.get_bank_name(org_code)

        if connect_type == 'card':
            # 카드 목록 조회 후 등록
            cards = codef.get_card_list(connected_id, org_code, client_type=client_type)
            ok_count = 0
            fail_list = []
            for card in cards:
                try:
                    current_app.db.insert_bank_account({
                        'connected_id': connected_id,
                        'bank_code': org_code,
                        'bank_name': org_name,
                        'account_number': card.get('resCardNo', card.get('cardNo', '')),
                        'account_holder': card.get('resCardName', card.get('cardName', '')),
                        'account_type': '카드',
                        'client_type': client_type,
                    })
                    ok_count += 1
                except Exception as e:
                    fail_list.append(f"{card.get('resCardNo','?')}: {e}")

            _log_action('connect_card',
                        detail=f'{org_name} {ok_count}/{len(cards)}개 카드 연결')
            flash(f'{org_name} {ok_count}개 카드 연결 완료 (총 {len(cards)}개)', 'success')
            if fail_list:
                flash(f'실패: {"; ".join(fail_list)}', 'warning')
        else:
            # 은행 보유계좌 조회 후 등록
            accounts = codef.get_account_list(connected_id, org_code, client_type=client_type)
            cat_map = {
                'resDepositTrust': '예금', 'resLoan': '대출',
                'resFund': '펀드', 'resForeignCurrency': '외화',
                'resInsurance': '보험', 'resList': '기타',
            }
            ok_count = 0
            fail_list = []
            for acc in accounts:
                try:
                    cat = acc.get('_category', '')
                    current_app.db.insert_bank_account({
                        'connected_id': connected_id,
                        'bank_code': org_code,
                        'bank_name': org_name,
                        'account_number': acc.get('resAccount', acc.get('account', '')),
                        'account_holder': acc.get('resAccountName', acc.get('accountName', '')),
                        'account_type': cat_map.get(cat, cat),
                        'client_type': client_type,
                    })
                    ok_count += 1
                except Exception as e:
                    fail_list.append(f"{acc.get('resAccount','?')}: {e}")

            _log_action('connect_bank',
                        detail=f'{org_name} {ok_count}/{len(accounts)}개 계좌 연결')
            flash(f'{org_name} {ok_count}개 계좌 연결 완료 (총 {len(accounts)}개)', 'success')
            if fail_list:
                flash(f'실패: {"; ".join(fail_list)}', 'warning')
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
    """전체 계좌 + 카드 일괄 동기화"""
    try:
        # 은행 계좌 동기화
        from services.bank_service import sync_all_accounts
        bank_results = sync_all_accounts(current_app.db, current_app.codef)
        bank_new = sum(r.get('new_count', 0) for r in bank_results)

        # 카드 동기화
        from services.card_service import sync_all_card_accounts
        card_results = sync_all_card_accounts(current_app.db, current_app.codef)
        card_new = sum(r.get('new_count', 0) for r in card_results)

        _log_action('sync_all',
                    detail=f'은행 {len(bank_results)}개 신규 {bank_new}건, '
                           f'카드 {len(card_results)}개 신규 {card_new}건')
        flash(f'전체 동기화 완료: 은행 {len(bank_results)}개({bank_new}건), '
              f'카드 {len(card_results)}개({card_new}건)', 'success')
    except Exception as e:
        flash(f'전체 동기화 오류: {e}', 'danger')
    return redirect(url_for('bank.index'))


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


@bank_bp.route('/reset-transactions', methods=['POST'])
@role_required('admin')
def reset_transactions():
    """거래내역 전체 삭제 후 재동기화"""
    try:
        db = current_app.db
        # last_synced_date 초기화
        accounts = db.query_bank_accounts()
        for acc in accounts:
            db.update_bank_account(acc['id'], {'last_synced_date': None, 'last_synced_at': None})
        # 거래내역 전체 삭제
        db.delete_all_bank_transactions()
        # 재동기화
        from services.bank_service import sync_all_accounts
        results = sync_all_accounts(db, current_app.codef)
        total_new = sum(r.get('new_count', 0) for r in results)
        flash(f'초기화 후 재동기화 완료: {total_new}건', 'success')
    except Exception as e:
        flash(f'초기화 오류: {e}', 'danger')
    return redirect(url_for('bank.transactions'))


@bank_bp.route('/card-transactions')
@role_required('admin', 'ceo', 'manager', 'general')
def card_transactions():
    """카드 이용내역 조회"""
    db = current_app.db
    date_from = request.args.get('date_from', days_ago_kst(30))
    date_to = request.args.get('date_to', today_kst())
    account_id = request.args.get('account_id', '')
    search = request.args.get('search', '')

    accounts = [a for a in db.query_bank_accounts() if a.get('account_type') == '카드']
    txns = db.query_card_transactions(
        date_from=date_from, date_to=date_to,
        bank_account_id=int(account_id) if account_id else None,
        search=search if search else None,
    )

    from services.card_service import get_card_summary, CARD_CATEGORIES
    summary = get_card_summary(
        db, date_from=date_from, date_to=date_to,
        bank_account_id=int(account_id) if account_id else None,
    )

    return render_template('bank/card_transactions.html',
                           transactions=txns, accounts=accounts,
                           summary=summary, categories=CARD_CATEGORIES,
                           date_from=date_from, date_to=date_to,
                           account_id=account_id, search=search)


@bank_bp.route('/sync-card/<int:account_id>', methods=['POST'])
@role_required('admin', 'manager')
def sync_card(account_id):
    """카드 이용내역 동기화"""
    try:
        from services.card_service import sync_card_transactions
        result = sync_card_transactions(current_app.db, current_app.codef, account_id)
        _log_action('sync_card',
                    detail=f'카드 {account_id}: 신규 {result["new_count"]}건')
        flash(f'카드 동기화 완료: 신규 {result["new_count"]}건', 'success')
    except Exception as e:
        flash(f'카드 동기화 오류: {e}', 'danger')
    return redirect(url_for('bank.card_transactions'))


@bank_bp.route('/sync-card-all', methods=['POST'])
@role_required('admin', 'manager')
def sync_card_all():
    """전체 카드 일괄 동기화"""
    try:
        from services.card_service import sync_all_card_accounts
        results = sync_all_card_accounts(current_app.db, current_app.codef)
        total_new = sum(r.get('new_count', 0) for r in results)
        _log_action('sync_card_all',
                    detail=f'{len(results)}개 카드, 신규 {total_new}건')
        flash(f'전체 카드 동기화 완료: {len(results)}개 카드, 신규 {total_new}건', 'success')
    except Exception as e:
        flash(f'전체 카드 동기화 오류: {e}', 'danger')
    return redirect(url_for('bank.card_transactions'))


@bank_bp.route('/api/card-transaction/<int:tx_id>/category', methods=['PUT'])
@role_required('admin', 'manager', 'general')
def update_card_category(tx_id):
    """카드 이용내역 분류(카테고리) 수정 API"""
    data = request.get_json()
    category = data.get('category', '')
    try:
        current_app.db.update_card_transaction(tx_id, {'category': category})
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


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


@bank_bp.route('/accounts/add', methods=['POST'])
@role_required('admin', 'manager')
def add_account():
    """계좌 수동 등록 (CODEF 없이 엑셀 업로드용)"""
    bank_name = request.form.get('bank_name', '').strip()
    bank_code = request.form.get('bank_code', '').strip()
    account_number = request.form.get('account_number', '').strip()
    account_holder = request.form.get('account_holder', '').strip()

    if not bank_name or not account_number:
        flash('은행명과 계좌번호는 필수입니다.', 'danger')
        return redirect(url_for('bank.index'))

    try:
        current_app.db.insert_bank_account({
            'bank_code': bank_code,
            'bank_name': bank_name,
            'account_number': account_number,
            'account_holder': account_holder,
            'account_type': '예금',
            'connected_id': None,
        })
        _log_action('add_bank_account',
                    detail=f'{bank_name} {account_number} 수동 등록')
        flash(f'{bank_name} {account_number} 계좌가 등록되었습니다.', 'success')
    except Exception as e:
        flash(f'계좌 등록 오류: {e}', 'danger')

    return redirect(url_for('bank.index'))


@bank_bp.route('/upload', methods=['GET', 'POST'])
@role_required('admin', 'manager', 'general')
def upload():
    """은행 거래내역 엑셀 업로드"""
    db = current_app.db

    if request.method == 'GET':
        accounts = db.query_bank_accounts()
        from services.bank_excel_service import MANUAL_BANK_LIST
        return render_template('bank/upload.html',
                               accounts=accounts,
                               bank_list=MANUAL_BANK_LIST)

    # POST: 엑셀 업로드 처리
    account_id = request.form.get('account_id')
    excel_file = request.files.get('excel_file')
    auto_match = request.form.get('auto_match')

    if not account_id or not excel_file:
        flash('계좌와 엑셀 파일을 선택하세요.', 'danger')
        return redirect(url_for('bank.upload'))

    # 계좌 정보 확인
    account = db.query_bank_account_by_id(int(account_id))
    if not account:
        flash('유효하지 않은 계좌입니다.', 'danger')
        return redirect(url_for('bank.upload'))

    # 엑셀 파싱
    from services.bank_excel_service import parse_bank_excel, save_transactions
    result = parse_bank_excel(
        excel_file, bank_code=account.get('bank_code', '004'),
        filename=excel_file.filename)

    if result['errors']:
        for err in result['errors']:
            flash(err, 'danger')
        return redirect(url_for('bank.upload'))

    if not result['transactions']:
        flash('파싱할 거래내역이 없습니다.', 'warning')
        return redirect(url_for('bank.upload'))

    # DB 저장
    save_result = save_transactions(db, int(account_id), result['transactions'])

    summary = result['summary']
    _log_action('bank_excel_upload',
                detail=f'{account.get("bank_name","")} {account.get("account_number","")}: '
                       f'총 {summary["total"]}건 (입금 {summary["deposits"]}, 출금 {summary["withdrawals"]}), '
                       f'신규 {save_result["new_count"]}건, 중복스킵 {save_result["skipped_count"]}건')

    flash(f'업로드 완료: 신규 {save_result["new_count"]}건 저장 '
          f'(중복 스킵 {save_result["skipped_count"]}건, '
          f'입금 {summary["deposits"]}건, 출금 {summary["withdrawals"]}건)',
          'success')

    # 자동매칭 실행
    if auto_match and save_result['new_count'] > 0:
        from services.matching_service import (
            auto_match_invoices, auto_match_settlements, auto_match_payables,
            confirm_match, confirm_settlement_match, confirm_payable_match,
        )

        total_matched = 0

        # 매출-입금 매칭
        try:
            inv_result = auto_match_invoices(db)
            for c in inv_result.get('candidates', []):
                try:
                    confirm_match(db, c['invoice_id'], c['transaction_id'],
                                  matched_by='excel_auto')
                    total_matched += 1
                except Exception:
                    pass
        except Exception as e:
            flash(f'매출-입금 매칭 오류: {e}', 'warning')

        # 정산-입금 매칭
        try:
            stl_result = auto_match_settlements(db)
            for c in stl_result.get('candidates', []):
                try:
                    confirm_settlement_match(db, c['settlement_id'], c['transaction_id'],
                                             matched_by='excel_auto')
                    total_matched += 1
                except Exception:
                    pass
        except Exception as e:
            flash(f'정산-입금 매칭 오류: {e}', 'warning')

        # 매입-출금 매칭
        try:
            pay_result = auto_match_payables(db)
            for c in pay_result.get('candidates', []):
                try:
                    confirm_payable_match(db, c['invoice_id'], c['transaction_id'],
                                          matched_by='excel_auto')
                    total_matched += 1
                except Exception:
                    pass
        except Exception as e:
            flash(f'매입-출금 매칭 오류: {e}', 'warning')

        if total_matched > 0:
            flash(f'자동매칭: {total_matched}건 매칭 완료', 'info')

    return redirect(url_for('bank.transactions'))
