"""
closing.py — 일일마감 관리 Blueprint.
매출마감(영업부) / 재고출고마감(물류팀) 분리 운영.
"""
from datetime import datetime
from flask import (
    Blueprint, render_template, request, current_app,
    flash, redirect, url_for, jsonify,
)
from flask_login import login_required, current_user

from auth import role_required

closing_bp = Blueprint('closing', __name__, url_prefix='/closing')


# ================================================================
# 메인 페이지
# ================================================================

@closing_bp.route('/')
@login_required
def index():
    """마감 현황 페이지"""
    db = current_app.db

    # 기본: 최근 14일
    from datetime import timedelta
    today = datetime.now().strftime('%Y-%m-%d')
    two_weeks_ago = (datetime.now() - timedelta(days=14)).strftime('%Y-%m-%d')

    date_from = request.args.get('date_from', two_weeks_ago)
    date_to = request.args.get('date_to', today)

    revenue_list = db.query_closing_list(date_from, date_to, 'revenue')
    stock_list = db.query_closing_list(date_from, date_to, 'stock')

    # 날짜별 매핑
    closing_map = {}
    for r in revenue_list:
        d = r['closing_date']
        if d not in closing_map:
            closing_map[d] = {'date': d, 'revenue': None, 'stock': None}
        closing_map[d]['revenue'] = r

    for s in stock_list:
        d = s['closing_date']
        if d not in closing_map:
            closing_map[d] = {'date': d, 'revenue': None, 'stock': None}
        closing_map[d]['stock'] = s

    # 정렬
    closing_dates = sorted(closing_map.values(), key=lambda x: x['date'], reverse=True)

    cutoff_time = current_app.config.get('DAILY_CUTOFF_TIME', '15:05')

    return render_template('closing/index.html',
                           closing_dates=closing_dates,
                           date_from=date_from,
                           date_to=date_to,
                           today=today,
                           cutoff_time=cutoff_time,
                           user_role=current_user.role)


# ================================================================
# 마감 실행 API
# ================================================================

@closing_bp.route('/api/close', methods=['POST'])
@login_required
def api_close():
    """마감 실행"""
    db = current_app.db
    data = request.get_json() or {}
    closing_date = data.get('date')
    closing_type = data.get('type')  # 'revenue' or 'stock'
    memo = data.get('memo', '')

    if not closing_date or closing_type not in ('revenue', 'stock'):
        return jsonify({'error': '날짜와 마감 유형을 확인하세요.'}), 400

    # 권한 체크: 매출마감=영업부(admin,manager,sales), 재고마감=물류팀(admin,manager,logistics)
    role = current_user.role
    if closing_type == 'revenue' and role not in ('admin', 'manager', 'sales'):
        return jsonify({'error': '매출마감은 영업부만 가능합니다.'}), 403
    if closing_type == 'stock' and role not in ('admin', 'manager', 'logistics'):
        return jsonify({'error': '재고마감은 물류팀만 가능합니다.'}), 403

    # 이미 마감인지 체크
    if db.is_closed(closing_date, closing_type):
        return jsonify({'error': f'{closing_date} {_type_label(closing_type)}은 이미 마감되었습니다.'}), 400

    cutoff_time = current_app.config.get('DAILY_CUTOFF_TIME', '15:05')
    db.close_day(closing_date, closing_type, current_user.username, cutoff_time, memo)

    return jsonify({
        'success': True,
        'message': f'{closing_date} {_type_label(closing_type)} 완료 (by {current_user.username})'
    })


# ================================================================
# 마감 해제 API
# ================================================================

@closing_bp.route('/api/reopen', methods=['POST'])
@login_required
def api_reopen():
    """마감 해제"""
    db = current_app.db
    data = request.get_json() or {}
    closing_date = data.get('date')
    closing_type = data.get('type')
    memo = data.get('memo', '')

    if not closing_date or closing_type not in ('revenue', 'stock'):
        return jsonify({'error': '날짜와 마감 유형을 확인하세요.'}), 400

    # 권한 체크 (동일)
    role = current_user.role
    if closing_type == 'revenue' and role not in ('admin', 'manager', 'sales'):
        return jsonify({'error': '매출마감 해제는 영업부만 가능합니다.'}), 403
    if closing_type == 'stock' and role not in ('admin', 'manager', 'logistics'):
        return jsonify({'error': '재고마감 해제는 물류팀만 가능합니다.'}), 403

    if not db.is_closed(closing_date, closing_type):
        return jsonify({'error': f'{closing_date} {_type_label(closing_type)}은 마감 상태가 아닙니다.'}), 400

    db.reopen_day(closing_date, closing_type, current_user.username, memo)

    return jsonify({
        'success': True,
        'message': f'{closing_date} {_type_label(closing_type)} 해제됨 (by {current_user.username})'
    })


# ================================================================
# 마감 상태 조회 API (다른 모듈에서 호출용)
# ================================================================

@closing_bp.route('/api/status')
@login_required
def api_status():
    """특정 날짜의 마감 상태 조회"""
    db = current_app.db
    closing_date = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))

    revenue_status = db.get_closing_status(closing_date, 'revenue')
    stock_status = db.get_closing_status(closing_date, 'stock')

    return jsonify({
        'date': closing_date,
        'revenue': {
            'closed': revenue_status is not None and revenue_status.get('status') == 'closed',
            'closed_by': revenue_status.get('closed_by', '') if revenue_status else '',
            'closed_at': revenue_status.get('closed_at', '') if revenue_status else '',
        },
        'stock': {
            'closed': stock_status is not None and stock_status.get('status') == 'closed',
            'closed_by': stock_status.get('closed_by', '') if stock_status else '',
            'closed_at': stock_status.get('closed_at', '') if stock_status else '',
        },
    })


def _type_label(closing_type):
    return '매출마감' if closing_type == 'revenue' else '재고출고마감'
