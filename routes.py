from flask import Blueprint, render_template
from flask_login import login_required, current_user

main_bp = Blueprint('main', __name__)


@main_bp.route('/')
@login_required
def dashboard():
    """메인 대시보드 - 역할별 메뉴 표시"""
    # 역할별로 보이는 메뉴 정의
    menus = []

    # 공통 메뉴
    menus.append({'name': '대시보드', 'icon': 'bi-house', 'url': '/', 'roles': 'all'})

    # 영업팀 메뉴
    if current_user.role in ('admin', 'manager', 'sales'):
        menus.append({'name': '주문 관리', 'icon': 'bi-cart', 'url': '#', 'roles': 'sales'})
        menus.append({'name': '거래처 관리', 'icon': 'bi-building', 'url': '#', 'roles': 'sales'})

    # 물류팀 메뉴
    if current_user.role in ('admin', 'manager', 'logistics'):
        menus.append({'name': '출고 관리', 'icon': 'bi-truck', 'url': '#', 'roles': 'logistics'})
        menus.append({'name': '재고 조회', 'icon': 'bi-box', 'url': '#', 'roles': 'logistics'})

    # 생산팀 메뉴
    if current_user.role in ('admin', 'manager', 'production'):
        menus.append({'name': '생산 관리', 'icon': 'bi-gear', 'url': '#', 'roles': 'production'})
        menus.append({'name': '원자재 관리', 'icon': 'bi-clipboard-data', 'url': '#', 'roles': 'production'})

    # 책임자 이상 메뉴
    if current_user.can_view_all():
        menus.append({'name': '통합 현황', 'icon': 'bi-graph-up', 'url': '#', 'roles': 'manager'})
        menus.append({'name': '일일 매출', 'icon': 'bi-currency-won', 'url': '#', 'roles': 'manager'})

    # 관리자 전용
    if current_user.is_admin():
        menus.append({'name': '사용자 관리', 'icon': 'bi-people', 'url': '/admin/users', 'roles': 'admin'})
        menus.append({'name': '감사 로그', 'icon': 'bi-journal-text', 'url': '/admin/logs', 'roles': 'admin'})

    return render_template('dashboard.html', menus=menus)


# ============================================================
# 아래에 회사에서 check DB 기능 라우트를 추가하세요
# 예시:
#
# @main_bp.route('/orders')
# @login_required
# def orders():
#     # check DB에서 가져온 주문 데이터 표시
#     pass
#
# @main_bp.route('/customers')
# @login_required
# def customers():
#     # 거래처 관리
#     pass
# ============================================================
