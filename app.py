import os

from flask import Flask
from flask_login import LoginManager, current_user
from flask_wtf.csrf import CSRFProtect

from config import Config, DevelopmentConfig
from models import User


def create_app(config_class=None):
    app = Flask(__name__)

    # 환경에 따라 설정 로드
    if config_class is None:
        if os.environ.get('FLASK_ENV') == 'production':
            from config import ProductionConfig
            config_class = ProductionConfig
        else:
            config_class = DevelopmentConfig

    app.config.from_object(config_class)

    # CSRF
    CSRFProtect(app)

    # Supabase DB 초기화
    from db_supabase import SupabaseDB
    app.db = SupabaseDB()
    app.db.connect()

    # 로그인 매니저
    login_manager = LoginManager()
    login_manager.init_app(app)
    login_manager.login_view = 'auth.login'
    login_manager.login_message = '로그인이 필요합니다.'
    login_manager.login_message_category = 'warning'

    @login_manager.user_loader
    def load_user(user_id):
        row = app.db.query_user_by_id(int(user_id))
        return User(row) if row else None

    # 사이드바 메뉴 (모든 페이지에서 사용)
    @app.context_processor
    def inject_sidebar():
        if not current_user.is_authenticated:
            return dict(sidebar_menus=[], pending_users=0)

        menus = [{'name': '대시보드', 'icon': 'bi-house', 'url': '/'}]

        if current_user.role in ('admin', 'manager', 'sales'):
            menus.append({'name': '주문 처리', 'icon': 'bi-cart', 'url': '/orders'})
            menus.append({'name': '통합 집계', 'icon': 'bi-calculator', 'url': '/aggregation'})
            menus.append({'name': '거래처 관리', 'icon': 'bi-building', 'url': '/trade'})

        if current_user.role in ('admin', 'manager', 'logistics'):
            menus.append({'name': '출고 관리', 'icon': 'bi-truck', 'url': '/outbound'})
            menus.append({'name': '재고 현황', 'icon': 'bi-box', 'url': '/stock'})
            menus.append({'name': '창고 이동', 'icon': 'bi-arrow-left-right', 'url': '/transfer'})

        if current_user.role in ('admin', 'manager', 'production'):
            menus.append({'name': '생산/입고', 'icon': 'bi-gear', 'url': '/production'})
            menus.append({'name': '소분 관리', 'icon': 'bi-scissors', 'url': '/repack'})

        if current_user.can_view_all():
            menus.append({'name': '수불장', 'icon': 'bi-journal-text', 'url': '/ledger'})
            menus.append({'name': '매출 관리', 'icon': 'bi-currency-won', 'url': '/revenue'})

        if current_user.is_admin():
            menus.append({'name': '마스터 관리', 'icon': 'bi-database', 'url': '/master'})
            menus.append({'name': '이력 관리', 'icon': 'bi-clock-history', 'url': '/history'})
            menus.append({'name': '기초 데이터', 'icon': 'bi-hdd', 'url': '/base-data'})
            menus.append({'name': '사용자 관리', 'icon': 'bi-people', 'url': '/admin/users'})
            menus.append({'name': '감사 로그', 'icon': 'bi-shield-check', 'url': '/admin/logs'})

        pending_users = app.db.count_pending_users() if current_user.is_admin() else 0

        return dict(sidebar_menus=menus, pending_users=pending_users)

    # Blueprint 등록
    from auth import auth_bp
    from admin import admin_bp
    from blueprints.dashboard import dashboard_bp
    from blueprints.stock import stock_bp
    from blueprints.production import production_bp
    from blueprints.outbound import outbound_bp
    from blueprints.transfer import transfer_bp
    from blueprints.base_data import base_data_bp
    from blueprints.history import history_bp
    from blueprints.revenue import revenue_bp
    from blueprints.master import master_bp
    from blueprints.ledger import ledger_bp
    from blueprints.repack import repack_bp
    from blueprints.trade import trade_bp
    from blueprints.orders import orders_bp
    from blueprints.aggregation import aggregation_bp

    for bp in [auth_bp, admin_bp, dashboard_bp, stock_bp, production_bp,
               outbound_bp, transfer_bp, base_data_bp, history_bp, revenue_bp,
               master_bp, ledger_bp, repack_bp, trade_bp, orders_bp, aggregation_bp]:
        app.register_blueprint(bp)

    # 폴더 생성
    os.makedirs(app.config.get('UPLOAD_FOLDER', 'uploads'), exist_ok=True)
    os.makedirs(app.config.get('OUTPUT_FOLDER', 'output'), exist_ok=True)

    return app


def init_db(app):
    """기본 관리자 계정 확인/생성 (app_users 테이블이 없으면 건너뜀)"""
    with app.app_context():
        try:
            existing = app.db.query_user_by_username('admin')
            if not existing:
                admin_user = User()
                admin_user.set_password('admin1234!')
                app.db.insert_user({
                    'username': 'admin',
                    'name': '관리자',
                    'password_hash': admin_user.password_hash,
                    'role': 'admin',
                    'is_approved': True,
                    'is_active_user': True,
                })
                print('[초기화] 관리자 계정 생성 완료')
                print('  아이디: admin / 비밀번호: admin1234!')
        except Exception as e:
            print(f'[경고] 사용자 테이블 초기화 실패: {e}')
            print('  Supabase에서 app_users, audit_logs 테이블을 먼저 생성하세요.')


if __name__ == '__main__':
    app = create_app()
    init_db(app)

    port = int(os.environ.get('PORT', 5000))
    print(f'\n  오토툴 서버 시작: http://localhost:{port}')
    print(f'  종료: Ctrl+C\n')

    app.run(host='0.0.0.0', port=port, debug=True)
