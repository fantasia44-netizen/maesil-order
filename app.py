import os
import sys

from flask import Flask, redirect, url_for
from flask_login import LoginManager, current_user
from flask_wtf.csrf import CSRFProtect
from flask_migrate import Migrate

from config import Config, DevelopmentConfig
from models import db, User


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

    # 확장 초기화
    db.init_app(app)
    migrate = Migrate(app, db)
    csrf = CSRFProtect(app)

    # 로그인 매니저
    login_manager = LoginManager()
    login_manager.init_app(app)
    login_manager.login_view = 'auth.login'
    login_manager.login_message = '로그인이 필요합니다.'
    login_manager.login_message_category = 'warning'

    @login_manager.user_loader
    def load_user(user_id):
        return db.session.get(User, int(user_id))

    # 사이드바 메뉴 (모든 페이지에서 사용)
    @app.context_processor
    def inject_sidebar():
        if not current_user.is_authenticated:
            return dict(sidebar_menus=[])

        menus = [{'name': '대시보드', 'icon': 'bi-house', 'url': '/'}]

        if current_user.role in ('admin', 'manager', 'sales'):
            menus.append({'name': '주문 관리', 'icon': 'bi-cart', 'url': '#'})
            menus.append({'name': '거래처 관리', 'icon': 'bi-building', 'url': '#'})

        if current_user.role in ('admin', 'manager', 'logistics'):
            menus.append({'name': '출고 관리', 'icon': 'bi-truck', 'url': '#'})
            menus.append({'name': '재고 조회', 'icon': 'bi-box', 'url': '#'})

        if current_user.role in ('admin', 'manager', 'production'):
            menus.append({'name': '생산 관리', 'icon': 'bi-gear', 'url': '#'})
            menus.append({'name': '원자재 관리', 'icon': 'bi-clipboard-data', 'url': '#'})

        if current_user.can_view_all():
            menus.append({'name': '통합 현황', 'icon': 'bi-graph-up', 'url': '#'})
            menus.append({'name': '일일 매출', 'icon': 'bi-currency-won', 'url': '#'})

        if current_user.is_admin():
            menus.append({'name': '사용자 관리', 'icon': 'bi-people', 'url': '/admin/users'})
            menus.append({'name': '감사 로그', 'icon': 'bi-journal-text', 'url': '/admin/logs'})

        # 대시보드에서 승인 대기 알림용
        pending_users = User.query.filter_by(is_approved=False).count() if current_user.is_admin() else 0

        return dict(sidebar_menus=menus, pending_users=pending_users)

    # 블루프린트 등록
    from auth import auth_bp
    from admin import admin_bp
    from routes import main_bp

    app.register_blueprint(auth_bp)
    app.register_blueprint(admin_bp)
    app.register_blueprint(main_bp)

    # 인스턴스/업로드 폴더 생성
    os.makedirs(app.instance_path, exist_ok=True)
    os.makedirs(app.config.get('UPLOAD_FOLDER', 'uploads'), exist_ok=True)

    return app


def init_db(app):
    """DB 초기화 및 기본 관리자 계정 생성"""
    with app.app_context():
        db.create_all()

        # 관리자 계정이 없으면 생성
        if not User.query.filter_by(role='admin').first():
            admin = User(
                username='admin',
                name='관리자',
                role='admin',
                is_approved=True,
                is_active_user=True
            )
            admin.set_password('admin1234!')
            db.session.add(admin)
            db.session.commit()
            print('[초기화] 관리자 계정 생성 완료')
            print('  아이디: admin')
            print('  비밀번호: admin1234!')
            print('  ※ 첫 로그인 후 반드시 비밀번호를 변경하세요!')


if __name__ == '__main__':
    app = create_app()
    init_db(app)

    port = int(os.environ.get('PORT', 5000))
    print(f'\n  오토툴 서버 시작: http://localhost:{port}')
    print(f'  종료: Ctrl+C\n')

    app.run(host='0.0.0.0', port=port, debug=True)
