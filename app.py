import os
import time

from flask import Flask, redirect, request, session, url_for, flash, jsonify
from flask_login import LoginManager, current_user, logout_user
from flask_wtf.csrf import CSRFProtect
from werkzeug.middleware.proxy_fix import ProxyFix

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

    # 리버스 프록시 지원 (Render, Nginx 등)
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

    # CSRF
    CSRFProtect(app)

    # Supabase DB 초기화
    from db_supabase import SupabaseDB
    app.db = SupabaseDB()
    app.db.connect()

    # 권한 테이블 기본값 초기화 (테이블이 비어있으면 PAGE_REGISTRY 기본값 삽입)
    try:
        from models import PAGE_REGISTRY
        app.db.seed_default_permissions(PAGE_REGISTRY)
    except Exception as e:
        print(f"[WARN] seed_default_permissions: {e}")

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

    @login_manager.unauthorized_handler
    def unauthorized_api():
        """API 요청 시 JSON 반환, 일반 요청 시 로그인 페이지 리다이렉트"""
        from auth import _is_api_request
        if _is_api_request():
            return jsonify({'error': '로그인이 필요합니다. 페이지를 새로고침 해주세요.'}), 401
        flash('로그인이 필요합니다.', 'warning')
        return redirect(url_for('auth.login', next=request.url))

    # ── 보안: HTTPS 강제 (리버스프록시 환경) ──
    @app.before_request
    def enforce_https():
        """리버스프록시 뒤에서 HTTPS 강제 리다이렉트"""
        if app.config.get('SESSION_COOKIE_SECURE'):
            # X-Forwarded-Proto 헤더로 원본 프로토콜 확인
            if request.headers.get('X-Forwarded-Proto', 'https') == 'http':
                url = request.url.replace('http://', 'https://', 1)
                return redirect(url, code=301)

    # ── 보안: 세션 비활동 타임아웃 ──
    @app.before_request
    def check_session_timeout():
        """비활동 시간 초과 시 자동 로그아웃"""
        if current_user.is_authenticated:
            now = time.time()
            last_active = session.get('_last_active', now)
            timeout_min = app.config.get('SESSION_INACTIVITY_TIMEOUT', 60)

            if now - last_active > timeout_min * 60:
                logout_user()
                session.clear()
                from auth import _is_api_request
                if _is_api_request():
                    return jsonify({'error': '세션이 만료되었습니다. 페이지를 새로고침 해주세요.'}), 401
                flash('장시간 미사용으로 자동 로그아웃되었습니다.', 'warning')
                return redirect(url_for('auth.login'))

            session['_last_active'] = now

    # ── 보안: 응답 헤더 추가 ──
    @app.after_request
    def add_security_headers(response):
        """보안 HTTP 헤더 추가"""
        # 클릭재킹 방지
        response.headers['X-Frame-Options'] = 'SAMEORIGIN'
        # MIME 스니핑 방지
        response.headers['X-Content-Type-Options'] = 'nosniff'
        # XSS 필터
        response.headers['X-XSS-Protection'] = '1; mode=block'
        # Referrer 정보 제한
        response.headers['Referrer-Policy'] = 'strict-origin-when-cross-origin'
        # 캐시 제어 (인증 필요 페이지)
        if current_user.is_authenticated:
            response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate'
            response.headers['Pragma'] = 'no-cache'
        return response

    # ── 에러 핸들러: API 요청 시 JSON 응답 ──
    from flask_wtf.csrf import CSRFError
    from auth import _is_api_request

    @app.errorhandler(CSRFError)
    def handle_csrf_error(e):
        """CSRF 토큰 만료/누락 시 API는 JSON, 일반은 리다이렉트"""
        if _is_api_request():
            return jsonify({'error': '세션이 만료되었습니다. 페이지를 새로고침 해주세요.'}), 400
        flash('세션이 만료되었습니다. 다시 시도해주세요.', 'warning')
        return redirect(request.referrer or url_for('auth.login'))

    @app.errorhandler(400)
    def handle_400(e):
        if _is_api_request():
            return jsonify({'error': '잘못된 요청입니다.'}), 400
        flash('잘못된 요청입니다.', 'warning')
        return redirect(request.referrer or url_for('main.dashboard'))

    @app.errorhandler(404)
    def handle_404(e):
        if _is_api_request():
            return jsonify({'error': '요청한 리소스를 찾을 수 없습니다.'}), 404
        # favicon 등 정적 리소스 요청은 flash 없이 조용히 처리
        req_path = request.path or ''
        if req_path.endswith(('.ico', '.png', '.jpg', '.css', '.js', '.map', '.woff', '.woff2')):
            return '', 404
        flash('페이지를 찾을 수 없습니다.', 'warning')
        return redirect(url_for('main.dashboard'))

    @app.errorhandler(500)
    def handle_500(e):
        if _is_api_request():
            return jsonify({'error': '서버 내부 오류가 발생했습니다.'}), 500
        flash('서버 오류가 발생했습니다. 잠시 후 다시 시도해주세요.', 'danger')
        return redirect(url_for('main.dashboard'))

    # 사이드바 메뉴 (DB 기반 동적 권한)
    @app.context_processor
    def inject_sidebar():
        if not current_user.is_authenticated:
            return dict(sidebar_menus=[], pending_users=0)

        from models import PAGE_REGISTRY
        r = current_user.role

        # DB 권한 조회 (TTL 캐시)
        perms = app.db.query_role_permissions()
        role_perms = perms.get(r, {})

        menus = []
        for page_key, name, icon, url, defaults in PAGE_REGISTRY:
            # DB에 설정 있으면 DB 우선, 없으면 기본값(defaults) 사용
            if role_perms.get(page_key, r in defaults):
                menus.append({'name': name, 'icon': icon, 'url': url})

        pending_users = app.db.count_pending_users() if current_user.is_admin() else 0

        return dict(sidebar_menus=menus, pending_users=pending_users)

    # Blueprint 등록
    from auth import auth_bp
    from admin import admin_bp
    from blueprints.dashboard import dashboard_bp
    from blueprints.stock import stock_bp
    from blueprints.production import production_bp
    from blueprints.inbound import inbound_bp
    from blueprints.adjustment import adjustment_bp
    from blueprints.outbound import outbound_bp
    from blueprints.transfer import transfer_bp
    from blueprints.base_data import base_data_bp
    from blueprints.history import history_bp
    from blueprints.revenue import revenue_bp
    from blueprints.master import master_bp
    from blueprints.ledger import ledger_bp
    from blueprints.repack import repack_bp
    from blueprints.set_assembly import set_assembly_bp
    from blueprints.etc_outbound import etc_outbound_bp
    from blueprints.trade import trade_bp
    from blueprints.orders import orders_bp
    from blueprints.aggregation import aggregation_bp
    from blueprints.mobile import mobile_bp
    from blueprints.bom_cost import bom_cost_bp
    from blueprints.yield_mgmt import yield_bp
    from blueprints.price_mgmt import price_mgmt_bp
    from blueprints.promotions import promotions_bp
    from blueprints.closing import closing_bp
    from blueprints.shipment import shipment_bp
    from blueprints.integrity import integrity_bp
    from blueprints.planning import planning_bp

    for bp in [auth_bp, admin_bp, dashboard_bp, stock_bp, production_bp,
               inbound_bp, adjustment_bp,
               outbound_bp, transfer_bp, base_data_bp, history_bp, revenue_bp,
               master_bp, ledger_bp, repack_bp, set_assembly_bp,
               etc_outbound_bp, trade_bp, orders_bp, aggregation_bp,
               mobile_bp, bom_cost_bp, yield_bp, price_mgmt_bp, promotions_bp,
               closing_bp, shipment_bp, integrity_bp, planning_bp]:
        app.register_blueprint(bp)

    # ── Jinja2 커스텀 필터 ──
    def fmt_qty(val):
        """수량 포맷: 정수면 쉼표만, 소수면 소수점 유지 (최대 2자리)"""
        try:
            n = float(val)
            if n == int(n):
                return f"{int(n):,}"
            return f"{n:,.2f}".rstrip('0').rstrip('.')
        except (ValueError, TypeError):
            return str(val) if val else '0'
    app.jinja_env.filters['fmt_qty'] = fmt_qty

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
    print(f'\n  배마마 통합시스템 서버 시작: http://localhost:{port}')
    print(f'  종료: Ctrl+C\n')

    app.config['TEMPLATES_AUTO_RELOAD'] = True
    app.run(host='127.0.0.1', port=port, debug=False, threaded=True)
