import os
import time

from flask import Flask, redirect, request, session, url_for, flash, jsonify, g
from flask_login import LoginManager, current_user, logout_user, login_required
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

    # ── 사업자별 Supabase DB Pool 초기화 ──
    from db_supabase import SupabaseDB
    from config import BUSINESSES, DEFAULT_BUSINESS

    app.db_pool = {}
    for biz_id, biz_conf in BUSINESSES.items():
        if biz_conf.get('supabase_url') and biz_conf.get('supabase_key'):
            db_inst = SupabaseDB()
            if db_inst.connect(biz_conf['supabase_url'], biz_conf['supabase_key']):
                app.db_pool[biz_id] = db_inst
                print(f'[DB] {biz_conf["name"]} 연결 성공')
            else:
                print(f'[DB] {biz_conf["name"]} 연결 실패')
        else:
            print(f'[DB] {biz_conf["name"]} — URL/KEY 미설정, 건너뜀')

    # 기본 사업자 DB (기존 호환)
    app.db = app.db_pool.get(DEFAULT_BUSINESS)
    if not app.db:
        raise RuntimeError(f'기본 사업자 "{DEFAULT_BUSINESS}" DB 연결 실패')

    # ── CODEF 싱글톤 ──
    from services.codef_service import CodefService
    app.codef = CodefService(app.config)

    # ── Popbill 싱글톤 ──
    from services.popbill_service import PopbillService
    app.popbill = PopbillService(app.config)

    # ── Marketplace API 싱글톤 ──
    from services.marketplace import MarketplaceManager
    app.marketplace = MarketplaceManager(app.db)

    # ── 네이버 검색광고 API 클라이언트 ──
    app.naver_ad = None
    try:
        from services.marketplace.naver_ad_client import NaverAdClient
        # DB extra_config에서 로드
        naver_cfgs = app.db.query_marketplace_api_configs(channel='스마트스토어')
        naver_cfg = naver_cfgs[0] if naver_cfgs else None
        if naver_cfg:
            ec = naver_cfg.get('extra_config') or {}
            if ec.get('ad_customer_id') and ec.get('ad_api_key') and ec.get('ad_secret_key'):
                app.naver_ad = NaverAdClient(ec)
                print(f"[INFO] NaverAdClient 초기화 완료")
        if not app.naver_ad:
            print("[INFO] NaverAdClient 미설정 (설정 페이지에서 광고 API 키 입력 필요)")
    except Exception as e:
        print(f"[WARN] NaverAdClient 초기화 실패: {e}")

    # 권한 테이블 기본값 초기화
    try:
        from models import PAGE_REGISTRY
        for biz_id, db_inst in app.db_pool.items():
            db_inst.seed_default_permissions(PAGE_REGISTRY)
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
        """API 요청 시 JSON 반환, 일반 요청 시 메인 페이지로 리다이렉트"""
        from auth import _is_api_request
        if _is_api_request():
            return jsonify({'error': '로그인이 필요합니다. 페이지를 새로고침 해주세요.'}), 401
        # 패킹센터 경로는 패킹 로그인으로
        if request.path.startswith('/packing'):
            flash('로그인이 필요합니다.', 'warning')
            return redirect(url_for('packing.packing_login'))
        # 세션 만료 시 메인 페이지로 이동 (에러 페이지 대신)
        return redirect(url_for('auth.login'))

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
                was_packing = current_user.role == 'packing'
                logout_user()
                session.clear()
                from auth import _is_api_request
                if _is_api_request():
                    return jsonify({'error': '세션이 만료되었습니다. 페이지를 새로고침 해주세요.'}), 401
                flash('장시간 미사용으로 자동 로그아웃되었습니다.', 'warning')
                if was_packing or request.path.startswith('/packing'):
                    return redirect(url_for('packing.packing_login'))
                return redirect(url_for('auth.login'))

            session['_last_active'] = now

    # ── 사업자 DB 스위칭 (세션 기반) ──
    @app.before_request
    def switch_db_by_session():
        biz = session.get('current_biz', DEFAULT_BUSINESS)
        if biz in app.db_pool:
            app.db = app.db_pool[biz]

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

    # ── 사업자 전환 라우트 (로그인 상태) ──
    @app.route('/switch-business/<biz_id>')
    @login_required
    def switch_business(biz_id):
        if biz_id not in BUSINESSES:
            flash('존재하지 않는 사업자입니다.', 'warning')
            return redirect(url_for('main.dashboard'))
        if biz_id not in app.db_pool:
            flash(f'"{BUSINESSES[biz_id]["name"]}" DB가 아직 설정되지 않았습니다.', 'warning')
            return redirect(url_for('main.dashboard'))
        # 사업자 전환 → 로그아웃 후 재로그인 (각 DB에 별도 사용자)
        logout_user()
        session.clear()
        session['current_biz'] = biz_id
        flash(f'"{BUSINESSES[biz_id]["name"]}" 사업자로 전환되었습니다. 로그인해주세요.', 'info')
        return redirect(url_for('auth.login'))

    # ── 사업자 전환 라우트 (비로그인 — 로그인 페이지에서 사용) ──
    @app.route('/switch-biz/<biz_id>')
    def switch_biz(biz_id):
        if biz_id in BUSINESSES and biz_id in app.db_pool:
            if current_user.is_authenticated:
                logout_user()
            session.clear()
            session['current_biz'] = biz_id
            flash(f'"{BUSINESSES[biz_id]["name"]}" 사업자로 전환되었습니다.', 'info')
        return redirect(url_for('auth.login'))

    # 사이드바 메뉴 (DB 기반 동적 권한 + 그룹핑 + 사업자별 필터링)
    @app.context_processor
    def inject_sidebar():
        # 현재 사업자 정보 (비로그인 상태에서도 필요)
        biz_id = session.get('current_biz', DEFAULT_BUSINESS)
        biz_conf = BUSINESSES.get(biz_id, BUSINESSES.get(DEFAULT_BUSINESS, {}))
        available_biz = {k: v for k, v in BUSINESSES.items() if k in app.db_pool}

        if not current_user.is_authenticated:
            return dict(sidebar_menus=[], sidebar_groups={},
                        sidebar_top_menu=None, pending_users=0,
                        current_biz=biz_conf, current_biz_id=biz_id,
                        businesses=available_biz)

        # 패킹 사용자는 사이드바 불필요 (전용 템플릿 사용)
        if current_user.role == 'packing':
            return dict(sidebar_menus=[], sidebar_groups={},
                        sidebar_top_menu=None, pending_users=0,
                        current_biz=biz_conf, current_biz_id=biz_id,
                        businesses=available_biz)

        # ── 사이드바 세션 캐시 (역할+사업자 조합 기준, 5분 TTL) ──
        import time as _time
        cache_key = f'_sidebar_{current_user.role}_{biz_id}'
        cached = session.get(cache_key)
        now = _time.time()

        if cached and (now - cached.get('ts', 0)) < 300:
            # 캐시 히트 — pending_users만 갱신 (TTL 캐시됨)
            # 세션 직렬화로 dict 순서가 깨질 수 있으므로 MENU_GROUPS 순서로 복원
            from collections import OrderedDict
            from models import MENU_GROUPS
            raw_groups = cached['groups']
            ordered = OrderedDict()
            for g in MENU_GROUPS:
                if g in raw_groups:
                    ordered[g] = raw_groups[g]
            pending_users = app.db.count_pending_users() if current_user.is_admin() else 0
            return dict(sidebar_menus=cached['menus'],
                        sidebar_groups=ordered,
                        sidebar_top_menu=cached['top'],
                        pending_users=pending_users,
                        current_biz=biz_conf, current_biz_id=biz_id,
                        businesses=available_biz,
                        channel_labels=Config.CHANNEL_LABELS)

        from collections import OrderedDict
        from models import PAGE_REGISTRY, MENU_GROUPS
        r = current_user.role

        # DB 권한 조회 (TTL 캐시)
        perms = app.db.query_role_permissions()
        role_perms = perms.get(r, {})

        # 사업자별 제외 페이지
        exclude_pages = set(biz_conf.get('exclude_pages', []))

        flat_menus = []
        top_menu = None
        groups = OrderedDict((g, []) for g in MENU_GROUPS)

        for page_key, name, icon, url, defaults, group in PAGE_REGISTRY:
            if page_key in exclude_pages:
                continue
            if not role_perms.get(page_key, r in defaults):
                continue
            item = {'name': name, 'icon': icon, 'url': url}
            flat_menus.append(item)
            if group is None:
                top_menu = item
            elif group in groups:
                groups[group].append(item)

        # 빈 그룹 제거
        groups = OrderedDict((k, v) for k, v in groups.items() if v)

        # 세션에 캐시 저장
        session[cache_key] = {
            'menus': flat_menus,
            'groups': dict(groups),
            'top': top_menu,
            'ts': now,
        }

        pending_users = app.db.count_pending_users() if current_user.is_admin() else 0

        return dict(sidebar_menus=flat_menus, sidebar_groups=groups,
                    sidebar_top_menu=top_menu, pending_users=pending_users,
                    current_biz=biz_conf, current_biz_id=biz_id,
                    businesses=available_biz,
                    channel_labels=Config.CHANNEL_LABELS)

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
    from blueprints.packing import packing_bp
    from blueprints.reconciliation import reconciliation_bp
    from blueprints.finance import finance_bp
    from blueprints.hr import hr_bp
    from blueprints.accounting import accounting_bp
    from blueprints.bank import bank_bp
    from blueprints.tax_invoice import tax_invoice_bp
    from blueprints.journal import journal_bp
    from blueprints.marketplace import marketplace_bp

    for bp in [auth_bp, admin_bp, dashboard_bp, stock_bp, production_bp,
               inbound_bp, adjustment_bp,
               outbound_bp, transfer_bp, base_data_bp, history_bp, revenue_bp,
               master_bp, ledger_bp, repack_bp, set_assembly_bp,
               etc_outbound_bp, trade_bp, orders_bp, aggregation_bp,
               mobile_bp, bom_cost_bp, yield_bp, price_mgmt_bp, promotions_bp,
               closing_bp, shipment_bp, integrity_bp, planning_bp,
               packing_bp, reconciliation_bp, finance_bp, hr_bp,
               accounting_bp, bank_bp, tax_invoice_bp, journal_bp,
               marketplace_bp]:
        app.register_blueprint(bp)

    # ── Cafe24 OAuth 콜백 리다이렉트 ──
    # Cafe24 개발자센터 Redirect URI: /cafe24/callback
    # 실제 처리: /marketplace/oauth/callback/자사몰
    @app.route('/cafe24/callback')
    def cafe24_oauth_redirect():
        qs = request.query_string.decode('utf-8')
        target = url_for('marketplace.oauth_callback', channel='자사몰')
        if qs:
            target += '?' + qs
        return redirect(target)

    # ── 패킹 사용자 메인 시스템 접근 차단 ──
    @app.before_request
    def block_packing_from_main():
        """packing role은 /packing/*, /static/* 만 접근 가능"""
        if current_user.is_authenticated and current_user.role == 'packing':
            allowed = ('/packing', '/static')
            if not any(request.path.startswith(p) for p in allowed):
                flash('접근 권한이 없습니다.', 'danger')
                return redirect(url_for('packing.index'))

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

    def fmt_money(val):
        """금액 포맷 (1,000,000)"""
        try:
            return f"{int(val):,}"
        except (ValueError, TypeError):
            return '0'
    app.jinja_env.filters['fmt_money'] = fmt_money

    def fmt_kst(val):
        """UTC → KST 날짜/시간 표시"""
        if not val:
            return ''
        try:
            from datetime import datetime, timedelta
            if isinstance(val, str):
                val = val.replace('Z', '+00:00')
                dt = datetime.fromisoformat(val)
            else:
                dt = val
            kst = dt + timedelta(hours=9)
            return kst.strftime('%Y-%m-%d %H:%M')
        except Exception:
            return str(val)[:16] if val else ''
    app.jinja_env.filters['fmt_kst'] = fmt_kst

    # 폴더 생성
    os.makedirs(app.config.get('UPLOAD_FOLDER', 'uploads'), exist_ok=True)
    os.makedirs(app.config.get('OUTPUT_FOLDER', 'output'), exist_ok=True)

    return app


def init_db(app):
    """모든 연결된 사업자 DB에 기본 관리자 계정 확인/생성"""
    with app.app_context():
        for biz_id, db_inst in app.db_pool.items():
            try:
                print(f'[{biz_id}] 관리자 계정 확인 중...')
                existing = db_inst.query_user_by_username('admin')
                if existing:
                    print(f'[{biz_id}] 관리자 계정 이미 존재 (id={existing.get("id")})')
                else:
                    print(f'[{biz_id}] 관리자 계정 없음 → 생성 시도')
                    admin_user = User()
                    admin_user.set_password('admin1234!')
                    try:
                        db_inst.insert_user({
                            'username': 'admin',
                            'name': '관리자',
                            'password_hash': admin_user.password_hash,
                            'role': 'admin',
                            'is_approved': True,
                            'is_active_user': True,
                        })
                        print(f'[{biz_id}] 관리자 계정 생성 완료 (admin / admin1234!)')
                    except Exception as ie:
                        if '23505' in str(ie):
                            print(f'[{biz_id}] 관리자 계정 이미 존재 (OK)')
                        else:
                            raise
            except Exception as e:
                print(f'[{biz_id}] 사용자 테이블 초기화 실패: {e}')


if __name__ == '__main__':
    from config import BUSINESSES
    app = create_app()
    init_db(app)

    port = int(os.environ.get('PORT', 5000))
    print(f'\n  통합시스템 서버 시작: http://localhost:{port}')
    biz_names = [v['name'] for k, v in BUSINESSES.items() if k in app.db_pool]
    print(f'  연결된 사업자: {", ".join(biz_names)}')
    print(f'  종료: Ctrl+C\n')

    app.config['TEMPLATES_AUTO_RELOAD'] = True
    app.run(host='127.0.0.1', port=port, debug=False, threaded=True)
