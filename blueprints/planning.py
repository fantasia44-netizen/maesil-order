"""
blueprints/planning.py — 생산계획 Blueprint.
수량 기반 생산계획 엔진 (Production Planning v1).
"""
from flask import (
    Blueprint, render_template, request, current_app, jsonify,
)
from auth import role_required, _log_action
from services.tz_utils import today_kst

planning_bp = Blueprint('planning', __name__, url_prefix='/planning')


@planning_bp.route('/')
@role_required('admin', 'ceo', 'manager', 'production')
def index():
    """생산계획 메인 페이지"""
    return render_template('planning/index.html')


# ── API: 생산계획 계산 실행 ──

@planning_bp.route('/api/calculate', methods=['POST'])
@role_required('admin', 'manager', 'production')
def api_calculate():
    """생산계획 계산 실행"""
    try:
        from services.planning_service import calculate_production_plan

        data = request.get_json(silent=True) or {}
        window = int(data.get('sales_window', 7))
        if window < 1 or window > 90:
            window = 7

        result = calculate_production_plan(
            current_app.db,
            sales_window=window,
            save=True,
        )

        _log_action('production_plan',
                     detail=f"생산계획 생성: {result['summary']['total_targets']}품목, "
                            f"생산필요 {result['summary']['need_production']}건")

        return jsonify(result)
    except Exception as e:
        return jsonify({'error': f'생산계획 계산 오류: {e}'}), 500


# ── API: 생산계획 이력 조회 ──

@planning_bp.route('/api/history')
@role_required('admin', 'ceo', 'manager', 'production')
def api_history():
    """생산계획 이력"""
    try:
        from services.planning_service import get_plan_history
        return jsonify(get_plan_history(current_app.db, limit=30))
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── API: 특정 날짜 계획 조회 ──

@planning_bp.route('/api/plan/<plan_date>')
@role_required('admin', 'ceo', 'manager', 'production')
def api_plan_by_date(plan_date):
    """특정 날짜 생산계획 상세"""
    try:
        from services.planning_service import get_plan_by_date
        items = get_plan_by_date(current_app.db, plan_date)
        return jsonify(items)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── API: 품목별 생산계획 설정 수정 ──

@planning_bp.route('/api/config', methods=['POST'])
@role_required('admin', 'manager')
def api_update_config():
    """품목별 안전재고/리드타임/생산대상 설정"""
    try:
        from services.planning_service import update_product_planning_config

        data = request.get_json(silent=True) or {}
        product_name = data.get('product_name', '').strip()
        if not product_name:
            return jsonify({'error': '품목명을 입력하세요.'}), 400

        ok = update_product_planning_config(
            current_app.db,
            product_name=product_name,
            safety_stock=data.get('safety_stock'),
            lead_time_days=data.get('lead_time_days'),
            is_production_target=data.get('is_production_target'),
        )

        if ok:
            _log_action('update_planning_config', target=product_name,
                         detail=str(data))
            return jsonify({'success': True})
        return jsonify({'error': '수정 실패'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── API: 생산대상 품목 목록 + 현재 설정 ──

@planning_bp.route('/api/targets')
@role_required('admin', 'ceo', 'manager', 'production')
def api_targets():
    """생산대상 품목 + 설정 조회"""
    try:
        cost_map = current_app.db.query_product_costs()
        targets = []
        for name, info in cost_map.items():
            cost_type = info.get('cost_type', '')
            material_type = info.get('material_type', '')
            is_target = info.get('is_production_target')

            if is_target is False:
                continue
            if is_target is not True:
                if cost_type != '생산' and material_type not in ('완제품', '반제품'):
                    continue

            targets.append({
                'product_name': name,
                'safety_stock': int(info.get('safety_stock', 0) or 0),
                'lead_time_days': int(info.get('lead_time_days', 0) or 3),
                'is_production_target': is_target if is_target is not None else True,
                'material_type': material_type,
                'unit': info.get('unit', '개') or '개',
            })

        targets.sort(key=lambda x: x['product_name'])
        return jsonify(targets)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
