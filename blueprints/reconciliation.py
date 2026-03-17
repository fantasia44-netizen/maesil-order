"""
blueprints/reconciliation.py — 숫자 대조표(Reconciliation) API.
주문-출고, 출고-매출, 매출 요약, 재고 정합성 검증 엔드포인트 제공.
UI 없음 — API 전용.
"""
from flask import Blueprint, request, current_app, jsonify
from auth import role_required
from db_utils import get_db

reconciliation_bp = Blueprint('reconciliation', __name__,
                              url_prefix='/api/reconciliation')


@reconciliation_bp.route('/order-outbound')
@role_required('admin', 'manager')
def api_order_outbound():
    """주문(출고완료) 수량 vs stock_ledger SALES_OUT 대조."""
    date_from = request.args.get('from')
    date_to = request.args.get('to')
    if not date_from or not date_to:
        return jsonify({'error': 'from, to 파라미터가 필요합니다.'}), 400

    try:
        from services.reconciliation_service import validate_order_outbound
        result = validate_order_outbound(get_db(), date_from, date_to)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': f'주문-출고 대조 오류: {e}'}), 500


@reconciliation_bp.route('/outbound-revenue')
@role_required('admin', 'manager')
def api_outbound_revenue():
    """stock_ledger SALES_OUT 수량 vs daily_revenue 수량 대조."""
    date_from = request.args.get('from')
    date_to = request.args.get('to')
    if not date_from or not date_to:
        return jsonify({'error': 'from, to 파라미터가 필요합니다.'}), 400

    try:
        from services.reconciliation_service import validate_outbound_revenue
        result = validate_outbound_revenue(get_db(), date_from, date_to)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': f'출고-매출 대조 오류: {e}'}), 500


@reconciliation_bp.route('/revenue-summary')
@role_required('admin', 'manager')
def api_revenue_summary():
    """특정 날짜의 매출 요약."""
    date_str = request.args.get('date')
    if not date_str:
        return jsonify({'error': 'date 파라미터가 필요합니다.'}), 400

    try:
        from services.reconciliation_service import validate_revenue_summary
        result = validate_revenue_summary(get_db(), date_str)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': f'매출 요약 오류: {e}'}), 500


@reconciliation_bp.route('/stock-integrity')
@role_required('admin', 'manager')
def api_stock_integrity():
    """재고 정합성 검증 (마이너스 재고 탐지)."""
    date_str = request.args.get('date')
    if not date_str:
        return jsonify({'error': 'date 파라미터가 필요합니다.'}), 400

    try:
        from services.reconciliation_service import validate_stock_integrity
        result = validate_stock_integrity(get_db(), date_str)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': f'재고 정합성 검증 오류: {e}'}), 500
