"""tax_invoice.py -- 세금계산서 관리 Blueprint.
홈택스 엑셀 업로드 방식 (매출/매입).
팝빌 연동 코드는 주석 처리 → 추후 팝빌 사용 시 복원.
"""
import os
import json
from io import BytesIO
from flask import (
    Blueprint, render_template, request, current_app,
    flash, redirect, url_for, jsonify, send_file,
)
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename
from auth import role_required, _log_action
from services.tz_utils import today_kst, days_ago_kst

tax_invoice_bp = Blueprint('tax_invoice', __name__, url_prefix='/tax-invoice')

ALLOWED_EXT = {'xlsx', 'xls'}


def _allowed(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXT


# ══════════════════════════════════════════════
#  목록 / 상세 / 취소  (기존 유지)
# ══════════════════════════════════════════════

@tax_invoice_bp.route('/')
@role_required('admin', 'ceo', 'manager', 'general')
def index():
    """세금계산서 목록"""
    db = current_app.db
    direction = request.args.get('direction', '전체')
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')

    invoices = db.query_tax_invoices(
        direction={'매출': 'sales', '매입': 'purchase'}.get(direction),
        date_from=date_from or None,
        date_to=date_to or None,
    )

    # 취소건 제외한 합계
    sales_total = sum(i.get('total_amount', 0) for i in invoices
                      if i.get('direction') == 'sales' and i.get('status') != 'cancelled')
    purchase_total = sum(i.get('total_amount', 0) for i in invoices
                         if i.get('direction') == 'purchase' and i.get('status') != 'cancelled')

    return render_template('tax_invoice/index.html',
                           invoices=invoices,
                           sales_total=sales_total,
                           purchase_total=purchase_total,
                           direction=direction,
                           date_from=date_from, date_to=date_to)


@tax_invoice_bp.route('/detail/<int:invoice_id>')
@role_required('admin', 'ceo', 'manager', 'general')
def detail(invoice_id):
    """세금계산서 상세"""
    invoice = current_app.db.query_tax_invoice_by_id(invoice_id)
    if not invoice:
        flash('세금계산서를 찾을 수 없습니다.', 'danger')
        return redirect(url_for('tax_invoice.index'))
    return render_template('tax_invoice/detail.html', invoice=invoice)


@tax_invoice_bp.route('/cancel/<int:invoice_id>', methods=['POST'])
@role_required('admin', 'manager')
def cancel(invoice_id):
    """세금계산서 취소"""
    db = current_app.db
    invoice = db.query_tax_invoice_by_id(invoice_id)
    if not invoice:
        flash('세금계산서를 찾을 수 없습니다.', 'danger')
        return redirect(url_for('tax_invoice.index'))

    try:
        db.update_tax_invoice(invoice_id, {'status': 'cancelled'})
        _log_action('cancel_tax_invoice', detail=f'ID={invoice_id}')
        flash('세금계산서가 취소되었습니다.', 'success')
    except Exception as e:
        flash(f'취소 오류: {e}', 'danger')

    return redirect(url_for('tax_invoice.index'))


# ══════════════════════════════════════════════
#  홈택스 엑셀 업로드 (매출 / 매입)
# ══════════════════════════════════════════════

@tax_invoice_bp.route('/upload', methods=['POST'])
@role_required('admin', 'manager', 'general')
def upload():
    """홈택스 세금계산서 엑셀 업로드 (매출 또는 매입)"""
    import pandas as pd

    db = current_app.db
    direction = request.form.get('direction', 'sales')  # sales / purchase
    file = request.files.get('file')

    if not file or not _allowed(file.filename):
        flash('엑셀 파일(.xlsx/.xls)을 선택하세요.', 'danger')
        return redirect(url_for('tax_invoice.index'))

    upload_dir = current_app.config.get('UPLOAD_FOLDER', 'uploads')
    os.makedirs(upload_dir, exist_ok=True)
    filename = secure_filename(file.filename)
    filepath = os.path.join(upload_dir, filename)

    try:
        file.save(filepath)
        df = pd.read_excel(filepath, dtype=str).fillna('')

        from services.tax_invoice_service import parse_hometax_excel
        invoices_data = parse_hometax_excel(df, direction)

        new_count = 0
        skip_count = 0
        for inv in invoices_data:
            # 중복 체크 (승인번호 기준)
            nts_num = inv.get('invoice_number', '')
            existing = db.check_tax_invoice_exists(
                invoice_number=nts_num if nts_num else None)
            if existing:
                skip_count += 1
                continue

            db.insert_tax_invoice(inv)
            new_count += 1

        dir_label = '매출' if direction == 'sales' else '매입'
        flash(f'{dir_label} 세금계산서 업로드 완료: 신규 {new_count}건, 중복 스킵 {skip_count}건', 'success')
        _log_action('upload_tax_invoice',
                     detail=f'{dir_label} 엑셀업로드: 신규 {new_count}건, 스킵 {skip_count}건')

    except Exception as e:
        flash(f'엑셀 업로드 오류: {e}', 'danger')
    finally:
        if os.path.exists(filepath):
            os.remove(filepath)

    return redirect(url_for('tax_invoice.index'))


# ══════════════════════════════════════════════
#  엑셀 다운로드 (현재 조회된 세금계산서)
# ══════════════════════════════════════════════

@tax_invoice_bp.route('/download')
@role_required('admin', 'ceo', 'manager', 'general')
def download():
    """세금계산서 목록 엑셀 다운로드"""
    import pandas as pd

    db = current_app.db
    direction = request.args.get('direction', '')
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')

    invoices = db.query_tax_invoices(
        direction={'매출': 'sales', '매입': 'purchase'}.get(direction),
        date_from=date_from or None,
        date_to=date_to or None,
    )

    rows = []
    for inv in invoices:
        rows.append({
            '구분': '매출' if inv.get('direction') == 'sales' else '매입',
            '작성일자': inv.get('write_date', ''),
            '발급일자': inv.get('issue_date', ''),
            '승인번호': inv.get('invoice_number', ''),
            '공급자 사업자번호': inv.get('supplier_corp_num', ''),
            '공급자 상호': inv.get('supplier_corp_name', ''),
            '공급자 대표자': inv.get('supplier_ceo_name', ''),
            '공급받는자 사업자번호': inv.get('buyer_corp_num', ''),
            '공급받는자 상호': inv.get('buyer_corp_name', ''),
            '공급받는자 대표자': inv.get('buyer_ceo_name', ''),
            '공급가액': inv.get('supply_cost_total', 0),
            '세액': inv.get('tax_total', 0),
            '합계금액': inv.get('total_amount', 0),
            '과세유형': inv.get('tax_type', '과세'),
            '상태': {'issued': '발행', 'draft': '임시', 'cancelled': '취소'}.get(
                inv.get('status', ''), inv.get('status', '')),
        })

    df = pd.DataFrame(rows)
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='세금계산서')
    buf.seek(0)

    filename = f'세금계산서_{today_kst()}.xlsx'
    _log_action('download_tax_invoice', detail=f'{len(rows)}건 엑셀 다운로드')

    return send_file(buf, as_attachment=True, download_name=filename,
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


# ══════════════════════════════════════════════
#  엑셀 샘플 양식 다운로드
# ══════════════════════════════════════════════

@tax_invoice_bp.route('/download-template')
@role_required('admin', 'manager', 'general')
def download_template():
    """홈택스 업로드용 엑셀 양식 다운로드 (참고용)"""
    import pandas as pd

    direction = request.args.get('direction', 'sales')
    columns = [
        '승인번호', '작성일자', '발급일자',
        '공급자 사업자번호', '공급자 상호', '공급자 대표자',
        '공급받는자 사업자번호', '공급받는자 상호', '공급받는자 대표자',
        '공급가액', '세액', '합계금액', '과세유형', '비고',
    ]

    # 샘플 1행
    sample = {
        '승인번호': '20260309-41000000-12345678',
        '작성일자': '2026-03-09',
        '발급일자': '2026-03-09',
        '공급자 사업자번호': '123-45-67890',
        '공급자 상호': '(주)공급사',
        '공급자 대표자': '홍길동',
        '공급받는자 사업자번호': '098-76-54321',
        '공급받는자 상호': '(주)구매사',
        '공급받는자 대표자': '김철수',
        '공급가액': '1000000',
        '세액': '100000',
        '합계금액': '1100000',
        '과세유형': '과세',
        '비고': '',
    }

    df = pd.DataFrame([sample], columns=columns)
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='세금계산서')
    buf.seek(0)

    dir_label = '매출' if direction == 'sales' else '매입'
    return send_file(buf, as_attachment=True,
                     download_name=f'세금계산서_{dir_label}_양식.xlsx',
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


# ══════════════════════════════════════════════
#  거래처 API (자동완성용 — 기존 유지)
# ══════════════════════════════════════════════

@tax_invoice_bp.route('/api/partners')
@login_required
def api_partners():
    """거래처 목록 JSON (자동완성용)"""
    partners = current_app.db.query_partners()
    return jsonify([{
        'id': p['id'],
        'name': p.get('partner_name', ''),
        'business_number': p.get('business_number', ''),
        'representative': p.get('representative', ''),
    } for p in partners])


# ══════════════════════════════════════════════
#  팝빌 관련 라우트 (보류 — 추후 복원용)
#  아래는 팝빌 비용 확인 후 사용할 때 주석 해제
# ══════════════════════════════════════════════

# @tax_invoice_bp.route('/issue', methods=['GET', 'POST'])
# @role_required('admin', 'manager', 'general')
# def issue():
#     """세금계산서 발행 (팝빌 연동)"""
#     ... (팝빌 사용 시 복원)

# @tax_invoice_bp.route('/sync', methods=['POST'])
# @role_required('admin', 'manager')
# def sync():
#     """팝빌에서 세금계산서 동기화 (매출+매입)"""
#     ... (팝빌 사용 시 복원)

# @tax_invoice_bp.route('/sync-sell', methods=['POST'])
# @tax_invoice_bp.route('/sync-buy', methods=['POST'])
# @tax_invoice_bp.route('/popbill-join', methods=['POST'])
# @tax_invoice_bp.route('/api/popbill-status')
# @tax_invoice_bp.route('/api/popbill-cert-url')
