"""
trade.py — 거래처/거래 관리 Blueprint.
거래처 CRUD, 수동 거래 등록, 거래명세서 PDF, 엑셀 일괄등록.
"""
import os
import io
from datetime import datetime

import pandas as pd
from flask import (
    Blueprint, render_template, request, current_app,
    flash, redirect, url_for, send_file, abort,
)
from flask_login import login_required, current_user

from auth import role_required, _log_action

trade_bp = Blueprint('trade', __name__, url_prefix='/trade')


# ── 거래처 관리 ──

@trade_bp.route('/')
@role_required('admin', 'manager', 'sales', 'general')
def index():
    """거래처 목록 + 최근 거래"""
    db = current_app.db
    partners = []
    trade_list = []
    try:
        partners = db.query_partners()
    except Exception as e:
        flash(f'거래처 조회 중 오류: {e}', 'danger')

    try:
        trade_list = db.query_manual_trades()
    except Exception as e:
        flash(f'거래 조회 중 오류: {e}', 'danger')

    my_biz_list = []
    try:
        my_biz_list = db.query_my_business()
    except Exception as e:
        flash(f'사업장 조회 중 오류: {e}', 'danger')

    return render_template('trade/index.html',
                           partners=partners, trades=trade_list,
                           my_businesses=my_biz_list)


# ── 본사 사업장 관리 ──

@trade_bp.route('/business/add', methods=['POST'])
@role_required('admin', 'manager', 'sales', 'general')
def add_business():
    """본사 사업장 등록"""
    biz_name = request.form.get('business_name', '').strip()
    if not biz_name:
        flash('상호를 입력하세요.', 'danger')
        return redirect(url_for('trade.index'))

    payload = {
        'business_name': biz_name,
        'business_number': request.form.get('business_number', '').strip() or None,
        'representative': request.form.get('representative', '').strip() or None,
        'address': request.form.get('address', '').strip() or None,
        'contact': request.form.get('contact', '').strip() or None,
        'fax': request.form.get('fax', '').strip() or None,
        'email': request.form.get('email', '').strip() or None,
        'is_default': False,
    }

    try:
        current_app.db.upsert_my_business(payload)
        _log_action('add_business', target=biz_name)
        flash(f'사업장 "{biz_name}" 등록 완료', 'success')
    except Exception as e:
        flash(f'사업장 등록 중 오류: {e}', 'danger')

    return redirect(url_for('trade.index'))


@trade_bp.route('/business/default/<int:biz_id>', methods=['POST'])
@role_required('admin', 'manager', 'sales', 'general')
def set_default_business(biz_id):
    """기본 사업장 지정"""
    try:
        current_app.db.set_default_business(biz_id)
        flash('기본 사업장이 변경되었습니다.', 'success')
    except Exception as e:
        flash(f'기본 사업장 변경 중 오류: {e}', 'danger')

    return redirect(url_for('trade.index'))


@trade_bp.route('/business/delete/<int:biz_id>', methods=['POST'])
@role_required('admin', 'manager', 'sales', 'general')
def delete_business(biz_id):
    """본사 사업장 삭제"""
    try:
        current_app.db.delete_my_business(biz_id)
        _log_action('delete_business', target=str(biz_id))
        flash('사업장 삭제 완료', 'success')
    except Exception as e:
        flash(f'사업장 삭제 중 오류: {e}', 'danger')

    return redirect(url_for('trade.index'))


@trade_bp.route('/add', methods=['POST'])
@role_required('admin', 'manager', 'sales', 'general')
def add_partner():
    """거래처 등록"""
    partner_name = request.form.get('name', '').strip()
    if not partner_name:
        flash('거래처명을 입력하세요.', 'danger')
        return redirect(url_for('trade.index'))

    payload = {
        'partner_name': partner_name,
        'business_number': request.form.get('business_number', '').strip() or None,
        'representative': request.form.get('contact_name', '').strip() or None,
        'address': request.form.get('address', '').strip() or None,
        'type': request.form.get('type', '').strip() or None,
        'business_item': request.form.get('business_item', '').strip() or None,
        'phone': request.form.get('contact_phone', '').strip() or None,
        'fax': request.form.get('fax', '').strip() or None,
        'email': request.form.get('email', '').strip() or None,
        'notes': request.form.get('notes', '').strip() or None,
    }

    try:
        current_app.db.insert_partner(payload)
        _log_action('add_partner', target=partner_name)
        flash(f'거래처 "{partner_name}" 등록 완료', 'success')
    except Exception as e:
        flash(f'거래처 등록 중 오류: {e}', 'danger')

    return redirect(url_for('trade.index'))


@trade_bp.route('/upload-partners', methods=['POST'])
@role_required('admin', 'manager', 'sales', 'general')
def upload_partners():
    """거래처 엑셀 일괄 등록"""
    f = request.files.get('file')
    if not f or not f.filename:
        flash('파일을 선택하세요.', 'danger')
        return redirect(url_for('trade.index'))

    if not f.filename.lower().endswith(('.xlsx', '.xls')):
        flash('엑셀 파일(.xlsx/.xls)만 업로드 가능합니다.', 'danger')
        return redirect(url_for('trade.index'))

    try:
        df = pd.read_excel(f, dtype=str)
        df = df.fillna('')

        # 컬럼 매핑 (유연하게 처리)
        col_map = {
            '업체명': 'partner_name', '거래처명': 'partner_name', '상호': 'partner_name',
            '소재지': 'address', '주소': 'address',
            '담당자': 'representative', '대표자': 'representative',
            '연락처': 'phone', '전화번호': 'phone', '전화': 'phone', 'TEL': 'phone',
            '팩스번호': 'fax', '팩스': 'fax', 'FAX': 'fax',
            '이메일': 'email', 'E-mail': 'email', 'EMAIL': 'email',
            '사업자등록번호': 'business_number', '사업자번호': 'business_number',
            '유형': 'type', '업종': 'business_item', '비고': 'notes',
        }

        renamed = {}
        for col in df.columns:
            clean = col.strip()
            if clean in col_map:
                renamed[col] = col_map[clean]
        df = df.rename(columns=renamed)

        if 'partner_name' not in df.columns:
            flash('엑셀에 "업체명" 또는 "거래처명" 컬럼이 필요합니다.', 'danger')
            return redirect(url_for('trade.index'))

        # 빈 업체명 제거
        df = df[df['partner_name'].str.strip() != '']

        if df.empty:
            flash('등록할 거래처가 없습니다. 업체명을 확인하세요.', 'warning')
            return redirect(url_for('trade.index'))

        # DB 필드만 추출
        valid_fields = ['partner_name', 'address', 'representative', 'phone',
                        'fax', 'email', 'business_number', 'type', 'business_item', 'notes']
        payload_list = []
        for _, row in df.iterrows():
            rec = {}
            for field in valid_fields:
                val = str(row.get(field, '')).strip()
                if val:
                    rec[field] = val
            if rec.get('partner_name'):
                payload_list.append(rec)

        current_app.db.insert_partners_batch(payload_list)
        _log_action('upload_partners', target=f'{len(payload_list)}건 일괄등록')
        flash(f'거래처 {len(payload_list)}건 일괄 등록 완료!', 'success')

    except Exception as e:
        flash(f'엑셀 처리 중 오류: {e}', 'danger')

    return redirect(url_for('trade.index'))


@trade_bp.route('/download-partner-template')
@role_required('admin', 'manager', 'sales', 'general')
def download_partner_template():
    """거래처 일괄등록 엑셀 양식 다운로드"""
    df = pd.DataFrame(columns=['업체명', '소재지', '담당자', '연락처', '팩스번호', '이메일', '사업자등록번호', '유형', '비고'])
    # 샘플 데이터 추가
    df.loc[0] = ['(주)테스트업체', '서울시 강남구', '홍길동', '02-1234-5678', '02-1234-5679', 'test@example.com', '123-45-67890', '매입', '']

    buf = io.BytesIO()
    df.to_excel(buf, index=False, engine='openpyxl')
    buf.seek(0)

    return send_file(
        buf,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name='거래처_일괄등록_양식.xlsx',
    )


@trade_bp.route('/delete/<int:partner_id>', methods=['POST'])
@role_required('admin', 'manager', 'sales', 'general')
def delete_partner(partner_id):
    """거래처 삭제"""
    try:
        current_app.db.delete_partner(partner_id)
        _log_action('delete_partner', target=str(partner_id))
        flash('거래처 삭제 완료', 'success')
    except Exception as e:
        flash(f'거래처 삭제 중 오류: {e}', 'danger')

    return redirect(url_for('trade.index'))


# ── 거래 관리 ──

@trade_bp.route('/trades')
@role_required('admin', 'manager', 'sales', 'general')
def trades():
    """거래 목록"""
    db = current_app.db

    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    partner_name = request.args.get('partner_name', '전체')

    trade_list = []
    partners = []
    try:
        partners = db.query_partners()
        trade_list = db.query_manual_trades(
            date_from=date_from or None,
            date_to=date_to or None,
            partner_name=partner_name if partner_name != '전체' else None,
        )
    except Exception as e:
        flash(f'거래 조회 중 오류: {e}', 'danger')

    return render_template('trade/index.html',
                           trades=trade_list, partners=partners,
                           date_from=date_from, date_to=date_to,
                           partner_name=partner_name)


@trade_bp.route('/trades/add', methods=['POST'])
@role_required('admin', 'manager', 'sales', 'general')
def add_trade():
    """수동 거래 등록"""
    partner_id = request.form.get('partner_id', '').strip()
    product_name = request.form.get('product_name', '').strip()
    trade_date = request.form.get('date', datetime.now().strftime('%Y-%m-%d'))

    # partner_id로 거래처명 조회
    partner_name = ''
    if partner_id:
        try:
            partners = current_app.db.query_partners()
            partner = next(
                (p for p in partners if str(p.get('id')) == partner_id), None
            )
            if partner:
                partner_name = partner.get('partner_name', '')
        except Exception:
            pass

    if not partner_name or not product_name:
        flash('거래처명과 품목명을 입력하세요.', 'danger')
        return redirect(url_for('trade.trades'))

    try:
        qty = int(request.form.get('qty', 0))
        unit_price = int(request.form.get('unit_price', 0))
    except (ValueError, TypeError):
        flash('수량과 단가는 숫자로 입력하세요.', 'danger')
        return redirect(url_for('trade.trades'))

    payload = {
        'partner_name': partner_name,
        'product_name': product_name,
        'trade_date': trade_date,
        'trade_type': request.form.get('trade_type', '판매'),
        'qty': qty,
        'unit': request.form.get('unit', '개').strip(),
        'unit_price': unit_price,
        'amount': qty * unit_price,
        'memo': request.form.get('memo', '').strip() or None,
        'registered_by': current_user.username,
    }

    try:
        current_app.db.insert_manual_trade(payload)
        _log_action('add_trade', target=f'{partner_name}/{product_name}')
        flash(f'거래 등록 완료: {partner_name} — {product_name}', 'success')
    except Exception as e:
        flash(f'거래 등록 중 오류: {e}', 'danger')

    return redirect(url_for('trade.trades'))


# ── 거래명세서 PDF ──

@trade_bp.route('/invoice/<int:trade_id>')
@role_required('admin', 'manager', 'sales', 'general')
def invoice(trade_id):
    """거래명세서 PDF 생성/다운로드"""
    db = current_app.db

    try:
        # 거래 정보 조회
        trades = db.query_manual_trades()
        trade = next((t for t in trades if t.get('id') == trade_id), None)
        if not trade:
            abort(404)

        # 거래처 정보 조회
        partners = db.query_partners()
        partner = next(
            (p for p in partners if p.get('partner_name') == trade.get('partner_name')),
            None
        )

        # 내 사업장 정보
        my_biz_list = db.query_my_business()
        my_biz = my_biz_list[0] if my_biz_list else {}

        # PDF 생성
        from reports.invoice_report import generate_invoice_pdf
        output_dir = current_app.config['OUTPUT_FOLDER']
        os.makedirs(output_dir, exist_ok=True)

        fname = f"거래명세서_{trade.get('partner_name', '')}_{trade.get('trade_date', '')}.pdf"
        pdf_path = os.path.join(output_dir, fname)

        generate_invoice_pdf(pdf_path, my_biz, partner or {}, [trade],
                             trade_date=trade.get('trade_date', ''))

        return send_file(
            pdf_path,
            mimetype='application/pdf',
            as_attachment=True,
            download_name=fname,
        )
    except Exception as e:
        flash(f'거래명세서 생성 중 오류: {e}', 'danger')
        return redirect(url_for('trade.trades'))
