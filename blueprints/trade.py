"""
trade.py — 거래처/거래 관리 Blueprint.
거래처 CRUD, 수동 거래 등록, 거래명세서 PDF, 엑셀 일괄등록, 발주서.
"""
import os
import io
import json
from datetime import datetime
from services.tz_utils import today_kst

import pandas as pd
from flask import (
    Blueprint, render_template, request, current_app,
    flash, redirect, url_for, send_file, abort, jsonify,
)
from flask_login import login_required, current_user

from auth import role_required, _log_action

trade_bp = Blueprint('trade', __name__, url_prefix='/trade')


# ── 거래처 관리 ──

@trade_bp.route('/')
@role_required('admin', 'ceo', 'manager', 'sales', 'general')
def index():
    """거래처 목록 + 본사 사업장 관리"""
    db = current_app.db
    partners = []

    try:
        partners = db.query_partners()
    except Exception as e:
        flash(f'거래처 조회 중 오류: {e}', 'danger')

    my_biz_list = []
    try:
        my_biz_list = db.query_my_business()
    except Exception as e:
        flash(f'사업장 조회 중 오류: {e}', 'danger')

    return render_template('trade/index.html',
                           partners=partners,
                           my_businesses=my_biz_list)


# ── 본사 사업장 관리 ──

@trade_bp.route('/business/add', methods=['POST'])
@role_required('admin', 'ceo', 'manager', 'sales', 'general')
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
@role_required('admin', 'ceo', 'manager', 'sales', 'general')
def set_default_business(biz_id):
    """기본 사업장 지정"""
    try:
        current_app.db.set_default_business(biz_id)
        flash('기본 사업장이 변경되었습니다.', 'success')
    except Exception as e:
        flash(f'기본 사업장 변경 중 오류: {e}', 'danger')

    return redirect(url_for('trade.index'))


@trade_bp.route('/business/delete/<int:biz_id>', methods=['POST'])
@role_required('admin')
def delete_business(biz_id):
    """본사 사업장 삭제 (admin 전용)"""
    try:
        old_record = None
        try:
            res = current_app.db.client.table("my_business").select("*").eq("id", biz_id).execute()
            old_record = res.data[0] if res.data else None
        except Exception:
            pass
        current_app.db.delete_my_business(biz_id)
        _log_action('delete_business', target=str(biz_id), old_value=old_record)
        flash('사업장 삭제 완료', 'success')
    except Exception as e:
        flash(f'사업장 삭제 중 오류: {e}', 'danger')

    return redirect(url_for('trade.index'))


@trade_bp.route('/add', methods=['POST'])
@role_required('admin', 'ceo', 'manager', 'sales', 'general')
def add_partner():
    """거래처 등록"""
    partner_name = request.form.get('name', '').strip()
    if not partner_name:
        flash('거래처명을 입력하세요.', 'danger')
        return redirect(url_for('trade.index'))

    payload = {
        'partner_name': partner_name,
        'business_number': request.form.get('business_number', '').strip() or None,
        'representative': request.form.get('representative', '').strip() or None,
        'contact_person': request.form.get('contact_person', '').strip() or None,
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
@role_required('admin', 'ceo', 'manager', 'sales', 'general')
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
            '대표자': 'representative', '대표자명': 'representative',
            '담당자': 'contact_person', '담당자명': 'contact_person',
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
        valid_fields = ['partner_name', 'address', 'representative', 'contact_person',
                        'phone', 'fax', 'email', 'business_number', 'type', 'business_item', 'notes']
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
@role_required('admin', 'ceo', 'manager', 'sales', 'general')
def download_partner_template():
    """거래처 일괄등록 엑셀 양식 다운로드"""
    df = pd.DataFrame(columns=['업체명', '소재지', '대표자', '담당자', '연락처', '팩스번호', '이메일', '사업자등록번호', '유형', '비고'])
    # 샘플 데이터 추가
    df.loc[0] = ['(주)테스트업체', '서울시 강남구', '김대표', '홍길동', '02-1234-5678', '02-1234-5679', 'test@example.com', '123-45-67890', '매입', '']

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
@role_required('admin')
def delete_partner(partner_id):
    """거래처 삭제 (admin 전용)"""
    try:
        old_record = None
        try:
            res = current_app.db.client.table("business_partners").select("*").eq("id", partner_id).execute()
            old_record = res.data[0] if res.data else None
        except Exception:
            pass
        current_app.db.delete_partner(partner_id)
        _log_action('delete_partner', target=str(partner_id), old_value=old_record)
        flash('거래처 삭제 완료', 'success')
    except Exception as e:
        flash(f'거래처 삭제 중 오류: {e}', 'danger')

    return redirect(url_for('trade.index'))


@trade_bp.route('/api/partner/<int:partner_id>', methods=['PUT'])
@role_required('admin', 'ceo', 'manager', 'sales', 'general')
def api_update_partner(partner_id):
    """거래처 정보 수정 API"""
    data = request.get_json()
    if not data:
        return jsonify({'error': '데이터가 없습니다.'}), 400

    # 업데이트 가능 필드
    allowed = ['partner_name', 'business_number', 'representative', 'contact_person',
               'address', 'type', 'business_item', 'phone', 'fax', 'email', 'notes']
    payload = {}
    for key in allowed:
        if key in data:
            val = (data[key] or '').strip() if data[key] else None
            payload[key] = val

    if not payload:
        return jsonify({'error': '수정할 데이터가 없습니다.'}), 400

    try:
        # 수정 전 원본 조회 (되돌리기용)
        old_list = current_app.db.client.table("business_partners") \
            .select("*").eq("id", partner_id).limit(1).execute()
        old_record = old_list.data[0] if old_list.data else None
        current_app.db.update_partner(partner_id, payload)
        _log_action('update_partner', target=str(partner_id),
                     old_value=old_record, new_value=payload)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@trade_bp.route('/api/partners')
@role_required('admin', 'ceo', 'manager', 'sales', 'general')
def api_partners():
    """거래처 목록 JSON"""
    try:
        partners = current_app.db.query_partners()
        return jsonify({'success': True, 'partners': partners})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ── 거래 관리 ──

@trade_bp.route('/trades')
@role_required('admin', 'ceo', 'manager', 'sales', 'general')
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
@role_required('admin', 'ceo', 'manager', 'sales', 'general')
def add_trade():
    """수동 거래 등록"""
    partner_id = request.form.get('partner_id', '').strip()
    product_name = request.form.get('product_name', '').strip()
    trade_date = request.form.get('date', today_kst())

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


@trade_bp.route('/trades/delete/<int:trade_id>', methods=['POST'])
@role_required('admin')
def delete_trade(trade_id):
    """거래 삭제 (manual_trades + daily_revenue + stock_ledger 연동 삭제)"""
    db = current_app.db
    try:
        # 삭제 전 거래 정보 조회
        trade = db.query_manual_trade_by_id(trade_id)

        # manual_trades 삭제
        db.delete_manual_trade(trade_id)

        if trade:
            # ── 재고 복원 (stock_ledger SALES_OUT 삭제) ──
            try:
                memo = trade.get('memo', '')
                location = ''
                if '(' in memo and ')' in memo:
                    location = memo.split('(')[1].split(')')[0].strip()

                if location and trade.get('product_name') and trade.get('qty'):
                    restored = db.delete_stock_ledger_sales_out(
                        date_str=trade.get('trade_date', ''),
                        product_name=trade.get('product_name', ''),
                        location=location,
                        qty=int(trade.get('qty', 0)),
                    )
                    if restored > 0:
                        current_app.logger.info(
                            f'재고 복원: {trade["product_name"]} x{trade["qty"]} '
                            f'({location}) — {restored}건 SALES_OUT 삭제'
                        )
            except Exception as stock_err:
                current_app.logger.warning(f'재고 복원 실패: {stock_err}')

            # ── daily_revenue 연동 삭제 ──
            try:
                db.delete_revenue_specific(
                    revenue_date=trade.get('trade_date', ''),
                    product_name=trade.get('product_name', ''),
                    category='거래처매출',
                )
            except Exception as rev_err:
                current_app.logger.warning(f'매출 연동 삭제 실패: {rev_err}')

        _log_action('delete_trade', target=str(trade_id), old_value=trade)
        flash('거래 삭제 완료 (재고 복원 + 매출 데이터 함께 삭제됨)', 'success')
    except Exception as e:
        flash(f'거래 삭제 중 오류: {e}', 'danger')

    return redirect(url_for('outbound.index'))


@trade_bp.route('/api/products')
@role_required('admin', 'ceo', 'manager', 'sales', 'general')
def api_products():
    """재고 품목 목록 JSON 반환 (자동완성용, 전체 창고 합산)"""
    try:
        from services.stock_service import query_stock_snapshot
        today = today_kst()
        snapshot = query_stock_snapshot(current_app.db, today)
        # 품목별 합산 (여러 창고 동일 품목 합산)
        agg = {}
        for row in snapshot:
            name = row.get('product_name', '')
            if not name:
                continue
            if name not in agg:
                agg[name] = {'name': name, 'qty': 0, 'unit': row.get('unit', '개')}
            agg[name]['qty'] += row.get('qty', 0)
        products = sorted(agg.values(), key=lambda x: x['name'])
        return jsonify(products)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── 거래명세서 PDF ──

@trade_bp.route('/invoice/<int:trade_id>')
@role_required('admin', 'ceo', 'manager', 'sales', 'general')
def invoice(trade_id):
    """거래명세서 PDF 생성/다운로드 (단일 거래 기준)"""
    db = current_app.db

    try:
        trades_data = db.query_manual_trades()
        trade = next((t for t in trades_data if t.get('id') == trade_id), None)
        if not trade:
            abort(404)

        partners = db.query_partners()
        partner = next(
            (p for p in partners if p.get('partner_name') == trade.get('partner_name')),
            None
        )

        my_biz_list = db.query_my_business()
        my_biz = next((b for b in my_biz_list if b.get('is_default')), my_biz_list[0] if my_biz_list else {})

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
        return redirect(url_for('trade.index'))


@trade_bp.route('/invoice-batch')
@role_required('admin', 'ceo', 'manager', 'sales', 'general')
def invoice_batch():
    """거래명세서 PDF — 같은 거래처+날짜 묶어서 생성"""
    db = current_app.db
    p_name = request.args.get('partner_name', '')
    t_date = request.args.get('trade_date', '')

    if not p_name or not t_date:
        flash('거래처명과 거래일을 지정하세요.', 'danger')
        return redirect(url_for('trade.index'))

    try:
        trade_list = db.query_manual_trades(
            date_from=t_date, date_to=t_date, partner_name=p_name
        )
        if not trade_list:
            flash('해당 거래내역이 없습니다.', 'warning')
            return redirect(url_for('trade.index'))

        partners = db.query_partners()
        partner = next(
            (p for p in partners if p.get('partner_name') == p_name), None
        )

        my_biz_list = db.query_my_business()
        my_biz = next((b for b in my_biz_list if b.get('is_default')), my_biz_list[0] if my_biz_list else {})

        from reports.invoice_report import generate_invoice_pdf
        output_dir = current_app.config['OUTPUT_FOLDER']
        os.makedirs(output_dir, exist_ok=True)

        fname = f"거래명세서_{p_name}_{t_date}.pdf"
        pdf_path = os.path.join(output_dir, fname)

        generate_invoice_pdf(pdf_path, my_biz, partner or {}, trade_list,
                             trade_date=t_date)

        return send_file(
            pdf_path,
            mimetype='application/pdf',
            as_attachment=True,
            download_name=fname,
        )
    except Exception as e:
        flash(f'거래명세서 생성 중 오류: {e}', 'danger')
        return redirect(url_for('trade.index'))


# ── 발주서 관리 ──

@trade_bp.route('/purchase-order')
@role_required('admin', 'ceo', 'manager', 'sales', 'general')
def purchase_order():
    """발주서 작성 + 이력 조회 페이지"""
    db = current_app.db
    partners = []
    my_biz_list = []
    po_list = []

    # 검색 파라미터
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    partner_filter = request.args.get('partner_filter', '전체')

    try:
        partners = db.query_partners()
    except Exception as e:
        flash(f'거래처 조회 중 오류: {e}', 'danger')

    try:
        my_biz_list = db.query_my_business()
    except Exception as e:
        flash(f'사업장 조회 중 오류: {e}', 'danger')

    # 발주서 이력 조회
    try:
        po_list = db.query_purchase_orders(
            date_from=date_from or None,
            date_to=date_to or None,
            partner_name=partner_filter if partner_filter != '전체' else None,
        )
        # items가 JSON 문자열이면 파싱
        for po in po_list:
            if isinstance(po.get('items'), str):
                try:
                    po['items'] = json.loads(po['items'])
                except (json.JSONDecodeError, TypeError):
                    po['items'] = []
    except Exception as e:
        flash(f'발주서 이력 조회 중 오류: {e}', 'danger')

    return render_template('trade/purchase_order.html',
                           partners=partners,
                           my_businesses=my_biz_list,
                           po_list=po_list,
                           date_from=date_from,
                           date_to=date_to,
                           partner_filter=partner_filter)


@trade_bp.route('/purchase-order/generate', methods=['POST'])
@role_required('admin', 'ceo', 'manager', 'sales', 'general')
def generate_purchase_order():
    """발주서 PDF 생성/다운로드"""
    import json
    db = current_app.db

    try:
        # 발주처 (본사 사업장)
        my_biz_id = request.form.get('my_biz_id', '')
        my_biz_list = db.query_my_business()
        my_biz = {}
        if my_biz_id:
            my_biz = next((b for b in my_biz_list if str(b.get('id')) == my_biz_id), {})
        if not my_biz:
            my_biz = next((b for b in my_biz_list if b.get('is_default')),
                          my_biz_list[0] if my_biz_list else {})

        # 공급업체 (거래처)
        partner_id = request.form.get('partner_id', '')
        partner = {}
        if partner_id:
            partners = db.query_partners()
            partner = next(
                (p for p in partners if str(p.get('id')) == partner_id), {}
            )

        # 발주내역
        items_json = request.form.get('items', '[]')
        items = json.loads(items_json)
        if not items:
            flash('발주내역을 입력하세요.', 'danger')
            return redirect(url_for('trade.purchase_order'))

        # 발주 정보
        delivery_note = request.form.get('delivery_note', '').strip()
        caution_text = request.form.get('caution_text', '').strip()
        order_date = request.form.get('order_date', today_kst())
        request_date = request.form.get('request_date', '').strip()
        order_manager = request.form.get('order_manager', '').strip()
        invoice_manager = request.form.get('invoice_manager', '').strip()
        manager_contact = request.form.get('manager_contact', '').strip()

        # PDF 생성
        from reports.purchase_order_report import generate_purchase_order_pdf
        output_dir = current_app.config['OUTPUT_FOLDER']
        os.makedirs(output_dir, exist_ok=True)

        supplier_name = partner.get('partner_name', '공급업체')
        fname = f"발주서_{supplier_name}_{order_date}.pdf"
        pdf_path = os.path.join(output_dir, fname)

        generate_purchase_order_pdf(
            path=pdf_path,
            my_biz=my_biz,
            supplier=partner,
            items=items,
            order_date=order_date,
            request_date=request_date,
            delivery_note=delivery_note,
            caution_text=caution_text,
            order_manager=order_manager,
            invoice_manager=invoice_manager,
            manager_contact=manager_contact,
        )

        # DB에 발주서 이력 저장
        try:
            po_payload = {
                'order_date': order_date,
                'partner_id': int(partner_id) if partner_id else None,
                'partner_name': partner.get('partner_name', ''),
                'my_biz_name': my_biz.get('business_name', ''),
                'request_date': request_date or None,
                'delivery_note': delivery_note or None,
                'order_manager': order_manager or None,
                'invoice_manager': invoice_manager or None,
                'manager_contact': manager_contact or None,
                'caution_text': caution_text or None,
                'items': items,
                'item_count': len(items),
                'registered_by': current_user.username,
            }
            db.insert_purchase_order(po_payload)
        except Exception as save_err:
            current_app.logger.warning(f'발주서 이력 저장 실패: {save_err}')

        _log_action('generate_purchase_order',
                     target=supplier_name,
                     detail=f'{len(items)}건 품목, 발주일={order_date}')

        return send_file(
            pdf_path,
            mimetype='application/pdf',
            as_attachment=True,
            download_name=fname,
        )
    except Exception as e:
        flash(f'발주서 생성 중 오류: {e}', 'danger')
        return redirect(url_for('trade.purchase_order'))


@trade_bp.route('/purchase-order/delete/<int:po_id>', methods=['POST'])
@role_required('admin')
def delete_purchase_order(po_id):
    """발주서 이력 삭제 (admin 전용)"""
    try:
        old_record = current_app.db.query_purchase_order_by_id(po_id)
        current_app.db.delete_purchase_order(po_id)
        _log_action('delete_purchase_order', target=str(po_id), old_value=old_record)
        flash('발주서 이력이 삭제되었습니다.', 'success')
    except Exception as e:
        flash(f'발주서 삭제 중 오류: {e}', 'danger')
    return redirect(url_for('trade.purchase_order'))


@trade_bp.route('/purchase-order/redownload/<int:po_id>')
@role_required('admin', 'ceo', 'manager', 'sales', 'general')
def redownload_purchase_order(po_id):
    """발주서 PDF 재다운로드 (저장된 이력으로 PDF 재생성)"""
    db = current_app.db

    try:
        po = db.query_purchase_order_by_id(po_id)
        if not po:
            flash('해당 발주서를 찾을 수 없습니다.', 'warning')
            return redirect(url_for('trade.purchase_order'))

        # 거래처 정보 조회
        partner = {}
        if po.get('partner_id'):
            partners = db.query_partners()
            partner = next(
                (p for p in partners if p.get('id') == po['partner_id']), {}
            )
        if not partner and po.get('partner_name'):
            partners = db.query_partners()
            partner = next(
                (p for p in partners if p.get('partner_name') == po['partner_name']), {}
            )

        # 본사 사업장 조회
        my_biz_list = db.query_my_business()
        my_biz = {}
        if po.get('my_biz_name'):
            my_biz = next(
                (b for b in my_biz_list if b.get('business_name') == po['my_biz_name']), {}
            )
        if not my_biz:
            my_biz = next((b for b in my_biz_list if b.get('is_default')),
                          my_biz_list[0] if my_biz_list else {})

        # items 파싱
        items = po.get('items', [])
        if isinstance(items, str):
            import json as json_mod
            items = json_mod.loads(items)

        from reports.purchase_order_report import generate_purchase_order_pdf
        output_dir = current_app.config['OUTPUT_FOLDER']
        os.makedirs(output_dir, exist_ok=True)

        supplier_name = po.get('partner_name', '공급업체')
        order_date = po.get('order_date', '')
        fname = f"발주서_{supplier_name}_{order_date}.pdf"
        pdf_path = os.path.join(output_dir, fname)

        generate_purchase_order_pdf(
            path=pdf_path,
            my_biz=my_biz,
            supplier=partner,
            items=items,
            order_date=order_date,
            request_date=po.get('request_date', ''),
            delivery_note=po.get('delivery_note', ''),
            caution_text=po.get('caution_text', ''),
            order_manager=po.get('order_manager', ''),
            invoice_manager=po.get('invoice_manager', ''),
            manager_contact=po.get('manager_contact', ''),
        )

        return send_file(
            pdf_path,
            mimetype='application/pdf',
            as_attachment=True,
            download_name=fname,
        )
    except Exception as e:
        flash(f'발주서 재다운로드 중 오류: {e}', 'danger')
        return redirect(url_for('trade.purchase_order'))
