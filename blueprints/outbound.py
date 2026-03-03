"""
outbound.py — 거래처주문처리 Blueprint.
단건 출고 (폼 기반), 일괄(batch) 출고 (엑셀), 거래명세서 PDF.
거래 이력 조회, 삭제, 거래명세서 출력.
"""
import json
import os
import uuid
from datetime import datetime
from services.tz_utils import today_kst

import pandas as pd
from flask import (
    Blueprint, render_template, request, current_app,
    flash, redirect, url_for, jsonify, session, send_file, abort,
)
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename

from auth import role_required, _log_action

outbound_bp = Blueprint('outbound', __name__, url_prefix='/outbound')

ALLOWED_EXT = {'xlsx', 'xls'}


def _allowed(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXT


@outbound_bp.route('/')
@role_required('admin', 'manager', 'sales', 'general')
def index():
    """거래처주문처리 폼 + 거래 이력 조회"""
    db = current_app.db
    locations = []
    partners = []
    my_businesses = []
    trade_list = []

    # 필터 파라미터
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    partner_filter = request.args.get('partner_filter', '전체')

    try:
        locations, _ = db.query_filter_options()
    except Exception:
        pass
    try:
        partners = db.query_partners()
    except Exception:
        pass
    try:
        my_businesses = db.query_my_business()
    except Exception:
        pass

    # 거래 이력 조회
    if date_from or date_to or (partner_filter and partner_filter != '전체'):
        try:
            trade_list = db.query_manual_trades(
                date_from=date_from or None,
                date_to=date_to or None,
                partner_name=partner_filter if partner_filter != '전체' else None,
            )
        except Exception as e:
            flash(f'거래 이력 조회 중 오류: {e}', 'danger')

    # 중복 제출 방지용 nonce 생성
    form_nonce = str(uuid.uuid4())
    session['outbound_nonce'] = form_nonce

    return render_template('outbound/index.html',
                           locations=locations, partners=partners,
                           my_businesses=my_businesses,
                           trades=trade_list,
                           date_from=date_from, date_to=date_to,
                           partner_filter=partner_filter,
                           form_nonce=form_nonce)


@outbound_bp.route('/api/products')
@role_required('admin', 'manager', 'sales', 'general')
def api_products():
    """창고별 재고 품목 목록 JSON 반환"""
    location = request.args.get('location', '')
    if not location:
        return jsonify([])

    try:
        from services.excel_io import build_stock_snapshot
        all_data = current_app.db.query_stock_by_location(location)
        snapshot = build_stock_snapshot(all_data)
        products = []
        for name, info in snapshot.items():
            if info['total'] > 0:
                products.append({
                    'name': name,
                    'qty': info['total'],
                    'unit': info.get('unit', '개'),
                })
        products.sort(key=lambda x: x['name'])
        return jsonify(products)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@outbound_bp.route('/single', methods=['POST'])
@role_required('admin', 'manager', 'sales', 'general')
def single():
    """단건 출고 — 폼 기반 (FIFO 재고차감 + 거래기록)"""

    # ── 중복 제출 방지 (idempotency token 검증) ──
    form_nonce = request.form.get('_form_nonce', '')
    saved_nonce = session.pop('outbound_nonce', None)

    if not form_nonce or form_nonce != saved_nonce:
        # nonce 불일치 → 이미 처리된 요청의 재전송
        # 결과 페이지가 있으면 그쪽으로 리다이렉트
        if session.get('outbound_result'):
            flash('이미 처리된 출고입니다. (중복 제출 방지)', 'warning')
            return redirect(url_for('outbound.result'))
        flash('이미 처리된 요청이거나 세션이 만료되었습니다. 다시 시도하세요.', 'warning')
        return redirect(url_for('outbound.index'))

    date_str = request.form.get('date', today_kst())
    location = request.form.get('location', '')
    partner_name = request.form.get('partner_name', '')
    my_biz_id = request.form.get('my_biz_id', '')

    if not location:
        flash('창고를 선택하세요.', 'danger')
        return redirect(url_for('outbound.index'))
    if not partner_name:
        flash('매출처를 선택하세요.', 'danger')
        return redirect(url_for('outbound.index'))

    # 동적 행 데이터 파싱
    items_json = request.form.get('items', '[]')
    try:
        items = json.loads(items_json)
    except (json.JSONDecodeError, TypeError):
        flash('품목 데이터가 올바르지 않습니다.', 'danger')
        return redirect(url_for('outbound.index'))

    # 부대비용 파싱
    extra_costs_json = request.form.get('extra_costs', '[]')
    try:
        extra_costs = json.loads(extra_costs_json) if extra_costs_json else []
    except (json.JSONDecodeError, TypeError):
        extra_costs = []

    if not items:
        flash('출고할 품목을 추가하세요.', 'danger')
        return redirect(url_for('outbound.index'))

    # 유효성 검증
    for item in items:
        if not item.get('product_name') or not item.get('qty'):
            flash('품목명과 수량을 모두 입력하세요.', 'danger')
            return redirect(url_for('outbound.index'))

    try:
        from services.outbound_service import process_single_outbound
        db = current_app.db

        # ── DB 중복 체크: 같은 날짜+거래처+품목+수량 이미 존재 여부 ──
        try:
            existing = db.query_manual_trades(
                date_from=date_str, date_to=date_str,
                partner_name=partner_name,
            )
            if existing:
                # 현재 요청의 품목 시그니처 생성
                new_sig = set()
                for item in items:
                    pname = str(item['product_name']).strip()
                    qty = abs(int(item['qty']))
                    new_sig.add(f"{pname}|{qty}")

                # 기존 거래의 시그니처
                old_sig = set()
                for t in existing:
                    old_sig.add(f"{t.get('product_name', '')}|{t.get('qty', 0)}")

                # 모든 품목이 이미 존재하면 중복으로 판단
                if new_sig and new_sig.issubset(old_sig):
                    flash('⚠️ 동일한 거래가 이미 등록되어 있습니다. 중복 입력을 방지합니다.', 'warning')
                    return redirect(url_for('outbound.index'))
        except Exception as dup_err:
            current_app.logger.warning(f'중복 체크 실패 (계속 진행): {dup_err}')

        result = process_single_outbound(db, date_str, location, items)

        if not result['success']:
            for s in result.get('shortage', []):
                flash(f'재고 부족: {s}', 'danger')
            return redirect(url_for('outbound.index'))

        # 거래기록 (manual_trades) 삽입 + 매출 데이터 수집
        revenue_payload = []
        for item in items:
            qty = abs(int(item['qty']))
            unit_price = int(item.get('unit_price', 0))
            product_name = str(item['product_name']).strip()
            db.insert_manual_trade({
                'partner_name': partner_name,
                'product_name': product_name,
                'trade_date': date_str,
                'trade_type': '판매',
                'qty': qty,
                'unit': item.get('unit', '개'),
                'unit_price': unit_price,
                'amount': qty * unit_price,
                'memo': f'단건출고 ({location})',
                'registered_by': current_user.username,
            })
            # daily_revenue 등록용 데이터
            if qty > 0 and unit_price > 0:
                revenue_payload.append({
                    'revenue_date': date_str,
                    'product_name': product_name,
                    'category': '거래처매출',
                    'qty': qty,
                    'unit_price': unit_price,
                    'revenue': qty * unit_price,
                })

        # 부대비용 → manual_trades + revenue_payload 추가
        for ec in extra_costs:
            ec_name = str(ec.get('name', '')).strip()
            ec_amount = int(ec.get('amount', 0))
            ec_memo = str(ec.get('memo', '')).strip()
            if ec_name and ec_amount > 0:
                db.insert_manual_trade({
                    'partner_name': partner_name,
                    'product_name': ec_name,
                    'trade_date': date_str,
                    'trade_type': '판매',
                    'qty': 1,
                    'unit': '식',
                    'unit_price': ec_amount,
                    'amount': ec_amount,
                    'memo': f'부대비용 - {ec_memo}' if ec_memo else '부대비용',
                    'registered_by': current_user.username,
                })
                revenue_payload.append({
                    'revenue_date': date_str,
                    'product_name': ec_name,
                    'category': '거래처매출',
                    'qty': 1,
                    'unit_price': ec_amount,
                    'revenue': ec_amount,
                })

        # 매출 관리(daily_revenue)에 거래처매출 자동 등록
        if revenue_payload:
            try:
                db.upsert_revenue(revenue_payload)
            except Exception as rev_err:
                current_app.logger.warning(f'거래처매출 등록 실패: {rev_err}')

        # 본사 정보를 세션에 미리 저장 (result 페이지에서 DB 조회 불필요)
        my_biz_info = {}
        try:
            if my_biz_id:
                all_biz = db.query_my_business()
                my_biz_info = next(
                    (b for b in all_biz if b.get('id') == int(my_biz_id)), {}
                )
            if not my_biz_info:
                my_biz_info = db.query_default_business()
        except Exception:
            my_biz_info = {}

        # 결과 데이터를 세션에 저장 (거래명세서 생성용)
        session['outbound_result'] = {
            'date': date_str,
            'location': location,
            'partner_name': partner_name,
            'my_biz_id': int(my_biz_id) if my_biz_id else None,
            'my_biz': my_biz_info,
            'items': items,
            'extra_costs': extra_costs,
            'count': result['count'],
        }

        flash(f"출고 처리 완료: {result['count']}건 (매출처: {partner_name})", 'success')
        return redirect(url_for('outbound.result'))

    except Exception as e:
        flash(f'출고 처리 중 오류: {e}', 'danger')

    return redirect(url_for('outbound.index'))


@outbound_bp.route('/result')
@role_required('admin', 'manager', 'sales', 'general')
def result():
    """단건 출고 결과 — 거래명세서 생성 버튼 포함 (DB 조회 없음)"""
    try:
        result_data = session.get('outbound_result')
        if not result_data:
            return redirect(url_for('outbound.index'))

        # 본사 정보는 세션에 저장된 것 사용 (DB 조회 불필요)
        my_biz = result_data.get('my_biz', {})

        # 합계 계산
        total_qty = sum(abs(int(i.get('qty', 0))) for i in result_data.get('items', []))
        total_amount = sum(
            abs(int(i.get('qty', 0))) * int(i.get('unit_price', 0))
            for i in result_data.get('items', [])
        )
        # 부대비용 합계
        extra_total = sum(int(ec.get('amount', 0)) for ec in result_data.get('extra_costs', []))

        return render_template('outbound/result.html',
                               result=result_data,
                               my_biz=my_biz,
                               total_qty=total_qty,
                               total_amount=total_amount,
                               extra_total=extra_total,
                               grand_total=total_amount + extra_total)
    except Exception as e:
        current_app.logger.error(f'결과 페이지 오류: {e}')
        flash('출고 처리는 완료되었습니다. (결과 페이지 표시 중 오류 발생)', 'warning')
        return redirect(url_for('outbound.index'))


@outbound_bp.route('/invoice')
@role_required('admin', 'manager', 'sales', 'general')
def invoice():
    """단건 출고 거래명세서 PDF 생성"""
    result_data = session.get('outbound_result')
    if not result_data:
        flash('출고 결과가 없습니다.', 'danger')
        return redirect(url_for('outbound.index'))

    db = current_app.db

    try:
        # 본사 정보
        my_biz = {}
        if result_data.get('my_biz_id'):
            all_biz = db.query_my_business()
            my_biz = next(
                (b for b in all_biz if b.get('id') == result_data['my_biz_id']),
                {}
            )
        if not my_biz:
            my_biz = db.query_default_business()

        # 거래처 정보
        partners = db.query_partners()
        partner = next(
            (p for p in partners
             if p.get('partner_name') == result_data['partner_name']),
            {}
        )

        # trades 데이터 구성
        trades = []
        for item in result_data.get('items', []):
            qty = abs(int(item.get('qty', 0)))
            unit_price = int(item.get('unit_price', 0))
            trades.append({
                'product_name': item['product_name'],
                'qty': qty,
                'unit': item.get('unit', '개'),
                'unit_price': unit_price,
                'amount': qty * unit_price,
                'memo': '',
            })

        # 부대비용 항목 추가 (인보이스에 포함)
        for ec in result_data.get('extra_costs', []):
            ec_amount = int(ec.get('amount', 0))
            if ec_amount > 0:
                trades.append({
                    'product_name': ec.get('name', ''),
                    'qty': 1,
                    'unit': '식',
                    'unit_price': ec_amount,
                    'amount': ec_amount,
                    'memo': ec.get('memo', ''),
                })

        # PDF 생성
        from reports.invoice_report import generate_invoice_pdf
        output_dir = current_app.config['OUTPUT_FOLDER']
        os.makedirs(output_dir, exist_ok=True)

        partner_name = result_data.get('partner_name', '')
        trade_date = result_data.get('date', '')
        fname = f"거래명세서_{partner_name}_{trade_date}.pdf"
        pdf_path = os.path.join(output_dir, fname)

        generate_invoice_pdf(pdf_path, my_biz, partner, trades,
                             trade_date=trade_date)

        return send_file(
            pdf_path,
            mimetype='application/pdf',
            as_attachment=True,
            download_name=fname,
        )
    except Exception as e:
        flash(f'거래명세서 생성 중 오류: {e}', 'danger')
        return redirect(url_for('outbound.result'))


@outbound_bp.route('/shipping-label')
@role_required('admin', 'manager', 'sales', 'general')
def shipping_label():
    """출고 결과 기반 운송장 Excel 생성 (온라인주문처리와 동일한 형식)"""
    result_data = session.get('outbound_result')
    if not result_data:
        flash('출고 결과가 없습니다.', 'danger')
        return redirect(url_for('outbound.index'))

    db = current_app.db

    try:
        # 거래처 정보 조회
        partners = db.query_partners()
        partner = next(
            (p for p in partners
             if p.get('partner_name') == result_data['partner_name']),
            {}
        )

        partner_name = result_data.get('partner_name', '')
        partner_addr = partner.get('address', '')
        partner_phone1 = partner.get('contact1', '') or partner.get('phone', '')
        partner_phone2 = partner.get('contact2', '') or ''
        trade_date = result_data.get('date', '')

        # 품목 문자열 생성 (온라인주문처리 송장 형식)
        items = result_data.get('items', [])
        item_parts = []
        total_qty = 0
        for item in items:
            pname = item.get('product_name', '')
            qty = abs(int(item.get('qty', 0)))
            total_qty += qty
            item_parts.append(f"{pname}x{qty}")
        item_str = ', '.join(item_parts) + f' 총{total_qty}개'

        # 운송장 데이터 (온라인주문처리 동일 컬럼)
        row = [
            partner_name,   # 수하인명
            '',             # B1
            partner_addr,   # 수하인주소
            partner_phone1, # 연락처1
            partner_phone2, # 연락처2
            '1',            # 박스
            '3000',         # 운임
            '',             # B2
            item_str,       # 품목명
            '',             # B3
            '',             # 배송메세지
        ]

        columns = [
            '수하인명', 'B1', '수하인주소', '연락처1', '연락처2',
            '박스', '운임', 'B2', '품목명', 'B3', '배송메세지',
        ]

        df = pd.DataFrame([row], columns=columns)

        output_dir = current_app.config['OUTPUT_FOLDER']
        os.makedirs(output_dir, exist_ok=True)
        fname = f"운송장_{partner_name}_{trade_date}.xlsx"
        xlsx_path = os.path.join(output_dir, fname)
        df.to_excel(xlsx_path, index=False)

        return send_file(
            xlsx_path,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=fname,
        )
    except Exception as e:
        flash(f'운송장 생성 중 오류: {e}', 'danger')
        return redirect(url_for('outbound.result'))


@outbound_bp.route('/batch', methods=['POST'])
@role_required('admin', 'manager', 'sales', 'general')
def batch():
    """일괄 출고 — 여러 엑셀 파일 동시 업로드"""
    files = request.files.getlist('files')
    if not files or all(f.filename == '' for f in files):
        flash('엑셀 파일을 하나 이상 선택하세요.', 'danger')
        return redirect(url_for('outbound.index'))

    date_str = request.form.get('date', today_kst())
    mode = request.form.get('mode', '신규입력')

    upload_dir = current_app.config['UPLOAD_FOLDER']
    os.makedirs(upload_dir, exist_ok=True)

    total_count = 0
    total_warnings = []
    errors = []

    for file in files:
        if not file or file.filename == '' or not _allowed(file.filename):
            continue

        fname = secure_filename(file.filename)
        filepath = os.path.join(upload_dir, fname)
        file.save(filepath)

        try:
            from services.outbound_service import process_outbound
            df = pd.read_excel(filepath).fillna("")
            result = process_outbound(
                current_app.db, df, date_str,
                filename=fname, mode=mode,
            )
            total_count += result.get('total_count', 0)
            total_warnings.extend(result.get('warnings', []))
        except Exception as e:
            errors.append(f'{fname}: {e}')
        finally:
            if os.path.exists(filepath):
                os.remove(filepath)

    if total_warnings:
        for w in total_warnings:
            flash(w, 'warning')
    if errors:
        for e in errors:
            flash(e, 'danger')

    flash(f"일괄 출고 완료: 총 {total_count}건 처리", 'success')
    return redirect(url_for('outbound.index'))


# ── 거래 이력 관리 ──

@outbound_bp.route('/trades/delete/<int:trade_id>', methods=['POST'])
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
                # memo 형식: "단건출고 (창고명)" → 창고명 추출
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


@outbound_bp.route('/invoice-trade/<int:trade_id>')
@role_required('admin', 'manager', 'sales', 'general')
def invoice_trade(trade_id):
    """거래명세서 PDF 생성 (단일 거래 기준)"""
    db = current_app.db

    try:
        trades = db.query_manual_trades()
        trade = next((t for t in trades if t.get('id') == trade_id), None)
        if not trade:
            abort(404)

        partners = db.query_partners()
        partner = next(
            (p for p in partners if p.get('partner_name') == trade.get('partner_name')),
            None
        )

        my_biz_list = db.query_my_business()
        my_biz = next(
            (b for b in my_biz_list if b.get('is_default')),
            my_biz_list[0] if my_biz_list else {}
        )

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
        return redirect(url_for('outbound.index'))


@outbound_bp.route('/invoice-selected', methods=['POST'])
@role_required('admin', 'manager', 'sales', 'general')
def invoice_selected():
    """선택한 거래 항목들을 합산한 거래명세서 PDF 생성"""
    db = current_app.db
    selected_ids = request.form.getlist('selected_trades')

    if not selected_ids:
        flash('거래명세서로 출력할 항목을 선택하세요.', 'danger')
        return redirect(url_for('outbound.index'))

    try:
        # 전체 거래 조회 후 선택된 ID 필터
        all_trades = db.query_manual_trades()
        selected_trades = [
            t for t in all_trades if str(t.get('id')) in selected_ids
        ]

        if not selected_trades:
            flash('선택된 거래내역을 찾을 수 없습니다.', 'warning')
            return redirect(url_for('outbound.index'))

        # 거래처 정보 (첫 번째 항목 기준)
        p_name = selected_trades[0].get('partner_name', '')
        t_date = selected_trades[0].get('trade_date', '')

        partners = db.query_partners()
        partner = next(
            (p for p in partners if p.get('partner_name') == p_name), None
        )

        my_biz_list = db.query_my_business()
        my_biz = next(
            (b for b in my_biz_list if b.get('is_default')),
            my_biz_list[0] if my_biz_list else {}
        )

        from reports.invoice_report import generate_invoice_pdf
        output_dir = current_app.config['OUTPUT_FOLDER']
        os.makedirs(output_dir, exist_ok=True)

        fname = f"거래명세서_{p_name}_{t_date}_합산.pdf"
        pdf_path = os.path.join(output_dir, fname)

        generate_invoice_pdf(pdf_path, my_biz, partner or {}, selected_trades,
                             trade_date=t_date)

        return send_file(
            pdf_path,
            mimetype='application/pdf',
            as_attachment=True,
            download_name=fname,
        )
    except Exception as e:
        flash(f'거래명세서 생성 중 오류: {e}', 'danger')
        return redirect(url_for('outbound.index'))


@outbound_bp.route('/invoice-batch-trade')
@role_required('admin', 'manager', 'sales', 'general')
def invoice_batch_trade():
    """거래명세서 PDF — 같은 거래처+날짜 묶어서 생성"""
    db = current_app.db
    p_name = request.args.get('partner_name', '')
    t_date = request.args.get('trade_date', '')

    if not p_name or not t_date:
        flash('거래처명과 거래일을 지정하세요.', 'danger')
        return redirect(url_for('outbound.index'))

    try:
        trade_list = db.query_manual_trades(
            date_from=t_date, date_to=t_date, partner_name=p_name
        )
        if not trade_list:
            flash('해당 거래내역이 없습니다.', 'warning')
            return redirect(url_for('outbound.index'))

        partners = db.query_partners()
        partner = next(
            (p for p in partners if p.get('partner_name') == p_name), None
        )

        my_biz_list = db.query_my_business()
        my_biz = next(
            (b for b in my_biz_list if b.get('is_default')),
            my_biz_list[0] if my_biz_list else {}
        )

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
        return redirect(url_for('outbound.index'))
