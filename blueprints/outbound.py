"""
outbound.py — 거래처주문처리 Blueprint.
단건 출고 (폼 기반), 일괄(batch) 출고 (엑셀), 거래명세서 PDF.
"""
import json
import os
from datetime import datetime

import pandas as pd
from flask import (
    Blueprint, render_template, request, current_app,
    flash, redirect, url_for, jsonify, session, send_file,
)
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename

from auth import role_required

outbound_bp = Blueprint('outbound', __name__, url_prefix='/outbound')

ALLOWED_EXT = {'xlsx', 'xls'}


def _allowed(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXT


@outbound_bp.route('/')
@role_required('admin', 'manager', 'sales', 'general')
def index():
    """거래처주문처리 폼"""
    db = current_app.db
    locations = []
    partners = []
    my_businesses = []
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
    return render_template('outbound/index.html',
                           locations=locations, partners=partners,
                           my_businesses=my_businesses)


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
    date_str = request.form.get('date', datetime.now().strftime('%Y-%m-%d'))
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

        # 매출 관리(daily_revenue)에 거래처매출 자동 등록
        if revenue_payload:
            try:
                db.upsert_revenue(revenue_payload)
            except Exception as rev_err:
                current_app.logger.warning(f'거래처매출 등록 실패: {rev_err}')

        # 결과 데이터를 세션에 저장 (거래명세서 생성용)
        session['outbound_result'] = {
            'date': date_str,
            'location': location,
            'partner_name': partner_name,
            'my_biz_id': int(my_biz_id) if my_biz_id else None,
            'items': items,
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
    """단건 출고 결과 — 거래명세서 생성 버튼 포함"""
    result_data = session.get('outbound_result')
    if not result_data:
        return redirect(url_for('outbound.index'))

    # 본사 정보 조회
    db = current_app.db
    my_biz = {}
    if result_data.get('my_biz_id'):
        try:
            all_biz = db.query_my_business()
            my_biz = next(
                (b for b in all_biz if b.get('id') == result_data['my_biz_id']),
                {}
            )
        except Exception:
            pass
    if not my_biz:
        try:
            my_biz = db.query_default_business()
        except Exception:
            pass

    # 합계 계산
    total_qty = sum(abs(int(i.get('qty', 0))) for i in result_data.get('items', []))
    total_amount = sum(
        abs(int(i.get('qty', 0))) * int(i.get('unit_price', 0))
        for i in result_data.get('items', [])
    )

    return render_template('outbound/result.html',
                           result=result_data,
                           my_biz=my_biz,
                           total_qty=total_qty,
                           total_amount=total_amount)


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


@outbound_bp.route('/batch', methods=['POST'])
@role_required('admin', 'manager', 'sales', 'general')
def batch():
    """일괄 출고 — 여러 엑셀 파일 동시 업로드"""
    files = request.files.getlist('files')
    if not files or all(f.filename == '' for f in files):
        flash('엑셀 파일을 하나 이상 선택하세요.', 'danger')
        return redirect(url_for('outbound.index'))

    date_str = request.form.get('date', datetime.now().strftime('%Y-%m-%d'))
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
