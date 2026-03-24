"""
orders_api.py — API 연동 주문관리 블루프린트.

주문관리 > API연동 탭에서 사용하는 엔드포인트:
- /api/api-collect: API 주문수집 → DB 저장 + 재고차감 (실패 시 롤백)
- /api/api-collect-preview: 미리보기 (DB 미반영)
- /api/api-collect-rollback: 수동 롤백
"""
import io
import os
import logging
import tempfile
import zipfile
from collections import defaultdict

from flask import (Blueprint, jsonify, request, g, send_file,
                   current_app)
from flask_login import current_user

from auth import role_required
from db_utils import get_db
from services.tz_utils import days_ago_kst, today_kst, now_kst
from services.api_order_converter import api_orders_to_excel_df

logger = logging.getLogger(__name__)

orders_api_bp = Blueprint('orders_api', __name__, url_prefix='/orders')


def _refresh_client(client, db):
    """토큰 갱신 시도. 실패하면 에러 문자열 반환, 성공하면 None."""
    if client.is_ready:
        # 토큰만 갱신 시도
        try:
            client.refresh_token(db)
        except Exception:
            pass
        return None
    try:
        client.refresh_token(db)
        if not client.is_ready:
            return '인증 미완료'
    except Exception as e:
        return f'토큰 갱신 실패: {e}'
    return None


@orders_api_bp.route('/api/api-collect', methods=['POST'])
@role_required('admin', 'manager')
def api_collect():
    """API 주문수집 → OrderProcessor(save_to_db=True) → DB 저장 + 재고차감.

    실패 시 rollback_import_run_full로 원상복구.
    """
    from services.order_processor import OrderProcessor

    db = get_db()
    mgr = g.marketplace
    channel = request.form.get('channel', '')
    date_from = request.form.get('date_from', days_ago_kst(7))
    date_to = request.form.get('date_to', today_kst())
    collection_date = request.form.get('collection_date', today_kst())

    channels = [channel] if channel != 'all' else mgr.get_active_channels()
    results = {}
    all_files = []
    all_logs = []
    import_run_ids = []  # 롤백용 추적

    output_dir = tempfile.mkdtemp(prefix='api_collect_')

    for ch in channels:
        ch_result = {'channel': ch, 'success': False}
        client = mgr.get_client(ch)
        if not client:
            ch_result['error'] = '클라이언트 없음'
            results[ch] = ch_result
            continue

        err = _refresh_client(client, db)
        if err:
            ch_result['error'] = err
            results[ch] = ch_result
            continue

        try:
            # 1) API 주문 수집
            orders = client.fetch_orders(date_from, date_to,
                                         status_filter='invoice_target')
            if not orders:
                ch_result['error'] = '0건 (수집할 주문 없음)'
                ch_result['fetched'] = 0
                results[ch] = ch_result
                continue

            # 1.1) 2차 보충 수집 (네이버 API 간헐적 누락 대응 — 3초 대기 후 재호출)
            import time
            first_count = len(orders)
            first_ids = set(o.get('api_line_id', '') for o in orders)
            time.sleep(3)
            try:
                orders_2nd = client.fetch_orders(date_from, date_to,
                                                  status_filter='invoice_target')
                if orders_2nd:
                    new_orders = [o for o in orders_2nd if o.get('api_line_id', '') not in first_ids]
                    if new_orders:
                        orders.extend(new_orders)
                        logger.info(f'[APICollect] {ch} 2차 보충: +{len(new_orders)}건 (1차:{first_count} → 합계:{len(orders)})')
                        ch_result['supplemented'] = len(new_orders)
            except Exception as e2:
                logger.warning(f'[APICollect] {ch} 2차 보충 실패 (무시): {e2}')

            # 1.5) api_orders 테이블에 원본 저장 (송장등록 시 매핑용)
            try:
                api_rows = []
                for o in orders:
                    api_rows.append({
                        'channel': ch,
                        'api_order_id': o.get('api_order_id', ''),
                        'api_line_id': o.get('api_line_id', ''),
                        'order_date': o.get('order_date', '')[:10] if o.get('order_date') else collection_date,
                        'match_status': 'matched',
                        'raw_data': o.get('raw_data', {}),
                    })
                if api_rows:
                    api_result = db.upsert_api_orders_batch(api_rows)
                    ch_result['api_orders_saved'] = api_result
                    logger.info(f'[APICollect] {ch} api_orders 저장: {api_result}')
            except Exception as api_err:
                logger.error(f'[APICollect] {ch} api_orders 저장 실패: {api_err}', exc_info=True)
                ch_result['api_orders_error'] = str(api_err)

            # 2) API raw_data → 엑셀 형식 DataFrame
            df = api_orders_to_excel_df(orders, ch)
            if df.empty:
                ch_result['error'] = 'DataFrame 변환 실패'
                results[ch] = ch_result
                continue

            # 3) DataFrame → BytesIO 엑셀
            excel_buf = io.BytesIO()
            df.to_excel(excel_buf, index=False, engine='openpyxl')
            excel_buf.seek(0)
            excel_buf.name = f'{ch}_api_orders.xlsx'

            # 4) OrderProcessor 실행 (save_to_db=True!)
            proc = OrderProcessor()
            result = proc.run(
                mode=ch,
                order_file=excel_buf,
                option_file=None,
                invoice_file=None,
                target_type='송장',
                output_dir=output_dir,
                db=db,
                option_source='db',
                save_to_db=True,
                uploaded_by=f'{current_user.username}(API)',
                collection_date=collection_date,
            )

            ch_result['fetched'] = len(orders)
            ch_result['logs'] = result.get('logs', [])
            all_logs.extend(result.get('logs', []))

            if result.get('success'):
                ch_result['success'] = True
                db_res = result.get('db_result', {})
                ch_result['inserted'] = db_res.get('inserted', 0)
                ch_result['updated'] = db_res.get('updated', 0)
                ch_result['skipped'] = db_res.get('skipped', 0)
                ch_result['files'] = [os.path.basename(f) for f in result.get('files', [])]
                all_files.extend(result.get('files', []))

                # 롤백 추적용 import_run_id 저장
                run_id = db_res.get('import_run_id')
                if run_id:
                    import_run_ids.append(run_id)
                    ch_result['import_run_id'] = run_id

            elif result.get('unmatched'):
                # 미매칭 발생 → 해당 채널 import_run 자동 롤백
                if import_run_ids:
                    last_run = import_run_ids[-1]
                    try:
                        rb = db.rollback_import_run_full(last_run, current_user.username)
                        ch_result['rollback'] = rb
                        import_run_ids.pop()
                        ch_result['rolled_back'] = True
                        logger.info('[APICollect] 미매칭 롤백 성공: ch=%s, run_id=%s', ch, last_run)
                    except Exception as rb_err:
                        logger.error('[APICollect] 미매칭 롤백 실패: ch=%s, run_id=%s, error=%s',
                                     ch, last_run, rb_err, exc_info=True)
                        ch_result['rollback_error'] = str(rb_err)
                ch_result['error'] = f"미매칭 {len(result['unmatched'])}건 — 자동 롤백됨"
                ch_result['unmatched'] = result['unmatched'][:50]
            else:
                ch_result['error'] = result.get('error', '처리 실패')

        except Exception as e:
            logger.error(f'[APICollect] {ch} 오류: {e}', exc_info=True)
            ch_result['error'] = str(e)

            # 실패 시 이 채널 + 이전 성공 채널 모두 롤백 (원자성 보장)
            rollback_targets = list(import_run_ids)  # 복사
            rollback_results = []
            for rid in reversed(rollback_targets):
                try:
                    rb = db.rollback_import_run_full(rid, current_user.username)
                    rollback_results.append({'run_id': rid, 'result': rb})
                    import_run_ids.remove(rid)
                    logger.info('[APICollect] 롤백 성공: run_id=%s', rid)
                except Exception as rb_err:
                    logger.error('[APICollect] 롤백 실패: run_id=%s, error=%s',
                                 rid, rb_err, exc_info=True)
                    rollback_results.append({'run_id': rid, 'error': str(rb_err)})
            ch_result['rollback'] = rollback_results
            ch_result['rolled_back_all'] = True

        results[ch] = ch_result

    return jsonify({
        'results': results,
        'import_run_ids': import_run_ids,
        'file_count': len(all_files),
        'logs': all_logs[-30:],
    })


@orders_api_bp.route('/api/api-collect-download', methods=['POST'])
@role_required('admin', 'manager')
def api_collect_download():
    """DB에 수집된 api_orders 기반 → 송장 파일 다운로드 (API 재호출 없음, 빠름)."""
    from services.order_processor import OrderProcessor

    db = get_db()
    channel = request.form.get('channel', '')
    date_from = request.form.get('date_from', days_ago_kst(7))
    date_to = request.form.get('date_to', today_kst())
    collection_date = request.form.get('collection_date', today_kst())

    # DB api_orders에서 가져오기 (API 재호출 없음 → 빠름)
    channels = [channel] if channel and channel != 'all' else [
        '스마트스토어_배마마', '스마트스토어_해미애찬', '쿠팡', '자사몰']
    all_files = []
    channel_status = {}
    total_unmatched = []
    output_dir = tempfile.mkdtemp(prefix='api_preview_')

    for ch in channels:
        try:
            # DB에서 api_orders 조회 (N배송 제외: expectedDeliveryCompany=CJGLS만)
            api_rows = db.query_api_orders(
                channel=ch, date_from=date_from, date_to=date_to)

            if not api_rows:
                channel_status[ch] = '0건 (DB에 수집된 건 없음)'
                continue

            # raw_data에서 주문 형식으로 변환
            orders = []
            for row in api_rows:
                raw = row.get('raw_data', {})
                po = raw.get('productOrder', {}) if 'productOrder' in raw else raw

                # N배송 제외 (네이버만 — CJGLS가 아닌 건 스킵)
                if ch.startswith('스마트스토어'):
                    company = po.get('expectedDeliveryCompany', '')
                    if company and company != 'CJGLS':
                        continue

                orders.append({
                    'api_order_id': row.get('api_order_id', ''),
                    'api_line_id': row.get('api_line_id', ''),
                    'raw_data': raw,
                })

            if not orders:
                channel_status[ch] = '0건 (N배송 제외 후)'
                continue

            df = api_orders_to_excel_df(orders, ch)
            if df.empty:
                channel_status[ch] = 'DataFrame 변환 실패'
                continue

            excel_buf = io.BytesIO()
            df.to_excel(excel_buf, index=False, engine='openpyxl')
            excel_buf.seek(0)
            excel_buf.name = f'{ch}_api_orders.xlsx'

            proc = OrderProcessor()
            result = proc.run(
                mode=ch,
                order_file=excel_buf,
                option_file=None,
                invoice_file=None,
                target_type='송장',
                output_dir=output_dir,
                db=db,
                option_source='db',
                save_to_db=False,
                uploaded_by='(API미리보기)',
            )

            if result.get('success'):
                all_files.extend(result.get('files', []))
                channel_status[ch] = f'{len(orders)}건 → 송장 생성'
            elif result.get('unmatched'):
                total_unmatched.extend(result['unmatched'])
                channel_status[ch] = f"미매칭 {len(result['unmatched'])}건"
            else:
                channel_status[ch] = f"실패: {result.get('error', '')[:80]}"

        except Exception as e:
            channel_status[ch] = f'오류: {e}'

    if not all_files:
        return jsonify({
            'error': '생성된 파일 없음 (먼저 API 주문수집을 실행하세요)',
            'channel_status': channel_status,
            'unmatched': total_unmatched[:30],
        })

    ts = now_kst().strftime('%Y%m%d_%H%M%S')
    ch_label = channel if channel != 'all' else '전체'

    if len(all_files) == 1:
        fp = all_files[0]
        mime = ('application/vnd.ms-excel' if fp.endswith('.xls')
                else 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        return send_file(fp, mimetype=mime, as_attachment=True,
                         download_name=os.path.basename(fp))

    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        for fp in all_files:
            zf.write(fp, os.path.basename(fp))
    zip_buf.seek(0)
    return send_file(zip_buf, mimetype='application/zip', as_attachment=True,
                     download_name=f'API송장_{ch_label}_{ts}.zip')


@orders_api_bp.route('/api/generate-shipping-from-db', methods=['POST'])
@role_required('admin', 'manager', 'general')
def generate_shipping_from_db():
    """DB 주문으로 일괄배송 파일(집계표+송장) 생성."""
    from services.order_processor import OrderProcessor

    db = get_db()
    mgr = g.marketplace
    channel = request.form.get('channel', 'all')
    date_from = request.form.get('date_from', today_kst())
    date_to = request.form.get('date_to', today_kst())

    channels = [channel] if channel != 'all' else mgr.get_active_channels()
    all_files = []
    output_dir = tempfile.mkdtemp(prefix='shipping_db_')

    for ch in channels:
        try:
            # DB에서 해당 기간 주문 조회
            txs = db.query_order_transactions(
                date_from=date_from, date_to=date_to,
                channel=ch, limit=10000,
            )
            if not txs:
                continue

            # api_orders에서 raw_data 가져와서 엑셀 변환
            api_rows = db.query_api_orders(
                channel=ch, date_from=date_from, date_to=date_to,
            )
            if not api_rows:
                continue

            from services.api_order_converter import api_orders_to_excel_df
            # api_rows를 fetch_orders 반환 형태로 변환
            orders = []
            for ar in api_rows:
                orders.append({
                    'api_order_id': ar.get('api_order_id', ''),
                    'api_line_id': ar.get('api_line_id', ''),
                    'order_date': ar.get('order_date', ''),
                    'raw_data': ar.get('raw_data', {}),
                })

            df = api_orders_to_excel_df(orders, ch)
            if df.empty:
                continue

            excel_buf = io.BytesIO()
            df.to_excel(excel_buf, index=False, engine='openpyxl')
            excel_buf.seek(0)
            excel_buf.name = f'{ch}_orders.xlsx'

            proc = OrderProcessor()
            result = proc.run(
                mode=ch,
                order_file=excel_buf,
                option_file=None,
                invoice_file=None,
                target_type='송장',
                output_dir=output_dir,
                db=db,
                option_source='db',
                save_to_db=False,
                uploaded_by=f'{current_user.username}(DB)',
            )

            if result.get('success') and result.get('files'):
                all_files.extend(result['files'])

        except Exception as e:
            logger.error(f'[GenShipping] {ch} 오류: {e}', exc_info=True)

    if not all_files:
        return jsonify({'error': '생성된 파일이 없습니다.'}), 400

    ts = now_kst().strftime('%Y%m%d_%H%M%S')

    if len(all_files) == 1:
        fp = all_files[0]
        mime = ('application/vnd.ms-excel' if fp.endswith('.xls')
                else 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        return send_file(fp, mimetype=mime, as_attachment=True,
                         download_name=os.path.basename(fp))

    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        for fp in all_files:
            zf.write(fp, os.path.basename(fp))
    zip_buf.seek(0)
    return send_file(zip_buf, mimetype='application/zip', as_attachment=True,
                     download_name=f'일괄배송_{ts}.zip')


@orders_api_bp.route('/api/api-collect-rollback', methods=['POST'])
@role_required('admin', 'manager')
def api_collect_rollback():
    """import_run 수동 롤백 (재고 복원 + 주문 취소)."""
    db = get_db()
    run_id = request.json.get('import_run_id')
    if not run_id:
        return jsonify({'error': 'import_run_id 필요'}), 400

    result = db.rollback_import_run_full(run_id, current_user.username)
    return jsonify(result)


@orders_api_bp.route('/api/api-status', methods=['GET'])
@role_required('admin', 'manager', 'general')
def api_status():
    """API 채널 연결 상태 조회."""
    mgr = g.marketplace
    db = get_db()
    channels = mgr.get_active_channels()
    status = {}
    for ch in channels:
        client = mgr.get_client(ch)
        if not client:
            status[ch] = {'ready': False, 'reason': '클라이언트 없음'}
            continue
        if not client.is_ready:
            try:
                client.refresh_token(db)
            except Exception:
                pass
        status[ch] = {
            'ready': client.is_ready,
            'channel': ch,
        }
    return jsonify(status)
