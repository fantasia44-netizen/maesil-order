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
                    except Exception as rb_err:
                        ch_result['rollback_error'] = str(rb_err)
                ch_result['error'] = f"미매칭 {len(result['unmatched'])}건 — 자동 롤백됨"
                ch_result['unmatched'] = result['unmatched'][:50]
            else:
                ch_result['error'] = result.get('error', '처리 실패')

        except Exception as e:
            logger.error(f'[APICollect] {ch} 오류: {e}', exc_info=True)
            ch_result['error'] = str(e)

            # 실패 시 이 채널에서 생성된 import_run 롤백
            if import_run_ids:
                last_run = import_run_ids[-1]
                try:
                    rb = db.rollback_import_run_full(last_run, current_user.username)
                    ch_result['rollback'] = rb
                    import_run_ids.pop()
                except Exception as rb_err:
                    ch_result['rollback_error'] = str(rb_err)

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
    """API 주문수집 → 송장 파일만 다운로드 (DB 미반영, 미리보기)."""
    from services.order_processor import OrderProcessor

    db = get_db()
    mgr = g.marketplace
    channel = request.form.get('channel', '')
    date_from = request.form.get('date_from', days_ago_kst(7))
    date_to = request.form.get('date_to', today_kst())

    channels = [channel] if channel != 'all' else mgr.get_active_channels()
    all_files = []
    channel_status = {}
    total_unmatched = []
    output_dir = tempfile.mkdtemp(prefix='api_preview_')

    for ch in channels:
        client = mgr.get_client(ch)
        if not client:
            channel_status[ch] = '클라이언트 없음'
            continue

        err = _refresh_client(client, db)
        if err:
            channel_status[ch] = err
            continue

        try:
            orders = client.fetch_orders(date_from, date_to,
                                         status_filter='invoice_target')
            if not orders:
                channel_status[ch] = '0건'
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
            'error': '생성된 파일 없음',
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
