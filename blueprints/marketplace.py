"""marketplace.py -- 마켓플레이스 API 연동 Blueprint.

네이버 스마트스토어 / 쿠팡 / Cafe24 API 설정, 동기화, 교차검증.
쿠팡 로켓배송 정산 엑셀 업로드 + 광고비 수동 기입.
API 매출 대시보드 (총무/경리용).
"""
import io
import logging
from collections import defaultdict
from flask import (Blueprint, render_template, request, current_app,
                   jsonify, flash, redirect, url_for)
from flask_login import login_required, current_user
from auth import role_required, _log_action
from services.tz_utils import today_kst, days_ago_kst

logger = logging.getLogger(__name__)


def _utc_to_kst_str(ts_str: str) -> str:
    """UTC 타임스탬프 문자열 → KST 'MM-DD HH:MM' 형식."""
    from datetime import datetime, timedelta, timezone
    try:
        # '2026-03-10T08:36:00+00:00' or '2026-03-10T08:36:00'
        s = ts_str.replace('Z', '+00:00')
        if '+' in s[10:] or s.count('-') > 2:
            dt = datetime.fromisoformat(s)
        else:
            dt = datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
        kst = dt.astimezone(timezone(timedelta(hours=9)))
        return kst.strftime('%Y-%m-%dT%H:%M')
    except Exception:
        return ts_str[:16] if ts_str else '-'


def _cafe24_callback_url() -> str:
    """Cafe24 OAuth redirect_uri — 항상 HTTPS."""
    u = url_for('cafe24_oauth_redirect', _external=True)
    return u.replace('http://', 'https://', 1)

marketplace_bp = Blueprint('marketplace', __name__, url_prefix='/marketplace')


@marketplace_bp.route('/')
@role_required('admin', 'general')
def index():
    """API 연동 설정 대시보드."""
    db = current_app.db
    mgr = current_app.marketplace

    channels = mgr.get_all_channels()

    # 각 채널의 DB config 로드
    configs = {}
    for ch in channels:
        ch_name = ch['channel']
        rows = db.query_marketplace_api_configs(channel=ch_name)
        configs[ch_name] = rows[0] if rows else {}

    # 최근 동기화 로그 (UTC→KST 변환)
    recent_logs = db.query_api_sync_logs(limit=10)
    for log in recent_logs:
        if log.get('started_at'):
            log['started_at_kst'] = _utc_to_kst_str(log['started_at'])

    return render_template('marketplace/index.html',
                           channels=channels,
                           configs=configs,
                           recent_logs=recent_logs)


@marketplace_bp.route('/config/<channel>', methods=['POST'])
@role_required('admin')
def save_config(channel):
    """채널 API 설정 저장."""
    db = current_app.db
    mgr = current_app.marketplace

    payload = {
        'channel': channel,
        'client_id': request.form.get('client_id', '').strip(),
        'client_secret': request.form.get('client_secret', '').strip(),
        'is_active': request.form.get('is_active') == 'on',
    }

    # 채널별 추가 필드
    if channel == '쿠팡':
        payload['vendor_id'] = request.form.get('vendor_id', '').strip()
    elif channel == '자사몰':
        payload['mall_id'] = request.form.get('mall_id', '').strip()

    db.upsert_marketplace_api_config(payload)
    _log_action(f'마켓플레이스 API 설정 저장: {channel}')

    # 클라이언트 재로드
    mgr._load_configs(db)

    flash(f'{channel} API 설정이 저장되었습니다.', 'success')
    return redirect(url_for('marketplace.index'))


@marketplace_bp.route('/test/<channel>', methods=['POST'])
@role_required('admin')
def test_connection(channel):
    """API 연결 테스트."""
    db = current_app.db
    mgr = current_app.marketplace
    client = mgr.get_client(channel)

    if not client:
        return jsonify({'success': False, 'message': f'{channel} 클라이언트 없음'})

    result = client.test_connection(db)

    # Cafe24 OAuth 인증이 필요한 경우 올바른 redirect_uri로 auth_url 재생성
    if result.get('auth_url'):
        callback_url = _cafe24_callback_url()
        result['auth_url'] = client.get_auth_url(callback_url, state='connect')

    return jsonify(result)


@marketplace_bp.route('/oauth/authorize/<channel>')
@role_required('admin')
def oauth_authorize(channel):
    """OAuth2 인증 시작 (Cafe24 등)."""
    mgr = current_app.marketplace
    client = mgr.get_client(channel)
    if not client:
        flash(f'{channel} 클라이언트 없음', 'danger')
        return redirect(url_for('marketplace.index'))

    callback_url = _cafe24_callback_url()
    auth_url = client.get_auth_url(callback_url, state='connect')
    return redirect(auth_url)


@marketplace_bp.route('/oauth/callback/<channel>')
@role_required('admin')
def oauth_callback(channel):
    """OAuth2 콜백 — 인가 코드를 토큰으로 교환."""
    db = current_app.db
    mgr = current_app.marketplace
    client = mgr.get_client(channel)

    code = request.args.get('code', '')
    error = request.args.get('error', '')

    if error:
        flash(f'{channel} OAuth 인증 거부: {error}', 'danger')
        return redirect(url_for('marketplace.index'))

    if not code:
        flash(f'{channel} 인가 코드가 없습니다', 'danger')
        return redirect(url_for('marketplace.index'))

    callback_url = _cafe24_callback_url()
    result = client.exchange_code(db, code, callback_url)

    if result is True:
        mgr._load_configs(db)
        _log_action(f'{channel} OAuth 인증 완료')
        flash(f'{channel} OAuth 인증 성공!', 'success')
    else:
        flash(f'{channel} 토큰 교환 실패: {result}', 'danger')

    return redirect(url_for('marketplace.index'))


@marketplace_bp.route('/sync', methods=['POST'])
@role_required('admin', 'general')
def sync():
    """수동 동기화 실행."""
    try:
        db = current_app.db
        mgr = current_app.marketplace

        channel = request.form.get('channel', '')
        sync_type = request.form.get('sync_type', 'orders')
        date_from = request.form.get('date_from', days_ago_kst(7))
        date_to = request.form.get('date_to', today_kst())

        from services.marketplace_sync_service import (
            sync_orders, sync_settlements, sync_revenue_fees)

        results = {}

        if sync_type in ('orders', 'all'):
            results['orders'] = sync_orders(
                db, mgr, channel, date_from, date_to,
                triggered_by=current_user.username
            )

        if sync_type in ('settlements', 'all'):
            results['settlements'] = sync_settlements(
                db, mgr, channel, date_from, date_to,
                triggered_by=current_user.username
            )

        # 쿠팡: 매출내역(revenue-history)으로 수수료/정산 업데이트
        if channel == '쿠팡' and sync_type in ('settlements', 'all'):
            results['revenue_fees'] = sync_revenue_fees(
                db, mgr, channel, date_from, date_to,
                triggered_by=current_user.username
            )

        _log_action(f'마켓플레이스 동기화: {channel} {sync_type} {date_from}~{date_to}')
        return jsonify(results)

    except Exception as e:
        logger.error(f'[Sync] 동기화 오류: {e}', exc_info=True)
        return jsonify({'error': str(e)}), 500


@marketplace_bp.route('/diag/<channel>')
@role_required('admin')
def diag(channel):
    """API 진단 — 실제 API 응답 확인."""
    db = current_app.db
    mgr = current_app.marketplace
    client = mgr.get_client(channel)

    if not client:
        return jsonify({'error': f'{channel} 클라이언트 없음'})

    info = {
        'channel': channel,
        'is_ready': client.is_ready,
        'config_keys': list(client.config.keys()),
        'has_token': bool(client.config.get('access_token')),
        'token_expires': client.config.get('token_expires_at', ''),
    }

    # 토큰 갱신 시도
    try:
        refresh_ok = client.refresh_token(db)
        info['refresh_ok'] = refresh_ok
    except Exception as e:
        info['refresh_error'] = str(e)

    # 주문 조회 (최근 1일, 최대 3건)
    try:
        from services.tz_utils import today_kst, days_ago_kst
        orders = client.fetch_orders(days_ago_kst(1), today_kst())
        info['orders_count'] = len(orders)
        if orders:
            info['sample_order'] = {k: str(v)[:50] for k, v in orders[0].items()}
    except Exception as e:
        info['orders_error'] = str(e)

    return jsonify(info)


@marketplace_bp.route('/sync/log')
@role_required('admin', 'general')
def sync_log():
    """동기화 이력."""
    db = current_app.db
    channel = request.args.get('channel', '')
    logs = db.query_api_sync_logs(channel=channel or None, limit=100)
    for log in logs:
        if log.get('started_at'):
            log['started_at_kst'] = _utc_to_kst_str(log['started_at'])
        if log.get('finished_at'):
            log['finished_at_kst'] = _utc_to_kst_str(log['finished_at'])
    return render_template('marketplace/sync_log.html', logs=logs, channel=channel)


@marketplace_bp.route('/validation')
@role_required('admin', 'general')
def validation():
    """교차검증 대시보드."""
    db = current_app.db
    channel = request.args.get('channel', '')
    date_from = request.args.get('date_from', days_ago_kst(7))
    date_to = request.args.get('date_to', today_kst())

    validation_result = None
    if channel:
        from services.marketplace_validation_service import validate_orders
        validation_result = validate_orders(db, channel, date_from, date_to)

    return render_template('marketplace/validation.html',
                           channel=channel,
                           date_from=date_from,
                           date_to=date_to,
                           result=validation_result)


@marketplace_bp.route('/api/validation/run', methods=['POST'])
@role_required('admin', 'general')
def run_validation():
    """교차검증 실행 (AJAX)."""
    db = current_app.db
    channel = request.form.get('channel', '')
    date_from = request.form.get('date_from', days_ago_kst(7))
    date_to = request.form.get('date_to', today_kst())

    if not channel:
        return jsonify({'error': '채널을 선택하세요'}), 400

    from services.marketplace_validation_service import validate_orders, validate_settlements

    result = {
        'orders': validate_orders(db, channel, date_from, date_to),
        'settlements': validate_settlements(db, channel, date_from, date_to),
    }

    _log_action(f'마켓플레이스 교차검증: {channel} {date_from}~{date_to}')
    return jsonify(result)


# ── 쿠팡 로켓배송 정산 엑셀 업로드 ──

def _parse_coupang_billing_xlsx(file_bytes):
    """쿠팡 Supplier Hub Billing Statement xlsx 파싱.

    쿠팡 엑셀은 inline string 사용 + 인코딩 깨짐 → XML 직접 파싱.
    컬럼 고정 순서: 계산서번호(A) 거래처명(B) 작성일자(C) 지급일자(D)
    과세유형(E) 발주유형(F) 정산유형(G) 공급가액(H) VAT(I) 지급예정금액(J)
    세금계산서확정일(K) 1차지급일(L) 1차지급액(M) 2차지급액(N) 발주정보(O) 반출정보(P)
    """
    import zipfile
    import xml.etree.ElementTree as ET

    zf = zipfile.ZipFile(io.BytesIO(file_bytes))
    ns = '{http://schemas.openxmlformats.org/spreadsheetml/2006/main}'
    sheet_xml = zf.read('xl/worksheets/sheet1.xml').decode('utf-8')
    root = ET.fromstring(sheet_xml)

    # 컬럼 인덱스 매핑 (A=0, B=1, ...)
    def _col_idx(ref):
        col = ''.join(c for c in ref if c.isalpha())
        return ord(col) - ord('A')

    def _cell_value(cell_el):
        is_el = cell_el.find(f'{ns}is')
        if is_el is not None:
            t_el = is_el.find(f'{ns}t')
            return t_el.text if t_el is not None else ''
        v_el = cell_el.find(f'{ns}v')
        return v_el.text if v_el is not None else ''

    rows_data = []
    for row_el in root.findall(f'.//{ns}row'):
        cells = {}
        for c in row_el.findall(f'{ns}c'):
            ref = c.get('r', '')
            idx = _col_idx(ref)
            cells[idx] = _cell_value(c)
        rows_data.append(cells)

    return rows_data  # [{col_idx: value, ...}, ...]


@marketplace_bp.route('/rocket/upload', methods=['POST'])
@role_required('admin', 'general')
def upload_rocket_settlement():
    """쿠팡 로켓배송(Supplier Hub) Billing Statement 업로드 → api_settlements 저장.

    과세/면세 구분하여 작성일자 기준으로 일별 집계.
    """
    db = current_app.db

    f = request.files.get('file')
    if not f or not f.filename:
        return jsonify({'error': '파일을 선택하세요'}), 400

    try:
        file_bytes = f.read()
        rows = _parse_coupang_billing_xlsx(file_bytes)

        if len(rows) < 2:
            return jsonify({'error': '데이터가 없습니다'}), 400

        # 헤더(row 0) 스킵, 데이터(row 1~)
        # 고정 컬럼: C=작성일자(2), E=과세유형(4), H=공급가액(7), I=VAT(8), J=지급예정금액(9)
        from collections import defaultdict as _dd
        daily_agg = _dd(lambda: {
            'supply_taxable': 0, 'vat_taxable': 0,
            'supply_exempt': 0,
            'total_payment': 0, 'count': 0,
        })

        def _to_int(v):
            if not v:
                return 0
            try:
                return int(float(str(v).replace(',', '')))
            except (ValueError, TypeError):
                return 0

        for row in rows[1:]:  # 데이터 행
            date_str = (row.get(2) or '').strip()[:10]  # C: 작성일자
            if not date_str or len(date_str) < 8:
                continue

            tax_type = row.get(4, '')  # E: 과세유형
            supply = _to_int(row.get(7))  # H: 공급가액
            vat = _to_int(row.get(8))  # I: VAT
            payment = _to_int(row.get(9))  # J: 지급예정금액

            d = daily_agg[date_str]
            if '면세' in tax_type:
                d['supply_exempt'] += supply
            else:
                d['supply_taxable'] += supply
                d['vat_taxable'] += vat
            d['total_payment'] += payment
            d['count'] += 1

        settlements = []
        for date_str in sorted(daily_agg):
            d = daily_agg[date_str]
            gross = d['supply_taxable'] + d['supply_exempt']  # 공급가액 합계 = 매출
            settlements.append({
                'channel': '쿠팡로켓',
                'settlement_date': date_str,
                'settlement_id': f'rocket_{date_str}',
                'gross_sales': gross,
                'total_commission': 0,  # 로켓은 매입 구조 → 수수료 없음
                'shipping_fee_income': 0,
                'shipping_fee_cost': 0,
                'coupon_discount': 0,
                'point_discount': 0,
                'other_deductions': 0,
                'net_settlement': d['total_payment'],  # 지급예정금액 (공급가액+VAT)
                'fee_breakdown': {
                    'source': 'billing_statement',
                    'supply_taxable': d['supply_taxable'],
                    'vat_taxable': d['vat_taxable'],
                    'supply_exempt': d['supply_exempt'],
                    'total_payment': d['total_payment'],
                    'invoice_count': d['count'],
                    'filename': f.filename,
                },
            })

        if not settlements:
            return jsonify({'error': '유효한 정산 데이터가 없습니다'}), 400

        db.upsert_api_settlements_batch(settlements)
        total_count = sum(d['count'] for d in daily_agg.values())
        _log_action(f'쿠팡로켓 정산 업로드: {len(settlements)}일, {total_count}건')

        return jsonify({
            'success': True,
            'count': total_count,
            'date_range': f'{settlements[0]["settlement_date"]} ~ {settlements[-1]["settlement_date"]}',
        })

    except Exception as e:
        logger.error(f'[로켓정산] 업로드 오류: {e}', exc_info=True)
        return jsonify({'error': str(e)}), 500


# ── 광고비 수동 기입 ──

@marketplace_bp.route('/ad-cost', methods=['POST'])
@role_required('admin', 'general')
def save_ad_cost():
    """채널별 광고비 수동 기입 → api_settlements 저장."""
    db = current_app.db

    channel = request.form.get('channel', '').strip()
    date_str = request.form.get('date', '').strip()
    cost = request.form.get('cost', '0').strip().replace(',', '')
    memo = request.form.get('memo', '').strip()

    if not channel or not date_str:
        return jsonify({'error': '채널과 날짜를 입력하세요'}), 400

    try:
        cost_int = int(float(cost))
    except (ValueError, TypeError):
        return jsonify({'error': '광고비 금액이 올바르지 않습니다'}), 400

    settlement = {
        'channel': channel,
        'settlement_date': date_str,
        'settlement_id': f'ad_cost_{date_str}',
        'gross_sales': 0,
        'total_commission': 0,
        'shipping_fee_income': 0,
        'shipping_fee_cost': 0,
        'coupon_discount': 0,
        'point_discount': 0,
        'other_deductions': cost_int,
        'net_settlement': -cost_int,
        'fee_breakdown': {
            'source': 'manual_input',
            'memo': memo,
        },
    }

    db.upsert_api_settlements_batch([settlement])
    _log_action(f'광고비 기입: {channel} {date_str} {cost_int:,}원')

    return jsonify({
        'success': True,
        'channel': channel,
        'date': date_str,
        'cost': cost_int,
    })


# ── 광고비 일괄 업로드 (엑셀) ──

@marketplace_bp.route('/ad-cost/upload', methods=['POST'])
@role_required('admin', 'general')
def upload_ad_cost():
    """광고비 엑셀 업로드 → api_settlements 저장.

    엑셀 형식: 날짜 | 채널 | 광고비 | (메모)
    """
    import openpyxl
    db = current_app.db

    f = request.files.get('file')
    if not f or not f.filename:
        return jsonify({'error': '파일을 선택하세요'}), 400

    try:
        wb = openpyxl.load_workbook(io.BytesIO(f.read()), read_only=True)
        ws = wb.active

        headers = [str(c.value or '').strip() for c in next(ws.iter_rows(min_row=1, max_row=1))]

        col_map = {}
        for i, h in enumerate(headers):
            hl = h.replace(' ', '')
            if '날짜' in hl or '일자' in hl or 'date' in hl.lower():
                col_map['date'] = i
            elif '채널' in hl or '매체' in hl or '플랫폼' in hl:
                col_map['channel'] = i
            elif '광고비' in hl or '비용' in hl or '금액' in hl or 'cost' in hl.lower():
                col_map['cost'] = i
            elif '메모' in hl or '비고' in hl:
                col_map['memo'] = i

        if 'cost' not in col_map:
            wb.close()
            return jsonify({'error': '광고비/비용/금액 컬럼을 찾을 수 없습니다'}), 400

        settlements = []
        for row in ws.iter_rows(min_row=2, values_only=True):
            row = list(row)
            if not row or all(v is None for v in row):
                continue

            date_val = row[col_map.get('date', 0)]
            if not date_val:
                continue
            if hasattr(date_val, 'strftime'):
                date_str = date_val.strftime('%Y-%m-%d')
            else:
                date_str = str(date_val).strip()[:10]

            channel = str(row[col_map['channel']]).strip() if 'channel' in col_map else '쿠팡'
            cost_val = row[col_map['cost']]
            try:
                cost_int = int(float(str(cost_val).replace(',', '')))
            except (ValueError, TypeError):
                continue
            if cost_int <= 0:
                continue

            memo = str(row[col_map.get('memo', -1)] or '') if 'memo' in col_map else ''

            settlements.append({
                'channel': channel,
                'settlement_date': date_str,
                'settlement_id': f'ad_cost_{date_str}',
                'gross_sales': 0,
                'total_commission': 0,
                'shipping_fee_income': 0,
                'shipping_fee_cost': 0,
                'coupon_discount': 0,
                'point_discount': 0,
                'other_deductions': cost_int,
                'net_settlement': -cost_int,
                'fee_breakdown': {
                    'source': 'excel_upload',
                    'memo': memo,
                    'filename': f.filename,
                },
            })

        wb.close()

        if not settlements:
            return jsonify({'error': '유효한 광고비 데이터가 없습니다'}), 400

        db.upsert_api_settlements_batch(settlements)
        _log_action(f'광고비 엑셀 업로드: {len(settlements)}건')

        return jsonify({
            'success': True,
            'count': len(settlements),
        })

    except Exception as e:
        logger.error(f'[광고비] 업로드 오류: {e}', exc_info=True)
        return jsonify({'error': str(e)}), 500


# ── API 매출 대시보드 (총무/경리) ──

@marketplace_bp.route('/sales')
@role_required('admin', 'general')
def sales():
    """API 매출/수수료/정산 대시보드."""
    import calendar
    from datetime import date

    month = request.args.get('month', '')
    if not month:
        t = date.today()
        month = t.strftime('%Y-%m')

    year, mon = int(month[:4]), int(month[5:7])
    last_day = calendar.monthrange(year, mon)[1]
    date_from = f'{month}-01'
    date_to = f'{month}-{last_day:02d}'

    return render_template('marketplace/sales.html',
                           month=month,
                           date_from=date_from,
                           date_to=date_to)


@marketplace_bp.route('/api/sales-data')
@role_required('admin', 'general')
def sales_data():
    """API 매출 집계 데이터 (AJAX)."""
    import calendar
    from datetime import date

    db = current_app.db
    month = request.args.get('month', '')
    if not month:
        t = date.today()
        month = t.strftime('%Y-%m')

    year, mon = int(month[:4]), int(month[5:7])
    last_day = calendar.monthrange(year, mon)[1]
    date_from = f'{month}-01'
    date_to = f'{month}-{last_day:02d}'

    # 1) api_orders: 주문 데이터 (네이버/쿠팡/자사몰)
    orders = db.query_api_orders(date_from=date_from, date_to=date_to)

    # 2) api_settlements: 로켓정산 + 광고비
    settlements = db.query_api_settlements(date_from=date_from, date_to=date_to)

    # ── 채널별 집계 ──
    # channel_key: (채널명, 서브타입)
    ch_daily = defaultdict(lambda: defaultdict(lambda: {
        'sales': 0, 'commission': 0, 'settlement': 0,
        'shipping': 0, 'orders_count': 0,
    }))

    # 취소/환불 상태 제외 함수
    def _is_cancelled(order_status, channel):
        """취소/환불/반품 주문 여부 판단."""
        s = (order_status or '').upper()
        if not s:
            return False
        # 네이버: CANCEL*, RETURN*, EXCHANGE*
        if 'CANCEL' in s or 'RETURN' in s or 'EXCHANGE' in s:
            return True
        # Cafe24: C로 시작하는 상태코드 (C00, C40 등)
        if channel == '자사몰' and s.startswith('C'):
            return True
        return False

    for o in orders:
        if _is_cancelled(o.get('order_status', ''), o.get('channel', '')):
            continue

        ch = o.get('channel', '')
        dt = o.get('order_date', '')[:10]
        fee = o.get('fee_detail') or {}

        # 네이버: delivery_type으로 N배송/일반 구분
        if ch == '스마트스토어':
            d_type = fee.get('delivery_type', '')
            if d_type == 'ARRIVAL_GUARANTEE':
                key = '스마트스토어_N배송'
            else:
                key = '스마트스토어_일반'
        else:
            key = ch

        d = ch_daily[key][dt]
        d['sales'] += int(o.get('total_amount') or 0)
        d['commission'] += int(o.get('commission') or 0)
        d['settlement'] += int(o.get('settlement_amount') or 0)
        d['shipping'] += int(o.get('shipping_fee') or 0)
        d['orders_count'] += 1

    # 정산 데이터 반영 (로켓 엑셀 + 쿠팡 revenue_fees)
    for s in settlements:
        ch = s.get('channel', '')
        sid = s.get('settlement_id', '')
        dt = s.get('settlement_date', '')[:10]

        if sid.startswith('ad_cost_'):
            continue  # 광고비는 아래에서 별도 처리

        if ch == '쿠팡로켓':
            # 로켓 Billing Statement 업로드 데이터
            d = ch_daily['쿠팡로켓'][dt]
            d['sales'] += int(s.get('gross_sales') or 0)
            d['commission'] += int(s.get('total_commission') or 0)
            d['settlement'] += int(s.get('net_settlement') or 0)
            d['shipping'] += int(s.get('shipping_fee_income') or 0) - int(s.get('shipping_fee_cost') or 0)

        elif ch == '쿠팡' and sid.startswith('revenue_'):
            # 쿠팡 Wing revenue-history: 수수료/정산 데이터 덮어쓰기
            # orders에서 매출/건수는 이미 있으므로, 수수료/정산만 반영
            d = ch_daily['쿠팡'][dt]
            d['commission'] = int(s.get('total_commission') or 0)
            d['settlement'] = int(s.get('net_settlement') or 0)
            d['shipping'] = int(s.get('shipping_fee_income') or 0) - int(s.get('shipping_fee_cost') or 0)

    # 쿠팡 Wing 수수료 추정 (revenue-history 미도착분 → 10.8% 추정)
    # revenue-history 실데이터가 들어오면 위에서 덮어쓰므로, 여기서는 0인 날만 추정
    for dt, d in ch_daily.get('쿠팡', {}).items():
        if d['sales'] > 0 and d['commission'] == 0:
            d['commission'] = int(d['sales'] * 0.108)
            d['settlement'] = d['sales'] - d['commission']

    # 자사몰 PG 수수료 추정 (API 미제공 → 3.3% 추정)
    for dt, d in ch_daily.get('자사몰', {}).items():
        if d['sales'] > 0 and d['commission'] == 0:
            d['commission'] = int(d['sales'] * 0.033)
            d['settlement'] = d['sales'] - d['commission']

    # ── 광고비 집계 ──
    ad_daily = defaultdict(lambda: defaultdict(int))  # {channel: {date: cost}}
    ad_items = []  # 개별 광고비 내역

    for s in settlements:
        sid = s.get('settlement_id', '')
        if sid.startswith('ad_cost_'):
            ch = s.get('channel', '')
            dt = s.get('settlement_date', '')[:10]
            cost = int(s.get('other_deductions') or 0)
            fb = s.get('fee_breakdown') or {}
            ad_daily[ch][dt] += cost
            ad_items.append({
                'channel': ch,
                'date': dt,
                'cost': cost,
                'memo': fb.get('memo', ''),
                'source': fb.get('source', ''),
            })

    # ── 채널별 월간 합계 ──
    channel_order = ['스마트스토어_N배송', '스마트스토어_일반', '쿠팡', '쿠팡로켓', '자사몰']
    channel_labels = {
        '스마트스토어_N배송': '네이버 N배송',
        '스마트스토어_일반': '네이버 일반',
        '쿠팡': '쿠팡 (Wing)',
        '쿠팡로켓': '쿠팡 로켓',
        '자사몰': '자사몰 (Cafe24)',
    }

    # ── 채널별 매출 합계 먼저 계산 (광고비 비율 배분용) ──
    ch_sales_totals = {}
    for key in channel_order:
        daily = ch_daily.get(key, {})
        ch_sales_totals[key] = sum(dd['sales'] for dd in daily.values())

    # 쿠팡 광고비: Wing + 로켓 매출 비율로 배분
    coupang_ad_total = sum(ad_daily.get('쿠팡', {}).values())
    coupang_wing_sales = ch_sales_totals.get('쿠팡', 0)
    coupang_rocket_sales = ch_sales_totals.get('쿠팡로켓', 0)
    coupang_total_sales = coupang_wing_sales + coupang_rocket_sales

    if coupang_total_sales > 0 and coupang_ad_total > 0:
        coupang_wing_ad = round(coupang_ad_total * coupang_wing_sales / coupang_total_sales)
        coupang_rocket_ad = coupang_ad_total - coupang_wing_ad  # 나머지
    else:
        coupang_wing_ad = coupang_ad_total
        coupang_rocket_ad = 0

    # 네이버 광고비: N배송 + 일반 매출 비율로 배분
    naver_ad_total = sum(ad_daily.get('스마트스토어', {}).values())
    naver_n_sales = ch_sales_totals.get('스마트스토어_N배송', 0)
    naver_normal_sales = ch_sales_totals.get('스마트스토어_일반', 0)
    naver_total_sales = naver_n_sales + naver_normal_sales

    if naver_total_sales > 0 and naver_ad_total > 0:
        naver_n_ad = round(naver_ad_total * naver_n_sales / naver_total_sales)
        naver_normal_ad = naver_ad_total - naver_n_ad
    else:
        naver_n_ad = naver_ad_total
        naver_normal_ad = 0

    # 채널별 광고비 배분 결과
    ad_cost_map = {
        '스마트스토어_N배송': naver_n_ad,
        '스마트스토어_일반': naver_normal_ad,
        '쿠팡': coupang_wing_ad,
        '쿠팡로켓': coupang_rocket_ad,
        '자사몰': 0,
    }

    channel_summary = []
    total = {'sales': 0, 'commission': 0, 'settlement': 0, 'shipping': 0,
             'ad_cost': 0, 'net': 0, 'orders_count': 0}

    for key in channel_order:
        daily = ch_daily.get(key, {})
        if not daily:
            continue

        s = {'sales': 0, 'commission': 0, 'settlement': 0, 'shipping': 0, 'orders_count': 0}
        for dd in daily.values():
            s['sales'] += dd['sales']
            s['commission'] += dd['commission']
            s['settlement'] += dd['settlement']
            s['shipping'] += dd['shipping']
            s['orders_count'] += dd['orders_count']

        ad_cost = ad_cost_map.get(key, 0)

        net = s['settlement'] - ad_cost
        comm_rate = (s['commission'] / s['sales'] * 100) if s['sales'] else 0

        channel_summary.append({
            'key': key,
            'label': channel_labels.get(key, key),
            'sales': s['sales'],
            'commission': s['commission'],
            'commission_rate': round(comm_rate, 1),
            'settlement': s['settlement'],
            'shipping': s['shipping'],
            'ad_cost': ad_cost,
            'net': net,
            'orders_count': s['orders_count'],
        })

        total['sales'] += s['sales']
        total['commission'] += s['commission']
        total['settlement'] += s['settlement']
        total['shipping'] += s['shipping']
        total['ad_cost'] += ad_cost
        total['net'] += net
        total['orders_count'] += s['orders_count']

    total['commission_rate'] = round(
        (total['commission'] / total['sales'] * 100) if total['sales'] else 0, 1
    )

    # ── 일별 합계 ──
    all_dates = set()
    for daily in ch_daily.values():
        all_dates.update(daily.keys())
    for ch_ads in ad_daily.values():
        all_dates.update(ch_ads.keys())

    daily_summary = []
    for dt in sorted(all_dates):
        ds = {'date': dt, 'sales': 0, 'commission': 0, 'settlement': 0, 'ad_cost': 0}
        for daily in ch_daily.values():
            dd = daily.get(dt, {})
            ds['sales'] += dd.get('sales', 0)
            ds['commission'] += dd.get('commission', 0)
            ds['settlement'] += dd.get('settlement', 0)
        for ch_ads in ad_daily.values():
            ds['ad_cost'] += ch_ads.get(dt, 0)
        ds['net'] = ds['settlement'] - ds['ad_cost']
        daily_summary.append(ds)

    return jsonify({
        'month': month,
        'channels': channel_summary,
        'total': total,
        'daily': daily_summary,
        'ad_items': sorted(ad_items, key=lambda x: x['date']),
    })
