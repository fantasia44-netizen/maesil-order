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
    extra = {}
    if channel == '쿠팡':
        payload['vendor_id'] = request.form.get('vendor_id', '').strip()
    elif channel == '자사몰':
        payload['mall_id'] = request.form.get('mall_id', '').strip()
    elif channel == '스마트스토어':
        # 네이버 검색광고 API 키 (extra_config에 저장)
        ad_cid = request.form.get('ad_customer_id', '').strip()
        ad_key = request.form.get('ad_api_key', '').strip()
        ad_secret = request.form.get('ad_secret_key', '').strip()
        if ad_cid or ad_key or ad_secret:
            extra['ad_customer_id'] = ad_cid
            extra['ad_api_key'] = ad_key
            extra['ad_secret_key'] = ad_secret

    if extra:
        # 기존 extra_config와 병합
        existing_list = db.query_marketplace_api_configs(channel=channel)
        existing = existing_list[0] if existing_list else None
        old_extra = (existing.get('extra_config') or {}) if existing else {}
        old_extra.update(extra)
        payload['extra_config'] = old_extra

    db.upsert_marketplace_api_config(payload)
    _log_action(f'마켓플레이스 API 설정 저장: {channel}')

    # 클라이언트 재로드
    mgr._load_configs(db)

    # 네이버 광고 클라이언트 재로드
    if channel == '스마트스토어' and extra.get('ad_customer_id'):
        try:
            from services.marketplace.naver_ad_client import NaverAdClient
            current_app.naver_ad = NaverAdClient(extra)
        except Exception as e:
            logger.warning(f'NaverAdClient 재로드 실패: {e}')

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
            sync_orders, sync_settlements, sync_revenue_fees, sync_ad_costs)

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

        # 네이버 검색광고 비용 동기화
        if channel == '스마트스토어' and sync_type in ('settlements', 'all'):
            ad_client = getattr(current_app, 'naver_ad', None)
            # 앱 시작 시 초기화 안 됐으면 DB에서 다시 로드 시도
            if not ad_client or not ad_client.is_ready:
                try:
                    from services.marketplace.naver_ad_client import NaverAdClient
                    cfgs = db.query_marketplace_api_configs(channel='스마트스토어')
                    if cfgs:
                        ec = (cfgs[0].get('extra_config') or {})
                        if ec.get('ad_customer_id'):
                            ad_client = NaverAdClient(ec)
                            current_app.naver_ad = ad_client
                except Exception:
                    pass
            if ad_client and ad_client.is_ready:
                results['ad_costs'] = sync_ad_costs(
                    db, ad_client, date_from, date_to,
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
    """채널별 광고비 수동 기입 → api_settlements 저장.
    input_mode: 'daily' (일별, date 필요) / 'monthly' (월별, month 필요)
    """
    db = current_app.db

    channel = request.form.get('channel', '').strip()
    input_mode = request.form.get('input_mode', 'daily').strip()
    save_mode = request.form.get('save_mode', 'replace').strip()
    cost = request.form.get('cost', '0').strip().replace(',', '')
    memo = request.form.get('memo', '').strip()

    if input_mode == 'monthly':
        month_str = request.form.get('month', '').strip()
        if not channel or not month_str:
            return jsonify({'error': '채널과 월을 입력하세요'}), 400
        date_str = f'{month_str}-01'
        settle_id = f'ad_cost_monthly_{channel}_{month_str}'
        label = month_str
    else:
        date_str = request.form.get('date', '').strip()
        if not channel or not date_str:
            return jsonify({'error': '채널과 날짜를 입력하세요'}), 400
        settle_id = f'ad_cost_{date_str}'
        label = date_str

    try:
        cost_int = int(float(cost))
    except (ValueError, TypeError):
        return jsonify({'error': '광고비 금액이 올바르지 않습니다'}), 400

    # 추가 모드: 기존 값에 합산
    if save_mode == 'add':
        existing = db.query_api_settlements(channel, date_str[:7])
        for s in existing:
            if s.get('settlement_id') == settle_id:
                cost_int += int(s.get('other_deductions') or 0)
                break

    settlement = {
        'channel': channel,
        'settlement_date': date_str,
        'settlement_id': settle_id,
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
            'input_mode': input_mode,
            'memo': memo,
        },
    }

    db.upsert_api_settlements_batch([settlement])
    _log_action(f'광고비 기입: {channel} {label} {cost_int:,}원 ({input_mode})')

    return jsonify({
        'success': True,
        'channel': channel,
        'date': label,
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

            raw_channel = str(row[col_map['channel']]).strip() if 'channel' in col_map else '쿠팡'
            # 채널명 표준화 (엑셀 입력값 → DB 표준 채널명)
            ch_lower = raw_channel.replace(' ', '')
            if '쿠팡' in ch_lower:
                channel = '쿠팡'
            elif '네이버' in ch_lower or '스마트스토어' in ch_lower or 'naver' in ch_lower.lower():
                channel = '스마트스토어'
            elif '자사몰' in ch_lower or 'cafe24' in ch_lower.lower():
                channel = '자사몰'
            else:
                channel = raw_channel
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


# ── 네이버 차감항목 (쿠폰차감/바우처적립) 월별 입력 ──

@marketplace_bp.route('/deductions', methods=['POST'])
@role_required('admin', 'general')
def save_deductions():
    """스마트스토어 월별 차감항목 저장 (쿠폰차감, 바우처적립)."""
    db = current_app.db
    month = request.form.get('month', '').strip()  # YYYY-MM
    coupon = request.form.get('coupon_deduct', '0').strip().replace(',', '')
    voucher = request.form.get('voucher_deduct', '0').strip().replace(',', '')

    if not month or len(month) != 7:
        return jsonify({'error': '월(YYYY-MM)을 입력하세요'}), 400

    try:
        coupon_int = int(float(coupon))
        voucher_int = int(float(voucher))
    except (ValueError, TypeError):
        return jsonify({'error': '금액이 올바르지 않습니다'}), 400

    settlements = []
    if coupon_int:
        settlements.append({
            'channel': '스마트스토어',
            'settlement_date': f'{month}-01',
            'settlement_id': f'deduct_coupon_{month}',
            'gross_sales': 0, 'total_commission': 0,
            'shipping_fee_income': 0, 'shipping_fee_cost': 0,
            'coupon_discount': coupon_int, 'point_discount': 0,
            'other_deductions': coupon_int,
            'net_settlement': -coupon_int,
            'fee_breakdown': {'source': 'manual_deduction', 'type': 'coupon',
                              'label': '쿠폰차감'},
        })
    if voucher_int:
        settlements.append({
            'channel': '스마트스토어',
            'settlement_date': f'{month}-01',
            'settlement_id': f'deduct_voucher_{month}',
            'gross_sales': 0, 'total_commission': 0,
            'shipping_fee_income': 0, 'shipping_fee_cost': 0,
            'coupon_discount': 0, 'point_discount': voucher_int,
            'other_deductions': voucher_int,
            'net_settlement': -voucher_int,
            'fee_breakdown': {'source': 'manual_deduction', 'type': 'voucher',
                              'label': '바우처적립'},
        })

    if settlements:
        db.upsert_api_settlements_batch(settlements)
        _log_action(f'차감항목 입력: {month} 쿠폰={coupon_int:,} 바우처={voucher_int:,}')

    return jsonify({
        'success': True,
        'month': month,
        'coupon_deduct': coupon_int,
        'voucher_deduct': voucher_int,
    })


# ── 정산서 업로드 (채널별 엑셀 파싱) ──

@marketplace_bp.route('/settlement/upload', methods=['POST'])
@role_required('admin', 'general')
def upload_settlement():
    """채널별 정산서 엑셀 업로드 → api_settlements 저장."""
    import io
    db = current_app.db
    channel = request.form.get('channel', '').strip()
    file = request.files.get('file')

    if not channel:
        return jsonify({'error': '채널을 선택하세요'}), 400
    if not file or not file.filename:
        return jsonify({'error': '파일을 선택하세요'}), 400

    file_bytes = file.read()
    fname = file.filename.lower()

    try:
        if channel in ('스마트스토어', '해미애찬'):
            result = _parse_naver_settlement(file_bytes, channel)
        elif channel == '쿠팡':
            result = _parse_coupang_wing_settlement(file_bytes)
        elif channel == '11번가':
            result = _parse_11st_settlement(file_bytes)
        elif channel == '자사몰':
            result = _parse_toss_settlement(file_bytes)
        elif channel == '오아시스':
            result = _parse_oasis_settlement(file_bytes)
        elif channel in ('옥션', '지마켓'):
            result = _parse_auction_settlement(file_bytes, channel)
        else:
            return jsonify({'error': f'지원하지 않는 채널: {channel}'}), 400

        if not result['settlements']:
            return jsonify({'error': '파싱된 데이터가 없습니다'}), 400

        db.upsert_api_settlements_batch(result['settlements'])
        _log_action(f'정산서 업로드: {channel} {result["summary"]}')

        return jsonify({
            'success': True,
            'channel': channel,
            'count': len(result['settlements']),
            'summary': result['summary'],
        })
    except Exception as e:
        logger.error(f'정산서 업로드 오류 ({channel}): {e}')
        return jsonify({'error': str(e)}), 500


def _parse_naver_settlement(file_bytes, channel):
    """네이버 항목별정산 (SellerDailySettle.xlsx) 파싱.
    Row 1 헤더: 정산예정일,정산완료일,정산금액,...,정산기준금액[6],수수료합계[7],혜택정산[8],...
    """
    import openpyxl, io
    wb = openpyxl.load_workbook(io.BytesIO(file_bytes))
    ws = wb[wb.sheetnames[0]]

    settlements = []
    total_sales = 0
    total_settle = 0
    month_set = set()

    for r in range(2, ws.max_row + 1):
        date_str = str(ws.cell(r, 1).value or '').strip()
        if not date_str or len(date_str) < 8:
            continue
        # 2026.03.03 → 2026-03-03
        date_str = date_str.replace('.', '-')
        if len(date_str) == 10:
            pass
        else:
            continue

        settle_amt = int(ws.cell(r, 3).value or 0)       # 정산금액
        base_amt = int(ws.cell(r, 6).value or 0)          # 정산기준금액
        fee_total = int(ws.cell(r, 7).value or 0)         # 수수료합계 (음수)
        benefit = int(ws.cell(r, 8).value or 0)            # 혜택정산 (음수)
        deduct = int(ws.cell(r, 9).value or 0)             # 일별 공제/환급

        settlements.append({
            'channel': channel,
            'settlement_date': date_str,
            'settlement_id': f'nsettle_{date_str}',
            'gross_sales': base_amt,
            'total_commission': abs(fee_total),
            'shipping_fee_income': 0,
            'shipping_fee_cost': 0,
            'coupon_discount': abs(benefit),
            'point_discount': 0,
            'other_deductions': abs(deduct),
            'net_settlement': settle_amt,
            'fee_breakdown': {
                'source': 'naver_settlement',
                'raw_fee': fee_total,
                'raw_benefit': benefit,
                'raw_deduct': deduct,
            },
        })
        total_sales += base_amt
        total_settle += settle_amt
        month_set.add(date_str[:7])

    wb.close()
    months = ','.join(sorted(month_set))
    return {
        'settlements': settlements,
        'summary': f'{months} {len(settlements)}일, 매출 {total_sales:,}원 → 정산 {total_settle:,}원',
    }


def _parse_coupang_wing_settlement(file_bytes):
    """쿠팡Wing 정산내역 (MSF_PAYMENT_REVENUE_DETAIL.xlsx) 파싱.
    Row 1 헤더: 주문번호[1],...,판매액[10],판매자할인쿠폰[11],...,판매수수료[14],...,정산금액[18],...,정산예정일[25]
    값이 문자열이므로 int 변환 필요. 정산예정일 기준 월별 집계.
    """
    import openpyxl, io
    from collections import defaultdict

    wb = openpyxl.load_workbook(io.BytesIO(file_bytes))
    ws = wb[wb.sheetnames[0]]

    def to_int(v):
        if v is None or v == '':
            return 0
        try:
            return int(float(str(v).replace(',', '')))
        except (ValueError, TypeError):
            return 0

    # 월별 집계
    monthly = defaultdict(lambda: {'sales': 0, 'fee': 0, 'coupon': 0, 'settle': 0, 'cnt': 0})
    for r in range(2, ws.max_row + 1):
        sales = to_int(ws.cell(r, 10).value)  # 판매액
        if not sales:
            continue
        coupon = to_int(ws.cell(r, 11).value)     # 판매자 할인쿠폰(A+B)
        fee = to_int(ws.cell(r, 14).value)         # 판매수수료
        settle = to_int(ws.cell(r, 18).value)      # 정산금액
        settle_date = str(ws.cell(r, 25).value or '')[:10]  # 정산예정일

        month_key = settle_date[:7] if len(settle_date) >= 7 else 'unknown'
        m = monthly[month_key]
        m['sales'] += sales
        m['fee'] += fee
        m['coupon'] += coupon
        m['settle'] += settle
        m['cnt'] += 1

    wb.close()

    settlements = []
    summaries = []
    for mk in sorted(monthly):
        m = monthly[mk]
        if mk == 'unknown':
            continue
        settlements.append({
            'channel': '쿠팡',
            'settlement_date': f'{mk}-01',
            'settlement_id': f'wsettle_{mk}',
            'gross_sales': m['sales'],
            'total_commission': m['fee'],
            'shipping_fee_income': 0,
            'shipping_fee_cost': 0,
            'coupon_discount': m['coupon'],
            'point_discount': 0,
            'other_deductions': 0,
            'net_settlement': m['settle'],
            'fee_breakdown': {
                'source': 'coupang_wing_settlement',
                'order_count': m['cnt'],
            },
        })
        summaries.append(f'{mk}: {m["sales"]:,}원→{m["settle"]:,}원')

    return {
        'settlements': settlements,
        'summary': ', '.join(summaries),
    }


def _parse_11st_settlement(file_bytes):
    """11번가 정산확정건 (.xls) 파싱.
    헤더 Row5(0-indexed): ...,정산금액[17],판매금액합계[18],...,공제금액합계[20],...
    서비스이용료(상품)[39],제휴마케팅(상품)[40],할인쿠폰이용료[43],후불광고비[53]
    """
    import xlrd, io, re

    wb = xlrd.open_workbook(file_contents=file_bytes)
    ws = wb.sheet_by_index(0)

    # 제목에서 기간 추출
    title = str(ws.cell_value(1, 0)) if ws.nrows > 1 else ''
    # 정산_확정건_ (2026/02/01~2026/02/28) → 2026-02
    match = re.search(r'(\d{4})/(\d{2})/\d{2}~\d{4}/(\d{2})/\d{2}', title)
    month_key = f'{match.group(1)}-{match.group(2)}' if match else 'unknown'

    def to_int(v):
        try:
            return int(float(v))
        except (ValueError, TypeError):
            return 0

    total_sales = 0
    total_fee = 0
    total_coupon = 0
    total_ad = 0
    total_settle = 0
    cnt = 0

    for r in range(6, ws.nrows):
        no = ws.cell_value(r, 0)
        if not no:
            continue
        cnt += 1
        total_sales += to_int(ws.cell_value(r, 18))    # 판매금액합계
        total_settle += to_int(ws.cell_value(r, 17))    # 정산금액
        # 수수료 = 서비스이용료(상품+배송) + 제휴마케팅(상품+배송)
        total_fee += (to_int(ws.cell_value(r, 39)) + to_int(ws.cell_value(r, 40))
                      + to_int(ws.cell_value(r, 41)) + to_int(ws.cell_value(r, 42)))
        total_coupon += to_int(ws.cell_value(r, 43))    # 할인쿠폰이용료
        total_ad += to_int(ws.cell_value(r, 53))         # 후불광고비

    settlements = []
    if cnt > 0 and month_key != 'unknown':
        settlements.append({
            'channel': '11번가',
            'settlement_date': f'{month_key}-01',
            'settlement_id': f'11settle_{month_key}',
            'gross_sales': total_sales,
            'total_commission': total_fee,
            'shipping_fee_income': 0,
            'shipping_fee_cost': 0,
            'coupon_discount': total_coupon,
            'point_discount': 0,
            'other_deductions': total_ad,
            'net_settlement': total_settle,
            'fee_breakdown': {
                'source': '11st_settlement',
                'order_count': cnt,
                'ad_cost': total_ad,
            },
        })

    return {
        'settlements': settlements,
        'summary': f'{month_key} {cnt}건, 매출 {total_sales:,}원 → 정산 {total_settle:,}원',
    }


def _parse_toss_settlement(file_bytes):
    """토스페이 PG 정산내역 (정산내역_요약.xlsx) 파싱.
    Row 1 헤더: 정산계좌[1],정산액입금일[2],매출일[3],...,매출액(A)[6],PG수수료합(D)[9],당일정산액(E)[10]
    """
    import openpyxl, io

    wb = openpyxl.load_workbook(io.BytesIO(file_bytes))
    ws = wb[wb.sheetnames[0]]

    settlements = []
    total_sales = 0
    total_settle = 0
    month_set = set()

    for r in range(2, ws.max_row + 1):
        sales_date = str(ws.cell(r, 3).value or '').strip()  # 매출일
        if not sales_date or len(sales_date) < 8:
            continue

        sales = int(float(ws.cell(r, 6).value or 0))    # 매출액
        fee = int(float(ws.cell(r, 9).value or 0))      # PG수수료 합 (B+C)
        settle = int(float(ws.cell(r, 10).value or 0))   # 당일 정산액

        settlements.append({
            'channel': '자사몰',
            'settlement_date': sales_date,
            'settlement_id': f'tsettle_{sales_date}',
            'gross_sales': sales,
            'total_commission': fee,
            'shipping_fee_income': 0,
            'shipping_fee_cost': 0,
            'coupon_discount': 0,
            'point_discount': 0,
            'other_deductions': 0,
            'net_settlement': settle,
            'fee_breakdown': {'source': 'toss_settlement'},
        })
        total_sales += sales
        total_settle += settle
        month_set.add(sales_date[:7])

    wb.close()
    months = ','.join(sorted(month_set))
    return {
        'settlements': settlements,
        'summary': f'{months} {len(settlements)}일, 매출 {total_sales:,}원 → 정산 {total_settle:,}원',
    }


def _parse_oasis_settlement(file_bytes):
    """오아시스 매출정산내역.xlsx 파싱.
    Row1=정산월, Row5 헤더: 주문번호,주문일,...,판매금액[8],배송비[9],합계금액[10]
    수수료 8% 고정 (판매+배송 합계 기준).
    """
    import openpyxl, io

    wb = openpyxl.load_workbook(io.BytesIO(file_bytes))
    ws = wb[wb.sheetnames[0]]

    # 메타: Row1 = 정산월 (예: 2026-02)
    month_raw = str(ws.cell(1, 2).value or '').strip()  # 2026-02
    month_key = month_raw[:7] if len(month_raw) >= 7 else 'unknown'

    total_sales = 0
    total_ship = 0
    cnt = 0

    for r in range(6, ws.max_row + 1):
        sales = ws.cell(r, 8).value   # 판매금액
        ship = ws.cell(r, 9).value    # 배송비
        if not sales:
            continue
        cnt += 1
        total_sales += int(float(sales))
        total_ship += int(float(ship or 0))

    wb.close()

    gross = total_sales + total_ship  # 판매+배송 합계
    commission = int(gross * 0.08)     # 8% 고정 수수료
    net = gross - commission

    settlements = []
    if cnt > 0 and month_key != 'unknown':
        settlements.append({
            'channel': '오아시스',
            'settlement_date': f'{month_key}-01',
            'settlement_id': f'osettle_{month_key}',
            'gross_sales': gross,
            'total_commission': commission,
            'shipping_fee_income': total_ship,
            'shipping_fee_cost': 0,
            'coupon_discount': 0,
            'point_discount': 0,
            'other_deductions': 0,
            'net_settlement': net,
            'fee_breakdown': {
                'source': 'oasis_settlement',
                'sales_only': total_sales,
                'shipping': total_ship,
                'commission_rate': 0.08,
                'order_count': cnt,
            },
        })

    return {
        'settlements': settlements,
        'summary': f'{month_key} {cnt}건, 합계 {gross:,}원 - 수수료(8%) {commission:,}원 = {net:,}원',
    }


def _parse_auction_settlement(file_bytes, channel):
    """옥션/지마켓 정산서 파싱.
    상품판매 파일: Row0 헤더(47컬럼) - 결제금액[22], 서비스이용료[30], 최종정산금[27]
    배송비 파일: Row0 헤더(14컬럼) - 배송비[10], 배송수수료[11], 배송비정산액[12]
    파일 유형은 컬럼 수로 자동 판별.
    """
    import io, xlrd
    wb = xlrd.open_workbook(file_contents=file_bytes)
    ws = wb.sheet_by_index(0)

    if ws.nrows < 2:
        raise ValueError('데이터가 없습니다')

    prefix = 'auction_' if channel == '옥션' else 'gmarket_'
    is_shipping = ws.ncols < 20  # 배송비 파일은 14컬럼

    total_sales = 0
    total_commission = 0
    total_settlement = 0
    cnt = 0
    month_set = set()

    for r in range(1, ws.nrows):
        if is_shipping:
            sales = int(float(ws.cell_value(r, 10) or 0))    # 배송비
            comm = int(float(ws.cell_value(r, 11) or 0))      # 배송수수료
            settle = int(float(ws.cell_value(r, 12) or 0))    # 배송비정산액
            date_val = str(ws.cell_value(r, 9))[:10]           # 정산완료일
        else:
            sales = int(float(ws.cell_value(r, 22) or 0))     # 결제금액
            comm = int(float(ws.cell_value(r, 30) or 0))      # 서비스이용료
            settle = int(float(ws.cell_value(r, 27) or 0))    # 최종정산금
            date_val = str(ws.cell_value(r, 15))[:10]          # 정산완료일

        total_sales += sales
        total_commission += comm
        total_settlement += settle
        cnt += 1
        if date_val and len(date_val) >= 7:
            month_set.add(date_val[:7])

    if not month_set:
        raise ValueError('날짜를 파싱할 수 없습니다')

    month_key = sorted(month_set)[0]
    file_type = 'ship' if is_shipping else 'sales'
    settle_id = f'{prefix}{file_type}_{month_key}'

    settlements = [{
        'channel': channel,
        'settlement_date': f'{month_key}-01',
        'settlement_id': settle_id,
        'gross_sales': total_sales,
        'total_commission': abs(total_commission),
        'coupon_discount': 0,
        'other_deductions': 0,
        'net_settlement': total_settlement,
        'fee_breakdown': {
            'source': f'{channel.lower()}_settlement',
            'file_type': file_type,
            'order_count': cnt,
        },
    }]

    return {
        'settlements': settlements,
        'summary': f'{channel} {file_type} {month_key} {cnt}건, 매출 {total_sales:,}원, 정산 {total_settlement:,}원',
    }


# ── 온라인 플랫폼 정산 대시보드 ──

@marketplace_bp.route('/sales')
@role_required('admin', 'general')
def sales():
    """온라인 플랫폼 정산 대시보드."""
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

    # 1) api_orders: 주문 데이터 (네이버/쿠팡/자사몰) — 필요 컬럼만 조회 (메모리 절약)
    _sales_cols = "channel,order_date,order_status,total_amount,commission,settlement_amount,shipping_fee,fee_detail"
    orders = db.query_api_orders(date_from=date_from, date_to=date_to, columns=_sales_cols)

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

        # 네이버: delivery_type으로 N배송/일반 구분 (통합 후 합산)
        if ch in ('스마트스토어', '해미애찬'):
            d_type = fee.get('delivery_type', '')
            if d_type == 'ARRIVAL_GUARANTEE':
                key = f'{ch}_N배송'
            else:
                key = f'{ch}_일반'
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

    # 차감항목 집계 (쿠폰차감/바우처적립)
    deductions = {'coupon': 0, 'voucher': 0}  # 월 합계

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
        elif sid.startswith('deduct_coupon_'):
            deductions['coupon'] += int(s.get('other_deductions') or 0)
        elif sid.startswith('deduct_voucher_'):
            deductions['voucher'] += int(s.get('other_deductions') or 0)

    # ── 정산서 업로드 데이터 처리 ──
    # settlement_id prefix로 정산서 데이터 구분
    settle_data = {}  # {channel: {sales, commission, coupon, settle, source}}
    for s in settlements:
        sid = s.get('settlement_id', '')
        ch = s.get('channel', '')

        if sid.startswith('nsettle_'):
            # 네이버 정산서 (스마트스토어 or 해미애찬)
            if ch not in settle_data:
                settle_data[ch] = {'sales': 0, 'commission': 0, 'coupon': 0,
                                   'settle': 0, 'ad_cost': 0, 'source': 'settlement'}
            sd = settle_data[ch]
            sd['sales'] += int(s.get('gross_sales') or 0)
            sd['commission'] += int(s.get('total_commission') or 0)
            sd['coupon'] += int(s.get('coupon_discount') or 0)
            sd['settle'] += int(s.get('net_settlement') or 0)

        elif sid.startswith('wsettle_'):
            # 쿠팡Wing 정산서
            if '쿠팡' not in settle_data:
                settle_data['쿠팡'] = {'sales': 0, 'commission': 0, 'coupon': 0,
                                      'settle': 0, 'ad_cost': 0, 'source': 'settlement'}
            sd = settle_data['쿠팡']
            sd['sales'] += int(s.get('gross_sales') or 0)
            sd['commission'] += int(s.get('total_commission') or 0)
            sd['coupon'] += int(s.get('coupon_discount') or 0)
            sd['settle'] += int(s.get('net_settlement') or 0)

        elif sid.startswith('11settle_'):
            if '11번가' not in settle_data:
                settle_data['11번가'] = {'sales': 0, 'commission': 0, 'coupon': 0,
                                        'settle': 0, 'ad_cost': 0, 'source': 'settlement'}
            sd = settle_data['11번가']
            sd['sales'] += int(s.get('gross_sales') or 0)
            sd['commission'] += int(s.get('total_commission') or 0)
            sd['coupon'] += int(s.get('coupon_discount') or 0)
            sd['settle'] += int(s.get('net_settlement') or 0)
            sd['ad_cost'] += int(s.get('other_deductions') or 0)  # 후불광고비

        elif sid.startswith('tsettle_'):
            if '자사몰' not in settle_data:
                settle_data['자사몰'] = {'sales': 0, 'commission': 0, 'coupon': 0,
                                        'settle': 0, 'ad_cost': 0, 'source': 'settlement'}
            sd = settle_data['자사몰']
            sd['sales'] += int(s.get('gross_sales') or 0)
            sd['commission'] += int(s.get('total_commission') or 0)
            sd['settle'] += int(s.get('net_settlement') or 0)

        elif sid.startswith('osettle_'):
            if '오아시스' not in settle_data:
                settle_data['오아시스'] = {'sales': 0, 'commission': 0, 'coupon': 0,
                                          'settle': 0, 'ad_cost': 0, 'source': 'settlement'}
            sd = settle_data['오아시스']
            sd['sales'] += int(s.get('gross_sales') or 0)
            sd['commission'] += int(s.get('total_commission') or 0)
            sd['settle'] += int(s.get('net_settlement') or 0)

        elif sid.startswith('auction_'):
            if '옥션' not in settle_data:
                settle_data['옥션'] = {'sales': 0, 'commission': 0, 'coupon': 0,
                                       'settle': 0, 'ad_cost': 0, 'source': 'settlement'}
            sd = settle_data['옥션']
            sd['sales'] += int(s.get('gross_sales') or 0)
            sd['commission'] += int(s.get('total_commission') or 0)
            sd['settle'] += int(s.get('net_settlement') or 0)

        elif sid.startswith('gmarket_'):
            if '지마켓' not in settle_data:
                settle_data['지마켓'] = {'sales': 0, 'commission': 0, 'coupon': 0,
                                         'settle': 0, 'ad_cost': 0, 'source': 'settlement'}
            sd = settle_data['지마켓']
            sd['sales'] += int(s.get('gross_sales') or 0)
            sd['commission'] += int(s.get('total_commission') or 0)
            sd['settle'] += int(s.get('net_settlement') or 0)

    # ── 채널별 월간 합계 ──
    channel_order = [
        '스마트스토어', '해미애찬',
        '쿠팡', '쿠팡로켓',
        '11번가', '자사몰', '오아시스',
        '옥션', '지마켓',
    ]
    channel_labels = {
        '스마트스토어': '스마트스토어(배마마)',
        '해미애찬': '스마트스토어(해미애찬)',
        '쿠팡': '쿠팡(Wing)',
        '쿠팡로켓': '쿠팡(로켓)',
        '11번가': '11번가',
        '자사몰': '자사몰(Cafe24)',
        '오아시스': '오아시스',
        '옥션': '옥션',
        '지마켓': '지마켓',
    }

    # ── API 기반 네이버 N배송+일반 → 스토어별 통합 ──
    for store in ('스마트스토어', '해미애찬'):
        if f'{store}_N배송' in ch_daily or f'{store}_일반' in ch_daily:
            merged = defaultdict(lambda: {'sales': 0, 'commission': 0, 'settlement': 0,
                                          'shipping': 0, 'orders_count': 0})
            for sub_key in (f'{store}_N배송', f'{store}_일반'):
                for dt, dd in ch_daily.get(sub_key, {}).items():
                    m = merged[dt]
                    for k in ('sales', 'commission', 'settlement', 'shipping', 'orders_count'):
                        m[k] += dd[k]
            ch_daily[store] = dict(merged)
            # 서브키 제거 (daily_summary에서 중복 집계 방지)
            ch_daily.pop(f'{store}_N배송', None)
            ch_daily.pop(f'{store}_일반', None)

    # ── 채널별 매출 합계 (광고비 비율 배분용) ──
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
        coupang_rocket_ad = coupang_ad_total - coupang_wing_ad
    else:
        coupang_wing_ad = coupang_ad_total
        coupang_rocket_ad = 0

    # 네이버 광고비: 배마마 + 해미애찬 매출 비율로 배분
    naver_ad_total = (sum(ad_daily.get('스마트스토어', {}).values())
                      + sum(ad_daily.get('해미애찬', {}).values()))
    naver_bm_sales = ch_sales_totals.get('스마트스토어', 0)
    naver_hm_sales = ch_sales_totals.get('해미애찬', 0)
    # 정산서 매출이 있으면 그 값 사용
    if settle_data.get('스마트스토어'):
        naver_bm_sales = settle_data['스마트스토어']['sales']
    if settle_data.get('해미애찬'):
        naver_hm_sales = settle_data['해미애찬']['sales']
    naver_total_sales = naver_bm_sales + naver_hm_sales

    if naver_total_sales > 0 and naver_ad_total > 0:
        naver_bm_ad = round(naver_ad_total * naver_bm_sales / naver_total_sales)
        naver_hm_ad = naver_ad_total - naver_bm_ad
    else:
        naver_bm_ad = naver_ad_total
        naver_hm_ad = 0

    # 채널별 광고비 배분
    ad_cost_map = {
        '스마트스토어': naver_bm_ad,
        '해미애찬': naver_hm_ad,
        '쿠팡': coupang_wing_ad,
        '쿠팡로켓': coupang_rocket_ad,
        '11번가': 0,
        '자사몰': 0,
        '오아시스': 0,
    }

    # 네이버 차감항목 (쿠폰차감+바우처적립): 배마마/해미애찬 매출 비율 배분
    total_deduct = deductions['coupon'] + deductions['voucher']
    if naver_total_sales > 0 and total_deduct != 0:
        deduct_bm = round(total_deduct * naver_bm_sales / naver_total_sales)
        deduct_hm = total_deduct - deduct_bm
    else:
        deduct_bm = total_deduct
        deduct_hm = 0
    deduct_map = {
        '스마트스토어': deduct_bm,
        '해미애찬': deduct_hm,
        '쿠팡': 0, '쿠팡로켓': 0, '11번가': 0, '자사몰': 0, '오아시스': 0,
    }

    channel_summary = []
    total = {'sales': 0, 'commission': 0, 'settlement': 0, 'shipping': 0,
             'ad_cost': 0, 'deductions': 0, 'net': 0, 'orders_count': 0}

    for key in channel_order:
        # 정산서 데이터 우선, 없으면 API 추정
        sd = settle_data.get(key)
        daily = ch_daily.get(key, {})

        if sd:
            # 정산서 기준
            s_sales = sd['sales']
            s_commission = sd['commission']
            s_settlement = sd['settle']
            s_coupon = sd['coupon']
            s_shipping = 0
            s_orders = sum(dd['orders_count'] for dd in daily.values()) if daily else 0
            source = 'settlement'
            # 정산서에 포함된 광고비 (11번가 후불광고비 등)
            settle_ad = sd.get('ad_cost', 0)
        elif daily:
            # API 추정
            s_sales = sum(dd['sales'] for dd in daily.values())
            s_commission = sum(dd['commission'] for dd in daily.values())
            s_settlement = sum(dd['settlement'] for dd in daily.values())
            s_coupon = 0
            s_shipping = sum(dd['shipping'] for dd in daily.values())
            s_orders = sum(dd['orders_count'] for dd in daily.values())
            source = 'api_estimate'
            settle_ad = 0
        else:
            continue

        ad_cost = ad_cost_map.get(key, 0) + settle_ad
        deduct = deduct_map.get(key, 0)
        # 정산서의 쿠폰차감은 이미 정산금액에 반영됨 → deduct에 포함하지 않음
        # 단, 수동 입력 차감(deduct_map)은 API 추정 모드에서만 의미 있음
        if source == 'settlement':
            deduct = 0  # 정산서는 이미 차감 반영됨

        net = s_settlement - ad_cost - deduct
        comm_rate = (s_commission / s_sales * 100) if s_sales else 0
        profit_rate = (net / s_sales * 100) if s_sales else 0

        channel_summary.append({
            'key': key,
            'label': channel_labels.get(key, key),
            'sales': s_sales,
            'commission': s_commission,
            'commission_rate': round(comm_rate, 1),
            'settlement': s_settlement,
            'shipping': s_shipping,
            'ad_cost': ad_cost,
            'deductions': deduct + s_coupon,  # 표시용: 쿠폰차감 포함
            'net': net,
            'orders_count': s_orders,
            'source': source,
            'profit_rate': round(profit_rate, 1),
        })

        total['sales'] += s_sales
        total['commission'] += s_commission
        total['settlement'] += s_settlement
        total['shipping'] += s_shipping
        total['ad_cost'] += ad_cost
        total['deductions'] += deduct + s_coupon
        total['net'] += net
        total['orders_count'] += s_orders

    total['commission_rate'] = round(
        (total['commission'] / total['sales'] * 100) if total['sales'] else 0, 1
    )
    total['profit_rate'] = round(
        (total['net'] / total['sales'] * 100) if total['sales'] else 0, 1
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
        'deductions': deductions,
    })
