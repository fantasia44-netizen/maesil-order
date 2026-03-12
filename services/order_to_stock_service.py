"""
order_to_stock_service.py — 주문 → 출고 자동처리 서비스 (Phase 2).

order_transactions에서 미처리 주문을 가져와:
1. BOM 분해 (세트 → 개별 자재)
2. 창고 라우팅 (라인코드 기반)
3. FIFO 재고 차감 (stock_ledger SALES_OUT)
4. order_transactions.is_outbound_done = True 업데이트

매출은 order_transactions에 이미 저장된 금액(total_amount, settlement, commission)을
조회 시 집계하므로 별도 매출 기록(daily_revenue)을 하지 않습니다.
"""
import logging
import unicodedata
from datetime import datetime
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

from services.channel_config import CHANNEL_REVENUE_MAP

logger = logging.getLogger(__name__)

KST = ZoneInfo("Asia/Seoul")


def _now_kst():
    """KST 기준 현재 시각."""
    return datetime.now(KST)


def _norm(text):
    """NFC 정규화 + strip"""
    return unicodedata.normalize('NFC', str(text).strip())


def _stock_date():
    """재고차감 기본 반영일 (오늘). order_date가 없는 경우의 fallback."""
    return _now_kst().strftime('%Y-%m-%d')


def _load_bom_map(db):
    """DB의 bom_master → {channel: {set_name: [(component, qty), ...]}} 구조 로드."""
    rows = db.query_bom_master_all()
    bom = {}  # { "모든채널": {set_name: [(comp, qty), ...]}, "쿠팡전용": {...} }
    for r in rows:
        ch = _norm(r.get('channel', '모든채널'))
        set_name = _norm(r.get('set_name', ''))
        comps_str = str(r.get('components', '')).strip()
        if not set_name or not comps_str:
            continue
        if ch not in bom:
            bom[ch] = {}
        # components: "품목Ax3,품목Bx2" 형식
        parsed = []
        for c in comps_str.split(','):
            c = c.strip()
            if 'x' in c:
                parts = c.rsplit('x', 1)
                try:
                    parsed.append((_norm(parts[0]), int(parts[1])))
                except (ValueError, IndexError):
                    parsed.append((_norm(c), 1))
            elif c:
                parsed.append((_norm(c), 1))
        if parsed:
            bom[ch][set_name] = parsed
    return bom


def _load_option_map(db):
    """option_master → {normalized_name: {sort_order, line_code}} 매핑 로드."""
    try:
        rows = db.query_option_master()
        if not rows:
            return {}
        opt = {}
        for r in rows:
            nm = _norm(r.get('product_name', ''))
            if not nm:
                continue
            if nm not in opt:
                opt[nm] = {
                    'sort_order': int(r.get('sort_order', 999) or 999),
                    'line_code': str(r.get('line_code', '0') or '0').strip(),
                }
        return opt
    except Exception:
        return {}


def _get_warehouse(name, opt_map):
    """품목명 → 출고 창고 결정 (라인코드 기반)."""
    n = _norm(name)
    if opt_map and n in opt_map:
        lc = opt_map[n].get('line_code', '0')
        return "해서" if lc == '5' else "넥스원"
    return "넥스원"


def _decompose(name, qty, current_bom, fallback_bom=None):
    """재귀 세트 분해. aggregator.py의 decompose()와 동일 로직."""
    n = _norm(name)
    if n in current_bom:
        bom = current_bom
    elif fallback_bom and n in fallback_bom:
        bom = fallback_bom
    else:
        return {name: qty}
    res = {}
    for c_nm, c_qty in bom[n]:
        sub = _decompose(c_nm, qty * c_qty, current_bom, fallback_bom)
        for k, v in sub.items():
            res[k] = res.get(k, 0) + v
    return res


def process_orders_to_stock(db, date_from=None, date_to=None, channel=None,
                             force_shortage=False):
    """미처리 주문 → 출고 자동 처리.

    매출은 order_transactions에 이미 저장되어 있으므로 별도 기록하지 않음.
    재고차감(SALES_OUT) + 주문완료표시(is_outbound_done)만 수행.

    Args:
        db: SupabaseDB instance
        date_from: 시작일 (YYYY-MM-DD)
        date_to: 종료일 (YYYY-MM-DD)
        channel: 채널 필터 (None=전체)
        force_shortage: True=재고 부족 무시하고 처리

    Returns:
        dict: {
            success: bool,
            outbound_count: int,    # 재고 차감 건수
            processed_orders: int,  # 처리된 주문 건수
            shortage: list,         # 재고 부족 목록
            errors: list,           # 에러 메시지
            logs: list,             # 처리 로그
        }
    """
    logs = []
    errors = []
    shortage_warnings = []

    def log(msg):
        t = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
        logs.append(t)
        try:
            print(t)
        except (UnicodeEncodeError, UnicodeDecodeError, OSError):
            pass

    log("자동처리 시작: 주문 → 출고")

    # 재고차감일: 개별 주문의 order_date 사용 (당일차감 원칙)
    today_date = _stock_date()  # fallback용

    # 대상 기간의 마감 상태 캐시 (주문별로 매번 DB 호출 방지)
    _closing_cache = {}

    def _is_date_closed(target_date, closing_type):
        """특정 날짜+유형의 마감 여부 (캐시 사용)."""
        key = (target_date, closing_type)
        if key not in _closing_cache:
            _closing_cache[key] = db.is_closed(target_date, closing_type)
        return _closing_cache[key]

    # 1. 미처리 주문 조회
    pending = db.query_pending_outbound_orders(
        date_from=date_from, date_to=date_to, channel=channel)
    if not pending:
        log("미처리 주문 없음")
        return {
            'success': True,
            'outbound_count': 0,
            'processed_orders': 0,
            'shortage': [],
            'errors': [],
            'logs': logs,
        }
    log(f"미처리 주문 {len(pending)}건 조회됨")

    # 2. 마스터 데이터 로드
    bom_map = _load_bom_map(db)
    opt_map = _load_option_map(db)

    bom_all = bom_map.get('모든채널', {})
    bom_coupang = bom_map.get('쿠팡전용', {})
    log(f"BOM 로드: 모든채널 {len(bom_all)}종, 쿠팡전용 {len(bom_coupang)}종")
    log(f"옵션마스터: {len(opt_map)}종")

    # 3. 주문별 처리
    from services.excel_io import build_stock_snapshot, snapshot_lookup

    # 날짜+창고별 출고 그룹핑
    outbound_groups = {}  # { (date, warehouse): [{"product_name": ..., "qty": ...}, ...] }
    order_ids_done = []    # 처리 완료된 order_transaction id 목록
    order_revenue_cats = {}  # order_id → revenue_category
    skipped_closed = 0     # 마감으로 스킵된 주문 수

    today_str = _now_kst().strftime('%Y-%m-%d')

    for order in pending:
        order_id = order['id']
        ch = order.get('channel', '')
        product_name = order.get('product_name', '')
        orig_qty = order.get('qty', 0) or 0
        order_date = order.get('order_date', today_str)

        if not product_name or orig_qty <= 0:
            continue

        # 재고차감일: 수집일 > 주문일 > 오늘 (로켓배송 등 주문일 기준 차감 보장)
        collection_date = order.get('collection_date', '')
        stk_date = collection_date if collection_date else (order_date if order_date else today_date)

        # 마감 체크
        stk_closed_for_date = _is_date_closed(stk_date, 'stock')
        if stk_closed_for_date:
            skipped_closed += 1
            continue

        # 매출 유형 결정
        rev_cat = CHANNEL_REVENUE_MAP.get(ch, '일반매출')

        # N배송 주문인 경우 분해하지 않음
        is_n_delivery = (ch == 'N배송_수동' or rev_cat == 'N배송')

        # BOM 분해 (쿠팡 채널은 쿠팡전용 BOM 우선)
        if is_n_delivery:
            decomposed = {product_name: orig_qty}
        else:
            if rev_cat in ('쿠팡매출', '로켓'):
                decomposed = _decompose(product_name, orig_qty,
                                         bom_coupang, bom_all)
            else:
                decomposed = _decompose(product_name, orig_qty,
                                         bom_all, None)

        # 분해된 품목별 창고 라우팅 + 출고 그룹 축적
        for item_name, item_qty in decomposed.items():
            wh = _get_warehouse(item_name, opt_map)
            if is_n_delivery:
                wh = "CJ용인"
            key = (stk_date, wh)
            if key not in outbound_groups:
                outbound_groups[key] = []
            outbound_groups[key].append({
                'product_name': item_name,
                'qty': item_qty,
                'order_id': order_id,
            })

        order_ids_done.append(order_id)
        order_revenue_cats[order_id] = rev_cat

    if skipped_closed:
        log(f"⚠ 마감된 날짜로 인해 {skipped_closed}건 스킵됨")
    log(f"출고 그룹: {len(outbound_groups)}개 (반영일+창고)")

    # 4. FIFO 재고 차감 (출고 그룹별, event_uid로 중복 방지)
    total_outbound = 0
    total_skipped = 0
    for (date_str, warehouse), items in outbound_groups.items():
        # 품목별 합산 + 관련 order_id 추적
        merged = {}      # {품목: 수량}
        merged_oids = {}  # {품목: [order_id, ...]}
        for item in items:
            nm = item['product_name']
            merged[nm] = merged.get(nm, 0) + item['qty']
            merged_oids.setdefault(nm, []).append(item['order_id'])

        # 재고 스냅샷 조회
        try:
            stock_data = db.query_stock_by_location(warehouse)
            stock_snap = build_stock_snapshot(stock_data)
        except Exception as e:
            errors.append(f"[{warehouse}] 재고 조회 실패: {e}")
            continue

        # 재고 부족 체크
        for nm, req_qty in merged.items():
            snap = snapshot_lookup(stock_snap, nm)
            available = snap.get('total', 0)
            if req_qty > available:
                msg = f"[{warehouse}] {nm}: 요청 {req_qty} / 재고 {available}"
                shortage_warnings.append(msg)

        if shortage_warnings and not force_shortage:
            # 재고 부족 시 처리 중단하지 않고 가능한 것만 처리
            log(f"재고 부족 {len(shortage_warnings)}건 (처리 계속)")

        # FIFO 출고 payload 생성 (event_uid 포함)
        payload = []
        for nm, req_qty in merged.items():
            snap = snapshot_lookup(stock_snap, nm)
            groups = snap.get('groups', [])
            remain = req_qty
            # event_uid: 관련 order_id들을 정렬 후 조합
            oids_key = "_".join(str(o) for o in sorted(set(merged_oids.get(nm, []))))
            base_uid = f"SO:{date_str}:{warehouse}:{_norm(nm)}:{oids_key}"

            if not groups:
                # 재고 데이터 없어도 마이너스로 기록
                payload.append({
                    "transaction_date": date_str,
                    "type": "SALES_OUT",
                    "product_name": nm,
                    "qty": -remain,
                    "location": warehouse,
                    "unit": snap.get('unit', '개'),
                    "category": snap.get('category', ''),
                    "storage_method": snap.get('storage_method', ''),
                    "manufacture_date": '',
                    "event_uid": f"{base_uid}:0",
                })
                continue

            for gi, g in enumerate(groups):
                if remain <= 0:
                    break
                deduct = min(remain, g['qty'])
                if deduct <= 0:
                    continue
                payload.append({
                    "transaction_date": date_str,
                    "type": "SALES_OUT",
                    "product_name": nm,
                    "qty": -deduct,
                    "location": warehouse,
                    "category": g.get('category', ''),
                    "expiry_date": g.get('expiry_date', ''),
                    "storage_method": g.get('storage_method', ''),
                    "unit": g.get('unit', '개'),
                    "manufacture_date": g.get('manufacture_date', ''),
                    "event_uid": f"{base_uid}:{gi}",
                })
                remain -= deduct

        if payload:
            try:
                for p in payload:
                    logger.info(f"[재고차감] {date_str} | {p['product_name']} | {p['qty']} | {warehouse} | SALES_OUT")
                inserted, skipped = db.upsert_stock_ledger_idempotent(payload)
                total_outbound += inserted
                total_skipped += skipped
                logger.info(f"[재고차감완료] {date_str} | {warehouse} | {inserted}건 성공 | 중복스킵 {skipped}건")
                if skipped:
                    log(f"  [{warehouse}] {date_str}: FIFO 출고 {inserted}건 (중복 스킵 {skipped}건)")
                else:
                    log(f"  [{warehouse}] {date_str}: FIFO 출고 {inserted}건")
            except Exception as e:
                logger.error(f"[재고차감실패] {date_str} | {warehouse} | {len(payload)}건 | {str(e)}")
                errors.append(f"[{warehouse}] stock_ledger INSERT 실패: {e}")

    if total_skipped:
        log(f"⚠ 총 {total_skipped}건 중복 스킵됨 (idempotency)")

    # 5. 주문 처리 완료 표시
    if order_ids_done:
        outbound_date = date_to or today_str
        cat_groups = {}
        for oid in order_ids_done:
            cat = order_revenue_cats.get(oid, '일반매출')
            cat_groups.setdefault(cat, []).append(oid)

        for cat, ids in cat_groups.items():
            try:
                db.mark_orders_outbound_done(ids, outbound_date, cat)
            except Exception as e:
                errors.append(f"mark_orders_outbound_done 실패 ({cat}): {e}")

        log(f"주문 {len(order_ids_done)}건 처리완료 표시")

    success = len(errors) == 0
    log(f"자동처리 {'완료' if success else '완료 (일부 오류)'}:"
        f" 출고 {total_outbound}건, 주문 {len(order_ids_done)}건 처리")

    return {
        'success': success,
        'outbound_count': total_outbound,
        'processed_orders': len(order_ids_done),
        'shortage': shortage_warnings,
        'errors': errors,
        'logs': logs,
    }


# ================================================================
# 실시간 처리: 주문 수집 직후 자동 출고
# ================================================================

def process_realtime_outbound(db, import_run_id):
    """주문 수집(송장생성) 직후 호출 — 해당 import_run의 미처리 주문을 즉시 출고 처리.

    BOM분해→FIFO 재고차감→주문완료표시.
    매출은 order_transactions에 이미 저장되어 있으므로 별도 기록하지 않음.

    Returns:
        dict: {outbound_count, processed_orders, errors, logs}
    """
    logs = []
    errors = []

    def log(msg):
        t = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
        logs.append(t)

    log(f"실시간 출고 시작 (import_run #{import_run_id})")

    # 1. 해당 import_run의 미처리 주문 조회
    pending = db.query_orders_by_import_run(import_run_id, outbound_done=False)
    if not pending:
        log("처리할 미출고 주문 없음")
        return {'outbound_count': 0, 'processed_orders': 0,
                'errors': [], 'logs': logs}

    log(f"미처리 주문 {len(pending)}건")

    # 2. 마스터 데이터 로드
    bom_map = _load_bom_map(db)
    opt_map = _load_option_map(db)

    bom_all = bom_map.get('모든채널', {})
    bom_coupang = bom_map.get('쿠팡전용', {})

    # 3. 주문별 분해 → 출고 그룹 구성
    from services.excel_io import build_stock_snapshot, snapshot_lookup

    outbound_groups = {}   # (date, warehouse): [{product_name, qty, order_id}, ...]
    order_ids_done = []
    order_cats = {}
    order_dates = {}  # oid → outbound_date (N배송: 매출일, 일반: 오늘)
    today_str = _stock_date()

    for order in pending:
        oid = order['id']
        ch = order.get('channel', '')
        pname = order.get('product_name', '')
        qty = int(order.get('qty', 0) or 0)
        odate = order.get('order_date', today_str)

        if not pname or qty <= 0:
            continue

        rev_cat = CHANNEL_REVENUE_MAP.get(ch, '일반매출')
        is_n = (ch == 'N배송_수동' or rev_cat == 'N배송')
        # N배송: 매출일(order_date) 기준, 일반: 수집일 > 주문일 > 오늘
        if is_n:
            stk_date = odate if odate else today_str
        else:
            coll_date = order.get('collection_date', '')
            stk_date = coll_date if coll_date else (odate if odate else today_str)

        # BOM 분해
        if is_n:
            decomposed = {pname: qty}
        elif rev_cat in ('쿠팡매출', '로켓'):
            decomposed = _decompose(pname, qty, bom_coupang, bom_all)
        else:
            decomposed = _decompose(pname, qty, bom_all, None)

        # 출고 그룹 축적
        for item, iqty in decomposed.items():
            wh = "CJ용인" if is_n else _get_warehouse(item, opt_map)
            key = (stk_date, wh)
            if key not in outbound_groups:
                outbound_groups[key] = []
            outbound_groups[key].append({'product_name': item, 'qty': iqty, 'order_id': oid})

        order_ids_done.append(oid)
        order_cats[oid] = rev_cat
        order_dates[oid] = stk_date  # N배송=매출일, 일반=수집일/오늘

    # 4. FIFO 재고 차감
    total_outbound = 0
    total_skipped = 0
    for (date_str, warehouse), items in outbound_groups.items():
        merged = {}
        merged_oids = {}
        for it in items:
            nm = it['product_name']
            merged[nm] = merged.get(nm, 0) + it['qty']
            merged_oids.setdefault(nm, []).append(it['order_id'])

        try:
            stock_data = db.query_stock_by_location(warehouse)
            stock_snap = build_stock_snapshot(stock_data)
        except Exception as e:
            errors.append(f"[{warehouse}] 재고 조회 실패: {e}")
            continue

        payload = []
        for nm, req in merged.items():
            snap = snapshot_lookup(stock_snap, nm)
            groups = snap.get('groups', [])
            remain = req
            oids_key = "_".join(str(o) for o in sorted(set(merged_oids.get(nm, []))))
            base_uid = f"SO:{date_str}:{warehouse}:{_norm(nm)}:{oids_key}"

            if not groups:
                payload.append({
                    "transaction_date": date_str, "type": "SALES_OUT",
                    "product_name": nm, "qty": -remain, "location": warehouse,
                    "unit": snap.get('unit', '개'), "category": snap.get('category', ''),
                    "storage_method": snap.get('storage_method', ''),
                    "manufacture_date": '',
                    "event_uid": f"{base_uid}:0",
                })
                continue

            for gi, g in enumerate(groups):
                if remain <= 0:
                    break
                deduct = min(remain, g['qty'])
                if deduct <= 0:
                    continue
                payload.append({
                    "transaction_date": date_str, "type": "SALES_OUT",
                    "product_name": nm, "qty": -deduct, "location": warehouse,
                    "category": g.get('category', ''),
                    "expiry_date": g.get('expiry_date', ''),
                    "storage_method": g.get('storage_method', ''),
                    "unit": g.get('unit', '개'),
                    "manufacture_date": g.get('manufacture_date', ''),
                    "event_uid": f"{base_uid}:{gi}",
                })
                remain -= deduct

        if payload:
            try:
                for p in payload:
                    logger.info(f"[재고차감] {date_str} | {p['product_name']} | {p['qty']} | {warehouse} | SALES_OUT | import_run#{import_run_id}")
                inserted, skipped = db.upsert_stock_ledger_idempotent(payload)
                total_outbound += inserted
                total_skipped += skipped
                logger.info(f"[재고차감완료] {date_str} | {warehouse} | {inserted}건 성공 | 중복스킵 {skipped}건 | import_run#{import_run_id}")
                if skipped:
                    log(f"  [{warehouse}] FIFO 출고 {inserted}건 (중복 스킵 {skipped}건)")
                else:
                    log(f"  [{warehouse}] FIFO 출고 {inserted}건")
            except Exception as e:
                logger.error(f"[재고차감실패] {date_str} | {warehouse} | {len(payload)}건 | {str(e)} | import_run#{import_run_id}")
                errors.append(f"[{warehouse}] stock_ledger 오류: {e}")

    # 5. 주문 처리 완료 표시 (N배송: 매출일 기준, 일반: 오늘 기준)
    if order_ids_done:
        # (outbound_date, revenue_category) 그룹별 처리
        done_groups = {}
        for oid in order_ids_done:
            cat = order_cats.get(oid, '일반매출')
            odt = order_dates.get(oid, today_str)
            done_groups.setdefault((odt, cat), []).append(oid)
        for (odt, cat), ids in done_groups.items():
            try:
                db.mark_orders_outbound_done(ids, odt, cat)
            except Exception as e:
                errors.append(f"mark_done 오류 ({cat}): {e}")

    log(f"실시간 처리 완료: 출고 {total_outbound}건, 주문 {len(order_ids_done)}건")

    return {
        'outbound_count': total_outbound,
        'processed_orders': len(order_ids_done),
        'errors': errors,
        'logs': logs,
    }


# ================================================================
# 패킹 완료 → 출고 처리 (단건)
# ================================================================

def process_packing_outbound(db, order_id):
    """패킹센터에서 영상 촬영 완료 시 단건 출고 처리.

    BOM 분해 → FIFO 재고 차감 → is_outbound_done 마킹.
    이미 출고 처리된 주문은 스킵.

    Args:
        db: SupabaseDB instance
        order_id: order_transactions.id

    Returns:
        dict: {success, outbound_count, errors, message}
    """
    errors = []
    order = db.query_order_transaction_by_id(order_id)
    if not order:
        return {'success': False, 'outbound_count': 0, 'errors': ['주문을 찾을 수 없습니다'],
                'message': f'order #{order_id} not found'}

    if order.get('is_outbound_done'):
        return {'success': True, 'outbound_count': 0, 'errors': [],
                'message': '이미 출고 처리된 주문'}

    if order.get('status') != '정상':
        return {'success': False, 'outbound_count': 0, 'errors': ['정상 상태가 아닌 주문'],
                'message': f"status={order.get('status')}"}

    ch = order.get('channel', '')
    pname = order.get('product_name', '')
    qty = int(order.get('qty', 0) or 0)
    odate = order.get('order_date', _stock_date())

    if not pname or qty <= 0:
        return {'success': False, 'outbound_count': 0, 'errors': ['품목명/수량 없음'],
                'message': 'missing product_name or qty'}

    # BOM / 라우팅
    bom_map = _load_bom_map(db)
    opt_map = _load_option_map(db)
    bom_all = bom_map.get('모든채널', {})
    bom_coupang = bom_map.get('쿠팡전용', {})

    rev_cat = CHANNEL_REVENUE_MAP.get(ch, '일반매출')
    is_n = (ch == 'N배송_수동' or rev_cat == 'N배송')

    if is_n:
        stk_date = odate if odate else _stock_date()
        decomposed = {pname: qty}
    else:
        coll_date = order.get('collection_date', '')
        stk_date = coll_date if coll_date else (odate if odate else _stock_date())
        if rev_cat in ('쿠팡매출', '로켓'):
            decomposed = _decompose(pname, qty, bom_coupang, bom_all)
        else:
            decomposed = _decompose(pname, qty, bom_all, None)

    # FIFO 재고 차감
    from services.excel_io import build_stock_snapshot, snapshot_lookup

    total_outbound = 0
    for item, iqty in decomposed.items():
        wh = "CJ용인" if is_n else _get_warehouse(item, opt_map)
        base_uid = f"SO:{stk_date}:{wh}:{_norm(item)}:{order_id}"

        try:
            stock_data = db.query_stock_by_location(wh)
            stock_snap = build_stock_snapshot(stock_data)
        except Exception as e:
            errors.append(f"[{wh}] 재고 조회 실패: {e}")
            continue

        snap = snapshot_lookup(stock_snap, item)
        groups = snap.get('groups', [])
        remain = iqty
        payload = []

        if not groups:
            payload.append({
                "transaction_date": stk_date, "type": "SALES_OUT",
                "product_name": item, "qty": -remain, "location": wh,
                "unit": snap.get('unit', '개'), "category": snap.get('category', ''),
                "storage_method": snap.get('storage_method', ''),
                "manufacture_date": '',
                "event_uid": f"{base_uid}:0",
            })
        else:
            for gi, g in enumerate(groups):
                if remain <= 0:
                    break
                deduct = min(remain, g['qty'])
                if deduct <= 0:
                    continue
                payload.append({
                    "transaction_date": stk_date, "type": "SALES_OUT",
                    "product_name": item, "qty": -deduct, "location": wh,
                    "category": g.get('category', ''),
                    "expiry_date": g.get('expiry_date', ''),
                    "storage_method": g.get('storage_method', ''),
                    "unit": g.get('unit', '개'),
                    "manufacture_date": g.get('manufacture_date', ''),
                    "event_uid": f"{base_uid}:{gi}",
                })
                remain -= deduct

        if payload:
            try:
                inserted, skipped = db.upsert_stock_ledger_idempotent(payload)
                total_outbound += inserted
                logger.info(f"[패킹출고] order#{order_id} | {item} | {iqty} | {wh} | {inserted}건")
            except Exception as e:
                errors.append(f"[{wh}] {item} 출고 실패: {e}")

    # 출고 완료 표시
    try:
        db.mark_orders_outbound_done([order_id], stk_date, rev_cat)
    except Exception as e:
        errors.append(f"mark_done 오류: {e}")

    return {
        'success': True,
        'outbound_count': total_outbound,
        'errors': errors,
        'message': f'출고 완료: 재고차감 {total_outbound}건',
    }


# ================================================================
# 역분개: 주문 취소/환불 시 재고 복원
# ================================================================

def reverse_order_stock(db, order_id):
    """주문 취소/환불 시 재고 복원(SALES_RETURN).

    감사추적을 위해 원본 SALES_OUT은 유지하고,
    SALES_RETURN(+qty)을 추가하여 재고를 복원합니다.

    Args:
        db: SupabaseDB instance
        order_id: order_transactions.id

    Returns:
        dict: {stock_reversed, errors}
    """
    errors = []

    # 1. 주문 조회
    order = db.query_order_transaction_by_id(order_id)
    if not order:
        return {'stock_reversed': 0, 'errors': ['주문을 찾을 수 없습니다']}

    if not order.get('is_outbound_done'):
        return {'stock_reversed': 0, 'errors': []}

    pname = order.get('product_name', '')
    qty = int(order.get('qty', 0) or 0)
    rev_cat = order.get('revenue_category', '')
    ch = order.get('channel', '')

    if not pname or qty <= 0:
        return {'stock_reversed': 0, 'errors': ['주문 데이터 부족']}

    # 2. BOM 분해 (출고 시와 동일 로직)
    bom_map = _load_bom_map(db)
    opt_map = _load_option_map(db)

    bom_all = bom_map.get('모든채널', {})
    bom_coupang = bom_map.get('쿠팡전용', {})

    is_n = (ch == 'N배송_수동' or rev_cat == 'N배송')

    if is_n:
        decomposed = {pname: qty}
    elif rev_cat in ('쿠팡매출', '로켓'):
        decomposed = _decompose(pname, qty, bom_coupang, bom_all)
    else:
        decomposed = _decompose(pname, qty, bom_all, None)

    # 3. SALES_RETURN 기록 (재고 복원 — 처리일 = 오늘, A안: 취소도 오늘 기준)
    reverse_stk_date = _stock_date()
    stock_reversed = 0
    for item, iqty in decomposed.items():
        wh = "CJ용인" if is_n else _get_warehouse(item, opt_map)
        # 원본 SALES_OUT의 event_uid 참조 (상쇄 관계 추적)
        ref_uid = f"SO:{reverse_stk_date}:{wh}:{_norm(item)}:{order_id}"
        return_uid = f"SR:{reverse_stk_date}:{wh}:{_norm(item)}:{order_id}"
        try:
            logger.info(f"[반품] {reverse_stk_date} | {item} | +{iqty} | {wh} | SALES_RETURN | order#{order_id}")
            db.insert_stock_ledger([{
                "transaction_date": reverse_stk_date,
                "type": "SALES_RETURN",
                "product_name": item,
                "qty": iqty,  # 양수 → 재고 복원
                "location": wh,
                "unit": "개",
                "category": "",
                "storage_method": "",
                "manufacture_date": "",
                "event_uid": return_uid,
                "ref_event_uid": ref_uid,
            }])
            logger.info(f"[반품완료] {reverse_stk_date} | {item} | +{iqty} | {wh} | order#{order_id}")
            stock_reversed += 1
        except Exception as e:
            logger.error(f"[반품실패] {reverse_stk_date} | {item} | +{iqty} | {wh} | order#{order_id} | {str(e)}")
            errors.append(f"SALES_RETURN 오류 ({item}): {e}")

    # 4. is_outbound_done 초기화
    try:
        db.reset_order_outbound(order_id)
    except Exception as e:
        errors.append(f"outbound 초기화 오류: {e}")

    return {
        'stock_reversed': stock_reversed,
        'errors': errors,
    }


def process_single_order_realtime(db, order_id):
    """단일 주문 실시간 재처리 (수량 정정 후 호출).

    order_id의 현재 데이터로 출고 처리.
    매출은 order_transactions에 이미 저장되어 있으므로 별도 기록 없음.
    """
    order = db.query_order_transaction_by_id(order_id)
    if not order or order.get('is_outbound_done'):
        return {'outbound_count': 0, 'errors': []}

    errors = []
    ch = order.get('channel', '')
    pname = order.get('product_name', '')
    qty = int(order.get('qty', 0) or 0)
    odate = order.get('order_date', '')

    if not pname or qty <= 0:
        return {'outbound_count': 0, 'errors': ['데이터 부족']}

    rev_cat = CHANNEL_REVENUE_MAP.get(ch, '일반매출')
    is_n = (ch == 'N배송_수동' or rev_cat == 'N배송')
    # N배송: 매출일(order_date) 기준, 일반: 수집일 > 주문일 > 오늘
    if is_n:
        stk_date = odate if odate else _stock_date()
    else:
        coll_date = order.get('collection_date', '')
        stk_date = coll_date if coll_date else (odate if odate else _stock_date())

    # BOM + 마스터 로드
    bom_map = _load_bom_map(db)
    opt_map = _load_option_map(db)

    bom_all = bom_map.get('모든채널', {})
    bom_coupang = bom_map.get('쿠팡전용', {})

    if is_n:
        decomposed = {pname: qty}
    elif rev_cat in ('쿠팡매출', '로켓'):
        decomposed = _decompose(pname, qty, bom_coupang, bom_all)
    else:
        decomposed = _decompose(pname, qty, bom_all, None)

    # FIFO 출고
    from services.excel_io import build_stock_snapshot, snapshot_lookup
    outbound_count = 0

    for item, iqty in decomposed.items():
        wh = "CJ용인" if is_n else _get_warehouse(item, opt_map)
        base_uid = f"SO:{stk_date}:{wh}:{_norm(item)}:{order_id}"
        try:
            stock_data = db.query_stock_by_location(wh)
            stock_snap = build_stock_snapshot(stock_data)
            snap = snapshot_lookup(stock_snap, item)
            groups = snap.get('groups', [])
            remain = iqty
            payload = []

            if not groups:
                payload.append({
                    "transaction_date": stk_date, "type": "SALES_OUT",
                    "product_name": item, "qty": -remain, "location": wh,
                    "unit": snap.get('unit', '개'), "category": "",
                    "storage_method": "", "manufacture_date": "",
                    "event_uid": f"{base_uid}:0",
                })
            else:
                for gi, g in enumerate(groups):
                    if remain <= 0:
                        break
                    deduct = min(remain, g['qty'])
                    if deduct <= 0:
                        continue
                    payload.append({
                        "transaction_date": stk_date, "type": "SALES_OUT",
                        "product_name": item, "qty": -deduct, "location": wh,
                        "category": g.get('category', ''),
                        "expiry_date": g.get('expiry_date', ''),
                        "storage_method": g.get('storage_method', ''),
                        "unit": g.get('unit', '개'),
                        "manufacture_date": g.get('manufacture_date', ''),
                        "event_uid": f"{base_uid}:{gi}",
                    })
                    remain -= deduct

            if payload:
                for p in payload:
                    logger.info(f"[재고차감] {stk_date} | {p['product_name']} | {p['qty']} | {wh} | SALES_OUT | order#{order_id}")
                inserted, skipped = db.upsert_stock_ledger_idempotent(payload)
                outbound_count += inserted
                logger.info(f"[재고차감완료] {stk_date} | {wh} | {inserted}건 성공 | order#{order_id}")
        except Exception as e:
            logger.error(f"[재고차감실패] {stk_date} | {item} | {wh} | order#{order_id} | {str(e)}")
            errors.append(f"출고 오류 ({item}): {e}")

    # 완료 표시
    try:
        db.mark_orders_outbound_done([order_id], odate, rev_cat)
    except Exception as e:
        errors.append(f"mark_done 오류: {e}")

    return {'outbound_count': outbound_count, 'errors': errors}
