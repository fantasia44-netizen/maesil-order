"""
order_to_stock_service.py — 주문 → 출고+매출 자동처리 서비스 (Phase 2).

order_transactions에서 미처리 주문을 가져와:
1. BOM 분해 (세트 → 개별 자재)
2. 창고 라우팅 (라인코드 기반)
3. FIFO 재고 차감 (stock_ledger SALES_OUT)
4. 매출 기록 (daily_revenue upsert)
5. order_transactions.is_outbound_done = True 업데이트

기존 수동 흐름(통합집계→출고→매출)과 독립적으로 동작.
"""
import unicodedata
from datetime import datetime
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

from services.channel_config import CHANNEL_REVENUE_MAP, CATEGORY_PRICE_COL

KST = ZoneInfo("Asia/Seoul")


def _now_kst():
    """KST 기준 현재 시각."""
    return datetime.now(KST)


def _norm(text):
    """NFC 정규화 + strip"""
    return unicodedata.normalize('NFC', str(text).strip())


def _revenue_date(order_date_str):
    """매출 반영일: 주문일시(엑셀 기록)의 날짜 그대로 사용 (회계 기준).
    0시 이전 주문 → 해당 일자, 0시 이후 주문 → 다음 일자.
    엑셀에서 이미 날짜 파트만 넘어오므로 그대로 반환.
    """
    return order_date_str


def _stock_date():
    """재고차감 반영일: 파일 처리일 = 오늘 (실제 출고 기준).
    온라인주문은 주문수집→출고가 다음날이므로,
    파일을 업로드(처리)하는 날 = 실제 출고일.
    """
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


def _preload_promotions(db):
    """활성 행사 전체를 한번에 로드 → {(product_name, category): promo_row}."""
    try:
        res = db.client.table("promotions").select("*") \
            .eq("is_active", True).execute()
        pmap = {}
        for r in (res.data or []):
            key = (_norm(r.get('product_name', '')), r.get('category', ''))
            # 같은 키에 여러 행사 → 최신 등록 우선 (created_at 내림차순)
            if key not in pmap:
                pmap[key] = r
        return pmap
    except Exception:
        return {}


def _preload_coupons(db):
    """활성 쿠폰 전체를 한번에 로드 → {(product_name, category): coupon_row}."""
    try:
        res = db.client.table("coupons").select("*") \
            .eq("is_active", True).execute()
        cmap = {}
        for r in (res.data or []):
            key = (_norm(r.get('product_name', '')), r.get('category', ''))
            if key not in cmap:
                cmap[key] = r
        return cmap
    except Exception:
        return {}


def _resolve_price_cached(product_name, category, target_date, price_map,
                           promo_map, coupon_map):
    """캐시된 행사/쿠폰 맵으로 단가 결정 (DB 호출 0회).
    Returns: (unit_price, source)
    """
    _CATEGORY_PRICE_COL = {
        "일반매출": "네이버판매가",
        "쿠팡매출": "쿠팡판매가",
        "로켓": "로켓판매가",
        "N배송(용인)": "네이버판매가",
    }

    key = (product_name, category)

    # 1) 행사가 확인
    promo = promo_map.get(key)
    if promo:
        sd = promo.get('start_date', '')
        ed = promo.get('end_date', '')
        if sd <= target_date <= ed:
            return float(promo['promo_price']), 'promotion'

    # 2) 쿠폰 확인
    coupon = coupon_map.get(key)
    if coupon:
        sd = coupon.get('start_date', '')
        ed = coupon.get('end_date', '')
        if sd <= target_date <= ed:
            price_col = _CATEGORY_PRICE_COL.get(category)
            base_price = float((price_map.get(product_name) or {}).get(price_col, 0) or 0)
            if base_price > 0:
                if coupon.get('discount_type') == '%':
                    discount = base_price * float(coupon['discount_value']) / 100
                else:
                    discount = float(coupon['discount_value'])
                return max(0, base_price - discount), 'coupon'

    # 3) 기본 판매가
    price_col = _CATEGORY_PRICE_COL.get(category)
    if price_map and price_col:
        base_price = float((price_map.get(product_name) or {}).get(price_col, 0) or 0)
        if base_price > 0:
            return base_price, 'master'

    return 0, 'none'


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
    """미처리 주문 → 출고+매출 자동 처리.

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
            revenue_count: int,     # 매출 기록 건수
            revenue_total: int,     # 총 매출액
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

    log("자동처리 시작: 주문 → 출고+매출")

    # 재고차감일 = 오늘(파일 처리일 = 실제 출고일)
    today_date = _stock_date()

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
            'revenue_count': 0,
            'revenue_total': 0,
            'processed_orders': 0,
            'shortage': [],
            'errors': [],
            'logs': logs,
        }
    log(f"미처리 주문 {len(pending)}건 조회됨")

    # 2. 마스터 데이터 로드
    bom_map = _load_bom_map(db)
    opt_map = _load_option_map(db)
    price_map = db.query_price_table()

    # 행사/쿠폰 프리로드 (주문건별 개별 DB 조회 N+1 방지)
    promo_map = _preload_promotions(db)
    coupon_map = _preload_coupons(db)

    bom_all = bom_map.get('모든채널', {})
    bom_coupang = bom_map.get('쿠팡전용', {})
    log(f"BOM 로드: 모든채널 {len(bom_all)}종, 쿠팡전용 {len(bom_coupang)}종")
    log(f"옵션마스터: {len(opt_map)}종, 가격표: {len(price_map)}종")

    # 3. 주문별 처리
    #    - 같은 날짜의 주문들을 모아서 한꺼번에 처리 (출고/매출 효율)
    from services.excel_io import build_stock_snapshot, snapshot_lookup

    # 날짜+창고별 출고 그룹핑
    outbound_groups = {}  # { (date, warehouse): [{"product_name": ..., "qty": ...}, ...] }
    revenue_batch = []     # daily_revenue upsert용 배열
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

        # 매출일 = 주문일(회계기준), 재고차감일 = 오늘(출고기준)
        rev_date = _revenue_date(order_date)
        stk_date = today_date

        # 마감 체크: 각각의 날짜 기준
        rev_closed_for_date = _is_date_closed(rev_date, 'revenue')
        stk_closed_for_date = _is_date_closed(stk_date, 'stock')
        if rev_closed_for_date and stk_closed_for_date:
            skipped_closed += 1
            continue

        # 매출 유형 결정
        rev_cat = CHANNEL_REVENUE_MAP.get(ch, '일반매출')

        # N배송 주문인 경우 분해하지 않음
        is_n_delivery = (ch == 'N배송_수동' or rev_cat == 'N배송(용인)')

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

        # 분해된 품목별 창고 라우팅 + 출고 그룹 축적 (재고차감일 기준)
        if not stk_closed_for_date:
            for item_name, item_qty in decomposed.items():
                wh = _get_warehouse(item_name, opt_map)
                # N배송은 CJ용인
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

        # 매출 기록 준비 (세트 미분해 원본 기준, 매출일 = 주문일) — 프리로드 사용
        price_col = CATEGORY_PRICE_COL.get(rev_cat)
        if price_col and not rev_closed_for_date:
            unit_price, _src = _resolve_price_cached(
                _norm(product_name), rev_cat, order_date, price_map, promo_map, coupon_map)
            revenue = orig_qty * unit_price
            if revenue > 0:
                revenue_batch.append({
                    'revenue_date': rev_date,
                    'product_name': _norm(product_name),
                    'category': rev_cat,
                    'channel': ch,
                    'qty': orig_qty,
                    'unit_price': int(unit_price),
                    'revenue': int(revenue),
                })

        order_ids_done.append(order_id)
        order_revenue_cats[order_id] = rev_cat

    if skipped_closed:
        log(f"⚠ 마감된 날짜로 인해 {skipped_closed}건 스킵됨")
    log(f"출고 그룹: {len(outbound_groups)}개 (반영일+창고)")
    log(f"매출 데이터: {len(revenue_batch)}건")

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
                inserted, skipped = db.upsert_stock_ledger_idempotent(payload)
                total_outbound += inserted
                total_skipped += skipped
                if skipped:
                    log(f"  [{warehouse}] {date_str}: FIFO 출고 {inserted}건 (중복 스킵 {skipped}건)")
                else:
                    log(f"  [{warehouse}] {date_str}: FIFO 출고 {inserted}건")
            except Exception as e:
                errors.append(f"[{warehouse}] stock_ledger INSERT 실패: {e}")

    if total_skipped:
        log(f"⚠ 총 {total_skipped}건 중복 스킵됨 (idempotency)")

    # 5. 매출 기록 (daily_revenue upsert)
    revenue_total = 0
    revenue_count = 0
    if revenue_batch:
        try:
            db.upsert_revenue(revenue_batch)
            revenue_count = len(revenue_batch)
            revenue_total = sum(r.get('revenue', 0) for r in revenue_batch)
            log(f"매출 기록: {revenue_count}건, 총 {revenue_total:,}원")
        except Exception as e:
            errors.append(f"daily_revenue upsert 실패: {e}")

    # 6. 주문 처리 완료 표시
    if order_ids_done:
        outbound_date = date_to or today_str
        # 매출 유형별로 그룹핑하여 업데이트
        cat_groups = {}
        for oid in order_ids_done:
            cat = order_revenue_cats.get(oid, '일반매출')
            if cat not in cat_groups:
                cat_groups[cat] = []
            cat_groups[cat].append(oid)

        for cat, ids in cat_groups.items():
            try:
                db.mark_orders_outbound_done(ids, outbound_date, cat)
            except Exception as e:
                errors.append(f"mark_orders_outbound_done 실패 ({cat}): {e}")

        log(f"주문 {len(order_ids_done)}건 처리완료 표시")

    success = len(errors) == 0
    log(f"자동처리 {'완료' if success else '완료 (일부 오류)'}:"
        f" 출고 {total_outbound}건, 매출 {revenue_count}건({revenue_total:,}원),"
        f" 주문 {len(order_ids_done)}건 처리")

    return {
        'success': success,
        'outbound_count': total_outbound,
        'revenue_count': revenue_count,
        'revenue_total': revenue_total,
        'processed_orders': len(order_ids_done),
        'shortage': shortage_warnings,
        'errors': errors,
        'logs': logs,
    }


# ================================================================
# 실시간 처리: 주문 수집 직후 자동 출고+매출
# ================================================================

def process_realtime_outbound(db, import_run_id):
    """주문 수집(송장생성) 직후 호출 — 해당 import_run의 미처리 주문을 즉시 출고+매출 처리.

    기존 process_orders_to_stock()과 동일한 BOM분해→FIFO→매출 로직이지만,
    import_run_id 기준으로 방금 수집된 주문만 처리.

    Returns:
        dict: {outbound_count, revenue_count, revenue_total, errors, logs}
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
        return {'outbound_count': 0, 'revenue_count': 0, 'revenue_total': 0,
                'errors': [], 'logs': logs}

    log(f"미처리 주문 {len(pending)}건")

    # 2. 마스터 데이터 로드 (기존 헬퍼 재활용)
    bom_map = _load_bom_map(db)
    opt_map = _load_option_map(db)
    price_map = db.query_price_table()

    # 행사/쿠폰 프리로드 (주문건별 개별 DB 조회 N+1 방지)
    promo_map = _preload_promotions(db)
    coupon_map = _preload_coupons(db)

    bom_all = bom_map.get('모든채널', {})
    bom_coupang = bom_map.get('쿠팡전용', {})

    # 3. 주문별 분해 → 출고 그룹 + 매출 배치 구성
    from services.excel_io import build_stock_snapshot, snapshot_lookup

    outbound_groups = {}   # (date, warehouse): [{product_name, qty}, ...]
    revenue_batch = []
    order_ids_done = []
    order_cats = {}
    today_str = _stock_date()  # 재고차감일 = 오늘(실제 출고일)

    for order in pending:
        oid = order['id']
        ch = order.get('channel', '')
        pname = order.get('product_name', '')
        qty = int(order.get('qty', 0) or 0)
        odate = order.get('order_date', today_str)

        if not pname or qty <= 0:
            continue

        # 매출일 = 주문일(회계기준), 재고차감일 = 오늘(출고기준)
        rev_date = _revenue_date(odate)
        stk_date = today_str

        rev_cat = CHANNEL_REVENUE_MAP.get(ch, '일반매출')
        is_n = (ch == 'N배송_수동' or rev_cat == 'N배송(용인)')

        # BOM 분해
        if is_n:
            decomposed = {pname: qty}
        elif rev_cat in ('쿠팡매출', '로켓'):
            decomposed = _decompose(pname, qty, bom_coupang, bom_all)
        else:
            decomposed = _decompose(pname, qty, bom_all, None)

        # 출고 그룹 축적 (재고차감일 기준, order_id 포함)
        for item, iqty in decomposed.items():
            wh = "CJ용인" if is_n else _get_warehouse(item, opt_map)
            key = (stk_date, wh)
            if key not in outbound_groups:
                outbound_groups[key] = []
            outbound_groups[key].append({'product_name': item, 'qty': iqty, 'order_id': oid})

        # 매출 준비 (매출일 = 주문일) — 프리로드된 행사/쿠폰 사용
        price_col = CATEGORY_PRICE_COL.get(rev_cat)
        if price_col:
            up, _ = _resolve_price_cached(
                _norm(pname), rev_cat, odate, price_map, promo_map, coupon_map
            )
            rev = qty * up
            if rev > 0:
                revenue_batch.append({
                    'revenue_date': rev_date, 'product_name': _norm(pname),
                    'category': rev_cat, 'channel': ch, 'qty': qty,
                    'unit_price': int(up), 'revenue': int(rev),
                })

        order_ids_done.append(oid)
        order_cats[oid] = rev_cat

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
                inserted, skipped = db.upsert_stock_ledger_idempotent(payload)
                total_outbound += inserted
                total_skipped += skipped
                if skipped:
                    log(f"  [{warehouse}] FIFO 출고 {inserted}건 (중복 스킵 {skipped}건)")
                else:
                    log(f"  [{warehouse}] FIFO 출고 {inserted}건")
            except Exception as e:
                errors.append(f"[{warehouse}] stock_ledger 오류: {e}")

    # 5. 매출 기록 (동일 날짜+상품+카테고리는 합산 후 배치 upsert)
    revenue_total = 0
    revenue_count = 0
    if revenue_batch:
        # 5a. 동일 키(날짜+상품+카테고리+채널) 합산
        agg = {}
        for r in revenue_batch:
            key = (r['revenue_date'], r['product_name'], r['category'], r.get('channel', ''))
            if key not in agg:
                agg[key] = {'revenue_date': r['revenue_date'],
                            'product_name': r['product_name'],
                            'category': r['category'],
                            'channel': r.get('channel', ''),
                            'qty': 0, 'unit_price': r['unit_price'], 'revenue': 0}
            agg[key]['qty'] += r['qty']
            agg[key]['revenue'] += r['revenue']
        merged_rev = list(agg.values())
        log(f"매출 배치: 원본 {len(revenue_batch)}건 → 합산 {len(merged_rev)}건")

        # 5b. 50건씩 배치 upsert
        BATCH = 50
        for i in range(0, len(merged_rev), BATCH):
            batch = merged_rev[i:i+BATCH]
            try:
                db.upsert_revenue(batch)
                revenue_count += len(batch)
                revenue_total += sum(r['revenue'] for r in batch)
            except Exception as e:
                errors.append(f"매출 기록 오류 (batch {i//BATCH+1}): {e}")
                log(f"⚠️ 매출 upsert 실패 batch#{i//BATCH+1}: {e}")
        log(f"매출 기록: {revenue_count}건, {revenue_total:,}원")

    # 6. 주문 처리 완료 표시
    if order_ids_done:
        odate_mark = today_str
        cat_groups = {}
        for oid in order_ids_done:
            cat = order_cats.get(oid, '일반매출')
            cat_groups.setdefault(cat, []).append(oid)
        for cat, ids in cat_groups.items():
            try:
                db.mark_orders_outbound_done(ids, odate_mark, cat)
            except Exception as e:
                errors.append(f"mark_done 오류 ({cat}): {e}")

    log(f"실시간 처리 완료: 출고 {total_outbound}건, 매출 {revenue_count}건({revenue_total:,}원)")

    return {
        'outbound_count': total_outbound,
        'revenue_count': revenue_count,
        'revenue_total': revenue_total,
        'processed_orders': len(order_ids_done),
        'errors': errors,
        'logs': logs,
    }


# ================================================================
# 매출 재처리: 출고 완료됐지만 매출 누락된 주문 복구
# ================================================================

def reprocess_revenue_only(db, date_from=None, date_to=None):
    """출고 처리(is_outbound_done=True)됐지만 매출이 누락된 주문의 매출만 재생성.
    재고(SALES_OUT)는 건드리지 않음.

    Returns:
        dict: {revenue_count, revenue_total, processed_orders, errors, logs}
    """
    logs = []
    errors = []

    def log(msg):
        t = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
        logs.append(t)

    log("매출 재처리 시작")

    # 출고 완료된 주문 조회
    q = db.client.table("order_transactions").select("*") \
        .eq("is_outbound_done", True).eq("status", "정상")
    if date_from:
        q = q.gte("order_date", date_from)
    if date_to:
        q = q.lte("order_date", date_to)
    res = q.order("order_date", desc=True).limit(5000).execute()
    orders = res.data or []

    if not orders:
        log("대상 주문 없음")
        return {'revenue_count': 0, 'revenue_total': 0,
                'processed_orders': 0, 'errors': [], 'logs': logs}

    log(f"대상 주문 {len(orders)}건")

    price_map = db.query_price_table()
    promo_map = _preload_promotions(db)
    coupon_map = _preload_coupons(db)
    revenue_raw = []

    for o in orders:
        ch = o.get('channel', '')
        pname = o.get('product_name', '')
        qty = int(o.get('qty', 0) or 0)
        odate = o.get('order_date', '')

        if not pname or qty <= 0:
            continue

        rev_cat = CHANNEL_REVENUE_MAP.get(ch, '일반매출')
        price_col = CATEGORY_PRICE_COL.get(rev_cat)
        if not price_col:
            continue

        up, _ = _resolve_price_cached(_norm(pname), rev_cat, odate, price_map, promo_map, coupon_map)
        rev = qty * up
        if rev > 0:
            revenue_raw.append({
                'revenue_date': odate, 'product_name': _norm(pname),
                'category': rev_cat, 'channel': ch, 'qty': qty,
                'unit_price': int(up), 'revenue': int(rev),
            })

    if not revenue_raw:
        log("매출 생성 대상 없음 (단가 0)")
        return {'revenue_count': 0, 'revenue_total': 0,
                'processed_orders': len(orders), 'errors': errors, 'logs': logs}

    # 합산 (채널별로 분리)
    agg = {}
    for r in revenue_raw:
        key = (r['revenue_date'], r['product_name'], r['category'], r.get('channel', ''))
        if key not in agg:
            agg[key] = dict(r)
            agg[key]['qty'] = 0
            agg[key]['revenue'] = 0
        agg[key]['qty'] += r['qty']
        agg[key]['revenue'] += r['revenue']
    merged = list(agg.values())
    log(f"원본 {len(revenue_raw)}건 → 합산 {len(merged)}건")

    # 배치 upsert
    revenue_count = 0
    revenue_total = 0
    BATCH = 50
    for i in range(0, len(merged), BATCH):
        batch = merged[i:i+BATCH]
        try:
            db.upsert_revenue(batch)
            revenue_count += len(batch)
            revenue_total += sum(r['revenue'] for r in batch)
        except Exception as e:
            errors.append(f"매출 upsert 오류 (batch {i//BATCH+1}): {e}")

    log(f"매출 재처리 완료: {revenue_count}건, {revenue_total:,}원")

    return {
        'revenue_count': revenue_count,
        'revenue_total': revenue_total,
        'processed_orders': len(orders),
        'errors': errors,
        'logs': logs,
    }


# ================================================================
# 역분개: 주문 취소/환불 시 재고 복원 + 매출 삭제
# ================================================================

def reverse_order_stock(db, order_id):
    """주문 취소/환불 시 재고 복원(SALES_RETURN) + 매출 삭제.

    감사추적을 위해 원본 SALES_OUT은 유지하고,
    SALES_RETURN(+qty)을 추가하여 재고를 복원합니다.

    Args:
        db: SupabaseDB instance
        order_id: order_transactions.id

    Returns:
        dict: {stock_reversed, revenue_reversed, errors}
    """
    errors = []

    # 1. 주문 조회
    order = db.query_order_transaction_by_id(order_id)
    if not order:
        return {'stock_reversed': 0, 'revenue_reversed': 0,
                'errors': ['주문을 찾을 수 없습니다']}

    if not order.get('is_outbound_done'):
        # 출고 처리되지 않은 주문 — 역분개 불필요
        return {'stock_reversed': 0, 'revenue_reversed': 0, 'errors': []}

    pname = order.get('product_name', '')
    qty = int(order.get('qty', 0) or 0)
    odate = order.get('order_date', '')
    rev_cat = order.get('revenue_category', '')
    ch = order.get('channel', '')

    if not pname or qty <= 0:
        return {'stock_reversed': 0, 'revenue_reversed': 0,
                'errors': ['주문 데이터 부족']}

    # 2. BOM 분해 (출고 시와 동일 로직)
    bom_map = _load_bom_map(db)
    opt_map = _load_option_map(db)

    bom_all = bom_map.get('모든채널', {})
    bom_coupang = bom_map.get('쿠팡전용', {})

    is_n = (ch == 'N배송_수동' or rev_cat == 'N배송(용인)')

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
            stock_reversed += 1
        except Exception as e:
            errors.append(f"SALES_RETURN 오류 ({item}): {e}")

    # 4. 매출 삭제 (매출일 = 주문일 기준)
    rev_date = _revenue_date(odate)
    revenue_reversed = 0
    if rev_cat:
        try:
            cnt = db.delete_revenue_specific(rev_date, _norm(pname), rev_cat)
            revenue_reversed = cnt
        except Exception as e:
            errors.append(f"매출 삭제 오류: {e}")

    # 5. is_outbound_done 초기화
    try:
        db.reset_order_outbound(order_id)
    except Exception as e:
        errors.append(f"outbound 초기화 오류: {e}")

    return {
        'stock_reversed': stock_reversed,
        'revenue_reversed': revenue_reversed,
        'errors': errors,
    }


def process_single_order_realtime(db, order_id):
    """단일 주문 실시간 재처리 (수량 정정 후 호출).

    order_id의 현재 데이터로 출고+매출 처리.
    """
    order = db.query_order_transaction_by_id(order_id)
    if not order or order.get('is_outbound_done'):
        return {'outbound_count': 0, 'revenue_count': 0, 'errors': []}

    errors = []
    ch = order.get('channel', '')
    pname = order.get('product_name', '')
    qty = int(order.get('qty', 0) or 0)
    odate = order.get('order_date', '')

    if not pname or qty <= 0:
        return {'outbound_count': 0, 'revenue_count': 0, 'errors': ['데이터 부족']}

    # 매출일 = 주문일(회계기준), 재고차감일 = 오늘(출고기준)
    rev_date = _revenue_date(odate)
    stk_date = _stock_date()

    rev_cat = CHANNEL_REVENUE_MAP.get(ch, '일반매출')
    is_n = (ch == 'N배송_수동' or rev_cat == 'N배송(용인)')

    # BOM + 마스터 로드
    bom_map = _load_bom_map(db)
    opt_map = _load_option_map(db)
    price_map = db.query_price_table()
    promo_map = _preload_promotions(db)
    coupon_map = _preload_coupons(db)

    bom_all = bom_map.get('모든채널', {})
    bom_coupang = bom_map.get('쿠팡전용', {})

    if is_n:
        decomposed = {pname: qty}
    elif rev_cat in ('쿠팡매출', '로켓'):
        decomposed = _decompose(pname, qty, bom_coupang, bom_all)
    else:
        decomposed = _decompose(pname, qty, bom_all, None)

    # FIFO 출고 (재고차감일 = 오늘)
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
                inserted, skipped = db.upsert_stock_ledger_idempotent(payload)
                outbound_count += inserted
        except Exception as e:
            errors.append(f"출고 오류 ({item}): {e}")

    # 매출 기록 (매출일 = 주문일) — 프리로드 사용
    revenue_count = 0
    price_col = CATEGORY_PRICE_COL.get(rev_cat)
    if price_col:
        up, _ = _resolve_price_cached(_norm(pname), rev_cat, odate, price_map, promo_map, coupon_map)
        rev = qty * up
        if rev > 0:
            try:
                db.upsert_revenue([{
                    'revenue_date': rev_date, 'product_name': _norm(pname),
                    'category': rev_cat, 'channel': ch, 'qty': qty,
                    'unit_price': int(up), 'revenue': int(rev),
                }])
                revenue_count = 1
            except Exception as e:
                errors.append(f"매출 오류: {e}")

    # 완료 표시
    try:
        db.mark_orders_outbound_done([order_id], odate, rev_cat)
    except Exception as e:
        errors.append(f"mark_done 오류: {e}")

    return {'outbound_count': outbound_count, 'revenue_count': revenue_count, 'errors': errors}
