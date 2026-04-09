"""
stock_service.py -- 재고 조회/통계 비즈니스 로직.
Tkinter UI 제거, 순수 데이터 반환.
"""
import pandas as pd
from datetime import datetime

from services.excel_io import build_stock_snapshot, snapshot_lookup, _snap_qty


# ─── 헬퍼 ───

# 재고관리 제외 품목 캐시 (서버 재시작 시 리셋)
_unmanaged_cache = {'data': None, 'ts': 0}

def _get_stock_unmanaged_set(db):
    """product_costs에서 is_stock_managed=false인 품목명 set 반환 (5분 캐시)."""
    import time
    now = time.time()
    if _unmanaged_cache['data'] is not None and now - _unmanaged_cache['ts'] < 300:
        return _unmanaged_cache['data']
    try:
        cost_map = db.query_product_costs()
        exclude = set()
        for name, info in cost_map.items():
            if info.get('is_stock_managed') is False:
                exclude.add(name)
                # 공백 변형도 추가
                exclude.add(name.replace(' ', ''))
        _unmanaged_cache['data'] = exclude
        _unmanaged_cache['ts'] = now
        return exclude
    except Exception:
        return set()


def _compute_daily_revenue_supplement(db, date_to, stock_ledger_rows):
    """daily_revenue(로켓/거래처매출) 중 stock_ledger SALES_OUT 에 미반영된 qty 계산.

    반환: stock_ledger 포맷의 synthetic 음수 row 리스트.
    이 row 들을 stock_ledger 원본에 합치면 snapshot groupby 가 자동으로 차감함.

    통합집계(aggregation.py) 와 동일 로직 — 두 화면의 수치 일관성 보장.

    gap 계산:
      per (canonical product_name):
        sl_sum = stock_ledger SALES_OUT/ETC_OUT/ADJUST 절대값 합 (창고 무관)
        dr_sum = daily_revenue qty 합 (로켓/거래처매출)
        gap_total = max(0, dr_sum - sl_sum)
      per (canonical product_name, warehouse):
        share = dr_per_wh / dr_sum
        supplement_qty = round(gap_total * share)
    """
    from services.product_name import canonical
    from collections import defaultdict

    # 1) stock_ledger SALES_OUT/ETC_OUT/ADJUST per canonical product
    sl_per_pn = defaultdict(int)
    for o in (stock_ledger_rows or []):
        t = o.get('type', '')
        if t not in ('SALES_OUT', 'ETC_OUT', 'ADJUST'):
            continue
        nm = canonical(o.get('product_name', ''))
        if not nm:
            continue
        sl_per_pn[nm] += abs(int(o.get('qty', 0) or 0))

    # 2) daily_revenue (로켓/거래처매출) 전체 조회 — date_to 까지
    dr_per_pn = defaultdict(int)
    dr_per_pn_wh = {}
    try:
        offset = 0
        while True:
            resp = db.client.table('daily_revenue') \
                .select('product_name,category,qty,warehouse') \
                .in_('category', ['거래처매출', '로켓']) \
                .lte('revenue_date', date_to) \
                .range(offset, offset + 999) \
                .execute()
            batch = resp.data or []
            for r in batch:
                nm = canonical(r.get('product_name', ''))
                if not nm:
                    continue
                wh = (r.get('warehouse') or '넥스원').strip() or '넥스원'
                q = int(r.get('qty', 0) or 0)
                if q <= 0:
                    continue
                dr_per_pn[nm] += q
                dr_per_pn_wh[(nm, wh)] = dr_per_pn_wh.get((nm, wh), 0) + q
            if len(batch) < 1000:
                break
            offset += 1000
    except Exception:
        return []

    if not dr_per_pn:
        return []

    # 3) gap 계산 및 synthetic 음수 row 생성
    supplement = []
    for (nm, wh), dr_qty in dr_per_pn_wh.items():
        dr_total = dr_per_pn.get(nm, 0)
        sl_total = sl_per_pn.get(nm, 0)
        gap_total = dr_total - sl_total
        if gap_total <= 0:
            continue
        share = dr_qty / dr_total if dr_total else 0
        add_qty = int(round(gap_total * share))
        if add_qty <= 0:
            continue
        supplement.append({
            'transaction_date': date_to,
            'type': 'SALES_OUT',
            'product_name': nm,
            'qty': -add_qty,
            'location': wh,
            'unit': '개',
            'category': '',
            'storage_method': '',
            'manufacture_date': '',
            'expiry_date': None,
            'origin': '',
            'food_type': '',
            'lot_number': '',
            'grade': '',
            'event_uid': f'DR_SUPPLEMENT:{date_to}:{wh}:{nm}',
            'memo': 'daily_revenue 보완 (stock_ledger 미반영분)',
        })
    return supplement


def validate_date(date_str):
    """날짜 형식 검증. 유효하면 True, 아니면 ValueError 발생."""
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        raise ValueError(f"날짜 형식이 올바르지 않습니다: {date_str}. YYYY-MM-DD 형식으로 입력하세요.")


# ─── 재고 스냅샷 ───

def get_stock_snapshot(db, date_str=None, location='전체'):
    """특정 창고의 재고 FIFO 스냅샷 딕셔너리를 반환.
    date_str 미지정 시 location 기반으로만 조회.
    returns: {product_name: {groups: [...], total: int, unit: str}}
    """
    try:
        loc = location if location and location != '전체' else None
        all_data = db.query_stock_by_location(loc) if loc else db.query_stock_by_location(location)
        return build_stock_snapshot(all_data)
    except Exception as e:
        print(f"재고 스냅샷 조회 에러: {e}")
        return {}


def load_stock_snapshot(db, location):
    """app.py의 load_stock_snapshot과 동일한 인터페이스.
    location 기반 재고 FIFO 스냅샷을 반환.
    returns: {product_name: {groups: [...], total: int, unit: str}}
    """
    try:
        all_data = db.query_stock_by_location(location)
        return build_stock_snapshot(all_data)
    except Exception as e:
        print(f"재고 스냅샷 조회 에러: {e}")
        return {}


# ─── 전체 재고 원장 데이터 조회 ───

def query_all_stock_data(db, date_to, date_from=None, location=None,
                         category=None, type_list=None, order_desc=False):
    """stock_ledger 전체 조회 → DataFrame 반환.
    app.py의 _query_all_stock_data + query_stock_ledger 호출 통합.

    Args:
        db: SupabaseDB instance
        date_to: 종료일 (필수)
        date_from: 시작일 (선택)
        location: 창고 필터 (선택, '전체'이면 None 처리)
        category: 종류 필터 (선택, '전체'이면 None 처리)
        type_list: 유형 필터 리스트 (선택)
        order_desc: 역순 정렬 여부

    Returns:
        pd.DataFrame
    """
    loc = location if location and location != '전체' else None
    cat = category if category and category != '전체' else None

    all_data = db.query_stock_ledger(
        date_to,
        date_from=date_from or None,
        location=loc,
        category=cat,
        type_list=type_list,
        order_desc=order_desc,
    )
    if not all_data:
        return pd.DataFrame()

    df = pd.DataFrame(all_data)

    # ── 재고관리 제외 품목 필터 (아이스팩, 드라이아이스 등) ──
    _exclude = _get_stock_unmanaged_set(db)
    if _exclude:
        _exclude_norm = _exclude | {n.replace(' ', '') for n in _exclude}
        df = df[~df['product_name'].isin(_exclude_norm)]
        if df.empty:
            return pd.DataFrame()
    # ── 품목명 공백 정규화 (INBOUND/SALES_OUT 그룹 일치 보장) ──
    if 'product_name' in df.columns:
        df['product_name'] = df['product_name'].astype(str).str.replace(' ', '', regex=False).str.strip()
    df['qty'] = pd.to_numeric(df['qty'], errors='coerce').fillna(0)
    df['unit'] = df['unit'].fillna('개') if 'unit' in df.columns else '개'
    for col in ['origin', 'manufacture_date', 'expiry_date', 'lot_number',
                'grade', 'storage_method', 'memo', 'repack_doc_no', 'category']:
        if col not in df.columns:
            df[col] = ''
        df[col] = df[col].fillna('')
    return df


# ─── 재고 현황 (스냅샷 뷰) ───

def query_stock_snapshot(db, date_str, location=None, category=None,
                         search=None, storage_method=None, food_type=None,
                         split_expiry=False, split_manufacture=False,
                         split_lot_number=False):
    """기준일 기준 재고현황을 조회하여 리스트로 반환.
    app.py의 _refresh_stock_view 로직과 동일.

    Args:
        db: SupabaseDB instance
        date_str: 기준일 (YYYY-MM-DD)
        location: 창고 필터 (선택, '전체' = 전체)
        category: 종류 필터 (선택, '전체' = 전체)
        search: 품목명 검색어 (선택)
        storage_method: 보관방법 필터 (선택, '전체' = 전체)
        split_expiry: 소비기한 분리 여부
        split_manufacture: 제조일 분리 여부
        split_lot_number: 이력번호 분리 여부

    Returns:
        list of dict: [
            {product_name, qty, unit, location, category, storage_method,
             expiry_date(optional), manufacture_date(optional),
             lot_number(optional), grade(optional), is_negative},
            ...
        ]
    """
    # ─────────────────────────────────────────────
    # Phase 3: 재고 스냅샷 RPC 우선 (OOM 차단)
    # stock_ledger 풀스캔 + pandas groupby 제거, SQL 집계로 대체
    # ─────────────────────────────────────────────
    split_mode = 'none'
    if split_manufacture:
        split_mode = 'manufacture'
    elif split_expiry:
        split_mode = 'expiry'
    elif split_lot_number:
        split_mode = 'lot_number'

    try:
        res = db.client.rpc('get_stock_snapshot_agg', {
            'p_date_to': date_str,
            'p_split_mode': split_mode,
        }).execute()
        snapshot_rows = res.data or []
        if snapshot_rows:
            all_data = snapshot_rows
        else:
            all_data = db.query_stock_ledger(date_str)
    except Exception as _rpc_err:
        import logging
        logging.getLogger(__name__).warning(
            f'[query_stock_snapshot] RPC 실패, 풀스캔 폴백: {_rpc_err}')
        all_data = db.query_stock_ledger(date_str)

    if not all_data:
        return []

    # ★ daily_revenue 보완 차감 — RPC 경로에선 이미 SQL 집계 완료 상태라 스킵
    #   raw 폴백 때만 적용
    _is_rpc_path = isinstance(all_data, list) and all_data and \
                   'transaction_date' not in all_data[0]
    if not _is_rpc_path:
        try:
            _supplement = _compute_daily_revenue_supplement(db, date_str, all_data)
            if _supplement:
                all_data = list(all_data) + _supplement
        except Exception as _e:
            import logging
            logging.getLogger(__name__).warning(
                f'[query_stock_snapshot] daily_revenue 보완 실패: {_e}')

    df = pd.DataFrame(all_data)
    if df.empty:
        return []

    # ── 재고관리 제외 품목 필터 (아이스팩, 드라이아이스 등) ──
    _exclude = _get_stock_unmanaged_set(db)
    if _exclude:
        _exclude_norm = _exclude | {n.replace(' ', '') for n in _exclude}
        df = df[~df['product_name'].isin(_exclude_norm)]
        if df.empty:
            return []

    # ── 품목명 공백 정규화 (수불장과 동일 기준 — 일관된 집계 보장) ──
    if 'product_name' in df.columns:
        df['product_name'] = df['product_name'].astype(str).str.replace(' ', '', regex=False).str.strip()

    for col in ['manufacture_date', 'category', 'storage_method', 'expiry_date', 'origin', 'food_type', 'lot_number', 'grade']:
        if col not in df.columns:
            df[col] = ''
        df[col] = df[col].fillna('')
    if 'unit' not in df.columns:
        df['unit'] = '개'
    df['unit'] = df['unit'].fillna('개')

    # ── 카테고리 빈값 통합: 같은 품목/위치에 카테고리가 있으면 채워넣기 ──
    cat_base = ['product_name', 'location']
    cat_map = df[df['category'] != ''].groupby(cat_base)['category'].first().to_dict()
    if cat_map:
        mask_cat = df['category'] == ''
        if mask_cat.any():
            df.loc[mask_cat, 'category'] = df.loc[mask_cat, cat_base].apply(
                lambda row: cat_map.get(tuple(row), ''), axis=1
            )

    # ── 보관방법 빈값 통합: 같은 품목/위치에 보관방법이 있으면 채워넣기 ──
    sm_base = ['product_name', 'location']
    sm_map = df[df['storage_method'] != ''].groupby(sm_base)['storage_method'].first().to_dict()
    if sm_map:
        mask = df['storage_method'] == ''
        if mask.any():
            df.loc[mask, 'storage_method'] = df.loc[mask, sm_base].apply(
                lambda row: sm_map.get(tuple(row), ''), axis=1
            )

    # ── product_costs 마스터에서 category/storage_method/food_type fallback ──
    pc_map = None
    try:
        pc_map = db.query_product_costs()
    except Exception:
        pass

    if pc_map:
        # product_costs → {품목명: {category, storage_method, food_type}} 맵 구축
        pc_cat = {}
        pc_sm = {}
        for pn, info in pc_map.items():
            cat_val = (info.get('category') or '').strip()
            sm_val = (info.get('storage_method') or '').strip()
            for name in (pn, pn.replace(' ', '')):
                if cat_val and name not in pc_cat:
                    pc_cat[name] = cat_val
                if sm_val and name not in pc_sm:
                    pc_sm[name] = sm_val

        # category fallback (stock_ledger 내부 상속 후 여전히 빈값인 것만)
        mask_cat2 = df['category'] == ''
        if mask_cat2.any() and pc_cat:
            df.loc[mask_cat2, 'category'] = df.loc[mask_cat2, 'product_name'].map(pc_cat).fillna('')

        # storage_method fallback
        mask_sm2 = df['storage_method'] == ''
        if mask_sm2.any() and pc_sm:
            df.loc[mask_sm2, 'storage_method'] = df.loc[mask_sm2, 'product_name'].map(pc_sm).fillna('')

    # food_type 빈값 통합: stock_ledger 내부 + product_costs fallback (공백 정규화)
    ft_map = df[df['food_type'] != ''].groupby('product_name')['food_type'].first().to_dict()
    # product_costs에서 food_type 가져와서 fallback (공백 있는/없는 이름 모두 매핑)
    if pc_map:
        for pn, info in pc_map.items():
            ft_val = (info.get('food_type') or '').strip()
            if ft_val:
                if pn not in ft_map:
                    ft_map[pn] = ft_val
                norm = pn.replace(' ', '')
                if norm != pn and norm not in ft_map:
                    ft_map[norm] = ft_val
    if ft_map:
        mask_ft = df['food_type'] == ''
        if mask_ft.any():
            df.loc[mask_ft, 'food_type'] = df.loc[mask_ft, 'product_name'].map(ft_map).fillna('')

    if split_manufacture:
        group_cols = ['product_name', 'location', 'category', 'storage_method', 'unit', 'manufacture_date']
    elif split_expiry:
        group_cols = ['product_name', 'location', 'category', 'storage_method', 'unit', 'expiry_date']
    elif split_lot_number:
        group_cols = ['product_name', 'location', 'category', 'storage_method', 'unit', 'lot_number', 'grade']
    else:
        group_cols = ['product_name', 'location', 'category', 'storage_method', 'unit']

    summary = df.groupby(group_cols, dropna=False)['qty'].sum().reset_index()
    summary = summary[summary['qty'] != 0]

    # food_type을 product_name 기준으로 매핑 (그룹 키에 포함하지 않고 별도 매핑)
    ft_lookup = ft_map  # {product_name: food_type}

    if search:
        summary = summary[summary['product_name'].str.contains(search, case=False, na=False)]
    if location and location != '전체':
        summary = summary[summary['location'] == location]
    if category and category != '전체':
        summary = summary[summary['category'] == category]
    if storage_method and storage_method != '전체':
        summary = summary[summary['storage_method'] == storage_method]

    # food_type 필터
    if food_type and food_type != '전체':
        summary = summary[summary['product_name'].map(lambda n: ft_lookup.get(n, '') == food_type)]

    results = []
    for _, r in summary.iterrows():
        unit = r['unit'] if pd.notna(r.get('unit')) else '개'
        qty_val = _snap_qty(r['qty'], unit)
        item = {
            'product_name': r['product_name'],
            'qty': qty_val,
            'qty_str': f"{qty_val:,}{unit}",
            'unit': unit,
            'location': r['location'],
            'category': r['category'],
            'storage_method': r['storage_method'],
            'food_type': ft_lookup.get(r['product_name'], ''),
            'is_negative': qty_val < 0,
        }
        if split_manufacture:
            item['manufacture_date'] = r['manufacture_date'] if pd.notna(r.get('manufacture_date')) else ''
        if split_expiry:
            item['expiry_date'] = r['expiry_date'] if pd.notna(r.get('expiry_date')) else ''
        if split_lot_number:
            item['lot_number'] = r['lot_number'] if pd.notna(r.get('lot_number')) else ''
            item['grade'] = r['grade'] if pd.notna(r.get('grade')) else ''
        results.append(item)

    return results


# ─── 이력 뷰 ───

def query_history_view(db, date_str, mode='전체이력', location=None,
                       category=None, search=None, storage_method=None):
    """이력 조회 모드별 필터링하여 리스트 반환.
    app.py의 _refresh_history_view 로직과 동일.

    Args:
        db: SupabaseDB instance
        date_str: 기준일 (종료일)
        mode: 조회 모드 (OUT(출고), SALES_OUT(매출출고), etc.)
        location: 창고 필터 ('전체' = 전체)
        category: 종류 필터 ('전체' = 전체)
        search: 품목명 검색어
        storage_method: 보관방법 필터 ('전체' = 전체)

    Returns:
        list of dict
    """
    type_map = {
        "OUT(출고)": ["SALES_OUT", "PROD_OUT", "REPACK_OUT", "SET_OUT", "ETC_OUT"],
        "SALES_OUT(매출출고)": ["SALES_OUT"],
        "PROD_OUT(생산출고)": ["PROD_OUT"],
        "ETC_OUT(기타출고)": ["ETC_OUT"],
        "ETC_IN(기타입고)": ["ETC_IN"],
        "IN(생산/입고)": ["PRODUCTION", "INBOUND", "REPACK_IN", "SET_IN", "ETC_IN"],
        "PRODUCTION(생산)": ["PRODUCTION"],
        "INBOUND(입고)": ["INBOUND"],
        "MOVE(창고이동)": ["MOVE_IN", "MOVE_OUT"],
        "REPACK(소분)": ["REPACK_OUT", "REPACK_IN"],
        "SET(세트)": ["SET_OUT", "SET_IN"],
        "INIT(기초)": ["INIT"],
        "전체이력": None,
    }
    filter_types = type_map.get(mode)

    # Phase 3: 이력 조회 SQL RPC 우선 (필터링 SQL 이관)
    all_data = None
    try:
        res = db.client.rpc('get_stock_history_view', {
            'p_date_to': date_str,
            'p_types': filter_types,
            'p_location': location if location and location != '전체' else None,
            'p_category': category if category and category != '전체' else None,
            'p_storage_method': storage_method if storage_method and storage_method != '전체' else None,
            'p_search': search if search else None,
            'p_limit': 5000,
        }).execute()
        all_data = res.data or []
    except Exception as _e:
        import logging
        logging.getLogger(__name__).warning(
            f'[query_history_view] RPC 실패, 풀스캔 폴백: {_e}')
        all_data = db.query_stock_ledger(date_str, order_desc=True)

    results = []
    for r in all_data:
        if r.get('qty', 0) == 0:
            continue
        if filter_types and r.get('type') not in filter_types:
            continue
        if search and search.lower() not in r.get('product_name', '').lower():
            continue
        if location and location != '전체' and r.get('location') != location:
            continue
        if category and category != '전체' and r.get('category') != category:
            continue
        if storage_method and storage_method != '전체' and r.get('storage_method') != storage_method:
            continue

        unit = r.get('unit', '개') or '개'
        results.append({
            'transaction_date': r.get('transaction_date', ''),
            'type': r.get('type', ''),
            'product_name': r.get('product_name', ''),
            'qty': r.get('qty', 0),
            'qty_str': f"{r.get('qty', 0)}{unit}",
            'unit': unit,
            'location': r.get('location', ''),
            'category': r.get('category', ''),
            'expiry_date': r.get('expiry_date', '') or '',
            'storage_method': r.get('storage_method', ''),
            'lot_number': r.get('lot_number', '') or '',
            'grade': r.get('grade', '') or '',
        })

    return results


# ─── 통합 조회 (refresh_stats 대체) ───

def get_stats(db, date_str, location='전체', category='전체',
              search=None, storage_method='전체', view_mode='재고현황'):
    """탭1 재고통계 조회 (refresh_stats 대체).
    view_mode에 따라 재고현황 또는 이력 데이터를 반환.

    Args:
        db: SupabaseDB instance
        date_str: 기준일 (YYYY-MM-DD)
        location: 창고 필터
        category: 종류 필터
        search: 품목명 검색어
        storage_method: 보관방법 필터
        view_mode: 조회 모드

    Returns:
        dict: {
            'mode': str,
            'columns': tuple of str,
            'rows': list of dict,
            'total_items': int,
            'total_qty': int (재고현황 모드일 때),
            'locations': list of str,
            'categories': list of str,
        }
    """
    if view_mode == '재고현황':
        columns = ("품목명", "수량(기준일)", "창고위치", "종류", "보관방법")
        rows = query_stock_snapshot(db, date_str, location=location,
                                    category=category, search=search,
                                    storage_method=storage_method)
    elif view_mode == '재고현황(소비기한분리)':
        columns = ("품목명", "수량(기준일)", "창고위치", "종류", "소비기한", "보관방법")
        rows = query_stock_snapshot(db, date_str, location=location,
                                    category=category, search=search,
                                    storage_method=storage_method,
                                    split_expiry=True)
    elif view_mode == '재고현황(제조일분리)':
        columns = ("품목명", "수량(기준일)", "창고위치", "종류", "제조일", "보관방법")
        rows = query_stock_snapshot(db, date_str, location=location,
                                    category=category, search=search,
                                    storage_method=storage_method,
                                    split_manufacture=True)
    else:
        columns = ("일자", "유형", "품목명", "수량", "창고위치", "종류",
                   "소비기한", "보관방법", "이력번호", "등급")
        rows = query_history_view(db, date_str, mode=view_mode,
                                  location=location, category=category,
                                  search=search, storage_method=storage_method)

    # 집계 정보
    total_qty = 0
    locations_set = set()
    categories_set = set()
    for r in rows:
        total_qty += r.get('qty', 0)
        loc = r.get('location', '')
        cat = r.get('category', '')
        if loc:
            locations_set.add(loc)
        if cat:
            categories_set.add(cat)

    return {
        'mode': view_mode,
        'columns': columns,
        'rows': rows,
        'total_items': len(rows),
        'total_qty': total_qty,
        'locations': sorted(locations_set),
        'categories': sorted(categories_set),
    }


# ─── 수불장 데이터 조회 ───

def query_ledger_data(db, date_from, date_to, location=None, category=None,
                      search=None, split_manufacture=False, split_expiry=False,
                      split_lot_number=False):
    """수불장(전일이월 + 기간거래) 데이터를 조회하여 dict로 반환.
    app.py의 _query_ledger_data 로직과 동일.

    Returns:
        dict: {
            'prev_dict': {key: int},           -- 전일이월 잔고
            'period_groups': {key: [rows]},     -- 기간 거래
            'sorted_keys': [key, ...],          -- 정렬된 그룹 키
            'group_keys': [str, ...],           -- 그룹핑 컬럼명
        }
    """
    df = query_all_stock_data(db, date_to)
    if df.empty:
        return {'prev_dict': {}, 'period_groups': {}, 'sorted_keys': [], 'group_keys': []}

    if search:
        df = df[df['product_name'].str.contains(search, case=False, na=False)]
    if location and location != '전체':
        df = df[df['location'] == location]
    if category and category != '전체':
        df = df[df['category'] == category]
    if df.empty:
        return {'prev_dict': {}, 'period_groups': {}, 'sorted_keys': [], 'group_keys': []}

    group_keys = ['product_name', 'location', 'category', 'unit', 'storage_method']
    if split_manufacture:
        group_keys = ['product_name', 'location', 'category', 'unit', 'storage_method', 'manufacture_date']
    elif split_expiry:
        group_keys = ['product_name', 'location', 'category', 'unit', 'storage_method', 'expiry_date']
    elif split_lot_number:
        group_keys = ['product_name', 'location', 'category', 'unit', 'storage_method', 'lot_number']

    # ── 카테고리 빈값 통합: 같은 품목/위치에 카테고리가 있으면 채워넣기 ──
    cat_base = ['product_name', 'location']
    cat_map = df[df['category'] != ''].groupby(cat_base)['category'].first().to_dict()
    if cat_map:
        mask_cat = df['category'] == ''
        if mask_cat.any():
            df.loc[mask_cat, 'category'] = df.loc[mask_cat, cat_base].apply(
                lambda row: cat_map.get(tuple(row), ''), axis=1
            )

    # ── 보관방법 빈값 통합: 같은 품목/위치에 보관방법이 있으면 채워넣기 ──
    sm_base = ['product_name', 'location']
    sm_map = df[df['storage_method'] != ''].groupby(sm_base)['storage_method'].first().to_dict()
    if sm_map:
        mask = df['storage_method'] == ''
        if mask.any():
            df.loc[mask, 'storage_method'] = df.loc[mask, sm_base].apply(
                lambda row: sm_map.get(tuple(row), ''), axis=1
            )

    # ── product_costs 마스터에서 category/storage_method fallback ──
    try:
        pc_map = db.query_product_costs()
        if pc_map:
            pc_cat = {}
            pc_sm = {}
            for pn, info in pc_map.items():
                cat_val = (info.get('category') or '').strip()
                sm_val = (info.get('storage_method') or '').strip()
                for name in (pn, pn.replace(' ', '')):
                    if cat_val and name not in pc_cat:
                        pc_cat[name] = cat_val
                    if sm_val and name not in pc_sm:
                        pc_sm[name] = sm_val
            mask_cat2 = df['category'] == ''
            if mask_cat2.any() and pc_cat:
                df.loc[mask_cat2, 'category'] = df.loc[mask_cat2, 'product_name'].map(pc_cat).fillna('')
            mask_sm2 = df['storage_method'] == ''
            if mask_sm2.any() and pc_sm:
                df.loc[mask_sm2, 'storage_method'] = df.loc[mask_sm2, 'product_name'].map(pc_sm).fillna('')
    except Exception:
        pass

    if date_from:
        df_before = df[df['transaction_date'] < date_from]
        df_period = df[(df['transaction_date'] >= date_from) & (df['transaction_date'] <= date_to)]
    else:
        df_before = pd.DataFrame(columns=df.columns)
        df_period = df.copy()

    prev_dict = {}
    if not df_before.empty:
        pb = df_before.groupby(group_keys)['qty'].sum().reset_index()
        for _, r in pb.iterrows():
            key = tuple(r[k] for k in group_keys)
            prev_dict[key] = _snap_qty(r['qty'], r.get('unit', '개'))

    sort_cols = ['transaction_date']
    if 'id' in df_period.columns:
        sort_cols.append('id')
    df_period = df_period.sort_values(by=sort_cols).reset_index(drop=True)

    period_groups = {}
    for _, row in df_period.iterrows():
        key = tuple(row[k] for k in group_keys)
        if key not in period_groups:
            period_groups[key] = []
        period_groups[key].append(row.to_dict())

    for key in prev_dict:
        if key not in period_groups:
            period_groups[key] = []

    sorted_keys = sorted(period_groups.keys(), key=lambda k: (k[1], k[2], k[0]))

    return {
        'prev_dict': prev_dict,
        'period_groups': period_groups,
        'sorted_keys': sorted_keys,
        'group_keys': group_keys,
    }


# ─── 생산 로그 조회 ───

def query_production_log(db, target_date, location=None):
    """특정 일자의 생산/생산출고 기록을 조회.
    app.py의 _query_production_log_data 로직과 동일.

    Returns:
        dict: {
            'production': list of dict,   -- PRODUCTION 기록
            'prod_out': list of dict,      -- PROD_OUT 기록
        }
    """
    df = query_all_stock_data(db, target_date)
    if df.empty:
        return {'production': [], 'prod_out': []}

    df = df[df['transaction_date'] == target_date]
    if location and location != '전체':
        df = df[df['location'] == location]

    df_prod = df[df['type'] == 'PRODUCTION']
    df_out = df[df['type'] == 'PROD_OUT']

    return {
        'production': df_prod.to_dict('records'),
        'prod_out': df_out.to_dict('records'),
    }


# ─── 필터 옵션 조회 ───

def get_filter_options(db):
    """창고/종류 필터 옵션 목록을 반환.

    Returns:
        dict: {
            'locations': list of str,
            'categories': list of str,
        }
    """
    try:
        locs, cats = db.query_filter_options()
        return {
            'locations': locs or [],
            'categories': cats or [],
        }
    except Exception:
        return {'locations': [], 'categories': []}
