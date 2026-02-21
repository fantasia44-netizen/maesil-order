"""
ledger_service.py — 생산 수불장(Tab 9) 비즈니스 로직.
Tkinter 의존 제거. db 파라미터(SupabaseDB 인스턴스)를 받고 결과를 dict/list로 반환.
"""
import pandas as pd
from datetime import datetime
from io import BytesIO

from models import INV_TYPE_LABELS


def _validate_date(date_str):
    """날짜 형식 검증. 실패 시 ValueError raise."""
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        raise ValueError(f"날짜 형식이 올바르지 않습니다: {date_str}. YYYY-MM-DD 형식으로 입력하세요.")


def get_inventory_ledger(db, date_from=None, date_to=None, location=None,
                         category=None, search=None):
    """수불장 데이터 조회 — 전일이월 + 기간 내 거래 + 잔고 계산.

    원장(stock_ledger)에서 기간 시작일 이전의 잔고를 '전일이월'로 표시하고,
    기간 내 각 거래의 입고/출고/현재고를 계산합니다.

    Args:
        db: SupabaseDB instance
        date_from: 시작일 (YYYY-MM-DD) or None (전체 기간)
        date_to: 종료일 (YYYY-MM-DD), 필수
        location: 창고 필터 ("전체"이면 전체 조회) or None
        category: 종류 필터 ("전체"이면 전체 조회) or None
        search: 품목명 검색어 or None

    Returns:
        dict: {
            "rows": list of dict,        # 수불장 행 데이터
                각 dict: {
                    date, type_label, product_name, location, category, unit,
                    prev_balance, in_qty, out_qty, current_balance,
                    ref_no, memo, is_carry_forward
                }
            "group_count": int,          # 품목 그룹 수
            "row_count": int,            # 총 행 수
            "period_str": str            # 조회 기간 문자열
        }

    Raises:
        ValueError: 종료일 미입력
    """
    if not date_to:
        raise ValueError("종료일을 입력하세요.")

    loc_filter = location if location and location != "전체" else None
    cat_filter = category if category and category != "전체" else None

    all_data = db.query_stock_ledger(
        date_to,
        location=loc_filter,
        category=cat_filter
    )

    if not all_data:
        return {
            "rows": [],
            "group_count": 0,
            "row_count": 0,
            "period_str": f"{date_from} ~ {date_to}" if date_from else f"~ {date_to}",
        }

    df = pd.DataFrame(all_data)

    # 품목명 검색 필터
    if search:
        df = df[df['product_name'].str.contains(search, case=False, na=False)]

    if df.empty:
        return {
            "rows": [],
            "group_count": 0,
            "row_count": 0,
            "period_str": f"{date_from} ~ {date_to}" if date_from else f"~ {date_to}",
        }

    # 데이터 정제
    df['qty'] = pd.to_numeric(df['qty'], errors='coerce').fillna(0)
    df['unit'] = df['unit'].fillna('개')
    for col in ['origin', 'manufacture_date', 'memo', 'repack_doc_no', 'lot_number']:
        if col not in df.columns:
            df[col] = ''
        df[col] = df[col].fillna('')

    # 그룹 키
    group_keys = ['product_name', 'location', 'category', 'unit']

    # 전일이월 계산
    if date_from:
        df_before = df[df['transaction_date'] < date_from]
        if not df_before.empty:
            prev_balance = df_before.groupby(group_keys)['qty'].sum().reset_index()
        else:
            prev_balance = pd.DataFrame(columns=group_keys + ['qty'])
        df_period = df[(df['transaction_date'] >= date_from) & (df['transaction_date'] <= date_to)]
    else:
        prev_balance = pd.DataFrame(columns=group_keys + ['qty'])
        df_period = df.copy()

    # 전일잔고 딕셔너리
    prev_dict = {}
    for _, pb in prev_balance.iterrows():
        key = tuple(pb[k] for k in group_keys)
        prev_dict[key] = int(pb['qty'])

    # 기간 내 데이터 정렬
    sort_cols = ['transaction_date']
    if 'id' in df_period.columns:
        sort_cols.append('id')
    df_period = df_period.sort_values(by=sort_cols).reset_index(drop=True)

    # 품목별 그룹핑
    period_groups = {}
    for _, row in df_period.iterrows():
        key = tuple(row[k] for k in group_keys)
        if key not in period_groups:
            period_groups[key] = []
        period_groups[key].append(row)

    # 전일잔고만 있고 기간 내 거래가 없는 그룹도 포함
    for key in prev_dict:
        if key not in period_groups:
            period_groups[key] = []

    # 정렬: 종류 > 품목명 > 창고
    sorted_group_keys = sorted(period_groups.keys(), key=lambda k: (k[2], k[0], k[1]))

    # 결과 행 생성
    rows = []
    for gkey in sorted_group_keys:
        product_name, location_val, category_val, unit = gkey
        running = prev_dict.get(gkey, 0)
        transactions = period_groups[gkey]

        # 전일이월 행
        if running != 0:
            rows.append({
                "date": date_from if date_from else "",
                "type_label": "전일이월",
                "product_name": product_name,
                "location": location_val,
                "category": category_val,
                "unit": unit,
                "prev_balance": f"{running:,}",
                "in_qty": "",
                "out_qty": "",
                "current_balance": f"{running:,}",
                "ref_no": "",
                "memo": "",
                "is_carry_forward": True,
            })

        # 기간 내 거래 행
        for row in transactions:
            qty = int(row['qty'])
            running += qty

            type_label = INV_TYPE_LABELS.get(row.get('type', ''), row.get('type', ''))
            in_qty = f"{qty:,}" if qty >= 0 else ""
            out_qty = f"{abs(qty):,}" if qty < 0 else ""

            # 증빙번호: lot_number > repack_doc_no
            ref_no = str(row.get('lot_number', '')).strip()
            if ref_no in ('', 'nan', 'None'):
                ref_no = str(row.get('repack_doc_no', '')).strip()
            if ref_no in ('', 'nan', 'None'):
                ref_no = ''

            memo_val = str(row.get('memo', '')).strip()
            if memo_val in ('nan', 'None'):
                memo_val = ''

            rows.append({
                "date": row.get('transaction_date', ''),
                "type_label": type_label,
                "product_name": product_name,
                "location": location_val,
                "category": category_val,
                "unit": unit,
                "prev_balance": "",
                "in_qty": in_qty,
                "out_qty": out_qty,
                "current_balance": f"{running:,}",
                "ref_no": ref_no,
                "memo": memo_val,
                "is_carry_forward": False,
            })

    period_str = f"{date_from} ~ {date_to}" if date_from else f"~ {date_to}"

    return {
        "rows": rows,
        "group_count": len(sorted_group_keys),
        "row_count": len(rows),
        "period_str": period_str,
    }


def export_ledger_excel(db, date_from=None, date_to=None, location=None,
                        category=None, search=None):
    """수불장 데이터를 엑셀용 DataFrame으로 반환.

    get_inventory_ledger를 호출하여 데이터를 조회한 후 DataFrame으로 변환합니다.

    Args:
        db: SupabaseDB instance
        date_from: 시작일 (YYYY-MM-DD) or None
        date_to: 종료일 (YYYY-MM-DD), 필수
        location: 창고 필터 ("전체"이면 전체 조회) or None
        category: 종류 필터 ("전체"이면 전체 조회) or None
        search: 품목명 검색어 or None

    Returns:
        dict: {
            "dataframe": pd.DataFrame,  # 엑셀용 데이터프레임
            "period_str": str,           # 조회 기간 문자열
            "row_count": int,            # 행 수
            "group_count": int,          # 품목 그룹 수
            "bytes": bytes              # 엑셀 파일 바이트 (BytesIO)
        }

    Raises:
        ValueError: 종료일 미입력 또는 데이터 없음
    """
    result = get_inventory_ledger(
        db,
        date_from=date_from,
        date_to=date_to,
        location=location,
        category=category,
        search=search
    )

    if not result["rows"]:
        raise ValueError("내보낼 수불장 데이터가 없습니다. 먼저 조회를 실행하세요.")

    # DataFrame 변환 (수불장 표준 컬럼)
    columns = ["일자", "구분", "품목명", "창고", "종류", "단위",
               "전일재고", "입고수량", "출고수량", "현재고", "증빙번호", "비고"]

    df_rows = []
    for row in result["rows"]:
        df_rows.append([
            row["date"],
            row["type_label"],
            row["product_name"],
            row["location"],
            row["category"],
            row["unit"],
            row["prev_balance"],
            row["in_qty"],
            row["out_qty"],
            row["current_balance"],
            row["ref_no"],
            row["memo"],
        ])

    df = pd.DataFrame(df_rows, columns=columns)

    # 엑셀 바이트 생성
    output = BytesIO()
    df.to_excel(output, index=False, engine='openpyxl')
    output.seek(0)

    return {
        "dataframe": df,
        "period_str": result["period_str"],
        "row_count": result["row_count"],
        "group_count": result["group_count"],
        "bytes": output.getvalue(),
    }
