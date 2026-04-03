#!/usr/bin/env python
"""
로켓매출 재고 소급 보정 스크립트
==================================
daily_revenue(category='로켓') 중 stock_ledger에 SALES_OUT이 누락된 건을
정밀 매칭(날짜+상품+창고)으로 식별하여 gap분만 일괄 생성합니다.

사용법:
  cd C:\\autotool
  python scripts/fix_rocket_stock.py          # dry-run (미리보기)
  python scripts/fix_rocket_stock.py --apply   # 실제 적용
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from supabase import create_client
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
SERVICE_ROLE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY', '')
key = SERVICE_ROLE_KEY if SERVICE_ROLE_KEY else SUPABASE_KEY

client = create_client(SUPABASE_URL, key)


def main():
    apply_mode = '--apply' in sys.argv

    print("=" * 70)
    print("  Rocket Sales Stock Retroactive Fix")
    print("=" * 70)
    print(f"  Mode: {'APPLY' if apply_mode else 'DRY-RUN (preview)'}")
    print()

    # 1. daily_revenue (category='로켓') 전체 조회
    revs = client.table('daily_revenue').select(
        'id,revenue_date,product_name,qty,warehouse'
    ).eq('category', '로켓').order('revenue_date').execute()
    revenues = revs.data or []
    print(f"[1] daily_revenue rocket: {len(revenues)} records")
    total_qty = sum(r.get('qty', 0) for r in revenues)
    print(f"    total qty: {total_qty:,}")
    print()

    # 2. 날짜별 stock_ledger SALES_OUT 조회 + 상품×창고 매칭
    dates = sorted(set(r['revenue_date'] for r in revenues))
    already = []
    missing = []

    for d in dates:
        sl = client.table('stock_ledger').select(
            'product_name,qty,location'
        ).eq('transaction_date', d).eq('type', 'SALES_OUT').execute()

        # 날짜+상품+창고별 SALES_OUT 합산
        sl_map = {}
        for s in (sl.data or []):
            k = (s['product_name'], s['location'])
            sl_map[k] = sl_map.get(k, 0) + abs(s['qty'])

        for r in [x for x in revenues if x['revenue_date'] == d]:
            wh = r.get('warehouse') or '해서'
            existing_qty = sl_map.get((r['product_name'], wh), 0)
            if existing_qty >= r['qty']:
                already.append(r)
            else:
                gap = r['qty'] - existing_qty
                missing.append({
                    'id': r['id'],
                    'revenue_date': r['revenue_date'],
                    'product_name': r['product_name'],
                    'qty': r['qty'],
                    'warehouse': wh,
                    'existing': existing_qty,
                    'gap': gap,
                })

    print(f"[2] Already matched: {len(already)} records, {sum(r['qty'] for r in already):,} units")
    print(f"    Missing (need fix): {len(missing)} records, {sum(m['gap'] for m in missing):,} units gap")
    print()

    if not missing:
        print("[OK] No missing entries -- nothing to fix")
        return

    # 3. 상세 목록
    print("[3] Fix targets:")
    print(f"    {'Date':<12} {'Product':<25} {'Rev':>5} {'Exist':>5} {'Gap':>5} {'WH':<8} rev_id")
    print("    " + "-" * 75)
    for m in missing:
        print(f"    {m['revenue_date']:<12} {m['product_name']:<25} "
              f"{m['qty']:>5} {m['existing']:>5} {m['gap']:>5} "
              f"{m['warehouse']:<8} #{m['id']}")
    total_gap = sum(m['gap'] for m in missing)
    print(f"\n    TOTAL gap: {total_gap:,} units")
    print()

    # 4. 적용
    if not apply_mode:
        print("=" * 70)
        print("  DRY-RUN complete. To apply:")
        print("    python scripts/fix_rocket_stock.py --apply")
        print("=" * 70)
        return

    print("[4] Inserting SALES_OUT entries...")
    success = 0
    fail = 0
    for m in missing:
        payload = {
            'transaction_date': m['revenue_date'],
            'type': 'SALES_OUT',
            'product_name': m['product_name'],
            'qty': -m['gap'],  # 음수 = 출고
            'location': m['warehouse'],
            'memo': f"rocket retrofix (rev#{m['id']})",
            'created_by': 'system_fix',
        }
        try:
            client.table('stock_ledger').insert(payload).execute()
            success += 1
            print(f"    [OK] {m['revenue_date']} {m['product_name']} -{m['gap']} ({m['warehouse']}) rev#{m['id']}")
        except Exception as e:
            fail += 1
            print(f"    [FAIL] {m['revenue_date']} {m['product_name']} -- {e}")

    print()
    print("=" * 70)
    print(f"  Result: OK={success} / FAIL={fail}")
    print(f"  Total deducted: -{total_gap:,} units")
    print("=" * 70)


if __name__ == '__main__':
    main()
