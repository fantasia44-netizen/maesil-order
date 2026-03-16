"""
migrate_match_keys.py
기존 option_master의 match_key를 새 _normalize() 기준으로 재계산.

실행: python migrate_match_keys.py [--dry-run]
"""
import sys
import unicodedata
from config import SUPABASE_URL, SUPABASE_KEY
from supabase import create_client

# option_matcher._normalize와 동일 로직 (순환 import 방지)
def _normalize(key: str) -> str:
    s = str(key or '')
    s = unicodedata.normalize('NFKC', s)
    s = s.replace(' ', '').upper()
    s = s.replace(',', ';')
    return s

def main():
    dry_run = '--dry-run' in sys.argv
    client = create_client(SUPABASE_URL, SUPABASE_KEY)

    # 전체 option_master 조회
    res = client.table("option_master").select("id, original_name, match_key").or_(
        "is_deleted.is.null,is_deleted.eq.false"
    ).execute()
    rows = res.data or []
    print(f"[INFO] option_master {len(rows)}건 조회")

    changed = []
    conflicts = {}  # new_key -> [rows]

    for row in rows:
        old_key = row.get('match_key', '')
        orig = row.get('original_name', '')
        new_key = _normalize(orig)

        if old_key != new_key:
            changed.append({
                'id': row['id'],
                'original_name': orig,
                'old_key': old_key,
                'new_key': new_key,
            })
            conflicts.setdefault(new_key, []).append(row['id'])

    print(f"[INFO] match_key 변경 필요: {len(changed)}건")

    # 중복 충돌 검사
    dup_keys = {k: ids for k, ids in conflicts.items() if len(ids) > 1}
    if dup_keys:
        print(f"[WARN] 중복 충돌 {len(dup_keys)}건 (수동 처리 필요):")
        for k, ids in dup_keys.items():
            print(f"  key={k[:60]}... -> ids={ids}")

    if dry_run:
        print("[DRY-RUN] 변경 내역:")
        for c in changed[:20]:
            print(f"  id={c['id']}: {c['old_key'][:40]} -> {c['new_key'][:40]}")
        if len(changed) > 20:
            print(f"  ... 외 {len(changed) - 20}건")
        return

    # 실제 업데이트 (중복 충돌 건 제외)
    skip_ids = set()
    for ids in dup_keys.values():
        skip_ids.update(ids)

    updated = 0
    failed = 0
    for c in changed:
        if c['id'] in skip_ids:
            print(f"  [SKIP] id={c['id']} (중복 충돌)")
            continue
        try:
            client.table("option_master").update(
                {"match_key": c['new_key']}
            ).eq("id", c['id']).execute()
            updated += 1
        except Exception as e:
            print(f"  [FAIL] id={c['id']}: {e}")
            failed += 1

    print(f"[DONE] updated={updated}, skipped={len(skip_ids)}, failed={failed}")

if __name__ == '__main__':
    main()
