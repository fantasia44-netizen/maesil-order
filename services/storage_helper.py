"""
Supabase Storage 헬퍼 — 파일 저장 후 자동 백업

사용법:
    from services.storage_helper import backup_to_storage
    backup_to_storage(db, local_path, bucket='upload', prefix='orders')
"""
import os
from datetime import datetime, timezone


def _date_prefix():
    """오늘 날짜 prefix (KST 기준)."""
    from services.tz_utils import now_kst
    try:
        return now_kst().strftime('%Y-%m-%d')
    except Exception:
        return datetime.now().strftime('%Y-%m-%d')


def backup_to_storage(db, local_path, bucket='output', prefix=''):
    """로컬 파일을 Supabase Storage에 백업.

    Args:
        db: SupabaseDB instance
        local_path: 로컬 파일 경로
        bucket: 'output' | 'upload' | 'report'
        prefix: Storage 내 추가 폴더 (예: 'orders', 'master')

    Returns:
        storage_path (str) or None
    """
    if not db or not local_path or not os.path.exists(local_path):
        return None
    try:
        fname = os.path.basename(local_path)
        date = _date_prefix()
        parts = [date]
        if prefix:
            parts.append(prefix)
        parts.append(fname)
        storage_path = '/'.join(parts)

        with open(local_path, 'rb') as f:
            file_bytes = f.read()

        if db._storage_upload(bucket, storage_path, file_bytes):
            return storage_path
        return None
    except Exception as e:
        print(f"[Storage] backup error: {e}")
        return None


def backup_bytes_to_storage(db, file_bytes, filename, bucket='output', prefix=''):
    """바이트 데이터를 Supabase Storage에 직접 저장 (로컬 파일 없이).

    Args:
        db: SupabaseDB instance
        file_bytes: bytes 데이터
        filename: 파일명 (확장자 포함)
        bucket: 'output' | 'upload' | 'report'
        prefix: Storage 내 추가 폴더

    Returns:
        storage_path (str) or None
    """
    if not db or not file_bytes:
        return None
    try:
        date = _date_prefix()
        parts = [date]
        if prefix:
            parts.append(prefix)
        parts.append(filename)
        storage_path = '/'.join(parts)

        if db._storage_upload(bucket, storage_path, file_bytes):
            return storage_path
        return None
    except Exception as e:
        print(f"[Storage] backup_bytes error: {e}")
        return None


def backup_multiple(db, file_paths, bucket='output', prefix=''):
    """여러 파일을 한번에 백업. Returns: list of storage_paths."""
    results = []
    for fp in file_paths:
        sp = backup_to_storage(db, fp, bucket, prefix)
        if sp:
            results.append(sp)
    return results
