"""메모리 / 캐시 / 임시파일 주기적 정리

start_cleanup_scheduler(app) 를 앱 시작 시 1회 호출.
10분마다:
  1. 임시 엑셀/CSV 파일 삭제 (output/ 폴더, 10분 이상 경과)
  2. 인메모리 캐시 초기화 (작업 중이면 건너뜀)
  3. gc.collect() 강제 실행

BusyContext:
  무거운 작업(엑셀생성 등) 진행 중 캐시 삭제 방지.
  with BusyContext(): 으로 감싸면 is_busy() == True → 캐시 삭제 skip.
"""
import gc
import logging
import os
import threading
import time

logger = logging.getLogger(__name__)

_INTERVAL = 600        # 10분
_FILE_MAX_AGE = 600    # 10분 이상 된 파일 삭제


# ──────────────────────────────────────
# BusyContext — 작업 중 캐시 삭제 방지
# ──────────────────────────────────────

_busy_count = 0
_busy_lock = threading.Lock()


class BusyContext:
    """무거운 작업(엑셀 생성 등) 진행 중 캐시 삭제를 방지하는 컨텍스트 매니저.

    Usage:
        from services.memory_utils import BusyContext
        with BusyContext():
            # 엑셀 생성 등 무거운 작업
    """
    def __enter__(self):
        global _busy_count
        with _busy_lock:
            _busy_count += 1
        return self

    def __exit__(self, *args):
        global _busy_count
        with _busy_lock:
            _busy_count -= 1


def is_busy() -> bool:
    """현재 무거운 작업이 진행 중이면 True."""
    with _busy_lock:
        return _busy_count > 0


# ──────────────────────────────────────
# 파일 정리
# ──────────────────────────────────────

def _cleanup_output_files(output_dir: str) -> int:
    """output 폴더에서 오래된 엑셀/CSV 삭제. 삭제 건수 반환."""
    if not output_dir or not os.path.isdir(output_dir):
        return 0
    deleted = 0
    now = time.time()
    for fname in os.listdir(output_dir):
        if not fname.endswith(('.xlsx', '.xls', '.csv')):
            continue
        fpath = os.path.join(output_dir, fname)
        try:
            age = now - os.path.getmtime(fpath)
            if age > _FILE_MAX_AGE:
                os.remove(fpath)
                deleted += 1
        except Exception:
            pass
    return deleted


# ──────────────────────────────────────
# 캐시 초기화
# ──────────────────────────────────────

def _clear_caches():
    """모듈별 인메모리 캐시 초기화."""
    cleared = []
    try:
        from services.dashboard_service import _dashboard_cache
        _dashboard_cache['data'] = None
        _dashboard_cache['ts'] = 0
        cleared.append('dashboard')
    except Exception:
        pass

    try:
        from services.stock_service import _unmanaged_cache
        _unmanaged_cache['data'] = None
        _unmanaged_cache['ts'] = 0
        cleared.append('stock')
    except Exception:
        pass

    try:
        from services.render_api import _CACHE
        _CACHE.clear()
        cleared.append('render_api')
    except Exception:
        pass

    return cleared


# ──────────────────────────────────────
# GC
# ──────────────────────────────────────

def _get_memory_mb() -> float:
    try:
        import psutil
        return round(psutil.Process(os.getpid()).memory_info().rss / 1048576, 1)
    except Exception:
        return 0.0


def force_gc(label: str = '') -> dict:
    before = _get_memory_mb()
    collected = gc.collect(generation=2)
    after = _get_memory_mb()
    freed = round(before - after, 1)
    logger.info(f'[GC] {label} collected={collected} '
                f'before={before}MB after={after}MB freed={freed}MB')
    return {'before': before, 'after': after, 'freed': freed}


# ──────────────────────────────────────
# 스케줄러
# ──────────────────────────────────────

def start_cleanup_scheduler(app=None):
    """백그라운드 스레드로 10분마다 정리 실행."""
    output_dir = ''
    if app:
        with app.app_context():
            output_dir = app.config.get('OUTPUT_FOLDER', 'output')

    def _run():
        time.sleep(60)  # 앱 시작 후 1분 대기
        while True:
            try:
                # 1. 파일 정리 (작업 중이어도 파일은 삭제 — 사용 중인 파일은 OS가 막음)
                deleted = _cleanup_output_files(output_dir)

                # 2. 캐시 초기화 (작업 중이면 건너뜀)
                if is_busy():
                    logger.info('[Cleanup] 작업 진행 중 — 캐시 초기화 건너뜀')
                    cleared = []
                else:
                    cleared = _clear_caches()

                # 3. GC
                result = force_gc('periodic')

                logger.info(
                    f'[Cleanup] 완료 — 파일삭제={deleted}개 '
                    f'캐시초기화={cleared} 메모리={result["after"]}MB'
                )
            except Exception as e:
                logger.debug(f'[Cleanup] 실패: {e}')

            time.sleep(_INTERVAL)

    t = threading.Thread(target=_run, daemon=True, name='cleanup-scheduler')
    t.start()
    logger.info(f'[Cleanup] 스케줄러 시작 (interval={_INTERVAL}s)')
