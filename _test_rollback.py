"""
AutoTool API 롤백 개선 시뮬레이션 테스트.

실행: python _test_rollback.py
DB 불필요 — 롤백 로직의 흐름만 검증.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))


class FakeDB:
    """rollback_import_run_full을 시뮬레이션하는 fake DB."""

    def __init__(self, fail_on_run_ids=None):
        self.rollback_calls = []
        self.fail_on_run_ids = fail_on_run_ids or set()

    def rollback_import_run_full(self, run_id, cancelled_by):
        self.rollback_calls.append(run_id)
        if run_id in self.fail_on_run_ids:
            raise Exception(f'롤백 실패: run_id={run_id}')
        return {'run_id': run_id, 'status': 'rolled_back', 'cancelled_by': cancelled_by}


def simulate_multi_channel_failure(db, channels, fail_at_channel=None):
    """다채널 API 수집을 시뮬레이션. fail_at_channel에서 Exception 발생."""
    import logging
    logger = logging.getLogger('test')

    import_run_ids = []
    results = {}

    for ch in channels:
        ch_result = {'channel': ch, 'success': False}

        try:
            if ch == fail_at_channel:
                raise Exception(f'{ch} API 호출 실패')

            # 성공 시뮬레이션
            run_id = f'run_{ch}'
            import_run_ids.append(run_id)
            ch_result['success'] = True
            ch_result['import_run_id'] = run_id

        except Exception as e:
            ch_result['error'] = str(e)

            # === 개선된 롤백: 이전 성공 채널 모두 롤백 ===
            rollback_targets = list(import_run_ids)
            rollback_results = []
            for rid in reversed(rollback_targets):
                try:
                    rb = db.rollback_import_run_full(rid, 'test_user')
                    rollback_results.append({'run_id': rid, 'result': rb})
                    import_run_ids.remove(rid)
                    logger.info('롤백 성공: run_id=%s', rid)
                except Exception as rb_err:
                    logger.error('롤백 실패: run_id=%s, error=%s', rid, rb_err)
                    rollback_results.append({'run_id': rid, 'error': str(rb_err)})
            ch_result['rollback'] = rollback_results
            ch_result['rolled_back_all'] = True

        results[ch] = ch_result

    return results, import_run_ids


def test_all_channels_success():
    """전체 성공: 롤백 없음."""
    db = FakeDB()
    results, remaining = simulate_multi_channel_failure(
        db, ['coupang', 'naver', 'cafe24'])

    assert len(remaining) == 3, f'3개 run_id 유지 기대, 실제 {len(remaining)}'
    assert len(db.rollback_calls) == 0, '롤백 호출 없어야 함'
    for ch in ['coupang', 'naver', 'cafe24']:
        assert results[ch]['success'], f'{ch} 성공이어야 함'

    print('  [PASS] 전체 성공: 3채널 모두 성공, 롤백 0건')


def test_third_channel_fails_rolls_back_all():
    """3번째 채널 실패 → 1,2번째도 전부 롤백."""
    db = FakeDB()
    results, remaining = simulate_multi_channel_failure(
        db, ['coupang', 'naver', 'cafe24'], fail_at_channel='cafe24')

    # 이전 2개 채널 모두 롤백됨
    assert len(remaining) == 0, f'전부 롤백되어야 함, 남은: {remaining}'
    assert set(db.rollback_calls) == {'run_naver', 'run_coupang'}, \
        f'coupang+naver 롤백 기대, 실제: {db.rollback_calls}'
    assert results['cafe24'].get('rolled_back_all'), 'cafe24에 rolled_back_all 플래그'

    print('  [PASS] 3번째 실패 → 이전 2개 모두 롤백')


def test_first_channel_fails_no_rollback():
    """1번째 채널 실패 → 롤백할 것 없음, 2번째는 정상 진행."""
    db = FakeDB()
    results, remaining = simulate_multi_channel_failure(
        db, ['coupang', 'naver'], fail_at_channel='coupang')

    assert len(db.rollback_calls) == 0, '롤백할 run_id가 없어야 함'
    assert results['coupang']['error'] == 'coupang API 호출 실패'
    assert results['naver']['success'], 'naver는 정상 처리'
    # naver는 정상이므로 run_id 남아있음
    assert remaining == ['run_naver'], f'naver run_id만 남아야 함, 실제: {remaining}'

    print('  [PASS] 1번째 실패 → 롤백 대상 없음, 2번째는 정상')


def test_middle_channel_fails():
    """2번째 실패 → 1번째만 롤백, 3번째는 실행 안됨."""
    db = FakeDB()
    results, remaining = simulate_multi_channel_failure(
        db, ['coupang', 'naver', 'cafe24'], fail_at_channel='naver')

    # coupang만 롤백됨
    assert 'run_coupang' in db.rollback_calls, 'coupang 롤백되어야 함'
    assert 'run_naver' not in db.rollback_calls, 'naver는 run_id 생성 전 실패'
    # cafe24는 naver 실패 후에도 계속 실행됨 (현재 로직)
    assert results['cafe24']['success'], 'cafe24는 정상 실행'
    # 남은 run_ids: cafe24만
    assert remaining == ['run_cafe24'], f'cafe24만 남아야 함, 실제: {remaining}'

    print('  [PASS] 2번째 실패 → 1번째 롤백, 3번째는 정상 실행')


def test_rollback_itself_fails():
    """롤백 자체가 실패해도 에러 기록만 하고 계속 진행."""
    db = FakeDB(fail_on_run_ids={'run_coupang'})
    results, remaining = simulate_multi_channel_failure(
        db, ['coupang', 'naver', 'cafe24'], fail_at_channel='cafe24')

    # naver 롤백 성공, coupang 롤백 실패
    rollback_results = results['cafe24']['rollback']
    assert len(rollback_results) == 2, '2개 롤백 시도'

    naver_rb = next(r for r in rollback_results if r['run_id'] == 'run_naver')
    assert 'result' in naver_rb, 'naver 롤백 성공'

    coupang_rb = next(r for r in rollback_results if r['run_id'] == 'run_coupang')
    assert 'error' in coupang_rb, 'coupang 롤백 실패 기록'

    # coupang은 롤백 실패로 import_run_ids에 남아있음
    assert 'run_coupang' in remaining, '롤백 실패한 run_id는 남아야 함'

    print('  [PASS] 롤백 실패 시 에러 기록 + 계속 진행')


if __name__ == '__main__':
    print('\n=== AutoTool API 롤백 시뮬레이션 테스트 ===\n')
    test_all_channels_success()
    test_third_channel_fails_rolls_back_all()
    test_first_channel_fails_no_rollback()
    test_middle_channel_fails()
    test_rollback_itself_fails()
    print('\n=== 전체 PASS ===\n')
