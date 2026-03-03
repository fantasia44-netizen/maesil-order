"""
core/integrity_monitor.py — 2차 사후 감시 (Integrity Monitor).

하루 1회 또는 수동 실행 시 데이터 정합성 검사.
이상 발견 → integrity_report 테이블 기록 + 경고.

사용법:
    from core.integrity_monitor import IntegrityMonitor

    monitor = IntegrityMonitor(db)
    report = monitor.run_all_checks()
    # report: {checks: [...], issues: [...], summary: str, passed: bool}

    # 개별 검사
    monitor.check_stock_balance()
    monitor.check_transfer_balance()
    monitor.check_negative_stock()
    monitor.check_adjustment_ratio()
    monitor.check_duplicate_uploads()
"""
from datetime import datetime, timedelta
from collections import defaultdict


class IntegrityIssue:
    """정합성 이상 항목."""

    SEVERITY_CRITICAL = 'critical'   # 즉시 조치 필요
    SEVERITY_WARNING = 'warning'     # 확인 필요
    SEVERITY_INFO = 'info'           # 참고

    def __init__(self, check_name, severity, message, details=None):
        self.check_name = check_name
        self.severity = severity
        self.message = message
        self.details = details or {}
        self.timestamp = datetime.now().isoformat()

    def to_dict(self):
        return {
            'check_name': self.check_name,
            'severity': self.severity,
            'message': self.message,
            'details': self.details,
            'timestamp': self.timestamp,
        }


class IntegrityMonitor:
    """데이터 정합성 사후 감시기."""

    def __init__(self, db):
        self.db = db
        self.issues = []
        self.checks_run = []

    def _add_issue(self, check_name, severity, message, details=None):
        issue = IntegrityIssue(check_name, severity, message, details)
        self.issues.append(issue)
        return issue

    # ─────────────────────────────────────────────
    # 1. 수불원장 데이터 정합성 (타입 유효성 + 필수값)
    # ─────────────────────────────────────────────
    def check_stock_balance(self, target_date=None):
        """수불원장 데이터 정합성 검증.

        - 알 수 없는 type 값 존재 여부
        - product_name 누락 레코드
        - location 누락 레코드
        - qty=0 레코드 (무의미 데이터)
        """
        check_name = 'stock_data_quality'
        self.checks_run.append(check_name)

        if target_date is None:
            target_date = datetime.now().strftime('%Y-%m-%d')

        # 최근 30일만 검사 (성능: 전체 풀스캔 방지)
        date_from = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

        try:
            all_data = self.db.query_stock_ledger(
                date_to=target_date, date_from=date_from)
            if not all_data:
                self._add_issue(check_name, IntegrityIssue.SEVERITY_INFO,
                                f'{date_from}~{target_date} 수불원장 데이터 없음')
                return

            VALID_TYPES = {
                'INBOUND', 'SALES_OUT', 'PRODUCTION', 'PROD_OUT',
                'ADJUST', 'SET_OUT', 'SET_IN', 'REPACK_OUT', 'REPACK_IN',
                'MOVE_OUT', 'MOVE_IN', 'ETC_OUT', 'ETC_IN',
                'SALES_RETURN',
            }

            unknown_types = defaultdict(int)
            missing_name = 0
            missing_loc = 0
            zero_qty = 0

            for r in all_data:
                rtype = r.get('type', '')
                if rtype and rtype not in VALID_TYPES:
                    unknown_types[rtype] += 1
                if not r.get('product_name', '').strip():
                    missing_name += 1
                if not r.get('location', '').strip():
                    missing_loc += 1
                if float(r.get('qty', 0) or 0) == 0:
                    zero_qty += 1

            issues_found = False

            if unknown_types:
                issues_found = True
                for t, cnt in unknown_types.items():
                    self._add_issue(check_name, IntegrityIssue.SEVERITY_WARNING,
                        f'알 수 없는 타입 "{t}": {cnt}건',
                        {'type': t, 'count': cnt})

            if missing_name > 0:
                issues_found = True
                self._add_issue(check_name, IntegrityIssue.SEVERITY_WARNING,
                    f'품목명 누락 레코드: {missing_name}건')

            if missing_loc > 0:
                issues_found = True
                self._add_issue(check_name, IntegrityIssue.SEVERITY_INFO,
                    f'위치 누락 레코드: {missing_loc}건')

            if zero_qty > 5:
                issues_found = True
                self._add_issue(check_name, IntegrityIssue.SEVERITY_INFO,
                    f'수량=0 레코드: {zero_qty}건 (무의미 데이터)')

            if not issues_found:
                self._add_issue(check_name, IntegrityIssue.SEVERITY_INFO,
                    f'수불원장 데이터 품질 정상 ({len(all_data)}건 검사)')

        except Exception as e:
            self._add_issue(check_name, IntegrityIssue.SEVERITY_CRITICAL,
                f'검사 실행 오류: {e}')

    # ─────────────────────────────────────────────
    # 2. 이동 출발수량 = 도착수량
    # ─────────────────────────────────────────────
    def check_transfer_balance(self, date_from=None, date_to=None):
        """이동(MOVE_OUT/MOVE_IN) 수량 대칭 검증."""
        check_name = 'transfer_balance'
        self.checks_run.append(check_name)

        if date_to is None:
            date_to = datetime.now().strftime('%Y-%m-%d')
        if date_from is None:
            date_from = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

        try:
            data = self.db.query_stock_ledger(
                date_from=date_from, date_to=date_to,
                type_list=['MOVE_OUT', 'MOVE_IN'])

            if not data:
                self._add_issue(check_name, IntegrityIssue.SEVERITY_INFO,
                    f'{date_from}~{date_to} 이동 데이터 없음')
                return

            # 날짜+품목별 그룹
            groups = defaultdict(lambda: {'out': 0.0, 'in': 0.0})
            for r in data:
                key = (r.get('transaction_date', ''), r.get('product_name', ''))
                qty = float(r.get('qty', 0) or 0)
                rtype = r.get('type', '')
                if rtype == 'MOVE_OUT':
                    groups[key]['out'] += abs(qty)
                elif rtype == 'MOVE_IN':
                    groups[key]['in'] += abs(qty)

            mismatch_count = 0
            for (date, name), totals in groups.items():
                diff = abs(totals['out'] - totals['in'])
                if diff > 0.001:
                    mismatch_count += 1
                    if mismatch_count <= 10:
                        self._add_issue(check_name, IntegrityIssue.SEVERITY_CRITICAL,
                            f'[{date}] {name}: 출발 {totals["out"]:.1f} ≠ 도착 {totals["in"]:.1f} (차이 {diff:.1f})',
                            {'date': date, 'product': name,
                             'move_out': totals['out'], 'move_in': totals['in']})

            if mismatch_count == 0:
                self._add_issue(check_name, IntegrityIssue.SEVERITY_INFO,
                    f'이동 수량 대칭 정상 ({len(groups)}건 검사)')
            elif mismatch_count > 10:
                self._add_issue(check_name, IntegrityIssue.SEVERITY_CRITICAL,
                    f'이동 불일치 총 {mismatch_count}건')

        except Exception as e:
            self._add_issue(check_name, IntegrityIssue.SEVERITY_CRITICAL,
                f'검사 실행 오류: {e}')

    # ─────────────────────────────────────────────
    # 3. 생산 투입량 = BOM 차감량
    # ─────────────────────────────────────────────
    def check_production_balance(self, date_from=None, date_to=None):
        """생산(PRODUCTION)과 투입(PROD_OUT) 대칭 검증.

        같은 날짜, 같은 batch_id 기준으로 생산과 재료투입이
        모두 존재하는지 확인.
        """
        check_name = 'production_balance'
        self.checks_run.append(check_name)

        if date_to is None:
            date_to = datetime.now().strftime('%Y-%m-%d')
        if date_from is None:
            date_from = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

        try:
            data = self.db.query_stock_ledger(
                date_from=date_from, date_to=date_to,
                type_list=['PRODUCTION', 'PROD_OUT'])

            if not data:
                self._add_issue(check_name, IntegrityIssue.SEVERITY_INFO,
                    f'{date_from}~{date_to} 생산 데이터 없음')
                return

            # batch_id별 그룹핑
            batches = defaultdict(lambda: {'production': [], 'prod_out': []})
            no_batch = {'production': 0, 'prod_out': 0}

            for r in data:
                bid = r.get('batch_id', '') or ''
                rtype = r.get('type', '')
                if bid:
                    if rtype == 'PRODUCTION':
                        batches[bid]['production'].append(r)
                    elif rtype == 'PROD_OUT':
                        batches[bid]['prod_out'].append(r)
                else:
                    if rtype == 'PRODUCTION':
                        no_batch['production'] += 1
                    elif rtype == 'PROD_OUT':
                        no_batch['prod_out'] += 1

            # batch_id 없는 레코드 경고
            if no_batch['production'] > 0 or no_batch['prod_out'] > 0:
                self._add_issue(check_name, IntegrityIssue.SEVERITY_WARNING,
                    f'batch_id 미지정: 생산 {no_batch["production"]}건, '
                    f'투입 {no_batch["prod_out"]}건 (추적 불가)')

            # batch별 생산-투입 페어 확인
            orphan_count = 0
            for bid, group in batches.items():
                if group['production'] and not group['prod_out']:
                    orphan_count += 1
                    if orphan_count <= 5:
                        names = [r.get('product_name', '') for r in group['production']]
                        self._add_issue(check_name, IntegrityIssue.SEVERITY_WARNING,
                            f'배치 {bid}: 생산만 있고 재료투입 없음 ({", ".join(names)})',
                            {'batch_id': bid})

            if orphan_count == 0 and not no_batch['production']:
                self._add_issue(check_name, IntegrityIssue.SEVERITY_INFO,
                    f'생산-투입 페어 정상 ({len(batches)}배치 검사)')

        except Exception as e:
            self._add_issue(check_name, IntegrityIssue.SEVERITY_CRITICAL,
                f'검사 실행 오류: {e}')

    # ─────────────────────────────────────────────
    # 4. 음수 재고 존재 여부 (창고별 현재 스냅샷)
    # ─────────────────────────────────────────────
    def check_negative_stock(self):
        """현재 시점 기준 음수 재고 존재 여부."""
        check_name = 'negative_stock'
        self.checks_run.append(check_name)

        try:
            from services.excel_io import build_stock_snapshot

            locations, _ = self.db.query_filter_options()
            total_negative = 0

            for loc in locations:
                raw = self.db.query_stock_by_location(loc)
                snapshot = build_stock_snapshot(raw)
                for name, info in snapshot.items():
                    total = info.get('total', 0)
                    if total < -0.001:
                        total_negative += 1
                        if total_negative <= 15:
                            self._add_issue(check_name, IntegrityIssue.SEVERITY_WARNING,
                                f'[{loc}] {name}: 음수 재고 {total:.2f}{info.get("unit", "개")}',
                                {'location': loc, 'product': name, 'qty': total})

            if total_negative == 0:
                self._add_issue(check_name, IntegrityIssue.SEVERITY_INFO,
                    f'음수 재고 없음 ({len(locations)}개 창고 검사)')
            elif total_negative > 15:
                self._add_issue(check_name, IntegrityIssue.SEVERITY_CRITICAL,
                    f'음수 재고 총 {total_negative}건')

        except Exception as e:
            self._add_issue(check_name, IntegrityIssue.SEVERITY_CRITICAL,
                f'검사 실행 오류: {e}')

    # ─────────────────────────────────────────────
    # 5. 조정(ADJUST) 비율 이상 여부
    # ─────────────────────────────────────────────
    def check_adjustment_ratio(self, date_from=None, date_to=None, threshold_pct=10):
        """기간 내 조정(ADJUST) 비율이 전체 트랜잭션 대비 일정% 초과 여부.

        Args:
            threshold_pct: 조정 비율 경고 임계값 (기본 10%)
        """
        check_name = 'adjustment_ratio'
        self.checks_run.append(check_name)

        if date_to is None:
            date_to = datetime.now().strftime('%Y-%m-%d')
        if date_from is None:
            date_from = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

        try:
            all_data = self.db.query_stock_ledger(date_from=date_from, date_to=date_to)
            if not all_data:
                self._add_issue(check_name, IntegrityIssue.SEVERITY_INFO,
                    '해당 기간 데이터 없음')
                return

            total_txn = len(all_data)
            adjust_txn = sum(1 for r in all_data if r.get('type') == 'ADJUST')

            if total_txn == 0:
                return

            ratio = (adjust_txn / total_txn) * 100

            if ratio > threshold_pct:
                self._add_issue(check_name, IntegrityIssue.SEVERITY_WARNING,
                    f'조정 비율 {ratio:.1f}% ({adjust_txn}/{total_txn}건) — '
                    f'임계값 {threshold_pct}% 초과',
                    {'ratio': ratio, 'adjust_count': adjust_txn, 'total': total_txn})
            else:
                self._add_issue(check_name, IntegrityIssue.SEVERITY_INFO,
                    f'조정 비율 정상: {ratio:.1f}% ({adjust_txn}/{total_txn}건)')

            # 대량 조정 품목 감지
            adjust_by_product = defaultdict(float)
            for r in all_data:
                if r.get('type') == 'ADJUST':
                    name = r.get('product_name', '')
                    adjust_by_product[name] += abs(float(r.get('qty', 0) or 0))

            # 상위 5개 대량 조정 품목
            top_adjusts = sorted(adjust_by_product.items(), key=lambda x: x[1], reverse=True)[:5]
            for name, total_qty in top_adjusts:
                if total_qty > 100:
                    self._add_issue(check_name, IntegrityIssue.SEVERITY_INFO,
                        f'대량 조정 품목: {name} (총 {total_qty:.0f})')

        except Exception as e:
            self._add_issue(check_name, IntegrityIssue.SEVERITY_CRITICAL,
                f'검사 실행 오류: {e}')

    # ─────────────────────────────────────────────
    # 6. 중복 파일 처리 여부
    # ─────────────────────────────────────────────
    def check_duplicate_uploads(self, days=30):
        """최근 N일 내 동일 file_hash로 처리된 건 존재 여부."""
        check_name = 'duplicate_uploads'
        self.checks_run.append(check_name)

        try:
            cutoff = (datetime.now() - timedelta(days=days)).isoformat()
            res = self.db.client.table('import_runs') \
                .select('file_hash, channel, filename, created_at') \
                .gte('created_at', cutoff) \
                .execute()

            if not res.data:
                self._add_issue(check_name, IntegrityIssue.SEVERITY_INFO,
                    f'최근 {days}일 업로드 이력 없음')
                return

            # file_hash별 그룹
            hash_count = defaultdict(int)
            for r in res.data:
                fh = r.get('file_hash', '')
                if fh:
                    hash_count[fh] += 1

            dupes = {k: v for k, v in hash_count.items() if v > 1}
            if dupes:
                self._add_issue(check_name, IntegrityIssue.SEVERITY_WARNING,
                    f'동일 파일 중복 처리 {len(dupes)}건 감지',
                    {'duplicates': dict(list(dupes.items())[:10])})
            else:
                self._add_issue(check_name, IntegrityIssue.SEVERITY_INFO,
                    f'중복 업로드 없음 ({len(hash_count)}개 파일)')

        except Exception as e:
            # import_runs 테이블 없을 수도 있음
            self._add_issue(check_name, IntegrityIssue.SEVERITY_INFO,
                f'중복 업로드 검사 스킵: {e}')

    # ─────────────────────────────────────────────
    # 7. 수불원장 타입 분포 이상
    # ─────────────────────────────────────────────
    def check_type_distribution(self, date_from=None, date_to=None):
        """수불원장 타입별 분포 확인 (이상 패턴 감지)."""
        check_name = 'type_distribution'
        self.checks_run.append(check_name)

        if date_to is None:
            date_to = datetime.now().strftime('%Y-%m-%d')
        if date_from is None:
            date_from = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

        try:
            all_data = self.db.query_stock_ledger(date_from=date_from, date_to=date_to)
            type_counts = defaultdict(int)
            for r in all_data:
                type_counts[r.get('type', 'UNKNOWN')] += 1

            summary_parts = [f"{t}: {c}건" for t, c in sorted(type_counts.items())]
            self._add_issue(check_name, IntegrityIssue.SEVERITY_INFO,
                f'{date_from}~{date_to} 타입 분포: {", ".join(summary_parts)}',
                {'distribution': dict(type_counts)})

        except Exception as e:
            self._add_issue(check_name, IntegrityIssue.SEVERITY_CRITICAL,
                f'검사 실행 오류: {e}')

    # ═══════════════════════════════════════════════
    # 전체 실행
    # ═══════════════════════════════════════════════

    def run_all_checks(self, date_from=None, date_to=None, save=True):
        """모든 정합성 검사 실행.

        Returns:
            dict: {
                checks: list of str,
                issues: list of dict,
                critical_count: int,
                warning_count: int,
                passed: bool,
                summary: str,
                run_at: str
            }
        """
        self.issues = []
        self.checks_run = []

        self.check_stock_balance(target_date=date_to)
        self.check_transfer_balance(date_from=date_from, date_to=date_to)
        self.check_production_balance(date_from=date_from, date_to=date_to)
        self.check_negative_stock()
        self.check_adjustment_ratio(date_from=date_from, date_to=date_to)
        self.check_duplicate_uploads()
        self.check_type_distribution(date_from=date_from, date_to=date_to)

        critical = sum(1 for i in self.issues if i.severity == IntegrityIssue.SEVERITY_CRITICAL)
        warning = sum(1 for i in self.issues if i.severity == IntegrityIssue.SEVERITY_WARNING)
        info = sum(1 for i in self.issues if i.severity == IntegrityIssue.SEVERITY_INFO)

        passed = critical == 0

        summary = (f"정합성 검사 완료: {len(self.checks_run)}개 항목 | "
                   f"{'✅ 통과' if passed else '❌ 이상 발견'} | "
                   f"Critical {critical} / Warning {warning} / Info {info}")

        report = {
            'checks': self.checks_run,
            'issues': [i.to_dict() for i in self.issues],
            'critical_count': critical,
            'warning_count': warning,
            'info_count': info,
            'passed': passed,
            'summary': summary,
            'run_at': datetime.now().isoformat(),
        }

        # DB에 결과 저장
        if save:
            self._save_report(report)

        return report

    def _save_report(self, report):
        """정합성 보고서를 integrity_report 테이블에 저장."""
        try:
            import json
            self.db.client.table('integrity_report').insert({
                'check_date': datetime.now().strftime('%Y-%m-%d'),
                'passed': report['passed'],
                'critical_count': report['critical_count'],
                'warning_count': report['warning_count'],
                'info_count': report['info_count'],
                'summary': report['summary'],
                'details': json.dumps(report['issues'], ensure_ascii=False),
            }).execute()
        except Exception as e:
            print(f"[IntegrityMonitor] 보고서 저장 실패: {e}")

    def get_recent_reports(self, limit=10):
        """최근 정합성 보고서 조회."""
        try:
            res = self.db.client.table('integrity_report') \
                .select('*') \
                .order('created_at', desc=True) \
                .limit(limit) \
                .execute()
            return res.data or []
        except Exception:
            return []
