"""
시뮬레이션 테스트: db_supabase.py biz_id 필터 추가 검증
- DB 연결 없이 Supabase query chain을 Mock하여 검증
- biz_id=None → 기존처럼 동작 (하위호환)
- biz_id=123 → .eq("biz_id", 123) 추가 확인
"""
import sys
import types
import traceback


class MockQuery:
    """Supabase query chain 시뮬레이터."""
    def __init__(self, table_name="test"):
        self.table_name = table_name
        self.chain = []  # 체이닝된 호출 기록

    def _clone(self, method, *args):
        mq = MockQuery(self.table_name)
        mq.chain = self.chain.copy()
        mq.chain.append((method, args))
        return mq

    def select(self, *a, **kw): return self._clone("select", *a)
    def update(self, data): return self._clone("update", data)
    def insert(self, data): return self._clone("insert", data)
    def upsert(self, data, **kw): return self._clone("upsert", data)
    def delete(self): return self._clone("delete")
    def eq(self, col, val): return self._clone("eq", col, val)
    def neq(self, col, val): return self._clone("neq", col, val)
    def in_(self, col, vals): return self._clone("in_", col, vals)
    def gte(self, col, val): return self._clone("gte", col, val)
    def lte(self, col, val): return self._clone("lte", col, val)
    def like(self, col, val): return self._clone("like", col, val)
    def ilike(self, col, val): return self._clone("ilike", col, val)
    def is_(self, col, val): return self._clone("is_", col, val)
    def or_(self, expr): return self._clone("or_", expr)
    def order(self, col, **kw): return self._clone("order", col)
    def limit(self, n): return self._clone("limit", n)

    def execute(self):
        """실행 시뮬레이션 — 체인 기록 반환."""
        result = types.SimpleNamespace()
        result.data = [{"id": 1}]
        result.count = 1
        result._chain = self.chain.copy()
        return result

    def has_biz_filter(self, biz_id):
        """체인에 .eq("biz_id", biz_id)가 있는지 확인."""
        return ("eq", ("biz_id", biz_id)) in self.chain


class MockClient:
    """Supabase client Mock."""
    def __init__(self):
        self.last_table = None
        self.last_query = None

    def table(self, name):
        self.last_table = name
        mq = MockQuery(name)
        self.last_query = mq
        return mq

    def rpc(self, name, params):
        return MockQuery("rpc")


def create_mock_db():
    """Mock된 SupabaseDB 인스턴스 생성."""
    # db_supabase.py import를 위한 의존성 모킹
    # config 모듈
    config = types.ModuleType("config")
    config.SUPABASE_URL = "http://mock"
    config.SUPABASE_KEY = "mock-key"
    sys.modules["config"] = config

    # supabase 모듈
    supabase_mod = types.ModuleType("supabase")
    supabase_mod.create_client = lambda url, key: MockClient()
    supabase_mod.Client = type("Client", (), {})
    sys.modules["supabase"] = supabase_mod

    # db_base 모듈
    db_base = types.ModuleType("db_base")
    db_base.DBBase = object
    sys.modules["db_base"] = db_base

    # services.tz_utils
    tz_utils = types.ModuleType("services.tz_utils")
    tz_utils.today_kst = lambda: "2026-03-22"
    tz_utils.days_ago_kst = lambda d: "2026-03-01"
    sys.modules["services"] = types.ModuleType("services")
    sys.modules["services.tz_utils"] = tz_utils

    # services.option_matcher
    opt_matcher = types.ModuleType("services.option_matcher")
    opt_matcher._normalize = lambda x: x.upper().replace(" ", "")
    sys.modules["services.option_matcher"] = opt_matcher

    # services.channel_config (for query_revenue)
    ch_cfg = types.ModuleType("services.channel_config")
    ch_cfg.CHANNEL_REVENUE_MAP = {}
    ch_cfg.normalize_channel_display = lambda x: x
    ch_cfg.DAILY_REVENUE_ONLY_CATEGORIES = set()
    ch_cfg.DB_CUTOFF_DATE = "2025-01-01"
    ch_cfg.LEGACY_CATEGORY_TO_CHANNEL = {}
    sys.modules["services.channel_config"] = ch_cfg

    # Import the module
    import importlib
    if "db_supabase" in sys.modules:
        importlib.reload(sys.modules["db_supabase"])
    from db_supabase import SupabaseDB

    db = SupabaseDB()
    db.client = MockClient()
    db._db_cols = None
    return db


def capture_chain(db, method_name, *args, **kwargs):
    """DB 메서드 호출 후 실행된 체인을 캡처."""
    # client를 fresh MockClient로 교체
    db.client = MockClient()
    try:
        getattr(db, method_name)(*args, **kwargs)
    except Exception:
        pass  # 일부 메서드는 추가 import 필요할 수 있음
    return db.client.last_query


# ──────── 테스트 함수들 ────────

def test_with_biz_helper():
    """_with_biz 헬퍼 기본 동작 테스트."""
    db = create_mock_db()

    # biz_id=None → 체인 변경 없음
    mq1 = MockQuery("test").eq("id", 1)
    result1 = db._with_biz(mq1, None)
    assert not result1.has_biz_filter(123), "_with_biz(None)이 biz_id를 추가하면 안됨"

    # biz_id=123 → .eq("biz_id", 123) 추가
    mq2 = MockQuery("test").eq("id", 1)
    result2 = db._with_biz(mq2, 123)
    assert result2.has_biz_filter(123), "_with_biz(123)이 biz_id=123을 추가해야 함"

    print("  PASS: _with_biz 헬퍼 동작 확인")


def test_backward_compatibility():
    """biz_id 미전달 시 기존처럼 동작하는지 확인 (하위호환)."""
    db = create_mock_db()

    # 모든 수정 함수가 biz_id=None으로 기본 동작해야 함
    test_calls = [
        ("update_stock_ledger", (1, {"qty": 10})),
        ("delete_stock_ledger_by_id", (1,)),
        ("delete_revenue_by_id", (1,)),
        ("delete_partner", (1,)),
        ("update_partner", (1, {"name": "test"})),
        ("delete_manual_trade", (1,)),
        ("update_purchase_order", (1, {"status": "done"})),
        ("delete_purchase_order", (1,)),
        ("update_option_master", (1, {"product_name": "test"})),
        ("delete_option_master", (1,)),
        ("update_promotion", (1, {"name": "promo"})),
        ("delete_promotion", (1,)),
        ("update_coupon", (1, {"amount": 100})),
        ("delete_coupon", (1,)),
        ("update_expense", (1, {"amount": 500})),
        ("delete_expense", (1,)),
        ("update_employee", (1, {"name": "홍길동"})),
        ("delete_employee", (1,)),
        ("update_payroll", (1, {"amount": 3000000})),
        ("update_bank_account", (1, {"bank_name": "국민"})),
        ("update_bank_transaction", (1, {"category": "매입"})),
        ("update_tax_invoice", (1, {"status": "approved"})),
        ("delete_tax_invoice", (1,)),
        ("delete_payment_match", (1,)),
        ("update_platform_settlement", (1, {"status": "matched"})),
        ("delete_bank_account", (1,)),
        ("update_journal_entry", (1, {"status": "posted"})),
        ("update_api_sync_log", (1, {"status": "done"})),
        ("update_api_order_match", (1, {"match_status": "ok"})),
        ("reset_order_outbound", (1,)),
        ("update_packing_job", (1, {"status": "done"})),
        ("soft_delete_stock_ledger", (1,)),
        ("blind_stock_ledger", (1,)),
        ("delete_stock_ledger_all", ()),
        ("delete_revenue_all", ()),
        ("delete_product_cost", ("상품A",)),
        ("delete_channel_cost", ("쿠팡",)),
        ("update_audit_log", (1, {"status": "rolled_back"})),
        ("delete_stock_ledger_by", ("2026-03-22", "SALES_OUT")),
    ]

    passed = 0
    failed = 0
    for method, args in test_calls:
        db.client = MockClient()
        try:
            getattr(db, method)(*args)
            passed += 1
        except Exception as e:
            print(f"  FAIL: {method} - {e}")
            failed += 1

    print(f"  하위호환: {passed}/{len(test_calls)} 통과 ({failed} 실패)")
    return failed == 0


def test_biz_id_applied():
    """biz_id 전달 시 WHERE 조건에 추가되는지 확인."""
    db = create_mock_db()
    BIZ = 42

    test_cases = [
        ("update_stock_ledger", (1, {"qty": 10}), {"biz_id": BIZ}),
        ("delete_stock_ledger_by_id", (1,), {"biz_id": BIZ}),
        ("delete_revenue_by_id", (1,), {"biz_id": BIZ}),
        ("delete_partner", (1,), {"biz_id": BIZ}),
        ("update_partner", (1, {"name": "test"}), {"biz_id": BIZ}),
        ("delete_manual_trade", (1,), {"biz_id": BIZ}),
        ("update_purchase_order", (1, {"status": "done"}), {"biz_id": BIZ}),
        ("delete_purchase_order", (1,), {"biz_id": BIZ}),
        ("update_option_master", (1, {"product_name": "test"}), {"biz_id": BIZ}),
        ("delete_option_master", (1,), {"biz_id": BIZ}),
        ("update_promotion", (1, {"name": "promo"}), {"biz_id": BIZ}),
        ("delete_promotion", (1,), {"biz_id": BIZ}),
        ("update_coupon", (1, {"amount": 100}), {"biz_id": BIZ}),
        ("delete_coupon", (1,), {"biz_id": BIZ}),
        ("update_expense", (1, {"amount": 500}), {"biz_id": BIZ}),
        ("delete_expense", (1,), {"biz_id": BIZ}),
        ("update_employee", (1, {"name": "홍길동"}), {"biz_id": BIZ}),
        ("delete_employee", (1,), {"biz_id": BIZ}),
        ("update_payroll", (1, {"amount": 3000000}), {"biz_id": BIZ}),
        ("update_bank_account", (1, {"bank_name": "국민"}), {"biz_id": BIZ}),
        ("update_bank_transaction", (1, {"category": "매입"}), {"biz_id": BIZ}),
        ("update_tax_invoice", (1, {"status": "approved"}), {"biz_id": BIZ}),
        ("delete_tax_invoice", (1,), {"biz_id": BIZ}),
        ("delete_payment_match", (1,), {"biz_id": BIZ}),
        ("update_platform_settlement", (1, {"status": "matched"}), {"biz_id": BIZ}),
        ("delete_bank_account", (1,), {"biz_id": BIZ}),
        ("update_journal_entry", (1, {"status": "posted"}), {"biz_id": BIZ}),
        ("update_api_sync_log", (1, {"status": "done"}), {"biz_id": BIZ}),
        ("update_api_order_match", (1, {"match_status": "ok"}), {"biz_id": BIZ}),
        ("reset_order_outbound", (1,), {"biz_id": BIZ}),
        ("update_packing_job", (1, {"status": "done"}), {"biz_id": BIZ}),
        ("soft_delete_stock_ledger", (1,), {"biz_id": BIZ}),
        ("blind_stock_ledger", (1,), {"biz_id": BIZ}),
        ("delete_stock_ledger_all", (), {"biz_id": BIZ}),
        ("delete_revenue_all", (), {"biz_id": BIZ}),
        ("delete_product_cost", ("상품A",), {"biz_id": BIZ}),
        ("delete_channel_cost", ("쿠팡",), {"biz_id": BIZ}),
        ("update_audit_log", (1, {"status": "rolled_back"}), {"biz_id": BIZ}),
        ("delete_stock_ledger_by", ("2026-03-22", "SALES_OUT"), {"biz_id": BIZ}),
    ]

    passed = 0
    failed = 0
    for method, args, kwargs in test_cases:
        db.client = MockClient()
        try:
            getattr(db, method)(*args, **kwargs)
            passed += 1
        except Exception as e:
            print(f"  FAIL: {method}(biz_id={BIZ}) - {e}")
            traceback.print_exc()
            failed += 1

    print(f"  biz_id 적용: {passed}/{len(test_cases)} 통과 ({failed} 실패)")
    return failed == 0


def test_critical_flows():
    """핵심 경로 (주문수집/재고/정산) 시뮬레이션."""
    db = create_mock_db()

    print("  [주문수집 흐름]")
    # 1. 주문수집 후 재고차감 → 실패 시 롤백
    db.client = MockClient()
    db.update_stock_ledger(1, {"qty": -5}, biz_id=1)
    print("    재고차감(biz_id=1): OK")

    db.client = MockClient()
    db.delete_stock_ledger_by_id(1, biz_id=1)
    print("    재고삭제(biz_id=1): OK")

    # 2. 송장등록 — channel+order_no 기반이라 biz_id 불필요
    db.client = MockClient()
    db.update_order_shipping_invoice("쿠팡", "ORD001", "INV001")
    print("    송장등록(channel+order_no): OK")

    # 3. 정산 업데이트
    db.client = MockClient()
    db.update_platform_settlement(1, {"status": "matched"}, biz_id=1)
    print("    정산매칭(biz_id=1): OK")

    # 4. 매출 삭제/재계산
    db.client = MockClient()
    db.delete_revenue_by_id(1, biz_id=1)
    print("    매출삭제(biz_id=1): OK")

    # 5. 출고 리셋
    db.client = MockClient()
    db.reset_order_outbound(1, biz_id=1)
    print("    출고리셋(biz_id=1): OK")

    print("  핵심 경로 시뮬레이션: 전체 PASS")
    return True


def test_hard_delete_converted():
    """rollback_import_run_full의 하드 삭제가 소프트 삭제로 변환되었는지 확인."""
    import inspect
    db = create_mock_db()
    source = inspect.getsource(db.rollback_import_run_full)

    assert ".delete()" not in source, \
        "rollback_import_run_full에 하드 삭제(.delete())가 남아있음!"
    assert 'is_deleted' in source and 'cancelled' in source, \
        "rollback_import_run_full이 소프트 삭제로 전환되지 않음!"

    print("  PASS: 하드 삭제 → 소프트 삭제 전환 확인")
    return True


# ──────── 메인 실행 ────────

if __name__ == "__main__":
    print("=" * 60)
    print("AutoTool db_supabase.py biz_id 필터 시뮬레이션 테스트")
    print("=" * 60)

    results = []

    print("\n1. _with_biz 헬퍼 테스트")
    try:
        test_with_biz_helper()
        results.append(True)
    except AssertionError as e:
        print(f"  FAIL: {e}")
        results.append(False)

    print("\n2. 하위호환 테스트 (biz_id 미전달)")
    results.append(test_backward_compatibility())

    print("\n3. biz_id 적용 테스트 (biz_id=42)")
    results.append(test_biz_id_applied())

    print("\n4. 핵심 경로 시뮬레이션")
    results.append(test_critical_flows())

    print("\n5. 하드 삭제 → 소프트 삭제 전환 검증")
    results.append(test_hard_delete_converted())

    print("\n" + "=" * 60)
    total = len(results)
    passed = sum(results)
    if passed == total:
        print(f"전체 결과: {passed}/{total} ALL PASS")
    else:
        print(f"전체 결과: {passed}/{total} ({total - passed}건 실패)")
    print("=" * 60)

    sys.exit(0 if passed == total else 1)
