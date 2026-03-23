"""
test_03_shipping.py — 송장 생성/갱신 테스트.

P0 테스트 #3: CJ 채번, invoice_no 저장, 마켓 push 상태.
"""
import pytest


class TestCJClient:
    """CJ택배 API 클라이언트."""

    def test_cj_client_import(self):
        """CJ 클라이언트 import."""
        from services.courier.cj_client import CJCourierClient
        assert CJCourierClient is not None

    def test_cj_client_test_mode(self):
        """CJ 테스트모드 채번."""
        from services.courier.cj_client import CJCourierClient
        client = CJCourierClient()  # cust_id 없으면 자동 test_mode
        assert client.test_mode is True
        inv = client.generate_invoice_no()
        assert len(inv) == 12
        assert inv.startswith('6')

    def test_cj_phone_split(self):
        """전화번호 분리."""
        from services.courier.cj_client import CJCourierClient
        assert CJCourierClient._split_phone('010-1234-5678') == ['010', '1234', '5678']
        assert CJCourierClient._split_phone('01012345678') == ['010', '1234', '5678']
        result = CJCourierClient._split_phone('031-217-7979')
        assert len(result) == 3, f"3파트여야 함: {result}"

    def test_cj_ab_routing(self):
        """CJ 고객번호 A/B 분기."""
        from services.cj_shipping_service import _split_order_by_cust_key

        # B만 (넥스원)
        order_b = {'products': [{'line_code': 1}, {'line_code': 3}]}
        splits = _split_order_by_cust_key(order_b)
        assert len(splits) == 1
        assert splits[0]['cust_key'] == 'B'

        # A만 (외부)
        order_a = {'products': [{'line_code': 5}]}
        splits = _split_order_by_cust_key(order_a)
        assert len(splits) == 1
        assert splits[0]['cust_key'] == 'A'

        # 혼합 → 2건
        order_mix = {'products': [{'line_code': 1}, {'line_code': 5}]}
        splits = _split_order_by_cust_key(order_mix)
        assert len(splits) == 2
        keys = {s['cust_key'] for s in splits}
        assert keys == {'A', 'B'}


class TestInvoiceMatching:
    """송장 매칭 서비스."""

    def test_parse_cj_excel_import(self):
        """CJ 엑셀 파서 import."""
        from services.invoice_matching_service import parse_cj_excel, match_invoices_to_orders
        assert callable(parse_cj_excel)
        assert callable(match_invoices_to_orders)

    def test_shipping_status_normalization(self):
        """배송상태 정규화."""
        from services.shipping_status_service import normalize_status
        assert normalize_status('naver', 'PAYED') == '발송대기'
        assert normalize_status('naver', 'DELIVERED') == '배송완료'
        assert normalize_status('coupang', 'DELIVERING') == '배송중'
        assert normalize_status('cafe24', 'N40') == '배송완료'


class TestShippingDB:
    """송장 DB 메서드."""

    def test_pending_invoice_query(self, db):
        """push 대기 조회가 에러 없이 동작."""
        result = db.query_pending_invoice_push(channel='쿠팡')
        assert isinstance(result, list)

    def test_delivery_summary(self, db):
        """배송상태 집계가 에러 없이 동작."""
        result = db.query_delivery_status_summary()
        assert isinstance(result, dict)
