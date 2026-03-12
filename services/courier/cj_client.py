"""
services/courier/cj_client.py — CJ대한통운 API 클라이언트
test_mode=True 시 실제 API 호출 없이 더미 데이터 반환 (데모용)
"""
import random
import string
import time
import logging
from datetime import datetime

import requests

log = logging.getLogger(__name__)


class CJCourierClient:
    """CJ대한통운 택배 API 클라이언트.

    Parameters
    ----------
    api_key : str
        CJ API 인증키
    customer_id : str
        CJ 고객코드 (계약 시 발급)
    base_url : str
        API 기본 URL
    test_mode : bool
        True이면 실제 API 호출 없이 더미 응답 반환
    """

    def __init__(self, api_key='', customer_id='', base_url='',
                 test_mode=True):
        self.api_key = api_key
        self.customer_id = customer_id
        self.base_url = base_url.rstrip('/') if base_url else 'https://api.cjlogistics.com'
        self.test_mode = test_mode
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.api_key}' if self.api_key else '',
        })

    # ── 송장 등록 (단건) ──

    def register_shipment(self, sender: dict, receiver: dict,
                          items: list, memo: str = '') -> dict:
        """단건 배송 등록.

        Args:
            sender: {name, phone, zipcode, address}
            receiver: {name, phone, zipcode, address}
            items: [{product_name, qty}]
            memo: 배송 메모

        Returns:
            {'ok': True, 'invoice_no': '...', 'courier': 'CJ대한통운'}
        """
        if self.test_mode:
            return self._mock_register(receiver)

        try:
            payload = {
                'customerCode': self.customer_id,
                'sender': sender,
                'receiver': receiver,
                'items': items,
                'memo': memo,
            }
            resp = self.session.post(
                f'{self.base_url}/v1/shipments',
                json=payload, timeout=15
            )
            resp.raise_for_status()
            data = resp.json()
            return {
                'ok': True,
                'invoice_no': data.get('invoiceNo', ''),
                'courier': 'CJ대한통운',
            }
        except Exception as e:
            log.error(f'CJ register_shipment error: {e}')
            return {'ok': False, 'error': str(e)}

    # ── 송장 일괄 등록 ──

    def register_shipments_bulk(self, shipments: list) -> list:
        """복수 배송 일괄 등록.

        Args:
            shipments: list of {sender, receiver, items, memo, channel, order_no}

        Returns:
            list of {channel, order_no, invoice_no, courier, ok, error?}
        """
        results = []
        for s in shipments:
            result = self.register_shipment(
                sender=s.get('sender', {}),
                receiver=s.get('receiver', {}),
                items=s.get('items', []),
                memo=s.get('memo', ''),
            )
            results.append({
                'channel': s.get('channel', ''),
                'order_no': s.get('order_no', ''),
                'invoice_no': result.get('invoice_no', ''),
                'courier': result.get('courier', 'CJ대한통운'),
                'ok': result.get('ok', False),
                'error': result.get('error', ''),
            })
        return results

    # ── 운송장 PDF ──

    def get_label_pdf(self, invoice_nos: list) -> dict:
        """운송장 라벨 PDF 조회.

        Args:
            invoice_nos: 송장번호 리스트

        Returns:
            {'ok': True, 'pdf_bytes': bytes} or {'ok': False, 'error': '...'}
        """
        if self.test_mode:
            return self._mock_label_pdf(invoice_nos)

        try:
            resp = self.session.post(
                f'{self.base_url}/v1/labels',
                json={'invoiceNos': invoice_nos},
                timeout=30,
            )
            resp.raise_for_status()
            return {'ok': True, 'pdf_bytes': resp.content}
        except Exception as e:
            log.error(f'CJ get_label_pdf error: {e}')
            return {'ok': False, 'error': str(e)}

    # ── 배송 추적 ──

    def get_tracking(self, invoice_no: str) -> dict:
        """배송 추적 정보 조회.

        Returns:
            {'ok': True, 'status': '배송중', 'steps': [...]}
        """
        if self.test_mode:
            return self._mock_tracking(invoice_no)

        try:
            resp = self.session.get(
                f'{self.base_url}/v1/tracking/{invoice_no}',
                timeout=10,
            )
            resp.raise_for_status()
            return {'ok': True, **resp.json()}
        except Exception as e:
            log.error(f'CJ get_tracking error: {e}')
            return {'ok': False, 'error': str(e)}

    # ── 배송 취소 ──

    def cancel_shipment(self, invoice_no: str) -> dict:
        """배송 취소 (픽업 전에만 가능).

        Returns:
            {'ok': True} or {'ok': False, 'error': '...'}
        """
        if self.test_mode:
            return {'ok': True, 'message': f'[TEST] {invoice_no} 취소 완료'}

        try:
            resp = self.session.delete(
                f'{self.base_url}/v1/shipments/{invoice_no}',
                timeout=10,
            )
            resp.raise_for_status()
            return {'ok': True}
        except Exception as e:
            log.error(f'CJ cancel_shipment error: {e}')
            return {'ok': False, 'error': str(e)}

    # ── Mock / Test 헬퍼 ──

    def _mock_register(self, receiver: dict) -> dict:
        """테스트용: 랜덤 송장번호 생성."""
        prefix = '6' + ''.join(random.choices(string.digits, k=11))
        time.sleep(0.05)  # 약간의 딜레이로 실제감
        return {
            'ok': True,
            'invoice_no': prefix,
            'courier': 'CJ대한통운',
        }

    def _mock_label_pdf(self, invoice_nos: list) -> dict:
        """테스트용: 더미 PDF (실제로는 빈 PDF)."""
        # 최소한의 유효 PDF
        pdf_header = b'%PDF-1.4\n1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n'
        pdf_header += b'2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj\n'
        pdf_header += b'3 0 obj<</Type/Page/Parent 2 0 R/MediaBox[0 0 595 842]>>endobj\n'
        pdf_header += b'xref\n0 4\ntrailer<</Size 4/Root 1 0 R>>\nstartxref\n0\n%%EOF'
        return {'ok': True, 'pdf_bytes': pdf_header}

    def _mock_tracking(self, invoice_no: str) -> dict:
        """테스트용: 더미 배송 추적."""
        now = datetime.now().strftime('%Y-%m-%d %H:%M')
        return {
            'ok': True,
            'status': '배송중',
            'courier': 'CJ대한통운',
            'invoice_no': invoice_no,
            'steps': [
                {'time': now, 'location': '서울 강남 집화처리', 'status': '접수'},
                {'time': now, 'location': '서울 Hub', 'status': '간선상차'},
                {'time': now, 'location': '배송중', 'status': '배달출발'},
            ],
        }
