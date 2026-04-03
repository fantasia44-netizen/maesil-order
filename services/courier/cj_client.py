"""
services/courier/cj_client.py — CJ대한통운 택배 표준 API 클라이언트 (V3.9.3)

API 문서: CJLAPI-택배 표준 API Developer Guide-V3.9.3
인증: 1Day 토큰 (24시간 유효, CUST_ID + BIZ_REG_NUM)
Base URL:
  개발: https://dxapi-dev.cjlogistics.com:5054
  운영: https://dxapi.cjlogistics.com:5052

test_mode=True 시 실제 API 호출 없이 더미 데이터 반환 (개발용)
"""
import logging
import random
import string
import time
from datetime import datetime, timedelta

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)

# CJ 화물상태 코드 → 정규화
CJ_STATUS_MAP = {
    '01': '집화지시',
    '11': '집화처리',
    '12': '미집화',
    '41': '간선상차',
    '42': '간선하차',
    '82': '배송출발',
    '84': '미배송',
    '91': '배송완료',
}

# 박스 타입
BOX_TYPES = {
    '극소': '01', '소': '02', '중': '03', '대1': '04',
    '이형': '05', '취급제한': '06', '대2': '07',
}


class CJCourierClient:
    """CJ대한통운 택배 표준 API 클라이언트.

    Parameters
    ----------
    cust_id : str
        CJ 고객ID (계약 시 발급된 고객사 코드)
    biz_reg_num : str
        사업자번호 (하이픈 없이 10자리)
    test_mode : bool
        True이면 개발서버 사용 + 더미 응답 (CUST_ID 없을 때)
    use_prod : bool
        True이면 운영서버 사용 (기본: 개발서버)
    """

    BASE_URL_DEV = 'https://dxapi-dev.cjlogistics.com:5054'
    BASE_URL_PROD = 'https://dxapi.cjlogistics.com:5052'

    def __init__(self, cust_id='', biz_reg_num='', test_mode=True,
                 use_prod=False):
        self.cust_id = cust_id
        self.biz_reg_num = biz_reg_num.replace('-', '')
        self.test_mode = test_mode and not cust_id  # cust_id 있으면 실제 모드
        self.base_url = self.BASE_URL_PROD if use_prod else self.BASE_URL_DEV

        # 토큰 캐시
        self._token = ''
        self._token_expires = None

        # HTTP 세션 (재시도 포함)
        self.session = requests.Session()
        retry = Retry(total=2, backoff_factor=1,
                      status_forcelist=[429, 500, 502, 503])
        self.session.mount('https://', HTTPAdapter(max_retries=retry))
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        })

    @property
    def is_ready(self) -> bool:
        """실제 API 호출 가능한 상태인지."""
        return bool(self.cust_id and self.biz_reg_num)

    # ══════════════════════════════════════════
    # 1. 토큰 발행 (ReqOneDayToken)
    # ══════════════════════════════════════════

    def _ensure_token(self):
        """토큰 유효성 확인 + 필요 시 재발급."""
        if self.test_mode:
            self._token = 'TEST_TOKEN'
            return

        # 만료 30분 전이면 갱신
        if (self._token and self._token_expires
                and datetime.now() < self._token_expires - timedelta(minutes=30)):
            return

        self._request_token()

    def _request_token(self):
        """1Day 토큰 발행."""
        try:
            resp = self.session.post(
                f'{self.base_url}/ReqOneDayToken',
                json={'DATA': {
                    'CUST_ID': self.cust_id,
                    'BIZ_REG_NUM': self.biz_reg_num,
                }},
                timeout=15,
            )
            data = resp.json()
            if data.get('RESULT_CD') == 'S':
                token_data = data.get('DATA', {})
                self._token = token_data.get('TOKEN_NUM', '')
                exp_str = token_data.get('TOKEN_EXPRTN_DTM', '')
                if exp_str:
                    self._token_expires = datetime.strptime(exp_str, '%Y%m%d%H%M%S')
                log.info(f'[CJ] 토큰 발급 성공, 만료: {self._token_expires}')
            else:
                log.error(f'[CJ] 토큰 발급 실패: {data.get("RESULT_DETAIL", "")}')
                raise Exception(f'CJ 토큰 발급 실패: {data.get("RESULT_DETAIL", "")}')
        except requests.RequestException as e:
            log.error(f'[CJ] 토큰 발급 네트워크 오류: {e}')
            raise

    def _get_headers(self) -> dict:
        """API 호출용 헤더."""
        return {
            'CJ-Gateway-APIKey': self._token,
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        }

    # ══════════════════════════════════════════
    # 2. 운송장 채번 (ReqInvcNo)
    # ══════════════════════════════════════════

    def generate_invoice_no(self) -> str:
        """운송장번호 1건 생성. 다수 필요 시 반복 호출."""
        if self.test_mode:
            return '6' + ''.join(random.choices(string.digits, k=11))

        self._ensure_token()
        try:
            resp = self.session.post(
                f'{self.base_url}/ReqInvcNo',
                headers=self._get_headers(),
                json={'DATA': {
                    'TOKEN_NUM': self._token,
                    'CLNTNUM': self.cust_id,
                }},
                timeout=15,
            )
            data = resp.json()
            if data.get('RESULT_CD') == 'S':
                invc_no = data.get('DATA', {}).get('INVC_NO', '')
                log.info(f'[CJ] 채번 성공: {invc_no}')
                return invc_no
            else:
                raise Exception(f'채번 실패: {data.get("RESULT_DETAIL", "")}')
        except Exception as e:
            log.error(f'[CJ] 채번 오류: {e}')
            raise

    def generate_invoice_nos(self, count: int) -> list:
        """운송장번호 다수 생성 (1초에 1건씩 호출 제한 준수)."""
        results = []
        for i in range(count):
            try:
                invc = self.generate_invoice_no()
                results.append({'ok': True, 'invoice_no': invc})
            except Exception as e:
                results.append({'ok': False, 'error': str(e)})
            if i < count - 1:
                time.sleep(0.3)  # rate limit 대비
        return results

    # ══════════════════════════════════════════
    # 3. 예약 접수 (RegBook) — 배송 등록
    # ══════════════════════════════════════════

    def register_shipment(self, sender: dict, receiver: dict,
                          items: list, invoice_no: str = '',
                          order_no: str = '', memo: str = '',
                          box_type: str = '소') -> dict:
        """단건 배송 예약 접수.

        Args:
            sender: {name, phone, zipcode, address, detail_address}
            receiver: {name, phone, zipcode, address, detail_address}
            items: [{product_name, qty}]
            invoice_no: 운송장번호 (미리 채번한 경우)
            order_no: 고객 주문번호 (CUST_USE_NO)
            memo: 배송 메모
            box_type: 박스 타입 ('극소'/'소'/'중'/'대1'/'대2'/'이형')

        Returns:
            {'ok': True, 'invoice_no': '...', 'courier': 'CJ대한통운'}
        """
        if self.test_mode:
            return self._mock_register(receiver)

        self._ensure_token()

        # 전화번호 분리 (010-1234-5678 → ['010','1234','5678'])
        s_tel = self._split_phone(sender.get('phone', ''))
        r_tel = self._split_phone(receiver.get('phone', ''))

        # NOT NULL 필드 빈값 방어
        for tel in [s_tel, r_tel]:
            for i in range(3):
                if not tel[i]:
                    tel[i] = '0000' if i > 0 else '010'

        today = datetime.now().strftime('%Y%m%d')
        cust_use_no = order_no or f'ORD_{today}_{random.randint(10000,99999)}'
        mpck_key = f'{today}_{self.cust_id}_{cust_use_no}'

        # 합포장 상품 배열 (CJ API: DATA.ARRAY[])
        goods_array = []
        for idx, item in enumerate(items, 1):
            goods_array.append({
                'MPCK_SEQ': str(idx),
                'GDS_CD': str(idx),
                'GDS_NM': item.get('product_name', '이유식'),
                'GDS_QTY': str(item.get('qty', 1)),
                'UNIT_CD': '',
                'UNIT_NM': '',
                'GDS_AMT': '',
            })
        if not goods_array:
            goods_array = [{'MPCK_SEQ': '1', 'GDS_CD': '1',
                           'GDS_NM': '이유식', 'GDS_QTY': '1',
                           'UNIT_CD': '', 'UNIT_NM': '', 'GDS_AMT': ''}]

        payload = {
            'DATA': {
                'TOKEN_NUM': self._token,
                'CUST_ID': self.cust_id,
                'RCPT_YMD': today,
                'CUST_USE_NO': cust_use_no,
                'RCPT_DV': '01',        # 일반
                'WORK_DV_CD': '01',      # 일반
                'REQ_DV_CD': '01',       # 요청
                'MPCK_KEY': mpck_key,
                'CAL_DV_CD': '01',       # 계약운임
                'FRT_DV_CD': '03',       # 신용
                'CNTR_ITEM_CD': '01',    # 일반품목
                'BOX_TYPE_CD': BOX_TYPES.get(box_type, '02'),
                'BOX_QTY': '1',
                'FRT': '',
                'CUST_MGMT_DLCM_CD': self.cust_id,
                'DLV_DV': '01',          # 택배

                # 보내는분 (NOT NULL 필드 빈값 방어)
                'SENDR_NM': sender.get('name', '') or '배마마',
                'SENDR_TEL_NO1': s_tel[0], 'SENDR_TEL_NO2': s_tel[1], 'SENDR_TEL_NO3': s_tel[2],
                'SENDR_CELL_NO1': s_tel[0], 'SENDR_CELL_NO2': s_tel[1], 'SENDR_CELL_NO3': s_tel[2],
                'SENDR_SAFE_NO1': '', 'SENDR_SAFE_NO2': '', 'SENDR_SAFE_NO3': '',
                'SENDR_ZIP_NO': sender.get('zipcode', '') or '00000',
                'SENDR_ADDR': sender.get('address', '') or '주소',
                'SENDR_DETAIL_ADDR': sender.get('detail_address', '') or '.',

                # 받는분 (NOT NULL 필드 빈값 방어)
                'RCVR_NM': receiver.get('name', '') or '수취인',
                'RCVR_TEL_NO1': r_tel[0], 'RCVR_TEL_NO2': r_tel[1], 'RCVR_TEL_NO3': r_tel[2],
                'RCVR_CELL_NO1': r_tel[0], 'RCVR_CELL_NO2': r_tel[1], 'RCVR_CELL_NO3': r_tel[2],
                'RCVR_SAFE_NO1': '', 'RCVR_SAFE_NO2': '', 'RCVR_SAFE_NO3': '',
                'RCVR_ZIP_NO': receiver.get('zipcode', '') or '00000',
                'RCVR_ADDR': receiver.get('address', '') or '주소',
                'RCVR_DETAIL_ADDR': receiver.get('detail_address', '') or '.',

                # 주문자 (=보내는분과 동일)
                'ORDRR_NM': sender.get('name', '배마마'),
                'ORDRR_TEL_NO1': s_tel[0], 'ORDRR_TEL_NO2': s_tel[1], 'ORDRR_TEL_NO3': s_tel[2],
                'ORDRR_CELL_NO1': s_tel[0], 'ORDRR_CELL_NO2': s_tel[1], 'ORDRR_CELL_NO3': s_tel[2],
                'ORDRR_SAFE_NO1': '', 'ORDRR_SAFE_NO2': '', 'ORDRR_SAFE_NO3': '',
                'ORDRR_ZIP_NO': sender.get('zipcode', ''),
                'ORDRR_ADDR': sender.get('address', ''),
                'ORDRR_DETAIL_ADDR': sender.get('detail_address', ''),

                # 운송장 (미리 채번한 경우)
                'INVC_NO': invoice_no or '',
                'ORI_INVC_NO': '',
                'ORI_ORD_NO': '',
                'PRT_ST': '03' if invoice_no else '01',  # 선발번 / 미출력
                'ARTICLE_AMT': '',

                # 비고
                'REMARK_1': memo,
                'REMARK_2': '',
                'REMARK_3': '',
                'COD_YN': 'N',
                'ETC_1': '', 'ETC_2': '', 'ETC_3': '', 'ETC_4': '', 'ETC_5': '',
                'RCPT_SERIAL': '',

                # 합포장 상품 배열
                'ARRAY': goods_array,
            }
        }

        try:
            resp = self.session.post(
                f'{self.base_url}/RegBook',
                headers=self._get_headers(),
                json=payload,
                timeout=30,
            )
            data = resp.json()

            if data.get('RESULT_CD') == 'S':
                result_data = data.get('DATA', {})
                result_invc = result_data.get('INVC_NO', invoice_no)
                log.info(f'[CJ] 예약접수 성공: {cust_use_no} → {result_invc}')
                return {
                    'ok': True,
                    'invoice_no': result_invc,
                    'courier': 'CJ대한통운',
                    'cust_use_no': cust_use_no,
                }
            else:
                err = data.get('RESULT_DETAIL', '알 수 없는 오류')
                log.error(f'[CJ] 예약접수 실패: {err}')
                return {'ok': False, 'error': err, 'courier': 'CJ대한통운'}

        except Exception as e:
            log.error(f'[CJ] 예약접수 오류: {e}')
            return {'ok': False, 'error': str(e), 'courier': 'CJ대한통운'}

    def register_shipments_bulk(self, shipments: list) -> list:
        """복수 배송 일괄 등록.

        Args:
            shipments: [{sender, receiver, items, memo, channel, order_no, invoice_no}]

        Returns:
            [{channel, order_no, invoice_no, courier, ok, error?}]
        """
        results = []
        for s in shipments:
            result = self.register_shipment(
                sender=s.get('sender', {}),
                receiver=s.get('receiver', {}),
                items=s.get('items', []),
                invoice_no=s.get('invoice_no', ''),
                order_no=s.get('order_no', ''),
                memo=s.get('memo', ''),
            )
            results.append({
                'channel': s.get('channel', ''),
                'order_no': s.get('order_no', ''),
                'invoice_no': result.get('invoice_no', ''),
                'courier': 'CJ대한통운',
                'ok': result.get('ok', False),
                'error': result.get('error', ''),
            })
        return results

    # ══════════════════════════════════════════
    # 4. 상품추적 (ReqOneGdsTrc) — 운송장번호 기준
    # ══════════════════════════════════════════

    def get_tracking(self, invoice_no: str) -> dict:
        """배송 추적 정보 조회 (운송장번호 기준 단건).

        Returns:
            {'ok': True, 'status': '배송완료', 'status_code': '91',
             'invoice_no': '...', 'steps': [...]}
        """
        if self.test_mode:
            return self._mock_tracking(invoice_no)

        self._ensure_token()
        try:
            resp = self.session.post(
                f'{self.base_url}/ReqOneGdsTrc',
                headers=self._get_headers(),
                json={'DATA': {
                    'TOKEN_NUM': self._token,
                    'CLNTNUM': self.cust_id,
                    'INVC_NO': invoice_no,
                }},
                timeout=15,
            )
            data = resp.json()
            if data.get('RESULT_CD') == 'S':
                steps = []
                for item in (data.get('DATA') or []):
                    steps.append({
                        'status_code': item.get('CRG_ST', ''),
                        'status': CJ_STATUS_MAP.get(item.get('CRG_ST', ''), item.get('CRG_ST_NM', '')),
                        'date': item.get('SCAN_YMD', ''),
                        'time': item.get('SCAN_HOUR', ''),
                        'location': item.get('DEALT_BRAN_NM', ''),
                        'tel': item.get('DEALT_BRAN_TEL', ''),
                        'worker': item.get('DEALT_EMP_NM', ''),
                        'signer': item.get('ACPTR_NM', ''),
                    })

                # 최신 상태 = 마지막 step
                latest = steps[-1] if steps else {}
                return {
                    'ok': True,
                    'invoice_no': invoice_no,
                    'status': latest.get('status', ''),
                    'status_code': latest.get('status_code', ''),
                    'steps': steps,
                }
            else:
                return {
                    'ok': False,
                    'invoice_no': invoice_no,
                    'error': data.get('RESULT_DETAIL', ''),
                }
        except Exception as e:
            log.error(f'[CJ] 추적 오류: {e}')
            return {'ok': False, 'invoice_no': invoice_no, 'error': str(e)}

    def get_tracking_bulk(self, invoice_nos: list) -> list:
        """복수 운송장 추적 (단건 API 반복 호출, 3시간 간격 권장)."""
        results = []
        for inv in invoice_nos:
            results.append(self.get_tracking(inv))
            time.sleep(0.2)  # rate limit 대비
        return results

    # ══════════════════════════════════════════
    # 5. 예약 취소 (CnclBook)
    # ══════════════════════════════════════════

    def cancel_shipment(self, order_no: str, rcpt_ymd: str = '') -> dict:
        """예약 취소 (집화 전에만 가능).

        Args:
            order_no: CUST_USE_NO (고객사용번호)
            rcpt_ymd: 접수일자 (YYYYMMDD)
        """
        if self.test_mode:
            return {'ok': True, 'message': f'[TEST] {order_no} 취소 완료'}

        self._ensure_token()
        today = rcpt_ymd or datetime.now().strftime('%Y%m%d')
        try:
            resp = self.session.post(
                f'{self.base_url}/CnclBook',
                headers=self._get_headers(),
                json={'DATA': {
                    'TOKEN_NUM': self._token,
                    'CUST_ID': self.cust_id,
                    'RCPT_YMD': today,
                    'CUST_USE_NO': order_no,
                    'RCPT_DV': '01',
                    'WORK_DV_CD': '01',
                    'REQ_DV_CD': '02',  # 02 = 취소
                }},
                timeout=15,
            )
            data = resp.json()
            if data.get('RESULT_CD') == 'S':
                return {'ok': True, 'message': f'{order_no} 취소 완료'}
            else:
                return {'ok': False, 'error': data.get('RESULT_DETAIL', '')}
        except Exception as e:
            return {'ok': False, 'error': str(e)}

    # ══════════════════════════════════════════
    # 6. 주소정제 (ReqAddrRfnSm)
    # ══════════════════════════════════════════

    def refine_address(self, address: str) -> dict:
        """주소 정제 → 분류코드·배달점소 등 라벨 인쇄에 필요한 전체 정보 반환.

        Returns:
            ok, dest_code(분류코드), sub_dest_code(서브분류코드),
            short_addr(주소약칭), branch(배달점소),
            sm_code(배달사원별칭), region(권역), p2p_code(P2P코드)
        """
        if self.test_mode:
            return {
                'ok': True,
                'dest_code': '380',
                'sub_dest_code': '01',
                'short_addr': '강남구 역삼동',
                'branch': '역삼1',
                'sm_code': 'A01',
                'region': 'TEST',
                'p2p_code': '',
            }

        self._ensure_token()
        try:
            resp = self.session.post(
                f'{self.base_url}/ReqAddrRfnSm',
                headers=self._get_headers(),
                json={'DATA': {
                    'TOKEN_NUM': self._token,
                    'CLNTNUM': self.cust_id,
                    'CLNTMGMCUSTCD': self.cust_id,
                    'ADDRESS': address,
                }},
                timeout=10,
            )
            data = resp.json()
            if data.get('RESULT_CD') == 'S':
                d = data.get('DATA', {})
                return {
                    'ok': True,
                    'dest_code': d.get('CLSFCD', ''),
                    'sub_dest_code': d.get('SUBCLSFCD', ''),
                    'short_addr': d.get('CLSFADDR', ''),
                    'branch': d.get('CLLDLVBRANNM', ''),
                    'sm_code': d.get('CLLDLVEMPNICKNM', ''),
                    'region': d.get('RSPSDIV', ''),
                    'p2p_code': d.get('P2PCD', ''),
                }
            else:
                return {'ok': False, 'error': data.get('RESULT_DETAIL', '')}
        except Exception as e:
            return {'ok': False, 'error': str(e)}

    # ══════════════════════════════════════════
    # 유틸리티
    # ══════════════════════════════════════════

    @staticmethod
    def _split_phone(phone: str) -> list:
        """전화번호를 3파트로 분리. '010-1234-5678' → ['010','1234','5678']"""
        import re
        digits = re.sub(r'[^0-9]', '', str(phone or ''))
        if len(digits) == 11:  # 01012345678
            return [digits[:3], digits[3:7], digits[7:]]
        elif len(digits) == 10:  # 0212345678
            return [digits[:2], digits[2:6], digits[6:]]
        elif len(digits) >= 7:
            return [digits[:3], digits[3:min(7, len(digits))], digits[min(7, len(digits)):]]
        return [digits, '', '']

    # ══════════════════════════════════════════
    # Mock / Test 헬퍼
    # ══════════════════════════════════════════

    def _mock_register(self, receiver: dict) -> dict:
        """테스트용: 랜덤 송장번호 생성."""
        prefix = '6' + ''.join(random.choices(string.digits, k=11))
        time.sleep(0.05)
        return {'ok': True, 'invoice_no': prefix, 'courier': 'CJ대한통운'}

    def _mock_tracking(self, invoice_no: str) -> dict:
        """테스트용: 더미 배송 추적."""
        now = datetime.now().strftime('%Y-%m-%d %H:%M')
        return {
            'ok': True, 'status': '배송중', 'status_code': '82',
            'invoice_no': invoice_no,
            'steps': [
                {'status': '집화처리', 'status_code': '11', 'date': now, 'location': '서울 강남'},
                {'status': '간선상차', 'status_code': '41', 'date': now, 'location': '서울 Hub'},
                {'status': '배송출발', 'status_code': '82', 'date': now, 'location': '배송지역'},
            ],
        }
