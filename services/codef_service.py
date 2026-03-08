"""
codef_service.py -- CODEF API 래퍼.
easycodefpy를 사용한 은행/카드 계좌 연결 및 거래내역 조회.
인스턴스를 앱 레벨 싱글톤으로 유지 (app.codef).

서비스 모드:
  - sandbox: 고정 테스트 응답 (개발용, 실제 은행 연동 없음)
  - demo:    실제 은행 연동 테스트 (데모 키 사용, 실제 은행 ID/PW 필요)
  - product: 운영 환경
"""
import json
import logging
from easycodefpy import Codef, ServiceType, encrypt_rsa

logger = logging.getLogger(__name__)

# ── 은행 기관코드 매핑 ──
BANK_CODES = {
    '0002': 'KDB산업은행', '0003': 'IBK기업은행', '0004': 'KB국민은행',
    '0007': '수협은행',    '0011': 'NH농협은행',  '0020': '우리은행',
    '0023': 'SC제일은행',  '0027': '한국씨티은행', '0031': '대구은행',
    '0032': '부산은행',    '0034': '광주은행',    '0035': '제주은행',
    '0037': '전북은행',    '0039': '경남은행',    '0045': '새마을금고',
    '0048': '신협',       '0071': '우체국',      '0081': '하나은행',
    '0088': '신한은행',    '0089': 'K뱅크',      '0090': '카카오뱅크',
    '0092': '토스뱅크',
}

# ── 카드사 기관코드 매핑 ──
CARD_CODES = {
    '0301': 'KB국민카드',  '0302': '현대카드',    '0303': '삼성카드',
    '0304': 'NH농협카드',  '0305': '하나카드',    '0306': 'BC카드',
    '0307': '신한카드',    '0309': '씨티카드',    '0311': '롯데카드',
    '0313': '우리카드',
}


class CodefService:
    """CODEF API 래퍼 (싱글톤).

    모드:
      - CODEF_MODE=sandbox → 샌드박스 (고정 응답, 개발용)
      - CODEF_MODE=demo    → 데모 (실제 은행 연동 테스트)
      - CODEF_MODE=product → 운영
    """

    def __init__(self, config):
        self.codef = Codef()
        self.codef.public_key = config.get('CODEF_PUBLIC_KEY', '')

        # 모드 결정: sandbox / demo / product
        mode = config.get('CODEF_MODE', '').lower()
        if not mode:
            # 호환성: CODEF_IS_TEST=true → sandbox (개발 안전), false → product
            is_test = config.get('CODEF_IS_TEST', True)
            if isinstance(is_test, str):
                is_test = is_test.lower() == 'true'
            mode = 'sandbox' if is_test else 'product'

        self.mode = mode

        if mode == 'sandbox':
            # 샌드박스: 내장 클라이언트 (고정 응답)
            self.service_type = ServiceType.SANDBOX
            logger.info("CODEF 초기화: 샌드박스 모드 (고정 테스트 응답)")
        elif mode == 'demo':
            # 데모: 실제 은행 연동 테스트
            self.codef.set_demo_client_info(
                config.get('CODEF_DEMO_CLIENT_ID', ''),
                config.get('CODEF_DEMO_CLIENT_SECRET', ''),
            )
            self.service_type = ServiceType.DEMO
            logger.info("CODEF 초기화: 데모 모드 (실제 은행 연동)")
        else:
            # 운영
            self.codef.set_client_info(
                config.get('CODEF_CLIENT_ID', ''),
                config.get('CODEF_CLIENT_SECRET', ''),
            )
            self.service_type = ServiceType.PRODUCT
            logger.info("CODEF 초기화: 운영 모드")

    # ── 응답 파싱 ──

    def _parse_response(self, response_str):
        """CODEF 응답 JSON 파싱. 성공 시 data 반환, 실패 시 예외."""
        resp = json.loads(response_str) if isinstance(response_str, str) else response_str
        result = resp.get('result', {})
        code = result.get('code', '')
        if code not in ('CF-00000', 'CF-00001'):
            # CF-00001 = 데이터 없음 (정상이지만 결과 없음)
            msg = result.get('message', '알 수 없는 오류')
            logger.error(f"CODEF 오류: [{code}] {msg}")
            raise CodefError(code, msg)
        return resp.get('data', {})

    # ══════════════════════════════════════════
    # 계정 연결 (ConnectedID)
    # ══════════════════════════════════════════

    def create_connected_id(self, bank_code, login_type, login_id, login_pw,
                            client_type='P', business_type='BK',
                            cert_der_base64='', cert_key_base64=''):
        """은행/카드 계좌 연결 → connectedId 반환.

        Args:
            bank_code: 기관코드 (예: '0004')
            login_type: '0'=인증서, '1'=ID/PW
            login_id: 로그인 ID (인증서 모드에서는 빈 문자열 가능)
            login_pw: 비밀번호 또는 인증서 비밀번호 (평문 → RSA 암호화 후 전송)
            client_type: 'P'=개인, 'B'=기업
            business_type: 'BK'=은행, 'CD'=카드
            cert_der_base64: 인증서 .der 파일 Base64 문자열 (인증서 모드)
            cert_key_base64: 인증서 .key 파일 Base64 문자열 (인증서 모드)

        Returns:
            str: connectedId
        """
        # 비밀번호 RSA 암호화
        encrypted_pw = encrypt_rsa(login_pw, self.codef.public_key)
        account = {
            'countryCode': 'KR',
            'businessType': business_type,
            'clientType': client_type,
            'organization': bank_code,
            'loginType': login_type,
            'password': encrypted_pw,
        }

        if login_type == '0':
            # 공인인증서 로그인
            account['derFile'] = cert_der_base64
            account['keyFile'] = cert_key_base64
        else:
            # ID/PW 로그인
            account['id'] = login_id

        account_list = [account]
        response = self.codef.create_account(
            self.service_type,
            {'accountList': account_list}
        )
        data = self._parse_response(response)
        connected_id = data.get('connectedId', '')
        if connected_id:
            logger.info(f"ConnectedID 생성: {connected_id[:8]}... (기관: {bank_code})")
        return connected_id

    def add_account(self, connected_id, bank_code, login_type, login_id, login_pw,
                    client_type='P', business_type='BK'):
        """기존 connectedId에 계정 추가."""
        encrypted_pw = encrypt_rsa(login_pw, self.codef.public_key)
        account_list = [{
            'countryCode': 'KR',
            'businessType': business_type,
            'clientType': client_type,
            'organization': bank_code,
            'loginType': login_type,
            'id': login_id,
            'password': encrypted_pw,
        }]
        response = self.codef.add_account(
            self.service_type,
            {'connectedId': connected_id, 'accountList': account_list}
        )
        return self._parse_response(response)

    # ══════════════════════════════════════════
    # 은행 API
    # ══════════════════════════════════════════

    def get_account_list(self, connected_id, bank_code, client_type='P'):
        """보유 계좌 목록 조회.

        샌드박스는 카테고리별(resDepositTrust, resLoan 등) 분류로 반환하므로
        이를 평탄화하여 일관된 리스트로 반환한다.
        """
        prefix = 'b' if client_type == 'B' else 'p'
        params = {
            'connectedId': connected_id,
            'organization': bank_code,
        }
        response = self.codef.request_product(
            f'/v1/kr/bank/{prefix}/account/account-list',
            self.service_type,
            params
        )
        data = self._parse_response(response)

        # 응답 형태에 따라 파싱
        if isinstance(data, list):
            return data

        # 카테고리별 구조 (샌드박스/실제 모두 가능)
        # resDepositTrust(예금/신탁), resLoan(대출), resFund(펀드) 등
        accounts = []
        category_keys = [
            'resDepositTrust', 'resLoan', 'resFund',
            'resForeignCurrency', 'resInsurance', 'resList',
        ]
        for key in category_keys:
            items = data.get(key, [])
            if isinstance(items, list):
                for item in items:
                    item['_category'] = key  # 어떤 카테고리인지 태그
                    accounts.append(item)

        # 카테고리 구조가 아닌 경우 (단일 딕트 또는 resList)
        if not accounts and data:
            accounts = [data]

        return accounts

    def get_transactions(self, connected_id, bank_code, account,
                         start_date, end_date, order_by='0', client_type='P'):
        """수시입출 거래내역 조회.

        Args:
            start_date / end_date: YYYYMMDD 형식
            order_by: '0'=최신순, '1'=과거순

        Returns:
            list: [{resAccountTrDate, resAccountTrTime, resAccountIn,
                    resAccountOut, resAccountBalance, resAccountDesc1, ...}]
        """
        prefix = 'b' if client_type == 'B' else 'p'
        params = {
            'connectedId': connected_id,
            'organization': bank_code,
            'account': account,
            'startDate': start_date,
            'endDate': end_date,
            'orderBy': order_by,
            'inquiryType': '1',  # 1=입출금 전체
        }
        response = self.codef.request_product(
            f'/v1/kr/bank/{prefix}/account/transaction-list',
            self.service_type,
            params
        )
        data = self._parse_response(response)
        result = data if isinstance(data, list) else data.get('resList', [])
        logger.info(f"거래내역 조회: {bank_code}/{account} → {len(result)}건")
        return result

    # ══════════════════════════════════════════
    # 카드 API
    # ══════════════════════════════════════════

    def get_card_list(self, connected_id, card_code, client_type='P'):
        """보유 카드 목록 조회."""
        prefix = 'b' if client_type == 'B' else 'p'
        params = {
            'connectedId': connected_id,
            'organization': card_code,
        }
        response = self.codef.request_product(
            f'/v1/kr/card/{prefix}/account/card-list',
            self.service_type,
            params
        )
        data = self._parse_response(response)
        return data if isinstance(data, list) else data.get('resList', [])

    def get_card_transactions(self, connected_id, card_code, card_no,
                              start_date, end_date, client_type='P'):
        """카드 이용내역 조회."""
        prefix = 'b' if client_type == 'B' else 'p'
        params = {
            'connectedId': connected_id,
            'organization': card_code,
            'cardNo': card_no,
            'startDate': start_date,
            'endDate': end_date,
            'orderBy': '0',
            'inquiryType': '1',
        }
        response = self.codef.request_product(
            f'/v1/kr/card/{prefix}/account/approval-list',
            self.service_type,
            params
        )
        data = self._parse_response(response)
        result = data if isinstance(data, list) else data.get('resList', [])
        logger.info(f"카드내역 조회: {card_code}/{card_no} → {len(result)}건")
        return result

    # ── 유틸리티 ──

    @staticmethod
    def get_bank_name(code):
        """기관코드 → 은행명."""
        return BANK_CODES.get(code, CARD_CODES.get(code, f'기타({code})'))

    @staticmethod
    def get_all_bank_codes():
        """은행 기관코드 전체 목록."""
        return BANK_CODES.copy()

    @staticmethod
    def get_all_card_codes():
        """카드사 기관코드 전체 목록."""
        return CARD_CODES.copy()


class CodefError(Exception):
    """CODEF API 오류."""
    def __init__(self, code, message):
        self.code = code
        self.message = message
        super().__init__(f"[{code}] {message}")
