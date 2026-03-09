"""
popbill_service.py -- Popbill 세금계산서 API 래퍼.
TaxinvoiceService 인스턴스를 앱 레벨 싱글톤으로 유지 (app.popbill).
"""
import logging

logger = logging.getLogger(__name__)

# Popbill SDK 임포트 (설치 안 되어 있으면 더미 모드)
try:
    from popbill import (
        TaxinvoiceService, Taxinvoice, TaxinvoiceDetail,
        PopbillException, JoinForm,
    )
    POPBILL_AVAILABLE = True
except ImportError:
    POPBILL_AVAILABLE = False
    logger.warning("popbill SDK 미설치. 세금계산서 기능 비활성화.")


class PopbillService:
    """Popbill 세금계산서 API 래퍼 (싱글톤)."""

    def __init__(self, config):
        self.corp_num = config.get('POPBILL_CORP_NUM', '')
        self.is_test = config.get('POPBILL_IS_TEST', True)
        self.svc = None

        if not POPBILL_AVAILABLE:
            logger.warning("Popbill SDK 미설치 — 세금계산서 기능 비활성화")
            return

        link_id = config.get('POPBILL_LINK_ID', 'TESTER')
        secret_key = config.get('POPBILL_SECRET_KEY', '')

        if not secret_key:
            logger.warning("Popbill SecretKey 미설정 — 키 발급 후 .env에 입력하세요")
            return

        self.svc = TaxinvoiceService(link_id, secret_key)
        self.svc.IsTest = self.is_test
        self.svc.IPRestrictOnOff = config.get('POPBILL_IP_RESTRICT', False)
        self.svc.UseStaticIP = False
        self.svc.UseLocalTimeYN = True

        logger.info(f"Popbill 초기화 완료 ({'테스트' if self.is_test else '운영'})")

    @property
    def is_ready(self):
        """Popbill 서비스가 사용 가능한지 확인."""
        return self.svc is not None

    def _ensure_ready(self):
        if not self.is_ready:
            raise PopbillNotReadyError("Popbill SDK가 초기화되지 않았습니다. SecretKey를 확인하세요.")

    # ══════════════════════════════════════════
    # 회원 관리 (연동회원 가입/확인)
    # ══════════════════════════════════════════

    def check_is_member(self, corp_num=None):
        """사업자번호의 팝빌 연동회원 가입 여부 확인.

        Returns:
            dict: {is_member: bool, code: int, message: str}
        """
        self._ensure_ready()
        target = corp_num or self.corp_num
        try:
            result = self.svc.checkIsMember(target)
            code = getattr(result, 'code', 0)
            msg = getattr(result, 'message', '')
            is_member = (code == 1)
            logger.info(f"팝빌 회원 확인: {target} → {'회원' if is_member else '비회원'} [{code}] {msg}")
            return {'is_member': is_member, 'code': code, 'message': msg}
        except PopbillException as e:
            logger.error(f"팝빌 회원 확인 오류: [{e.code}] {e.message}")
            return {'is_member': False, 'code': e.code, 'message': e.message}

    def join_member(self, corp_num, corp_name, ceo_name, addr='',
                    biz_type='', biz_class='', contact_name='',
                    contact_tel='', contact_email='', user_id=None, user_pw=None):
        """팝빌 연동회원 가입 (API).

        우리 앱에서 사용자가 팝빌 회원이 아닐 때 자동으로 가입시킴.

        Args:
            corp_num: 사업자번호 (10자리, 하이픈 없이)
            corp_name: 상호
            ceo_name: 대표자명
            user_id: 팝빌 아이디 (미지정 시 사업자번호 사용)
            user_pw: 팝빌 비밀번호 (미지정 시 기본값)

        Returns:
            dict: {success: bool, code: int, message: str}
        """
        self._ensure_ready()

        join_info = JoinForm(
            CorpNum=corp_num.replace('-', ''),
            CorpName=corp_name,
            CEOName=ceo_name,
            Addr=addr,
            BizType=biz_type,
            BizClass=biz_class,
            ContactName=contact_name or ceo_name,
            ContactTEL=contact_tel,
            ContactEmail=contact_email,
            ID=user_id or corp_num.replace('-', ''),
            PWD=user_pw or 'popbill1234!',
        )

        try:
            result = self.svc.joinMember(join_info)
            code = getattr(result, 'code', 0)
            msg = getattr(result, 'message', '')
            logger.info(f"팝빌 회원가입 결과: {corp_num} [{code}] {msg}")
            return {'success': code == 1, 'code': code, 'message': msg}
        except PopbillException as e:
            logger.error(f"팝빌 회원가입 오류: [{e.code}] {e.message}")
            return {'success': False, 'code': e.code, 'message': e.message}

    # ══════════════════════════════════════════
    # 인증서 관리
    # ══════════════════════════════════════════

    def get_tax_cert_url(self, user_id=''):
        """세금계산서용 인증서 등록 팝업 URL (getTaxCertURL).

        실사용자가 브라우저에서 이 URL을 열면 인증서 등록 팝업이 뜸.
        팝빌 웹사이트에 별도 로그인 필요 없이 바로 인증서 등록 가능.

        Args:
            user_id: 팝빌 서브계정 아이디 (빈 문자열=마스터 계정)

        Returns:
            str: URL or None
        """
        self._ensure_ready()
        try:
            logger.info(f"팝빌 getTaxCertURL 호출: corp={self.corp_num}, uid={repr(user_id)}")
            url = self.svc.getTaxCertURL(self.corp_num, user_id)
            logger.info(f"팝빌 인증서 등록 URL 발급 성공: {url[:80]}...")
            return url
        except PopbillException as e:
            logger.error(f"팝빌 getTaxCertURL 실패: [{e.code}] {e.message}")
            return None
        except Exception as e:
            logger.error(f"팝빌 getTaxCertURL 예외: {type(e).__name__}: {e}")
            return None

    def check_cert_validation(self, user_id=''):
        """인증서 유효성 검증 (checkCertValidation).

        Returns:
            dict: {valid: bool, code: int, message: str}
        """
        self._ensure_ready()
        try:
            result = self.svc.checkCertValidation(self.corp_num, user_id)
            code = getattr(result, 'code', 0)
            msg = getattr(result, 'message', '')
            logger.info(f"팝빌 인증서 검증: [{code}] {msg}")
            return {'valid': code == 1, 'code': code, 'message': msg}
        except PopbillException as e:
            logger.error(f"팝빌 인증서 검증 오류: [{e.code}] {e.message}")
            return {'valid': False, 'code': e.code, 'message': e.message}

    def get_tax_cert_info(self, user_id=''):
        """등록된 인증서 정보 조회 (getTaxCertInfo).

        Returns:
            dict: {subject, issuer, expire_date, ...} or None
        """
        self._ensure_ready()
        try:
            result = self.svc.getTaxCertInfo(self.corp_num, user_id)
            info = {
                'subject': getattr(result, 'regDT', ''),
                'issuer': getattr(result, 'issuerDN', ''),
                'serial': getattr(result, 'serialNum', ''),
                'expire_date': getattr(result, 'expireDate', ''),
                'reg_date': getattr(result, 'regDT', ''),
            }
            logger.info(f"팝빌 인증서 정보: 만료일={info['expire_date']}")
            return info
        except PopbillException as e:
            logger.error(f"팝빌 인증서 정보 조회 오류: [{e.code}] {e.message}")
            return None

    # ══════════════════════════════════════════
    # 세금계산서 발행
    # ══════════════════════════════════════════

    def issue_sales_invoice(self, invoice_data, user_id=None):
        """매출 세금계산서 즉시발행 (registIssue).

        Args:
            invoice_data: dict
                - write_date (YYYYMMDD)
                - mgt_key (관리번호, 최대 24자)
                - buyer_corp_num, buyer_corp_name, buyer_ceo_name, ...
                - supplier_corp_name, supplier_ceo_name, ...
                - items: [{name, qty, unit_cost, supply_cost, tax}]
                - supply_cost_total, tax_total, total_amount
            user_id: 팝빌 서브계정 (Optional)

        Returns:
            dict: {nts_confirm_num, code, message}
        """
        self._ensure_ready()

        ti = Taxinvoice()

        # 기본 정보
        ti.writeDate = invoice_data['write_date']
        ti.chargeDirection = '정과금'
        ti.issueType = '정발행'
        ti.purposeType = invoice_data.get('purpose_type', '영수')
        ti.taxType = invoice_data.get('tax_type', '과세')

        # 공급자 (우리)
        ti.invoicerCorpNum = self.corp_num
        ti.invoicerCorpName = invoice_data.get('supplier_corp_name', '')
        ti.invoicerCEOName = invoice_data.get('supplier_ceo_name', '')
        ti.invoicerAddr = invoice_data.get('supplier_addr', '')
        ti.invoicerBizType = invoice_data.get('supplier_biz_type', '')
        ti.invoicerBizClass = invoice_data.get('supplier_biz_class', '')
        ti.invoicerEmail = invoice_data.get('supplier_email', '')
        ti.invoicerMgtKey = invoice_data.get('mgt_key', '')

        # 공급받는자 (거래처)
        ti.invoiceeType = '사업자'
        ti.invoiceeCorpNum = invoice_data['buyer_corp_num']
        ti.invoiceeCorpName = invoice_data.get('buyer_corp_name', '')
        ti.invoiceeCEOName = invoice_data.get('buyer_ceo_name', '')
        ti.invoiceeAddr = invoice_data.get('buyer_addr', '')
        ti.invoiceeBizType = invoice_data.get('buyer_biz_type', '')
        ti.invoiceeBizClass = invoice_data.get('buyer_biz_class', '')
        ti.invoiceeEmail = invoice_data.get('buyer_email', '')

        # 금액
        ti.supplyCostTotal = str(invoice_data.get('supply_cost_total', 0))
        ti.taxTotal = str(invoice_data.get('tax_total', 0))
        ti.totalAmount = str(invoice_data.get('total_amount', 0))

        # 품목 상세
        ti.detailList = []
        for idx, item in enumerate(invoice_data.get('items', []), 1):
            detail = TaxinvoiceDetail()
            detail.serialNum = idx
            detail.itemName = item.get('name', '')
            detail.qty = str(item.get('qty', 0))
            detail.unitCost = str(item.get('unit_cost', 0))
            detail.supplyCost = str(item.get('supply_cost', 0))
            detail.tax = str(item.get('tax', 0))
            ti.detailList.append(detail)

        try:
            result = self.svc.registIssue(
                self.corp_num, ti,
                writeSpecification=False,
                forceIssue=False,
                memo='AutoTool 자동발행',
                UserID=user_id,
            )
            logger.info(f"세금계산서 발행 성공: {invoice_data.get('mgt_key')}")
            return {
                'nts_confirm_num': getattr(result, 'ntsConfirmNum', ''),
                'code': getattr(result, 'code', 1),
                'message': getattr(result, 'message', '성공'),
            }
        except PopbillException as e:
            logger.error(f"세금계산서 발행 실패: [{e.code}] {e.message}")
            raise

    # ══════════════════════════════════════════
    # 조회 / 검색
    # ══════════════════════════════════════════

    def search_invoices(self, direction='SELL', start_date='', end_date='',
                        state_list=None, tax_type_list=None,
                        page=1, per_page=50):
        """세금계산서 목록 검색.

        Args:
            direction: 'SELL'(매출) / 'BUY'(매입)
        """
        self._ensure_ready()
        # 전체 상태 조회 (2**=임시, 3**=발행, 4**=국세청 전송)
        state_list = state_list or ['2**', '3**', '4**']
        tax_type_list = tax_type_list or ['T', 'N', 'Z']

        logger.info(f"팝빌 세금계산서 검색: {direction}, {start_date}~{end_date}, "
                    f"상태={state_list}, page={page}")

        result = self.svc.search(
            self.corp_num,
            MgtKeyType=direction,
            DType='W',  # 작성일 기준
            SDate=start_date,
            EDate=end_date,
            State=state_list,
            Type=['N', 'M'],
            TaxType=tax_type_list,
            LateOnly='',         # 지연발행 여부 (빈값=전체)
            TaxRegIDYN='',       # 종사업장번호 유무 (빈값=전체)
            TaxRegIDType='',     # 종사업장번호 유형 (빈값=전체)
            TaxRegID='',         # 종사업장번호 (빈값=전체)
            Page=page,
            PerPage=per_page,
            Order='D',
        )

        total = getattr(result, 'total', 0) or 0
        items = getattr(result, 'list', None) or []
        logger.info(f"팝빌 검색 결과: {direction} total={total}, items={len(items)}")

        return result

    def get_detail(self, mgt_key, direction='SELL'):
        """세금계산서 상세 조회."""
        self._ensure_ready()
        return self.svc.getDetailInfo(self.corp_num, direction, mgt_key)

    def get_info(self, mgt_key, direction='SELL'):
        """세금계산서 상태 조회."""
        self._ensure_ready()
        return self.svc.getInfo(self.corp_num, direction, mgt_key)

    def cancel_issue(self, mgt_key, memo=''):
        """발행 취소."""
        self._ensure_ready()
        return self.svc.cancelIssue(self.corp_num, 'SELL', mgt_key, memo)

    # ══════════════════════════════════════════
    # 유틸리티
    # ══════════════════════════════════════════

    def get_balance(self):
        """팝빌 포인트 잔액 조회."""
        self._ensure_ready()
        return self.svc.getBalance(self.corp_num)

    def get_popbill_url(self, to_go='TBOX', user_id=''):
        """팝빌 웹 URL (TBOX=문서함, PBOX=포인트충전, CERT=인증서 등록)."""
        self._ensure_ready()
        return self.svc.getPopbillURL(self.corp_num, user_id, to_go)

    def get_cert_url(self):
        """인증서 등록 페이지 URL 반환 (getPopbillURL CERT)."""
        self._ensure_ready()
        try:
            return self.svc.getPopbillURL(self.corp_num, '', 'CERT')
        except Exception as e:
            logger.error(f"인증서 등록 URL 조회 실패: {e}")
            return None

    def check_cert_valid(self):
        """공동인증서 등록 상태 확인.

        Returns:
            dict: {registered: bool, message: str, cert_url: str|None}
        """
        if not self.is_ready:
            return {
                'registered': False,
                'message': 'Popbill SDK 미초기화 (SecretKey 확인 필요)',
                'cert_url': None,
            }

        try:
            # getCertificateExpireDate: 인증서 만료일 조회
            # 인증서 미등록이면 PopbillException 발생
            expire = self.svc.getCertificateExpireDate(self.corp_num)
            logger.info(f"팝빌 인증서 만료일: {expire}")
            return {
                'registered': True,
                'message': f'인증서 등록 완료 (만료일: {expire})',
                'expire_date': str(expire),
                'cert_url': None,  # 등록 완료면 URL 불필요
            }
        except PopbillException as e:
            # 인증서 미등록 또는 오류 → cert_url 발급 시도
            cert_url = None
            try:
                cert_url = self.get_tax_cert_url() or self.get_cert_url()
            except Exception:
                pass  # URL 발급 실패해도 계속 진행

            if e.code == -10004000:
                logger.warning("팝빌 인증서 미등록")
                return {
                    'registered': False,
                    'message': '공동인증서가 등록되지 않았습니다.',
                    'cert_url': cert_url,
                    'error_code': e.code,
                }
            else:
                logger.error(f"팝빌 인증서 확인 오류: [{e.code}] {e.message}")
                return {
                    'registered': False,
                    'message': f'인증서 확인 오류: {e.message}',
                    'cert_url': cert_url,
                    'error_code': e.code,
                }
        except Exception as e:
            logger.error(f"팝빌 인증서 확인 예외: {e}")
            return {
                'registered': False,
                'message': f'인증서 확인 실패: {e}',
                'cert_url': None,
            }

    def get_status_summary(self):
        """팝빌 연동 전체 상태 요약 (타임아웃 보호 포함).

        Returns:
            dict: {sdk_installed, initialized, is_test, corp_num,
                   is_member, cert_status, balance}
        """
        status = {
            'sdk_installed': POPBILL_AVAILABLE,
            'initialized': self.is_ready,
            'is_test': self.is_test,
            'corp_num': self.corp_num,
            'is_member': False,
            'cert_status': None,
            'balance': None,
        }

        if not self.is_ready:
            return status

        # 각 API 호출에 개별 try/except — 하나 실패해도 나머지 계속 진행

        # 1) 회원 상태
        try:
            member_result = self.check_is_member()
            status['is_member'] = member_result.get('is_member', False)
        except Exception as e:
            logger.error(f"팝빌 회원확인 실패 (타임아웃?): {e}")
            status['is_member'] = False
            status['cert_status'] = {
                'registered': False,
                'message': f'팝빌 서버 응답 없음: {e}',
                'cert_url': None,
            }
            return status

        if not status['is_member']:
            status['cert_status'] = {
                'registered': False,
                'message': '팝빌 연동회원 가입이 필요합니다.',
                'cert_url': None,
            }
            return status

        # 2) 인증서 상태
        try:
            status['cert_status'] = self.check_cert_valid()
        except Exception as e:
            logger.error(f"팝빌 인증서 확인 실패: {e}")
            status['cert_status'] = {
                'registered': False,
                'message': f'인증서 확인 실패: {e}',
                'cert_url': None,
            }

        # 3) 포인트 잔액
        try:
            status['balance'] = self.svc.getBalance(self.corp_num)
        except Exception as e:
            logger.error(f"팝빌 잔액 조회 실패: {e}")
            status['balance'] = None

        return status

    @staticmethod
    def is_cert_error(exception):
        """인증서 미등록 오류인지 판별."""
        if hasattr(exception, 'code'):
            return exception.code == -10004000
        return '-10004000' in str(exception)


class PopbillNotReadyError(Exception):
    """Popbill SDK 미초기화 오류."""
    pass


class PopbillCertError(Exception):
    """Popbill 인증서 미등록 오류."""
    def __init__(self, message='', cert_url=None):
        self.cert_url = cert_url
        super().__init__(message)
