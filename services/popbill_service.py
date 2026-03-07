"""
popbill_service.py -- Popbill 세금계산서 API 래퍼.
TaxinvoiceService 인스턴스를 앱 레벨 싱글톤으로 유지 (app.popbill).
"""
import logging

logger = logging.getLogger(__name__)

# Popbill SDK 임포트 (설치 안 되어 있으면 더미 모드)
try:
    from popbill import TaxinvoiceService, Taxinvoice, TaxinvoiceDetail, PopbillException
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
        state_list = state_list or ['3**', '4**']
        tax_type_list = tax_type_list or ['T', 'N', 'Z']

        result = self.svc.search(
            self.corp_num,
            MgtKeyType=direction,
            DType='W',  # 작성일 기준
            SDate=start_date,
            EDate=end_date,
            State=state_list,
            Type=['N', 'M'],
            TaxType=tax_type_list,
            Page=page,
            PerPage=per_page,
            Order='D',
        )
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

    def get_popbill_url(self, to_go='TBOX'):
        """팝빌 웹 URL (TBOX=문서함, PBOX=포인트충전)."""
        self._ensure_ready()
        return self.svc.getPopbillURL(self.corp_num, to_go)


class PopbillNotReadyError(Exception):
    """Popbill SDK 미초기화 오류."""
    pass
