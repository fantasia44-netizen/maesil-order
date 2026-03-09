"""
journal_service.py -- 전표(분개) 엔진.

복식부기 전표 생성·조회·역분개 + event_account_mapping 기반 자동 전표.
"""
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════
# 전표 생성
# ══════════════════════════════════════════

def create_journal(db, journal_date, journal_type, lines, description='',
                   ref_type=None, ref_id=None, created_by='system'):
    """전표 생성 (복식기입 차/대변 합계 검증 포함).

    Args:
        db: SupabaseDB
        journal_date: 전표일 (YYYY-MM-DD)
        journal_type: sales_invoice, purchase_invoice, receipt, payment, payroll, manual 등
        lines: list of dict
            [{'account_code': '108', 'account_name': '매출채권',
              'debit': 10000, 'credit': 0, 'description': '...'}, ...]
        description: 전표 적요
        ref_type: 참조 테이블 (tax_invoice, payment_match, bank_transaction 등)
        ref_id: 참조 ID
        created_by: 생성자

    Returns:
        int: journal_entry_id

    Raises:
        ValueError: 차/대변 합계 불일치
    """
    if not lines:
        raise ValueError("전표 라인이 비어있습니다")

    total_debit = sum(l.get('debit', 0) for l in lines)
    total_credit = sum(l.get('credit', 0) for l in lines)

    if total_debit != total_credit:
        raise ValueError(
            f"차/대변 합계 불일치: 차변 {total_debit:,}원 ≠ 대변 {total_credit:,}원"
        )

    # 전표 헤더 생성
    entry_payload = {
        'journal_date': journal_date,
        'journal_type': journal_type,
        'description': description,
        'total_debit': total_debit,
        'total_credit': total_credit,
        'status': 'posted',
        'ref_type': ref_type,
        'ref_id': ref_id,
        'created_by': created_by,
    }
    entry_id = db.insert_journal_entry(entry_payload)

    if not entry_id:
        raise RuntimeError("전표 헤더 생성 실패")

    # 전표 라인 생성
    for idx, line in enumerate(lines, 1):
        line_payload = {
            'journal_entry_id': entry_id,
            'line_no': idx,
            'account_code': line['account_code'],
            'account_name': line.get('account_name', ''),
            'debit_amount': line.get('debit', 0),
            'credit_amount': line.get('credit', 0),
            'description': line.get('description', ''),
        }
        db.insert_journal_line(line_payload)

    logger.info(
        f"[전표생성] ID={entry_id} | {journal_type} | {journal_date} | "
        f"차변={total_debit:,} 대변={total_credit:,} | {description[:50]}"
    )
    return entry_id


def create_auto_journal(db, event_type, amount, journal_date,
                        ref_type=None, ref_id=None, description='',
                        created_by='system', extra_lines=None):
    """event_account_mapping 기반 자동 전표 생성.

    단순 1:1 매핑은 mapping 테이블에서 차/대변 계정을 조회.
    부가세 분리 등 복잡한 전표는 extra_lines로 추가 라인 전달.

    Args:
        event_type: 이벤트 유형 (sales_invoice, receipt 등)
        amount: 금액
        journal_date: 전표일
        extra_lines: 추가 라인 (부가세 등). 지정 시 mapping 무시하고 직접 사용.

    Returns:
        int: journal_entry_id
    """
    if extra_lines:
        # 명시적 라인이 주어진 경우 (부가세 분리 등)
        desc = description or event_type
        return create_journal(db, journal_date, event_type, extra_lines,
                              description=desc, ref_type=ref_type, ref_id=ref_id,
                              created_by=created_by)

    # mapping 조회
    mapping = db.query_event_account_mapping(event_type)
    if not mapping:
        logger.warning(f"[전표] event_account_mapping 미등록: {event_type}")
        return None

    debit_code = mapping['debit_account']
    credit_code = mapping['credit_account']
    desc = description or mapping.get('description_template', event_type)

    # 계정명 조회
    debit_name = _get_account_name(db, debit_code)
    credit_name = _get_account_name(db, credit_code)

    lines = [
        {'account_code': debit_code, 'account_name': debit_name,
         'debit': amount, 'credit': 0, 'description': desc},
        {'account_code': credit_code, 'account_name': credit_name,
         'debit': 0, 'credit': amount, 'description': desc},
    ]

    return create_journal(db, journal_date, event_type, lines,
                          description=desc, ref_type=ref_type, ref_id=ref_id,
                          created_by=created_by)


# ══════════════════════════════════════════
# 역분개
# ══════════════════════════════════════════

def reverse_journal(db, journal_entry_id, reversed_by='system'):
    """역분개: 원 전표의 차/대변을 반대로 새 전표 생성.

    Returns:
        int: 역분개 전표 ID
    """
    entry = db.query_journal_entry_by_id(journal_entry_id)
    if not entry:
        raise ValueError(f"전표 ID {journal_entry_id}를 찾을 수 없습니다")

    if entry.get('status') == 'reversed':
        raise ValueError("이미 역분개된 전표입니다")

    lines = db.query_journal_lines_by_entry(journal_entry_id)
    if not lines:
        raise ValueError("전표 라인이 없습니다")

    # 차/대변 반전
    reversed_lines = []
    for line in lines:
        reversed_lines.append({
            'account_code': line['account_code'],
            'account_name': line.get('account_name', ''),
            'debit': line.get('credit_amount', 0),
            'credit': line.get('debit_amount', 0),
            'description': f"[역분개] {line.get('description', '')}",
        })

    rev_id = create_journal(
        db,
        journal_date=entry['journal_date'],
        journal_type=entry['journal_type'],
        lines=reversed_lines,
        description=f"[역분개] {entry.get('description', '')}",
        ref_type=entry.get('ref_type'),
        ref_id=entry.get('ref_id'),
        created_by=reversed_by,
    )

    # 원 전표 상태 변경
    db.update_journal_entry(journal_entry_id, {
        'status': 'reversed',
        'reversed_by': rev_id,
    })

    logger.info(f"[역분개] 원전표={journal_entry_id} → 역분개={rev_id}")
    return rev_id


# ══════════════════════════════════════════
# 조회
# ══════════════════════════════════════════

def get_journals(db, date_from=None, date_to=None, journal_type=None, status=None,
                 ref_type=None, ref_id=None):
    """전표 목록 조회."""
    return db.query_journal_entries(
        date_from=date_from, date_to=date_to,
        journal_type=journal_type, status=status,
        ref_type=ref_type, ref_id=ref_id,
    )


def get_journal_detail(db, journal_entry_id):
    """전표 + 라인 상세.

    Returns:
        dict: {entry: {...}, lines: [...]}
    """
    entry = db.query_journal_entry_by_id(journal_entry_id)
    if not entry:
        return None
    lines = db.query_journal_lines_by_entry(journal_entry_id)
    return {'entry': entry, 'lines': lines}


def get_trial_balance(db, date_from=None, date_to=None):
    """시산표: 계정별 차변/대변 합계 + 잔액.

    Returns:
        list: [{account_code, account_name, category, total_debit, total_credit, balance}]
    """
    logger.info(f"[시산표] 조회: {date_from or '전체'} ~ {date_to or '전체'}")
    entries = db.query_journal_entries(date_from=date_from, date_to=date_to, status='posted')
    if not entries:
        logger.info("[시산표] 해당 기간 전표 없음")
        return []

    entry_ids = [e['id'] for e in entries]

    # 전체 라인을 계정별로 집계
    account_totals = {}
    for eid in entry_ids:
        lines = db.query_journal_lines_by_entry(eid)
        for line in lines:
            code = line['account_code']
            if code not in account_totals:
                account_totals[code] = {
                    'account_code': code,
                    'account_name': line.get('account_name', ''),
                    'total_debit': 0,
                    'total_credit': 0,
                }
            account_totals[code]['total_debit'] += line.get('debit_amount', 0)
            account_totals[code]['total_credit'] += line.get('credit_amount', 0)

    # 잔액 계산 + 계정 카테고리
    accounts = {a['code']: a for a in db.query_account_codes()}
    result = []
    for code, totals in sorted(account_totals.items()):
        acc = accounts.get(code, {})
        category = acc.get('category', '')
        debit = totals['total_debit']
        credit = totals['total_credit']

        # 자산/비용 → 차변 잔액, 부채/자본/수익 → 대변 잔액
        if category in ('자산', '비용'):
            balance = debit - credit
        else:
            balance = credit - debit

        result.append({
            'account_code': code,
            'account_name': totals['account_name'] or acc.get('name', ''),
            'category': category,
            'total_debit': debit,
            'total_credit': credit,
            'balance': balance,
        })

    grand_debit = sum(r['total_debit'] for r in result)
    grand_credit = sum(r['total_credit'] for r in result)
    logger.info(f"[시산표] {len(result)}개 계정, 차변합계={grand_debit:,} 대변합계={grand_credit:,} "
                f"{'✓ 일치' if grand_debit == grand_credit else '✗ 불일치!'}")
    return result


# ══════════════════════════════════════════
# 자동 전표 생성 함수 (서비스 훅에서 호출)
# ══════════════════════════════════════════

def create_sales_invoice_journal(db, invoice_id, created_by='system'):
    """매출 세금계산서 발행 → 전표 자동 생성.

    DR 108(매출채권)     공급가액+세액
    CR 401(매출)         공급가액
    CR 204(부가세예수금)  세액 (과세일 때만)
    """
    inv = db.query_tax_invoice_by_id(invoice_id)
    if not inv:
        return None

    supply = inv.get('supply_cost_total', 0)
    tax = inv.get('tax_total', 0)
    total = inv.get('total_amount', 0) or (supply + tax)
    write_date = str(inv.get('write_date', ''))
    buyer = inv.get('buyer_corp_name', '')

    lines = [
        {'account_code': '108', 'account_name': '매출채권',
         'debit': total, 'credit': 0, 'description': f'매출채권 {buyer}'},
        {'account_code': '401', 'account_name': '매출',
         'debit': 0, 'credit': supply, 'description': f'매출 {buyer}'},
    ]

    if tax > 0:
        lines.append({
            'account_code': '204', 'account_name': '부가세예수금',
            'debit': 0, 'credit': tax, 'description': f'부가세 {buyer}',
        })

    return create_journal(db, write_date, 'sales_invoice', lines,
                          description=f'매출 세금계산서 {buyer} {total:,}원',
                          ref_type='tax_invoice', ref_id=invoice_id,
                          created_by=created_by)


def create_purchase_invoice_journal(db, invoice_id, created_by='system'):
    """매입 세금계산서 등록 → 전표 자동 생성.

    DR 501(매입)         공급가액
    DR 205(부가세대급금)  세액 (과세일 때만)
    CR 201(매입채무)     공급가액+세액
    """
    inv = db.query_tax_invoice_by_id(invoice_id)
    if not inv:
        return None

    supply = inv.get('supply_cost_total', 0)
    tax = inv.get('tax_total', 0)
    total = inv.get('total_amount', 0) or (supply + tax)
    write_date = str(inv.get('write_date', ''))
    supplier = inv.get('supplier_corp_name', '')

    lines = [
        {'account_code': '501', 'account_name': '매입',
         'debit': supply, 'credit': 0, 'description': f'매입 {supplier}'},
    ]

    if tax > 0:
        lines.append({
            'account_code': '205', 'account_name': '부가세대급금',
            'debit': tax, 'credit': 0, 'description': f'부가세 {supplier}',
        })

    lines.append({
        'account_code': '201', 'account_name': '매입채무',
        'debit': 0, 'credit': total, 'description': f'매입채무 {supplier}',
    })

    return create_journal(db, write_date, 'purchase_invoice', lines,
                          description=f'매입 세금계산서 {supplier} {total:,}원',
                          ref_type='tax_invoice', ref_id=invoice_id,
                          created_by=created_by)


def create_receipt_journal(db, match_id, created_by='system'):
    """매출입금 매칭 확정 → 전표 자동 생성.

    DR 102(보통예금)  입금액
    CR 108(매출채권)  입금액
    """
    match = db.query_payment_match_by_id(match_id)
    if not match:
        return None

    amount = match.get('matched_amount', 0)
    partner = match.get('partner_name', '')
    # 매칭일 기준 전표
    tx = db.query_bank_transaction_by_id(match.get('bank_transaction_id'))
    journal_date = str(tx.get('transaction_date', '')) if tx else str(match.get('matched_at', ''))[:10]

    return create_auto_journal(db, 'receipt', amount, journal_date,
                               ref_type='payment_match', ref_id=match_id,
                               description=f'매출대금 입금 {partner} {amount:,}원',
                               created_by=created_by)


def create_payment_journal(db, match_id, created_by='system'):
    """매입지급 매칭 확정 → 전표 자동 생성.

    DR 201(매입채무)  지급액
    CR 102(보통예금)  지급액
    """
    match = db.query_payment_match_by_id(match_id)
    if not match:
        return None

    amount = match.get('matched_amount', 0)
    partner = match.get('partner_name', '')
    tx = db.query_bank_transaction_by_id(match.get('bank_transaction_id'))
    journal_date = str(tx.get('transaction_date', '')) if tx else str(match.get('matched_at', ''))[:10]

    return create_auto_journal(db, 'payment', amount, journal_date,
                               ref_type='payment_match', ref_id=match_id,
                               description=f'매입대금 지급 {partner} {amount:,}원',
                               created_by=created_by)


# ══════════════════════════════════════════
# 내부 헬퍼
# ══════════════════════════════════════════

def _get_account_name(db, code):
    """계정코드 → 계정명."""
    accounts = db.query_account_codes()
    for a in accounts:
        if a.get('code') == code:
            return a.get('name', '')
    return ''
