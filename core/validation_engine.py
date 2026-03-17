"""
core/validation_engine.py — 1차 실시간 검증 엔진.

모든 트랜잭션(생산/출고/이동/조정/입고) 실행 전 강제 검증.
검증 실패 시 DB 저장 중단 + 명확한 오류 메시지 + 로그 기록.

사용법:
    from core.validation_engine import validate, ValidationError

    # 서비스 함수 내부에서
    validate.production(db, date_str, location, items)  # 실패 시 ValidationError raise
    validate.outbound(db, date_str, location, items)
    validate.transfer(db, date_str, product, qty, src, dst)
    validate.adjustment(db, date_str, items)
    validate.inbound(db, date_str, payload)
    validate.file_upload(db, file_bytes, channel, uploader)
"""
import hashlib
import re
import uuid
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from db_utils import get_db


# ═══════════════════════════════════════════════════════════════
# 예외 클래스
# ═══════════════════════════════════════════════════════════════

class ValidationError(ValueError):
    """검증 실패 예외. code로 에러 유형 구분."""
    def __init__(self, message, code='VALIDATION_ERROR', field=None, details=None):
        super().__init__(message)
        self.code = code
        self.field = field
        self.details = details or []


class DuplicateUploadError(ValidationError):
    """중복 파일 업로드 감지."""
    def __init__(self, message, file_hash=None, original_run_id=None):
        super().__init__(message, code='DUPLICATE_UPLOAD')
        self.file_hash = file_hash
        self.original_run_id = original_run_id


class StockShortageError(ValidationError):
    """재고 부족 (사전 시뮬레이션 결과)."""
    def __init__(self, message, shortages=None):
        super().__init__(message, code='STOCK_SHORTAGE')
        self.shortages = shortages or []


class SimilarNameWarning:
    """유사 품목명 경고 (에러 아님, 경고)."""
    def __init__(self, input_name, similar_names, scores):
        self.input_name = input_name
        self.similar_names = similar_names
        self.scores = scores

    def __str__(self):
        pairs = [f"'{n}'({s:.0%})" for n, s in zip(self.similar_names, self.scores)]
        return f"'{self.input_name}' → 유사 품목: {', '.join(pairs)}"


# ═══════════════════════════════════════════════════════════════
# 로깅 헬퍼
# ═══════════════════════════════════════════════════════════════

def _log_validation_failure(action, message, details=None):
    """검증 실패를 audit_logs에 기록. Flask 컨텍스트 없으면 스킵."""
    try:
        from flask import current_app, has_app_context
        if not has_app_context():
            return
        db = get_db()
        db.insert_audit_log({
            'action': f'validation_fail:{action}',
            'detail': message[:500],
            'target': str(details)[:200] if details else None,
        })
    except Exception:
        pass  # 로그 실패가 검증을 막으면 안 됨


# ═══════════════════════════════════════════════════════════════
# 공통 검증 함수
# ═══════════════════════════════════════════════════════════════

def _validate_date(date_str, field_name='날짜'):
    """날짜 형식 및 범위 검증."""
    if not date_str:
        raise ValidationError(f'{field_name}을(를) 입력하세요.', code='EMPTY_DATE', field=field_name)
    if not isinstance(date_str, str):
        date_str = str(date_str)
    # YYYY-MM-DD 형식
    if not re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
        raise ValidationError(
            f'{field_name} 형식이 올바르지 않습니다: {date_str}. YYYY-MM-DD 형식으로 입력하세요.',
            code='INVALID_DATE_FORMAT', field=field_name)
    try:
        dt = datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        raise ValidationError(
            f'{field_name}이(가) 유효하지 않습니다: {date_str}',
            code='INVALID_DATE', field=field_name)
    # 미래 30일 이상, 과거 2년 이상 → 오입력 가능성
    today = datetime.now()
    if dt > today + timedelta(days=30):
        raise ValidationError(
            f'{field_name}이(가) 30일 이상 미래입니다: {date_str}. 확인 후 다시 입력하세요.',
            code='FUTURE_DATE', field=field_name)
    if dt < today - timedelta(days=730):
        raise ValidationError(
            f'{field_name}이(가) 2년 이상 과거입니다: {date_str}. 확인 후 다시 입력하세요.',
            code='TOO_OLD_DATE', field=field_name)
    return date_str


def _validate_qty(qty, field_name='수량', allow_negative=False):
    """수량 검증. 기본적으로 양수만 허용."""
    if qty is None:
        raise ValidationError(f'{field_name}을(를) 입력하세요.', code='EMPTY_QTY', field=field_name)
    try:
        qty_val = float(qty)
    except (ValueError, TypeError):
        raise ValidationError(
            f'{field_name}이(가) 숫자가 아닙니다: {qty}',
            code='INVALID_QTY', field=field_name)
    if not allow_negative and qty_val <= 0:
        raise ValidationError(
            f'{field_name}은(는) 0보다 커야 합니다: {qty_val}',
            code='NON_POSITIVE_QTY', field=field_name)
    if allow_negative and qty_val == 0:
        raise ValidationError(
            f'{field_name}이(가) 0입니다.',
            code='ZERO_QTY', field=field_name)
    return qty_val


def _validate_location(location, field_name='위치'):
    """창고 위치 검증."""
    if not location or not str(location).strip():
        raise ValidationError(
            f'{field_name}을(를) 입력하세요.',
            code='EMPTY_LOCATION', field=field_name)
    return str(location).strip()


def _validate_product_name(name, field_name='품목명'):
    """품목명 검증."""
    if not name or not str(name).strip():
        raise ValidationError(
            f'{field_name}을(를) 입력하세요.',
            code='EMPTY_PRODUCT', field=field_name)
    cleaned = str(name).strip()
    if len(cleaned) > 200:
        raise ValidationError(
            f'{field_name}이(가) 너무 깁니다 (최대 200자).',
            code='PRODUCT_TOO_LONG', field=field_name)
    return cleaned


# ═══════════════════════════════════════════════════════════════
# 유사 품목명 검사
# ═══════════════════════════════════════════════════════════════

def check_similar_names(input_name, known_names, threshold=0.75, max_results=3):
    """품목명 유사도 검사. 정확 매칭 실패 시 유사 품목 제안.

    Args:
        input_name: 입력된 품목명
        known_names: 기존 품목명 리스트
        threshold: 유사도 임계값 (0~1)
        max_results: 최대 제안 수

    Returns:
        SimilarNameWarning or None
    """
    if not input_name or not known_names:
        return None

    input_clean = str(input_name).strip().replace(' ', '')

    # 정확 매칭 (공백 무시)
    for name in known_names:
        if str(name).strip().replace(' ', '') == input_clean:
            return None  # 정확 매칭 → 문제 없음

    # 유사도 계산
    scores = []
    for name in known_names:
        name_clean = str(name).strip().replace(' ', '')
        ratio = SequenceMatcher(None, input_clean, name_clean).ratio()
        if ratio >= threshold:
            scores.append((name, ratio))

    if not scores:
        return None

    scores.sort(key=lambda x: x[1], reverse=True)
    top = scores[:max_results]

    return SimilarNameWarning(
        input_name=input_name,
        similar_names=[s[0] for s in top],
        scores=[s[1] for s in top],
    )


# ═══════════════════════════════════════════════════════════════
# 재고 시뮬레이션 (음수 방지)
# ═══════════════════════════════════════════════════════════════

def simulate_stock_change(db, location, changes, date_str=None):
    """재고 변동 사전 시뮬레이션. 음수 발생 여부 확인.

    Args:
        db: SupabaseDB instance
        location: 창고명
        changes: list of (product_name, qty_change)
            qty_change < 0 이면 차감
        date_str: 기준일 (없으면 현재 재고 기준)

    Returns:
        list of dict: 부족 항목 [{product_name, current, required, shortage}]
    """
    from services.excel_io import build_stock_snapshot, snapshot_lookup

    try:
        raw = db.query_stock_by_location(location)
        snapshot = build_stock_snapshot(raw)
    except Exception:
        snapshot = {}

    shortages = []
    for name, qty_change in changes:
        if qty_change >= 0:
            continue  # 입고/생산은 부족 없음
        snap = snapshot_lookup(snapshot, name)
        current = snap.get('total', 0)
        required = abs(qty_change)
        if current < required:
            shortages.append({
                'product_name': name,
                'current': current,
                'required': required,
                'shortage': required - current,
                'unit': snap.get('unit', '개'),
            })
    return shortages


# ═══════════════════════════════════════════════════════════════
# 중복 파일 업로드 감지
# ═══════════════════════════════════════════════════════════════

def compute_file_hash(file_bytes):
    """파일 SHA256 해시 계산."""
    if isinstance(file_bytes, str):
        file_bytes = file_bytes.encode('utf-8')
    return hashlib.sha256(file_bytes).hexdigest()


def check_duplicate_upload(db, file_hash, channel=None):
    """이미 처리된 파일인지 확인.

    Returns:
        dict or None: 기존 업로드 정보 (있으면 중복)
    """
    try:
        q = db.client.table('import_runs').select('id, channel, filename, uploaded_by, created_at') \
            .eq('file_hash', file_hash)
        if channel:
            q = q.eq('channel', channel)
        q = q.order('created_at', desc=True).limit(1)
        res = q.execute()
        if res.data:
            return res.data[0]
    except Exception:
        pass
    return None


# ═══════════════════════════════════════════════════════════════
# 트랜잭션별 검증 함수
# ═══════════════════════════════════════════════════════════════

class _Validator:
    """트랜잭션별 검증 메서드를 모아놓은 네임스페이스."""

    @staticmethod
    def production(db, date_str, location, items):
        """생산 등록 전 검증.

        Args:
            db: SupabaseDB
            date_str: 생산일자
            location: 생산 위치
            items: list of dict with product_name, qty, materials: [...]

        Raises:
            ValidationError: 검증 실패
        """
        _validate_date(date_str, '생산일자')
        _validate_location(location, '생산 위치')

        if not items:
            raise ValidationError('생산 항목이 없습니다.', code='EMPTY_ITEMS')

        material_changes = []  # (name, -qty) for stock simulation

        for i, item in enumerate(items):
            prefix = f'{i+1}번째 항목'
            name = _validate_product_name(item.get('product_name', ''), f'{prefix} 품목명')
            qty = _validate_qty(item.get('qty', 0), f'{prefix}({name}) 생산수량')

            # 재료(materials) 검증
            for j, mat in enumerate(item.get('materials', [])):
                mat_prefix = f'{prefix} 재료{j+1}'
                mat_name = _validate_product_name(
                    mat.get('product_name', ''), f'{mat_prefix} 재료명')
                mat_qty = _validate_qty(mat.get('qty', 0), f'{mat_prefix}({mat_name}) 수량')
                material_changes.append((mat_name, -mat_qty))

        # 재료 차감 시뮬레이션 (음수 재고 체크)
        if material_changes:
            shortages = simulate_stock_change(db, location, material_changes)
            if shortages:
                msgs = []
                for s in shortages:
                    msgs.append(
                        f"  [{location}] {s['product_name']}: "
                        f"필요 {s['required']}{s['unit']} / "
                        f"재고 {s['current']}{s['unit']} "
                        f"(부족 {s['shortage']}{s['unit']})")
                _log_validation_failure('production', f"재고 부족 {len(shortages)}건", shortages)
                # 경고로 처리 (생산은 부족 시에도 경고 후 진행 가능)
                return shortages
        return []

    @staticmethod
    def outbound(db, date_str, location, items, force_shortage=False):
        """출고 처리 전 검증.

        Args:
            items: list of (product_name, qty) or list of dict
            force_shortage: True이면 재고 부족 시에도 경고만

        Raises:
            ValidationError, StockShortageError
        """
        _validate_date(date_str, '출고일자')
        _validate_location(location, '출고 위치')

        if not items:
            raise ValidationError('출고 항목이 없습니다.', code='EMPTY_ITEMS')

        changes = []
        for i, item in enumerate(items):
            if isinstance(item, dict):
                name = item.get('product_name', '')
                qty = item.get('qty', 0)
            else:
                name, qty = item[0], item[1]
            prefix = f'{i+1}번째'
            _validate_product_name(name, f'{prefix} 품목명')
            qty_val = _validate_qty(qty, f'{prefix}({name}) 수량')
            changes.append((name, -qty_val))

        shortages = simulate_stock_change(db, location, changes)
        if shortages and not force_shortage:
            msgs = [f"  {s['product_name']}: 재고 {s['current']} / 출고 {s['required']}"
                    for s in shortages]
            msg = f"재고 부족 {len(shortages)}건:\n" + "\n".join(msgs)
            _log_validation_failure('outbound', msg)
            raise StockShortageError(msg, shortages=shortages)

        return shortages

    @staticmethod
    def transfer(db, date_str, product_name, qty, from_location, to_location):
        """창고 이동 전 검증.

        Raises:
            ValidationError, StockShortageError
        """
        _validate_date(date_str, '이동일자')
        name = _validate_product_name(product_name, '이동 품목명')
        qty_val = _validate_qty(qty, '이동수량')
        src = _validate_location(from_location, '출발 창고')
        dst = _validate_location(to_location, '도착 창고')

        # 출발/도착 동일 확인
        from services.excel_io import normalize_location
        src_norm = normalize_location(src)
        dst_norm = normalize_location(dst)
        if src_norm == dst_norm:
            raise ValidationError(
                f'출발 창고와 도착 창고가 동일합니다: {src_norm}',
                code='SAME_WAREHOUSE')

        # 재고 부족 시뮬레이션
        shortages = simulate_stock_change(db, src_norm, [(name, -qty_val)])
        if shortages:
            s = shortages[0]
            _log_validation_failure('transfer',
                f"{src_norm}→{dst_norm} {name} 재고부족: {s['current']}/{s['required']}")
            # 이동은 경고로 처리 (기존 동작 호환)
        return shortages

    @staticmethod
    def adjustment(db, date_str, items):
        """재고 조정 전 검증.

        Raises:
            ValidationError
        """
        _validate_date(date_str, '조정일자')

        if not items:
            raise ValidationError('조정 항목이 없습니다.', code='EMPTY_ITEMS')

        for i, item in enumerate(items):
            prefix = f'{i+1}번째'
            name = _validate_product_name(item.get('product_name', ''), f'{prefix} 품목명')
            _validate_location(item.get('location', ''), f'{prefix}({name}) 위치')
            _validate_qty(item.get('qty', 0), f'{prefix}({name}) 수량', allow_negative=True)

            memo = str(item.get('memo', '')).strip()
            if not memo:
                raise ValidationError(
                    f'{prefix}({name}): 조정 사유를 입력하세요.',
                    code='EMPTY_MEMO', field='memo')

    @staticmethod
    def inbound(db, date_str, payload):
        """입고 전 검증.

        Raises:
            ValidationError
        """
        _validate_date(date_str, '입고일자')

        if not payload:
            raise ValidationError('입고 데이터가 없습니다.', code='EMPTY_PAYLOAD')

        for i, row in enumerate(payload):
            prefix = f'{i+1}번째'
            _validate_product_name(row.get('product_name', ''), f'{prefix} 품목명')
            _validate_qty(row.get('qty', 0), f'{prefix} 수량')

    @staticmethod
    def file_upload(db, file_bytes, channel='', uploader=''):
        """파일 업로드 중복 검증.

        Args:
            file_bytes: 파일 바이트 데이터
            channel: 채널명 (선택)
            uploader: 업로드한 사용자

        Returns:
            str: file_hash (중복 아닌 경우)

        Raises:
            DuplicateUploadError: 동일 파일이 이미 처리됨
        """
        file_hash = compute_file_hash(file_bytes)
        existing = check_duplicate_upload(db, file_hash, channel or None)
        if existing:
            msg = (f"동일한 파일이 이미 처리되었습니다.\n"
                   f"  처리일시: {existing.get('created_at', '?')}\n"
                   f"  파일명: {existing.get('filename', '?')}\n"
                   f"  처리자: {existing.get('uploaded_by', '?')}")
            _log_validation_failure('file_upload', msg, {'hash': file_hash})
            raise DuplicateUploadError(msg, file_hash=file_hash,
                                       original_run_id=existing.get('id'))
        return file_hash

    @staticmethod
    def product_name_safety(db, input_names):
        """품목명 안전 검사 (유사명 감지).

        Args:
            input_names: list of str

        Returns:
            list of SimilarNameWarning
        """
        try:
            known = db.query_unique_product_names()
            known_names = [p.get('name', '') if isinstance(p, dict) else str(p)
                           for p in known]
        except Exception:
            return []

        warnings = []
        for name in input_names:
            w = check_similar_names(name, known_names)
            if w:
                warnings.append(w)
        return warnings


# 싱글톤 인스턴스
validate = _Validator()


# ═══════════════════════════════════════════════════════════════
# 트랜잭션 ID 생성
# ═══════════════════════════════════════════════════════════════

def generate_transaction_id():
    """고유 트랜잭션 ID 생성."""
    return str(uuid.uuid4())[:12]
