import os, io, re, warnings, gc, unicodedata, hashlib, json
from datetime import datetime
import pandas as pd
import msoffcrypto

from services.channel_config import (
    build_column_map, detect_channel, validate_required_columns,
    get_field_label, is_encrypted, get_password, get_header_row, is_csv,
    MONEY_FIELDS, SIMPLE_INVOICE_CHANNELS,
)
from services.tz_utils import today_kst

warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')


def _write_xls(filepath, headers, rows):
    """xlwt로 .xls 파일 직접 생성 (리스트 데이터용)"""
    import xlwt
    wb = xlwt.Workbook(encoding='utf-8')
    ws = wb.add_sheet('Sheet1')
    for ci, h in enumerate(headers):
        ws.write(0, ci, h)
    for ri, row in enumerate(rows, 1):
        for ci, val in enumerate(row):
            ws.write(ri, ci, val)
    wb.save(filepath)


def _write_xls_from_df(filepath, df):
    """xlwt로 .xls 파일 직접 생성 (DataFrame용)"""
    import xlwt
    wb = xlwt.Workbook(encoding='utf-8')
    ws = wb.add_sheet('Sheet1')
    for ci, col in enumerate(df.columns):
        ws.write(0, ci, str(col))
    for ri, (_, row) in enumerate(df.iterrows(), 1):
        for ci, val in enumerate(row):
            cell = str(val) if pd.notna(val) else ''
            ws.write(ri, ci, cell)
    wb.save(filepath)


class OrderProcessor:
    def __init__(self):
        self.logs = []

    def log(self, msg):
        t = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
        try:
            print(t)
        except (UnicodeEncodeError, UnicodeDecodeError, OSError):
            try:
                print(t.encode('ascii', errors='replace').decode('ascii'))
            except Exception:
                pass
        self.logs.append(t)

    def get_safe_val(self, row, idx):
        """[135 에러 방지] 인덱스 초과 시 빈 값 반환"""
        try:
            if idx < len(row):
                return str(row.iloc[idx]).strip()
            return ""
        except:
            return ""

    def _parse_money(self, row, col_idx):
        """금액 컬럼 값을 숫자로 파싱 (쉼표/공백 제거)"""
        if col_idx is None:
            return 0
        raw = self.get_safe_val(row, col_idx)
        if not raw:
            return 0
        cleaned = re.sub(r'[^\d.\-]', '', raw)
        try:
            return float(cleaned) if cleaned else 0
        except ValueError:
            return 0

    def _compute_file_hash(self, file_input):
        """파일 SHA256 해시 계산"""
        try:
            buf = self._open_as_bytesio(file_input)
            content = buf.read()
            buf.seek(0)
            return hashlib.sha256(content).hexdigest()
        except Exception:
            return None

    def _open_as_bytesio(self, file_input):
        """파일 경로 또는 file-like 객체를 BytesIO로 반환"""
        if isinstance(file_input, (str, os.PathLike)):
            with open(file_input, "rb") as f:
                buf = io.BytesIO(f.read())
            return buf
        elif hasattr(file_input, 'read'):
            data = file_input.read()
            if isinstance(data, str):
                data = data.encode('utf-8')
            buf = io.BytesIO(data)
            return buf
        else:
            return file_input

    def _get_filename(self, file_input):
        """파일 이름 추출 (경로 또는 file-like 객체)"""
        if isinstance(file_input, (str, os.PathLike)):
            return str(file_input)
        elif hasattr(file_input, 'name'):
            return file_input.name
        return ""

    def load_smart_store_memory(self, file_input):
        """[131 에러 방지] 스마트스토어 메모리 복호화 엔진"""
        try:
            raw_buf = self._open_as_bytesio(file_input)
            dec_buffer = io.BytesIO()
            ms = msoffcrypto.OfficeFile(raw_buf)
            if ms.is_encrypted():
                ms.load_key(password="1111")
                ms.decrypt(dec_buffer)
                dec_buffer.seek(0)
                self.log("✅ [보안] 스마트스토어 암호 해제 성공")
            else:
                raw_buf.seek(0)
                dec_buffer.write(raw_buf.read())
                dec_buffer.seek(0)

            temp_df = pd.read_excel(dec_buffer, header=None, nrows=15, dtype=str)
            keywords = ['상품명', '수취인명', '수하인명', '주문상태']
            best_score, best_row = 0, 0
            for i, row in temp_df.iterrows():
                row_vals = "".join([str(v) for v in row.values])
                matched = sum(1 for k in keywords if k in row_vals)
                if matched > best_score:
                    best_score = matched
                    best_row = i

            dec_buffer.seek(0)
            df = pd.read_excel(dec_buffer, header=best_row, dtype=str)
            return df.fillna('').apply(lambda x: x.str.strip() if x.dtype == "object" else x)
        except Exception as e:
            self.log(f"❌ 읽기 에러: {e}")
            return None

    def load_generic(self, file_input, header=0):
        if not file_input:
            return None
        try:
            filename = self._get_filename(file_input)

            if isinstance(file_input, (str, os.PathLike)):
                path = str(file_input)
                if path.lower().endswith('.csv'):
                    try:
                        df = pd.read_csv(path, encoding='utf-8-sig', dtype=str)
                    except:
                        df = pd.read_csv(path, encoding='cp949', dtype=str)
                else:
                    try:
                        df = pd.read_excel(path, header=header, engine='openpyxl', dtype=str)
                    except:
                        df = pd.read_excel(path, header=header, engine='xlrd', dtype=str)
            else:
                # file-like object
                buf = self._open_as_bytesio(file_input)
                if filename.lower().endswith('.csv'):
                    try:
                        df = pd.read_csv(buf, encoding='utf-8-sig', dtype=str)
                    except:
                        buf.seek(0)
                        df = pd.read_csv(buf, encoding='cp949', dtype=str)
                else:
                    try:
                        df = pd.read_excel(buf, header=header, engine='openpyxl', dtype=str)
                    except:
                        buf.seek(0)
                        df = pd.read_excel(buf, header=header, engine='xlrd', dtype=str)

            return df.fillna('').apply(lambda x: x.str.strip() if x.dtype == "object" else x)
        except Exception as e:
            self.log(f"파일 로드 실패: {e}")
            return None

    def run(self, mode, order_file, option_file, invoice_file, target_type, output_dir,
            db=None, option_source='file', save_to_db=False, uploaded_by=None,
            collection_date=None, opt_list_override=None):
        """
        mode: '스마트스토어'|'자사몰'|'쿠팡'|'옥션/G마켓'|'오아시스'|'11번가'|'카카오'
        order_file: file-like object or path
        option_file: file-like object or path (optional if option_source='db')
        invoice_file: file-like object or path (optional, for 리얼패킹/외부일괄)
        target_type: '송장'|'리얼패킹'|'외부일괄'
        output_dir: directory for output files
        db: SupabaseDB instance (for option_source='db')
        option_source: 'file' or 'db'
        save_to_db: True면 주문을 DB에 저장 (Phase 1)
        uploaded_by: 업로드한 사용자명
        collection_date: 주문수집일 (YYYY-MM-DD, 미지정 시 송장생성 당일)
        opt_list_override: 옵션 리스트 직접 주입 (캐시 완전 우회, 재처리 시 사용)

        returns: {
            'success': bool,
            'files': [list of output file paths],
            'logs': [list of log messages],
            'error': str or None,
            'unmatched': list (미매칭 항목, 있을 때만),
            'db_result': dict (DB 저장 결과, save_to_db=True일 때)
        }
        """
        self.logs = []
        result = {
            'success': False,
            'files': [],
            'logs': self.logs,
            'error': None
        }

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        try:
            self.log(f"🚀 v17.0 [{mode}] {target_type} 가동")

            # [1] 옵션지 분석 (DB 또는 파일, override 우선)
            if opt_list_override is not None:
                opt_list = opt_list_override
                if not opt_list:
                    result['error'] = "옵션마스터 데이터가 비어있습니다."
                    return result
                # 헤더 잔여행/빈 데이터 제거
                _header_vals_ovr = {'standard_name', 'product_name', '품목명', 'original_name', '원문명'}
                opt_list = [o for o in opt_list
                            if str(o.get('원문명', '')).strip()
                            and str(o.get('품목명', '')).strip().lower() not in _header_vals_ovr
                            and str(o.get('원문명', '')).strip().lower() not in _header_vals_ovr]
                opt_raw = pd.DataFrame(opt_list)[['원문명', '품목명', '라인코드', '출력순서', '바코드']]
                opt_raw['출력순서'] = pd.to_numeric(opt_raw['출력순서'], errors='coerce').fillna(999)
                self.log(f"✅ 옵션마스터(DB직접) 로드: {len(opt_list)}건")
            elif option_source == 'db' and db is not None:
                opt_list = db.query_option_master_as_list()
                if not opt_list:
                    result['error'] = "옵션마스터 DB에 데이터가 없습니다.\n마스터 관리에서 옵션리스트를 동기화하세요."
                    return result
                # 헤더 잔여행/빈 데이터 제거 (Standard_Name, 빈 원문명 등)
                _header_vals = {'standard_name', 'product_name', '품목명', 'original_name', '원문명'}
                opt_list = [o for o in opt_list
                            if str(o.get('원문명', '')).strip()
                            and str(o.get('품목명', '')).strip().lower() not in _header_vals
                            and str(o.get('원문명', '')).strip().lower() not in _header_vals]
                opt_raw = pd.DataFrame(opt_list)[['원문명', '품목명', '라인코드', '출력순서', '바코드']]
                opt_raw['출력순서'] = pd.to_numeric(opt_raw['출력순서'], errors='coerce').fillna(999)
                self.log(f"✅ 옵션마스터(DB) 로드: {len(opt_list)}건")
            else:
                opt_df = self.load_generic(option_file)
                if opt_df is None or opt_df.empty:
                    result['error'] = "옵션리스트 오류"
                    return result
                opt_raw = opt_df.iloc[:, [0, 1, 2, 4, 5]].copy()
                opt_raw.columns = ['원문명', '품목명', '라인코드', '출력순서', '바코드']
                opt_raw['출력순서'] = pd.to_numeric(opt_raw['출력순서'], errors='coerce').fillna(999)
                opt_list = opt_raw.to_dict('records')
                for o in opt_list:
                    o['Key'] = str(o['원문명']).replace(" ", "").upper()

            # [검증] 같은 출력순서(E열)에 품목명(B열)이 다른 경우 체크
            # — 빈 품목명, 헤더 잔여값(Standard_Name 등), 출력순서 999(미지정)는 검증 제외
            _bad_names = {'', 'standard_name', 'product_name', '품목명'}
            check_df = opt_raw[
                (opt_raw['출력순서'] != 999) &
                (~opt_raw['품목명'].str.strip().str.lower().isin(_bad_names))
            ].copy()
            line_groups = check_df.groupby('출력순서')['품목명'].apply(lambda x: list(x.unique())).to_dict()
            conflict_lines = {k: v for k, v in line_groups.items() if len(v) > 1}
            if conflict_lines:
                err_msg = "⚠️ 옵션리스트 품목명 불일치!\n같은 라인(출력순서)에 품목명이 다릅니다.\n품목명을 통일해주세요.\n\n"
                for line, names in conflict_lines.items():
                    err_msg += f"  라인 {int(line)}: {' / '.join(names)}\n"
                self.log(err_msg)
                result['error'] = err_msg
                return result

            # 같은 출력순서(E열)끼리 품목명 통일 (B열이 동일한 경우 안전하게 정리)
            line_to_name = {}
            for _, row in opt_raw.iterrows():
                ln = row['출력순서']
                if ln not in line_to_name:
                    line_to_name[ln] = row['품목명']
                else:
                    row['품목명'] = line_to_name[ln]

            # DB에서 로드한 경우 Key가 이미 있음, 파일에서 로드한 경우 위에서 설정
            if option_source == 'db':
                pass  # Key already set from DB match_key
            else:
                pass  # Key already set above

            # [2] 파일 로드 + 컬럼 자동 인식 (v17 통합)
            res = []
            df = None

            if mode in ("스마트스토어", "해미애찬"):
                df = self.load_smart_store_memory(order_file)
                if df is None or df.empty:
                    result['error'] = f"[{mode}] 파일 읽기 실패"
                    return result
            else:
                header = get_header_row(mode)
                df = self.load_generic(order_file, header=header)

            if df is None or df.empty:
                result['error'] = f"[{mode}] 주문 파일 읽기 실패"
                return result

            # 컬럼 자동 인식 (channel_config 엔진)
            col_map = build_column_map(df, mode)

            # 기존 호환 매핑 (m 딕셔너리)
            m = {
                'n': col_map.get('name'),
                'a': col_map.get('address'),
                'a2': col_map.get('address2'),
                'p1': col_map.get('phone'),
                'p2': col_map.get('phone2'),
                'msg': col_map.get('memo'),
                'opt': col_map.get('option'),
                'prod': col_map.get('product'),
                'qty': col_map.get('qty'),
                'st': col_map.get('status'),
                'date': col_map.get('order_date'),
                'no': col_map.get('order_no'),
            }

            # 필수 컬럼 검증
            is_valid, missing_fields = validate_required_columns(col_map, mode)
            if not is_valid:
                missing_labels = [get_field_label(f) for f in missing_fields]
                result['error'] = f"[{mode}] 필수 컬럼을 찾을 수 없습니다:\n{', '.join(missing_labels)}"
                return result

            self.log(f"📋 컬럼 자동 매핑 ({len(df.columns)}열): "
                     + " ".join(f"{get_field_label(k)}=[{v}]" for k, v in
                               [('order_no', m['no']), ('name', m['n']),
                                ('product', m['prod']), ('option', m['opt']), ('qty', m['qty'])]
                               if v is not None))

            # 필터링
            target = df.copy()
            if m.get('st') is not None:
                target = target[~target.iloc[:, m['st']].astype(str).str.contains('취소|반품', na=False)].copy()

            # N배송 필터링 (스마트스토어)
            if mode == "스마트스토어":
                n_ship_idx = col_map.get('n_ship')
                if n_ship_idx is not None:
                    n_ship_col = target.iloc[:, n_ship_idx].astype(str)
                    n_excluded = n_ship_col.str.contains('N배송', na=False).sum()
                    if n_excluded > 0:
                        target = target[~n_ship_col.str.contains('N배송', na=False)].copy()
                        self.log(f"🚫 N배송 상품 {n_excluded}건 자동 제외 (잔여: {len(target)}건)")

            # [3] 매칭 프로세스
            # "옵션 없음" 판별용 키워드 — 이 값이 option에 있으면 상품명으로 폴백
            _NO_OPT = {'단일상품', '옵션없음', '옵션 없음', '기본', '해당없음',
                        '없음', '-', 'noption', 'none', 'n/a', '상품정보참조'}

            def _is_no_option(val):
                """option 값이 실질적으로 '없음'인지 판별"""
                if not val:
                    return True
                v = val.strip()
                if not v:
                    return True
                return v in _NO_OPT or any(nk in v for nk in ('단일상품',))

            unmatched = []  # 매칭 실패 항목 수집
            matched_keys = set()  # 매칭 성공한 Key 수집 (last_matched_at 갱신용)
            for i, r in target.iterrows():
                try:
                    v_opt = self.get_safe_val(r, m['opt'])   # 옵션값
                    v_prod = self.get_safe_val(r, m['prod'])  # 상품명

                    if mode == "쿠팡":
                        # 쿠팡: 단일상품/빈옵션 → 상품명만, 아니면 상품명+옵션
                        k = v_prod if _is_no_option(v_opt) else v_prod + v_opt
                    elif mode == "옥션/G마켓":
                        # 옥션/G마켓: 옵션에서 '/' 앞부분 사용, 없으면 상품명
                        k = v_opt.split('/')[0].strip() if v_opt and not _is_no_option(v_opt) else v_prod
                    else:
                        # 스마트스토어/자사몰/오아시스/11번가/카카오 등
                        # 옵션이 유효하면 옵션 사용, 단일상품/빈값이면 상품명 폴백
                        k = v_opt if v_opt and not _is_no_option(v_opt) else v_prod

                    c_k = k.replace(" ", "").upper()
                    # 1차: 정확 매칭 (우선)
                    match = next((o for o in opt_list if c_k == o['Key']), None)
                    # 2차: 부분 매칭 (가장 긴 Key 우선 → 오트밀가루 > 오트밀)
                    # — Key 최소 4자 + 품목명 비어있지 않은 항목만 (쓰레기 매칭 방지)
                    if not match:
                        candidates = [o for o in opt_list
                                      if len(o['Key']) >= 4
                                      and o.get('품목명', '').strip()
                                      and o['Key'] in c_k]
                        if candidates:
                            match = max(candidates, key=lambda o: len(o['Key']))

                    if match:
                        matched_keys.add(match['Key'])
                        # 주소 합산 (스마트스토어: 기본+상세, 기타: 단일)
                        addr_front = self.get_safe_val(r, m['a']) if m.get('a') is not None else ''
                        addr_detail = self.get_safe_val(r, m['a2']) if m.get('a2') is not None else ''
                        full_addr = f"{addr_front} {addr_detail}".strip() if addr_detail else addr_front
                        clean_addr = re.sub(r'\s+', '', full_addr)

                        qty_val = pd.to_numeric(self.get_safe_val(r, m['qty']), errors='coerce')
                        qty_int = int(qty_val) if not pd.isna(qty_val) else 1

                        line_code_val = pd.to_numeric(match['라인코드'], errors='coerce')
                        line_code_int = int(line_code_val) if not pd.isna(line_code_val) else 0

                        row_data = {
                            'name': self.get_safe_val(r, m['n']) if m.get('n') is not None else '',
                            'addr': full_addr,
                            'clean_addr': clean_addr,
                            'p1': re.sub(r'[^0-9]', '', self.get_safe_val(r, m['p1'])) if m.get('p1') is not None else '',
                            'qty': qty_int,
                            'display_nm': match['품목명'],
                            'barcode': match['바코드'],
                            'code': line_code_int,
                            'msg': self.get_safe_val(r, m['msg']) if m.get('msg') is not None else '',
                            'p2': self.get_safe_val(r, m.get('p2', m.get('p1'))) if m.get('p2') is not None or m.get('p1') is not None else '',
                            'sort': match['출력순서'],
                            'order_date': self.get_safe_val(r, m['date']) if m.get('date') is not None else '',
                            'order_no': self.get_safe_val(r, m['no']) if m.get('no') is not None else '',
                            '_order_group': self.get_safe_val(r, col_map.get('order_group')) if col_map.get('order_group') is not None else '',
                        }

                        # 금액 데이터 추출 (col_map에서)
                        row_data['_unit_price'] = self._parse_money(r, col_map.get('unit_price'))
                        row_data['_total_amount'] = self._parse_money(r, col_map.get('total'))
                        row_data['_discount'] = self._parse_money(r, col_map.get('discount'))
                        row_data['_settlement'] = self._parse_money(r, col_map.get('settlement'))
                        row_data['_commission'] = self._parse_money(r, col_map.get('commission'))
                        row_data['_shipping_fee'] = self._parse_money(r, col_map.get('shipping_fee'))

                        # 추가 금액 필드 (item_price, option_price 등)
                        _item_price = self._parse_money(r, col_map.get('item_price'))
                        _option_price = self._parse_money(r, col_map.get('option_price'))
                        _seller_discount = self._parse_money(r, col_map.get('seller_discount'))

                        # unit_price fallback: item_price + option_price
                        if not row_data['_unit_price'] and (_item_price or _option_price):
                            row_data['_unit_price'] = _item_price + _option_price - _seller_discount

                        # total_amount fallback: unit_price * qty
                        if not row_data['_total_amount'] and row_data['_unit_price']:
                            _qty = row_data.get('qty', 1) or 1
                            row_data['_total_amount'] = row_data['_unit_price'] * _qty

                        # 원본 옵션/상품명
                        row_data['_original_option'] = self.get_safe_val(r, m['opt']) if m.get('opt') is not None else ''
                        row_data['_original_product'] = self.get_safe_val(r, m['prod']) if m.get('prod') is not None else ''

                        # raw_data (전체 행 원본 → JSONB)
                        raw_dict = {str(df.columns[ci]): str(r.iloc[ci]) for ci in range(len(r)) if str(r.iloc[ci]).strip()}
                        row_data['_raw_data'] = raw_dict
                        row_data['_raw_hash'] = hashlib.sha256(json.dumps(raw_dict, sort_keys=True, ensure_ascii=False).encode()).hexdigest()

                        res.append(row_data)
                    else:
                        # 매칭 실패 → 실제 매칭 키(k) 수집 (옵션리스트 A열에 넣을 값)
                        if k and k not in unmatched:
                            unmatched.append(k)
                            c_k_debug = k.replace(" ", "").upper()
                            # 유사 키 탐색 (디버깅용)
                            similar = [o['Key'] for o in opt_list if c_k_debug[:6] in o['Key'] or o['Key'][:6] in c_k_debug][:3]
                            self.log(f"[UNMATCH] key='{k}' c_k='{c_k_debug}' | 유사: {similar if similar else '없음'} | opt_list: {len(opt_list)}건")
                except:
                    continue

            # 매칭 성공한 옵션 last_matched_at 갱신
            if matched_keys and option_source == 'db' and db is not None:
                try:
                    db.touch_option_matched(list(matched_keys))
                except Exception:
                    pass  # 갱신 실패해도 처리는 계속

            # 미매칭 항목 처리
            if unmatched:
                if target_type in ("리얼패킹", "외부일괄"):
                    # 리얼패킹/외부일괄은 송장 후처리 → 미매칭 경고만, 매칭된 건으로 계속 진행
                    self.log(f"⚠️ 옵션 미등록 {len(unmatched)}건 (리얼패킹이므로 매칭된 건만 처리)")
                else:
                    self.log(f"⚠️ 옵션 미등록 {len(unmatched)}건 발견 → 처리 중단")
                    msg = f"옵션리스트에 등록되지 않은 상품 {len(unmatched)}건:\n\n"
                    for nm in unmatched[:20]:
                        msg += f"  • {nm[:80]}\n"
                    if len(unmatched) > 20:
                        msg += f"  ... 외 {len(unmatched) - 20}건\n"
                    msg += f"\n옵션마스터에 위 상품명을 등록 후 다시 실행하세요."
                    result['error'] = msg
                    result['unmatched'] = unmatched
                    return result

            if not res:
                self.log("❌ 매칭 데이터 0건")
                result['error'] = "매칭 데이터 0건"
                return result

            # ─── 배송비 주문번호 기준 중복제거 ───
            # 동일 주문에 배송비가 반복 기록됨 → 첫 행만 유지, 나머지 0
            # order_group(주문번호) 우선, 없으면 order_no(상품주문번호) 사용
            _ship_seen = set()
            _ship_dedup_count = 0
            for rd in res:
                grp_key = rd.get('_order_group', '') or rd.get('order_no', '')
                if grp_key in _ship_seen:
                    if rd.get('_shipping_fee', 0) > 0:
                        rd['_shipping_fee'] = 0
                        _ship_dedup_count += 1
                else:
                    _ship_seen.add(grp_key)
            if _ship_dedup_count:
                self.log(f"📦 배송비 중복제거: {_ship_dedup_count}건 (동일주문 첫 행만 유지)")

            # ─── [Phase 1] DB 저장 (실패해도 송장 생성은 계속) ───
            if save_to_db and db is not None:
                try:
                    db_result = self._save_orders_to_db(
                        db, mode, res, order_file, uploaded_by, len(target),
                        collection_date=collection_date
                    )
                    result['db_result'] = db_result
                    cross_skip = db_result.get('cross_channel_skipped', 0)
                    cross_msg = f", 타채널중복 {cross_skip}건" if cross_skip else ""
                    self.log(f"DB 저장: 신규 {db_result.get('inserted', 0)}건, "
                             f"변경 {db_result.get('updated', 0)}건, "
                             f"스킵 {db_result.get('skipped', 0)}건, "
                             f"실패 {db_result.get('failed', 0)}건{cross_msg}")
                    if cross_skip:
                        self.log(f"⚠️ 다른 채널에 이미 등록된 주문 {cross_skip}건 스킵 (order_no/raw_hash 동일)")
                except Exception as db_err:
                    self.log(f"DB 저장 중 예외 발생 (송장은 계속): {db_err}")
                    result['db_result'] = {"inserted": 0, "updated": 0, "skipped": 0,
                                           "failed": len(res), "error": str(db_err)}

            res_df = pd.DataFrame(res)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_nm = mode.replace("/", "_")

            if target_type == "송장":
                # [집계표] 출력순서(E열) 기준 통계 - 같은 라인은 하나로 합산 + 출고지 포함
                sums = res_df.groupby(['sort', 'display_nm'])['qty'].sum().reset_index()
                sums_by_line = sums.groupby('sort').agg({'display_nm': 'first', 'qty': 'sum'}).reset_index()
                master_list = opt_raw[['출력순서', '품목명', '라인코드']].drop_duplicates('출력순서')
                qty_rep = pd.merge(master_list, sums_by_line, left_on='출력순서', right_on='sort', how='left')
                qty_rep['qty'] = pd.to_numeric(qty_rep['qty'], errors='coerce').fillna(0).astype(int)
                qty_rep['warehouse'] = qty_rep['라인코드'].apply(
                    lambda c: "해서" if (not pd.isna(v := pd.to_numeric(c, errors='coerce')) and int(v) == 5) else "넥스원"
                )
                summary_path = os.path.join(output_dir, f"{safe_nm}_집계표_{ts}.xlsx")
                qty_rep.sort_values('출력순서')[['품목명', 'qty', 'warehouse']].to_excel(summary_path, index=False)
                result['files'].append(summary_path)

                # [송장 생성 - 주소지 합포장]
                rosen = res_df[res_df['code'] != 5]
                ext = res_df[res_df['code'] == 5]
                s_nms = {0: "단없음", 1: "1단", 2: "2단", 3: "3단", 4: "기타", 5: "외부"}
                _is_simple = mode in SIMPLE_INVOICE_CHANNELS
                for d, nt in [(rosen, ""), (ext, "_외부")]:
                    if not d.empty:
                        inv = []
                        for a_c, gp in d.groupby(['clean_addr'], sort=False):
                            if _is_simple:
                                # 단순 채널: 단(段) 구분 없이 품목명만 나열
                                items = []
                                for _, rd in gp.iterrows():
                                    items.append(f"{rd['display_nm']}x{rd['qty']}")
                                item_str = ", ".join(items) + f" 총{gp['qty'].sum()}개"
                            else:
                                # 배마마: 단별 그룹핑 (1단/2단/3단)
                                stg = {}
                                for _, rd in gp.iterrows():
                                    c = rd['code']
                                    if c not in stg:
                                        stg[c] = []
                                    stg[c].append(f"{rd['display_nm']}x{rd['qty']}")
                                item_str = " ".join(
                                    [f"{s_nms.get(k, f'{k}라인')}({', '.join(v)})" for k, v in sorted(stg.items()) if v]
                                ) + f" 총{gp['qty'].sum()}개"
                            rep = gp.iloc[0]
                            inv.append([
                                rep['name'], "", rep['addr'], rep['p1'], rep['p2'],
                                "1", "3000", "", item_str, "", rep['msg']
                            ])
                        inv_path = os.path.join(output_dir, f"{safe_nm}{nt}송장_{ts}.xlsx")
                        pd.DataFrame(inv, columns=[
                            "수하인명", "B1", "수하인주소", "연락처1", "연락처2",
                            "박스", "운임", "B2", "품목명", "B3", "배송메세지"
                        ]).to_excel(inv_path, index=False)
                        result['files'].append(inv_path)

                result['success'] = True
                done_msg = f"[{mode}] 완료! 합포장 송장 {len(res_df.groupby(['clean_addr']))}건"
                self.log(done_msg)

            elif target_type == "리얼패킹":
                if not invoice_file:
                    result['error'] = "리얼패킹에는 '3번 송장결과' 파일이 필요합니다.\n로젠택배 접수 결과 엑셀을 선택해주세요."
                    return result
                inv_df = self.load_generic(invoice_file)
                if inv_df is None or inv_df.empty:
                    result['error'] = "송장결과 파일을 읽을 수 없습니다.\n파일 형식을 확인해주세요."
                    return result
                inv_df.columns = [str(c).replace(" ", "") for c in inv_df.columns]
                inv_cols = list(inv_df.columns)
                if len(inv_cols) < 22:
                    result['error'] = f"송장결과 파일 컬럼 수 부족 ({len(inv_cols)}열).\n로젠택배 접수 결과 파일이 맞는지 확인해주세요."
                    return result
                s_c, n_c, p_c = inv_cols[7], inv_cols[20], inv_cols[21]
                inv_df['p_cl'] = inv_df[p_c].astype(str).str.replace(r'[^0-9]', '', regex=True)

                rp_f, ss_bulk, m_cnt = [], [], 0
                rp_df = res_df[res_df['code'] != 5]  # 외부송장 제외
                if rp_df.empty:
                    result['error'] = "외부송장 제외 후 리얼패킹 대상이 없습니다."
                    return result
                for a_c, gp in rp_df.groupby(['clean_addr'], sort=False):
                    rep = gp.iloc[0]
                    match = inv_df[
                        (inv_df[n_c] == rep['name']) &
                        (inv_df['p_cl'].str.endswith(rep['p1'][-4:]))
                    ]
                    inv_no = str(match.iloc[0][s_c]).strip() if not match.empty else ""

                    if inv_no and inv_no != 'nan' and inv_no != '':
                        m_cnt += 1
                        for _, r in gp.iterrows():
                            rp_f.append([
                                r['order_date'], r['order_no'],
                                '택배발송 : 택배,등기,소포', 'CJ대한통운', inv_no,
                                r['name'], r['p1'], r['display_nm'], r['qty'], r['barcode']
                            ])
                            if mode == "스마트스토어":
                                ss_bulk.append([
                                    r['order_no'], '택배발송 : 택배,등기,소포',
                                    'CJ대한통운', inv_no, r['name'], r['p1']
                                ])

                if not rp_f:
                    result['error'] = "송장결과와 주문서 간 매칭 데이터가 없습니다.\n이름+연락처(뒤4자리)로 매칭합니다. 파일을 확인해주세요."
                    return result

                rp_path = os.path.join(output_dir, f"리얼패킹_{safe_nm}_{ts}.xlsx")
                pd.DataFrame(rp_f, columns=[
                    "주문일자", "주문번호", "배송방법", "택배사", "송장번호",
                    "이름", "연락처", "제품명", "수량", "바코드"
                ]).to_excel(rp_path, index=False)
                result['files'].append(rp_path)

                if mode == "스마트스토어" and ss_bulk:
                    ss_path = os.path.join(output_dir, f"스마트스토어_일괄배송입력_{ts}.xls")
                    _write_xls(ss_path,
                               ["상품주문번호", "배송방법", "택배사", "송장번호", "수취인", "전화번호"],
                               ss_bulk)
                    result['files'].append(ss_path)
                    result['success'] = True
                    self.log("리얼패킹 & 스마트스토어 일괄배송입력 생성 완료!")

                elif mode == "쿠팡":
                    # [쿠팡 일괄배송] DeliveryList 원본 복사 + E열(운송장번호)만 송장결과에서 매칭
                    cp_bulk = df.copy()
                    cp_cols = list(cp_bulk.columns)
                    cp_n_col = cp_cols[26]     # 수취인명
                    cp_p_col = cp_cols[27]     # 수취인연락처
                    cp_track_col = cp_cols[4]  # 운송장번호 (E열)
                    cp_bulk['_p_cl'] = cp_bulk[cp_p_col].astype(str).str.replace(r'[^0-9]', '', regex=True)
                    fill_cnt = 0
                    for idx, row in cp_bulk.iterrows():
                        r_name = str(row[cp_n_col]).strip()
                        r_phone = str(row['_p_cl']).strip()
                        if len(r_phone) < 4:
                            continue
                        match_inv = inv_df[
                            (inv_df[n_c] == r_name) &
                            (inv_df['p_cl'].str.endswith(r_phone[-4:]))
                        ]
                        if not match_inv.empty:
                            track_no = str(match_inv.iloc[0][s_c]).strip()
                            if track_no and track_no != 'nan':
                                cp_bulk.at[idx, cp_track_col] = track_no
                                fill_cnt += 1
                    cp_bulk.drop(columns=['_p_cl'], inplace=True)
                    cp_path = os.path.join(output_dir, f"쿠팡_일괄배송_{ts}.xlsx")
                    cp_bulk.to_excel(cp_path, index=False)
                    result['files'].append(cp_path)
                    self.log(f"✅ 쿠팡 일괄배송 파일 생성: {fill_cnt}건 송장 입력")
                    result['success'] = True
                    self.log(f"리얼패킹 & 쿠팡 일괄배송 생성 완료! 송장매칭: {fill_cnt}건")

                else:
                    result['success'] = True
                    self.log(f"리얼패킹 완료! 매칭: {m_cnt}건")

            elif target_type == "외부일괄":
                # [외부송장 일괄배송] code==5 건만 대상, 리얼패킹 없이 일괄배송만 생성
                ext_df = res_df[res_df['code'] == 5]
                if ext_df.empty:
                    result['error'] = "외부송장(5번) 대상 건이 없습니다."
                    return result
                self.log(f"📦 외부송장 일괄배송 대상: {len(ext_df)}건")

                if mode == "스마트스토어":
                    if not invoice_file:
                        result['error'] = "3번 외부 송장결과를 선택하세요."
                        return result
                    inv_df = self.load_generic(invoice_file)
                    inv_df.columns = [str(c).replace(" ", "") for c in inv_df.columns]
                    inv_cols = list(inv_df.columns)
                    s_c, n_c, p_c = inv_cols[7], inv_cols[20], inv_cols[21]
                    inv_df['p_cl'] = inv_df[p_c].astype(str).str.replace(r'[^0-9]', '', regex=True)

                    ss_ext = []
                    for a_c, gp in ext_df.groupby(['clean_addr'], sort=False):
                        rep = gp.iloc[0]
                        match = inv_df[
                            (inv_df[n_c] == rep['name']) &
                            (inv_df['p_cl'].str.endswith(rep['p1'][-4:]))
                        ]
                        inv_no = str(match.iloc[0][s_c]).strip() if not match.empty else ""
                        if inv_no and inv_no != 'nan' and inv_no != '':
                            for _, r in gp.iterrows():
                                ss_ext.append([
                                    r['order_no'], '택배발송 : 택배,등기,소포',
                                    'CJ대한통운', inv_no, r['name'], r['p1']
                                ])
                    if not ss_ext:
                        result['error'] = "외부송장 매칭 데이터 없음"
                        return result
                    ss_ext_path = os.path.join(output_dir, f"스마트스토어_외부_일괄배송_{ts}.xls")
                    _write_xls(ss_ext_path,
                               ["상품주문번호", "배송방법", "택배사", "송장번호", "수취인", "전화번호"],
                               ss_ext)
                    result['files'].append(ss_ext_path)
                    result['success'] = True
                    self.log(f"스마트스토어 외부송장 일괄배송 생성 완료! {len(ss_ext)}건")

                elif mode == "쿠팡":
                    if not invoice_file:
                        result['error'] = "3번 외부 송장결과를 선택하세요."
                        return result
                    inv_df = self.load_generic(invoice_file)
                    inv_df.columns = [str(c).replace(" ", "") for c in inv_df.columns]
                    inv_cols = list(inv_df.columns)
                    s_c, n_c, p_c = inv_cols[7], inv_cols[20], inv_cols[21]
                    inv_df['p_cl'] = inv_df[p_c].astype(str).str.replace(r'[^0-9]', '', regex=True)

                    cp_bulk = df.copy()
                    cp_cols = list(cp_bulk.columns)
                    cp_n_col = cp_cols[26]
                    cp_p_col = cp_cols[27]
                    cp_track_col = cp_cols[4]
                    cp_bulk['_p_cl'] = cp_bulk[cp_p_col].astype(str).str.replace(r'[^0-9]', '', regex=True)

                    # 외부송장 대상자 이름+전화 목록
                    ext_keys = set()
                    for _, r in ext_df.iterrows():
                        ext_keys.add((r['name'], r['p1'][-4:] if len(r['p1']) >= 4 else r['p1']))

                    fill_cnt = 0
                    for idx, row in cp_bulk.iterrows():
                        r_name = str(row[cp_n_col]).strip()
                        r_phone = str(row['_p_cl']).strip()
                        if len(r_phone) < 4:
                            continue
                        if (r_name, r_phone[-4:]) not in ext_keys:
                            continue
                        match_inv = inv_df[
                            (inv_df[n_c] == r_name) &
                            (inv_df['p_cl'].str.endswith(r_phone[-4:]))
                        ]
                        if not match_inv.empty:
                            track_no = str(match_inv.iloc[0][s_c]).strip()
                            if track_no and track_no != 'nan':
                                cp_bulk.at[idx, cp_track_col] = track_no
                                fill_cnt += 1
                    cp_bulk.drop(columns=['_p_cl'], inplace=True)
                    # 외부송장 대상만 필터 (E열 송장번호가 채워진 행만)
                    cp_filled = cp_bulk[cp_bulk.iloc[:, 4].astype(str).str.strip().ne('')]
                    cp_ext_path = os.path.join(output_dir, f"쿠팡_외부_일괄배송_{ts}.xlsx")
                    cp_filled.to_excel(cp_ext_path, index=False)
                    result['files'].append(cp_ext_path)
                    self.log(f"✅ 쿠팡 외부 일괄배송 파일 생성: {fill_cnt}건 송장 입력")
                    result['success'] = True
                    self.log(f"쿠팡 외부송장 일괄배송 생성 완료! {fill_cnt}건")

                else:
                    result['error'] = f"[{mode}] 외부송장 일괄배송은 스마트스토어/쿠팡만 지원됩니다."
                    return result

        except Exception as e:
            self.log(f"❌ 오류: {e}")
            result['error'] = str(e)
        finally:
            gc.collect()

        return result

    # ─── Phase 1: DB 저장 헬퍼 ───

    @staticmethod
    def _safe_int(val, default=0):
        """안전한 int 변환 (None/NaN/문자열/float 모두 처리)"""
        if val is None:
            return default
        try:
            f = float(val)
            return default if f != f else int(f)  # NaN 체크: NaN != NaN
        except (ValueError, TypeError):
            return default

    @staticmethod
    def _safe_float(val, default=0):
        """안전한 float 변환 (None/NaN/문자열 모두 처리)"""
        if val is None:
            return default
        try:
            f = float(val)
            return default if f != f else f  # NaN 체크
        except (ValueError, TypeError):
            return default

    def _save_orders_to_db(self, db, channel, matched_rows, order_file, uploaded_by, total_rows,
                           collection_date=None):
        """매칭 완료된 주문을 DB에 저장 (import_runs + upsert_order_batch)."""
        # 1. import_runs 생성
        file_hash = self._compute_file_hash(order_file)
        filename = self._get_filename(order_file)
        import_run_id, err = db.create_import_run(
            channel=channel,
            filename=os.path.basename(filename) if filename else '',
            file_hash=file_hash,
            uploaded_by=uploaded_by or '',
            total_rows=total_rows,
        )
        if not import_run_id:
            self.log(f"import_runs 생성 실패: {err}")
            return {"inserted": 0, "updated": 0, "skipped": 0, "failed": len(matched_rows)}

        self.log(f"import_run #{import_run_id} 생성 OK")

        # 2. 주문 배열 구성 (transaction + shipping 분리)
        orders = []
        line_counter = {}  # (channel, order_no) → line_no 카운터

        for row in matched_rows:
            order_no = str(row.get('order_no', '')).strip()
            key = (channel, order_no)
            line_counter[key] = line_counter.get(key, 0) + 1
            line_no = line_counter[key]

            # 주문일 파싱
            order_date_str = row.get('order_date', '')
            order_date = self._parse_date(order_date_str)
            order_datetime = self._parse_datetime(order_date_str)

            # 수집일: 사용자 지정값 or 송장생성 당일
            from services.tz_utils import today_kst
            coll_date = collection_date or today_kst()

            transaction = {
                "channel": channel,
                "order_date": order_date,
                "order_datetime": order_datetime,  # 원본 주문일시 (시간 포함)
                "collection_date": coll_date,      # 주문수집일 (재고차감 기준)
                "order_no": order_no,
                "line_no": line_no,
                "original_option": str(row.get('_original_option', ''))[:500],
                "original_product": str(row.get('_original_product', ''))[:500],
                "raw_data": row.get('_raw_data', {}),
                "raw_hash": str(row.get('_raw_hash', '')),
                "parser_version": "1.0",
                "product_name": str(row.get('display_nm', '')),
                "barcode": str(row.get('barcode', '')),
                "line_code": self._safe_int(row.get('code'), 0),
                "sort_order": self._safe_int(row.get('sort'), 999),
                "qty": self._safe_int(row.get('qty'), 1),
                "unit_price": self._safe_float(row.get('_unit_price'), 0),
                "total_amount": self._safe_float(row.get('_total_amount'), 0),
                "discount_amount": self._safe_float(row.get('_discount'), 0),
                "settlement": self._safe_float(row.get('_settlement'), 0),
                "commission": self._safe_float(row.get('_commission'), 0),
                "shipping_fee": self._safe_float(row.get('_shipping_fee'), 0),
            }

            # 개인정보 분리 (카카오는 배송정보 없음)
            shipping = None
            if row.get('name'):
                shipping = {
                    "name": str(row.get('name', '')),
                    "phone": str(row.get('p1', '')),
                    "phone2": str(row.get('p2', '')),
                    "address": str(row.get('addr', '')),
                    "memo": str(row.get('msg', '')),
                }

            orders.append({"transaction": transaction, "shipping": shipping})

        # 3. DB upsert (RPC 또는 fallback)
        self.log(f"DB upsert 시작: {len(orders)}건...")
        db_result = db.upsert_order_batch(import_run_id, orders)

        # RPC 에러 로깅 (있을 경우)
        if db_result.get('rpc_error'):
            self.log(f"RPC fallback 사용: {db_result['rpc_error'][:150]}")

        # 개별 에러 로깅 (첫 3건만)
        if db_result.get('errors'):
            for e in db_result['errors'][:3]:
                self.log(f"  Row {e.get('row')}: {str(e.get('error', ''))[:120]}")
            if len(db_result['errors']) > 3:
                self.log(f"  ... 외 {len(db_result['errors']) - 3}건 에러")

        # ── 실시간 출고+매출 처리 (주문 수집 즉시 재고차감+매출기록) ──
        if db_result.get('inserted', 0) + db_result.get('updated', 0) > 0:
            try:
                from services.order_to_stock_service import process_realtime_outbound
                rt = process_realtime_outbound(db, import_run_id)
                db_result['realtime'] = rt
                oc = rt.get('outbound_count', 0)
                rc = rt.get('revenue_count', 0)
                rt_total = rt.get('revenue_total', 0)
                self.log(f"✅ 실시간 출고: {oc}건, 매출: {rc}건 ({rt_total:,}원)")
                if rt.get('errors'):
                    for re_err in rt['errors'][:3]:
                        self.log(f"  ⚠️ {re_err}")
            except Exception as rt_err:
                self.log(f"⚠️ 실시간 처리 실패 (주문관리에서 수동처리 필요): {rt_err}")
                db_result['realtime_error'] = str(rt_err)

        return db_result

    def _parse_date(self, date_str):
        """주문일 문자열 → YYYY-MM-DD 형식으로 파싱 (KST 기준)"""
        if not date_str or str(date_str).strip().lower() in ('nan', 'nat', 'none', ''):
            return today_kst()

        date_str = str(date_str).strip()

        # 다양한 날짜 형식 시도
        formats = [
            '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d',
            '%Y/%m/%d %H:%M:%S', '%Y/%m/%d',
            '%Y.%m.%d %H:%M:%S', '%Y.%m.%d',
            '%m/%d/%Y', '%d/%m/%Y',
        ]
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
            except ValueError:
                continue

        # 숫자만 추출 시도 (20260209 형식)
        digits = re.sub(r'[^\d]', '', date_str)
        if len(digits) >= 8:
            try:
                return datetime.strptime(digits[:8], '%Y%m%d').strftime('%Y-%m-%d')
            except ValueError:
                pass

        return today_kst()

    def _parse_datetime(self, date_str):
        """주문일 문자열 → YYYY-MM-DD HH:MM:SS 형식으로 파싱 (시간 보존).
        시간 정보 없으면 00:00:00 추가.
        """
        if not date_str or str(date_str).strip().lower() in ('nan', 'nat', 'none', ''):
            return None

        date_str = str(date_str).strip()

        # 시간 포함 형식 우선 시도
        datetime_formats = [
            ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S'),
            ('%Y-%m-%d %H:%M', '%Y-%m-%d %H:%M:00'),
            ('%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M:%S'),
            ('%Y.%m.%d %H:%M:%S', '%Y-%m-%d %H:%M:%S'),
            ('%Y-%m-%d', '%Y-%m-%d'),
            ('%Y/%m/%d', '%Y-%m-%d'),
            ('%Y.%m.%d', '%Y-%m-%d'),
        ]
        for in_fmt, out_fmt in datetime_formats:
            try:
                dt = datetime.strptime(date_str, in_fmt)
                return dt.strftime(out_fmt)
            except ValueError:
                continue

        return None
