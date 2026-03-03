import os, io, warnings, unicodedata
from datetime import datetime
import pandas as pd

warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

# 파일명 → 매출유형 분류 규칙 (순서 중요: 쿠팡보다 로켓이 먼저 매칭되어야 함)
CATEGORY_RULES = [
    ("로켓", "로켓"),
    ("오배송", "오배송"),
    ("클레임", "클레임"),
    ("기타출고", "기타출고"),
    ("무상매출", "무상매출"),
    ("자사몰", "자사몰매출"),
    ("용인", "N배송(용인)"),
    ("N배송", "N배송"),
    ("쿠팡", "쿠팡매출"),
]
ALL_CATEGORIES = ["일반매출", "자사몰매출", "쿠팡매출", "오배송", "클레임", "기타출고", "무상매출", "N배송(용인)", "N배송", "로켓"]

# 매출 대상 카테고리 → 가격표 컬럼 매핑 (오배송/클레임/기타출고/무상매출 = 매출 제외)
REVENUE_CATEGORIES = {
    "일반매출": "네이버판매가",
    "자사몰매출": "자사몰판매가",
    "쿠팡매출": "쿠팡판매가",
    "로켓": "로켓판매가",
    "N배송": "네이버판매가",
    "N배송(용인)": "네이버판매가",
}


def classify_file(filename):
    """파일명에서 매출유형 분류. 매칭 안되면 일반매출"""
    for keyword, category in CATEGORY_RULES:
        if keyword in filename:
            return category
    return "일반매출"


def _norm(text):
    """한글 텍스트 NFC 정규화 (서버/클라이언트 인코딩 차이 방지)"""
    return unicodedata.normalize('NFC', str(text).strip())


class Aggregator:
    def __init__(self):
        self.logs = []
        self.bom_map = {}
        self.opt_map = {}
        self.price_map = {}

    def log(self, msg):
        t = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
        self.logs.append(t)

    def _get_filename(self, file_input):
        """파일 이름 추출 (경로 또는 file-like 객체)"""
        if isinstance(file_input, (str, os.PathLike)):
            return str(file_input)
        elif hasattr(file_input, 'name'):
            return file_input.name
        return ""

    def _read_excel(self, file_input, sheet_name=0, header=0):
        """file-like 또는 경로에서 Excel 읽기"""
        if isinstance(file_input, (str, os.PathLike)):
            return pd.read_excel(str(file_input), sheet_name=sheet_name, header=header)
        else:
            # file-like object
            if hasattr(file_input, 'seek'):
                file_input.seek(0)
            data = file_input.read()
            buf = io.BytesIO(data) if isinstance(data, bytes) else io.BytesIO(data.encode('utf-8'))
            return pd.read_excel(buf, sheet_name=sheet_name, header=header)

    def _read_csv_or_excel(self, file_input, header=0):
        """CSV 또는 Excel을 읽어 DataFrame 반환"""
        filename = self._get_filename(file_input)

        if isinstance(file_input, (str, os.PathLike)):
            path = str(file_input)
            if path.lower().endswith('.csv'):
                try:
                    return pd.read_csv(path, encoding='utf-8-sig').fillna('')
                except:
                    return pd.read_csv(path, encoding='cp949').fillna('')
            else:
                return pd.read_excel(path, header=header).fillna('')
        else:
            # file-like object
            if hasattr(file_input, 'seek'):
                file_input.seek(0)
            data = file_input.read()
            buf = io.BytesIO(data) if isinstance(data, bytes) else io.BytesIO(data.encode('utf-8'))

            if filename.lower().endswith('.csv'):
                try:
                    return pd.read_csv(buf, encoding='utf-8-sig').fillna('')
                except:
                    buf.seek(0)
                    return pd.read_csv(buf, encoding='cp949').fillna('')
            else:
                return pd.read_excel(buf, header=header).fillna('')

    def load_bom(self, file_input):
        try:
            # ExcelFile로 열어서 시트명을 NFC 정규화 매칭
            if isinstance(file_input, (str, os.PathLike)):
                xls = pd.ExcelFile(str(file_input))
            else:
                if hasattr(file_input, 'seek'):
                    file_input.seek(0)
                data = file_input.read()
                buf = io.BytesIO(data) if isinstance(data, bytes) else io.BytesIO(data.encode('utf-8'))
                xls = pd.ExcelFile(buf)

            # 시트명 NFC 정규화 매핑 (서버 인코딩 차이 방지)
            sheet_map = {_norm(sn): sn for sn in xls.sheet_names}
            self.log(f"📋 BOM 시트 목록: {xls.sheet_names}")

            all_ch_sheet = sheet_map.get(_norm("모든채널"))
            cp_sheet = sheet_map.get(_norm("쿠팡전용"))

            if not all_ch_sheet:
                self.log(f"❌ '모든채널' 시트를 찾을 수 없습니다.")
                return False

            all_ch = pd.read_excel(xls, sheet_name=all_ch_sheet).fillna("")
            if cp_sheet:
                cp_only = pd.read_excel(xls, sheet_name=cp_sheet).fillna("")
            else:
                cp_only = pd.DataFrame()
                self.log("⚠️ '쿠팡전용' 시트 없음 → 모든채널만 사용")

            def parse(df):
                if df.empty:
                    return {}
                # 컬럼명도 NFC 정규화
                df.columns = [_norm(c) for c in df.columns]
                col_set = _norm("세트명")
                col_comp = _norm("구성품")
                if col_set not in df.columns or col_comp not in df.columns:
                    self.log(f"⚠️ BOM 컬럼 매칭 실패: {list(df.columns)} (세트명/구성품 필요)")
                    return {}
                m = {}
                for _, r in df.iterrows():
                    s_nm = _norm(r[col_set])
                    comps = str(r[col_comp]).strip()
                    if not s_nm or not comps:
                        continue
                    m[s_nm] = [
                        (_norm(c.rsplit('x', 1)[0]), int(c.rsplit('x', 1)[1]))
                        for c in comps.split(',') if 'x' in c
                    ]
                return m

            self.bom_map = {"모든채널": parse(all_ch), "쿠팡전용": parse(cp_only)}

            # 디버그: 각 BOM에 로드된 세트명 로그
            for bk, bm in self.bom_map.items():
                if bm:
                    names = list(bm.keys())
                    self.log(f"  {bk} 세트: {', '.join(names[:8])}{'...' if len(names) > 8 else ''}")

            return True
        except Exception as e:
            self.log(f"❌ BOM 로드 에러: {e}")
            return False

    def load_option_from_db(self, db):
        """옵션리스트를 DB(option_master)에서 로드"""
        try:
            rows = db.query_option_master()
            if not rows:
                self.log("ℹ️ 옵션마스터(DB) 데이터 없음 → 입력순서 유지 모드")
                self.opt_map = {}
                return True
            self.opt_map = {}
            for r in rows:
                nm = _norm(r.get('product_name', ''))
                if not nm:
                    continue
                if nm not in self.opt_map:
                    self.opt_map[nm] = {
                        '출력순서': int(r.get('sort_order', 999) or 999),
                        '라인코드': str(r.get('line_code', '0') or '0').strip()
                    }
            self.log(f"✅ 옵션리스트(DB) 로드: {len(self.opt_map)}종 품목 매핑")
            return True
        except Exception as e:
            self.log(f"❌ 옵션리스트(DB) 로드 에러: {e}")
            self.opt_map = {}
            return True  # DB 실패해도 진행 허용

    def load_option_list(self, file_input):
        """옵션리스트 파일 로드: A=원문명, B=품목명, C=라인코드, E=출력순서"""
        try:
            df = self._read_csv_or_excel(file_input)

            if len(df.columns) < 5:
                self.log(f"⚠️ 옵션리스트 컬럼 부족 (최소 E열까지 필요), 컬럼수: {len(df.columns)}")
                return False

            opt_raw = df.iloc[:, [0, 1, 2, 4]].copy()
            opt_raw.columns = ['원문명', '품목명', '라인코드', '출력순서']
            opt_raw['출력순서'] = pd.to_numeric(opt_raw['출력순서'], errors='coerce').fillna(999).astype(int)

            self.opt_map = {}
            for _, row in opt_raw.iterrows():
                nm = _norm(row['품목명'])
                if not nm:
                    continue
                if nm not in self.opt_map:
                    self.opt_map[nm] = {
                        '출력순서': int(row['출력순서']),
                        '라인코드': str(row['라인코드']).strip()
                    }

            self.log(f"✅ 옵션리스트 로드: {len(self.opt_map)}종 품목 매핑")
            return True
        except Exception as e:
            self.log(f"❌ 옵션리스트 로드 에러: {e}")
            return False

    def decompose(self, name, qty, current_bom, fallback_bom=None):
        """세트 분해 (NFC 정규화 + fallback BOM 지원)"""
        n = _norm(name)
        if n in current_bom:
            bom = current_bom
        elif fallback_bom and n in fallback_bom:
            bom = fallback_bom
        else:
            return {name: qty}
        res = {}
        for c_nm, c_qty in bom[n]:
            sub = self.decompose(c_nm, qty * c_qty, current_bom, fallback_bom)
            for k, v in sub.items():
                res[k] = res.get(k, 0) + v
        return res

    def _get_warehouse(self, name, fallback="넥스원"):
        """품목명 → 출고지 결정 (opt_map 라인코드 기반)"""
        n = _norm(name)
        if self.opt_map and n in self.opt_map:
            lc = str(self.opt_map[n].get('라인코드', '0')).strip()
            return "해서" if lc == '5' else "넥스원"
        return fallback

    def load_price_from_db(self, db):
        """가격표를 DB(master_prices)에서 로드 → self.price_map"""
        try:
            self.price_map = db.query_price_table()
            if self.price_map:
                self.log(f"💰 가격표(DB) 로드: {len(self.price_map)}종")
            else:
                self.log("ℹ️ 가격표(DB) 데이터 없음 → 매출 계산 생략")
        except Exception as e:
            self.log(f"ℹ️ 가격표(DB) 로드 실패: {e}")
            self.price_map = {}

    def load_price_table(self, file_input):
        """[뼈대] 가격표(Sheet2) 로드 → self.price_map"""
        self.price_map = {}
        try:
            if isinstance(file_input, (str, os.PathLike)):
                xls = pd.ExcelFile(str(file_input))
            else:
                if hasattr(file_input, 'seek'):
                    file_input.seek(0)
                data = file_input.read()
                buf = io.BytesIO(data) if isinstance(data, bytes) else io.BytesIO(data.encode('utf-8'))
                xls = pd.ExcelFile(buf)
            # 시트명 NFC 정규화 매칭
            sheet_map = {_norm(sn): sn for sn in xls.sheet_names}
            price_sheet = sheet_map.get(_norm("가격표"))
            if not price_sheet:
                self.log("ℹ️ 가격표 시트 없음 → 매출 계산 생략")
                return
            df = pd.read_excel(xls, sheet_name=price_sheet).fillna(0)

            # 컬럼명 NFC 정규화
            df.columns = [_norm(c) for c in df.columns]
            for _, row in df.iterrows():
                nm = _norm(row.get(_norm('품목명'), ''))
                if not nm:
                    continue
                naver = float(row.get(_norm('네이버판매가'), 0))
                self.price_map[nm] = {
                    'SKU': str(row.get('SKU', '')),
                    '네이버판매가': naver,
                    '자사몰판매가': float(row.get(_norm('자사몰판매가'), 0)) or naver,
                    '쿠팡판매가': float(row.get(_norm('쿠팡판매가'), 0)),
                    '로켓판매가': float(row.get(_norm('로켓판매가'), 0))
                }
            self.log(f"💰 가격표 로드: {len(self.price_map)}종")
        except Exception:
            self.log("ℹ️ 가격표(Sheet2) 없음 → 매출 계산 생략")

    def run(self, order_files, option_file, bom_file, output_dir,
            original_names=None, db=None):
        """
        order_files: list of file-like objects or paths (집계표들)
        option_file: file-like object or path (옵션리스트, optional)
        bom_file: file-like object or path (세트옵션 BOM)
        output_dir: directory for output files
        original_names: dict {saved_path: original_korean_filename} (optional)

        returns: {
            'success': bool,
            'files': [list of output file paths],
            'logs': [list of log messages],
            'error': str or None,
            'summary': {total_items, total_qty, categories}
        }
        """
        if original_names is None:
            original_names = {}
        self.logs = []
        result = {
            'success': False,
            'files': [],
            'logs': self.logs,
            'error': None,
            'summary': {'total_items': 0, 'total_qty': 0, 'categories': {}}
        }

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        try:
            self.log("🚀 v3.1 통합 집계 (세트분해 + 매출유형 + 출고지분리)")
            if not self.load_bom(bom_file):
                result['error'] = "BOM 파일 로드 실패"
                return result

            # 옵션리스트 로드 (선택사항: 파일 → DB fallback)
            if option_file:
                if not self.load_option_list(option_file):
                    result['error'] = "옵션리스트 로드 실패"
                    return result
                # 가격표 뼈대 로드 시도 (xlsx일 때만)
                opt_filename = self._get_filename(option_file)
                if opt_filename.lower().endswith('.xlsx'):
                    self.load_price_table(option_file)
            elif db is not None:
                self.load_option_from_db(db)
            else:
                self.opt_map = {}
                self.log("ℹ️ 옵션리스트 미선택 → 입력순서 유지 모드")

            # 파일에서 가격표 로드 못 했으면 DB fallback
            if not self.price_map and db is not None:
                self.load_price_from_db(db)

            self.log(f"✅ BOM 로드: 모든채널 {len(self.bom_map['모든채널'])}종, 쿠팡전용 {len(self.bom_map['쿠팡전용'])}종")

            # {(품목명, warehouse): {카테고리: 수량}} 구조로 집계
            data = {}
            ordered = []  # (품목명, warehouse) 등장 순서
            wh_warn = []  # warehouse 미확정 품목

            # 매출용: 세트 미분해 원본 이름 보존
            rev_data = {}
            rev_ordered = []

            for file_input in order_files:
                saved_path = self._get_filename(file_input)
                # 원본 한글 파일명으로 분류 (secure_filename이 한글 제거하므로)
                f_nm = os.path.basename(original_names.get(saved_path, saved_path))
                cat = classify_file(f_nm)
                bom_key = "쿠팡전용" if cat in ("쿠팡매출", "로켓") else "모든채널"
                self.log(f"📂 {f_nm} → 유형: {cat} / BOM: {bom_key}")

                # 각 집계표 읽기 (Excel / CSV 모두 지원)
                try:
                    if isinstance(file_input, (str, os.PathLike)):
                        path_str = str(file_input)
                        if path_str.lower().endswith('.csv'):
                            try:
                                df = pd.read_csv(path_str, encoding='utf-8-sig').fillna('')
                            except Exception:
                                df = pd.read_csv(path_str, encoding='cp949').fillna('')
                        else:
                            df = pd.read_excel(path_str).fillna('')
                    else:
                        if hasattr(file_input, 'seek'):
                            file_input.seek(0)
                        file_data = file_input.read()
                        buf = io.BytesIO(file_data) if isinstance(file_data, bytes) else io.BytesIO(file_data.encode('utf-8'))
                        df = pd.read_excel(buf).fillna('')
                except Exception as read_err:
                    self.log(f"⚠️ {f_nm}: 파일 읽기 실패 — {read_err}")
                    continue

                cols = [str(c).strip() for c in df.columns]
                cols_nfc = [_norm(c) for c in cols]  # NFC 정규화 매칭용

                # 유연한 컬럼 탐색 (다양한 엑셀 양식 대응)
                PROD_NAMES = ['품목명', '상품명', '제품명', '옵션명', '품명', '상품', '제품']
                QTY_NAMES = ['합산', 'qty', '수량', '합계', '출고수량', '주문수량', '구매수량',
                             '출고량', '판매수량', 'Qty', 'QTY', '총수량']
                WH_NAMES = ['warehouse', '출고지', '창고', '창고위치', '출고창고']

                # NFC 정규화 후 매칭 (한글 인코딩 차이 방지)
                prod_col = next((cols[i] for i, c in enumerate(cols_nfc) if c in PROD_NAMES), None)
                qty_col = next((cols[i] for i, c in enumerate(cols_nfc) if c in QTY_NAMES), None)
                wh_col = next((cols[i] for i, c in enumerate(cols_nfc) if c in WH_NAMES), None)

                # 부분매칭 시도 (정확 매칭 실패 시)
                if not prod_col:
                    prod_col = next((cols[i] for i, c in enumerate(cols_nfc) if any(k in c for k in ['품목', '상품', '제품', '품명'])), None)
                if not qty_col:
                    qty_col = next((cols[i] for i, c in enumerate(cols_nfc) if any(k in c for k in ['수량', '합산', '합계', 'qty'])), None)

                if not prod_col or not qty_col:
                    self.log(f"⚠️ {f_nm}: 품목명/수량 컬럼을 찾을 수 없어 스킵 (컬럼: {cols})")
                    continue

                self.log(f"  📋 컬럼 매핑: 품목={prod_col}, 수량={qty_col}{f', 출고지={wh_col}' if wh_col else ''}")

                set_count = 0
                for _, row in df.iterrows():
                    name = _norm(row[prod_col])
                    qty = int(v) if not pd.isna(v := pd.to_numeric(row[qty_col], errors='coerce')) else 0
                    if not name or qty == 0:
                        continue

                    # 행의 warehouse 결정 (우선순위: 컬럼값 > opt_map > 기본값)
                    if wh_col and str(row[wh_col]).strip():
                        row_wh = str(row[wh_col]).strip()
                    else:
                        row_wh = self._get_warehouse(name)
                        if not self.opt_map or name not in self.opt_map:
                            if name not in wh_warn:
                                wh_warn.append(name)

                    # === 매출용: 원본 이름 그대로 (세트 미분해) ===
                    rev_wh = row_wh
                    rev_key = (name, rev_wh)
                    if rev_key not in rev_data:
                        rev_data[rev_key] = {c: 0 for c in ALL_CATEGORIES}
                        rev_ordered.append(rev_key)
                    rev_data[rev_key][cat] += qty

                    # === 출고용: 세트 분해 (N배송은 세트 그대로) ===
                    if cat in ("N배송(용인)", "N배송"):
                        # N배송은 세트 상태로 출고 → 분해하지 않음
                        key = (name, row_wh)
                        if key not in data:
                            data[key] = {c: 0 for c in ALL_CATEGORIES}
                            ordered.append(key)
                        data[key][cat] += qty
                    else:
                        current_bom = self.bom_map[bom_key]
                        fallback_bom = self.bom_map["모든채널"] if bom_key == "쿠팡전용" else None
                        n_name = _norm(name)

                        if n_name in current_bom:
                            set_count += 1
                            self.log(f"  🔄 세트 분해: {name} x{qty}")
                        elif fallback_bom and n_name in fallback_bom:
                            set_count += 1
                            self.log(f"  🔄 세트 분해 (모든채널 fallback): {name} x{qty}")

                        decomp = self.decompose(name, qty, current_bom, fallback_bom)

                        for k, v in decomp.items():
                            # 분해된 품목의 warehouse 개별 결정
                            # 집계표에 warehouse 명시 → 그 값 우선, 없으면 opt_map fallback
                            k_wh = row_wh if wh_col and str(row[wh_col]).strip() else self._get_warehouse(k, fallback=row_wh)

                            key = (k, k_wh)
                            if key not in data:
                                data[key] = {c: 0 for c in ALL_CATEGORIES}
                                ordered.append(key)
                            data[key][cat] += v

                self.log(f"  → {f_nm}: {len(df)}행 처리, 세트 {set_count}건 분해")

            if not data:
                self.log("❌ 집계할 유효 데이터가 없습니다.")
                result['error'] = "집계할 유효 데이터가 없습니다."
                return result

            # warehouse 미확정 경고
            if wh_warn:
                self.log(f"⚠️ 출고지 미확정 {len(wh_warn)}건 (기본 '넥스원' 적용): {', '.join(wh_warn[:5])}{'...' if len(wh_warn) > 5 else ''}")

            # BOM 세트가 미분해 상태로 남아있는지 검사
            all_bom_sets = set()
            for bm in self.bom_map.values():
                all_bom_sets.update(bm.keys())
            undecomposed = [nm for (nm, wh) in ordered if _norm(nm) in all_bom_sets]
            if undecomposed:
                unique_undec = list(dict.fromkeys(undecomposed))
                self.log(f"⚠️ 세트 미분해 경고: {', '.join(unique_undec)}")
                self.log(f"   → BOM 매칭 실패 가능성. BOM 파일 세트명 확인 필요!")

            # 옵션리스트 있으면 출력순서 정렬
            if self.opt_map:
                unmatched = [nm for nm, wh in ordered if nm not in self.opt_map]
                unmatched_unique = list(dict.fromkeys(unmatched))
                if unmatched_unique:
                    self.log(f"⚠️ 옵션리스트에 없는 품목 {len(unmatched_unique)}건: {', '.join(unmatched_unique[:5])}{'...' if len(unmatched_unique) > 5 else ''}")
                ordered.sort(key=lambda item: self.opt_map.get(item[0], {}).get('출력순서', 999))
                self.log(f"📋 옵션리스트 기준 정렬 적용")

            # 최종 DataFrame 생성
            rows = []
            for (nm, wh) in ordered:
                cats = data[(nm, wh)]
                total = sum(cats.values())
                if total == 0:
                    continue

                row = {
                    '출력순서': self.opt_map.get(nm, {}).get('출력순서', '') if self.opt_map else '',
                    '품목명': nm,
                    'warehouse': wh
                }
                for c in ALL_CATEGORIES:
                    row[c] = cats[c]
                row['합산'] = total
                rows.append(row)

            # 컬럼 구성
            if self.opt_map:
                col_order = ['출력순서', '품목명', 'warehouse'] + ALL_CATEGORIES + ['합산']
            else:
                col_order = ['품목명', 'warehouse'] + ALL_CATEGORIES + ['합산']

            final_df = pd.DataFrame(rows, columns=col_order)

            # 값이 0인 컬럼(카테고리) 전체 삭제 (해당 유형 파일이 없었던 경우)
            for c in ALL_CATEGORIES:
                if c in final_df.columns and final_df[c].sum() == 0:
                    final_df.drop(columns=[c], inplace=True)

            ts = datetime.now().strftime("%Y%m%d_%H%M%S")

            # 1) 전체 통합집계표 (기존 호환)
            out_path = os.path.join(output_dir, f"통합집계표_{ts}.xlsx")
            final_df.to_excel(out_path, index=False)
            result['files'].append(out_path)

            # 2) 출고지별 분리 (넥스원, 해서, CJ용인)
            wh_files = []
            for wh_name in ["넥스원", "해서", "CJ용인"]:
                wh_df = final_df[final_df['warehouse'] == wh_name].drop(columns=['warehouse']).copy()
                if wh_df.empty:
                    continue
                for c in ALL_CATEGORIES:
                    if c in wh_df.columns and wh_df[c].sum() == 0:
                        wh_df.drop(columns=[c], inplace=True)
                wh_path = os.path.join(output_dir, f"통합출고_{wh_name}_{ts}.xlsx")
                wh_df.to_excel(wh_path, index=False)
                result['files'].append(wh_path)
                wh_files.append(wh_name)
                self.log(f"📂 {wh_name} 출고: {len(wh_df)}종, {int(wh_df['합산'].sum())}개 → {os.path.basename(wh_path)}")

            # 4) 일일매출표 (세트 미분해, 단가 적용)
            rev_path = None
            if hasattr(self, 'price_map') and self.price_map and rev_data:
                if self.opt_map:
                    rev_ordered.sort(key=lambda item: self.opt_map.get(item[0], {}).get('출력순서', 999))

                rev_rows = []
                for (nm, wh) in rev_ordered:
                    cats = rev_data[(nm, wh)]
                    row = {'품목명': nm}
                    total_rev = 0
                    has_revenue_qty = False

                    today_str = datetime.now().strftime('%Y-%m-%d')
                    for c, price_col in REVENUE_CATEGORIES.items():
                        q = cats.get(c, 0)
                        if q == 0:
                            continue
                        has_revenue_qty = True
                        # 행사/쿠폰 우선 적용 (DB 연결 시)
                        if db is not None:
                            unit_price, _src = db.resolve_unit_price(
                                _norm(nm), c, today_str, self.price_map)
                        else:
                            unit_price = self.price_map.get(_norm(nm), {}).get(price_col, 0)
                        rev = q * unit_price
                        row[f'{c}_수량'] = q
                        row[f'{c}_단가'] = int(unit_price)
                        row[f'{c}_매출'] = int(rev)
                        total_rev += rev

                    if not has_revenue_qty:
                        continue
                    row['총매출'] = int(total_rev)
                    rev_rows.append(row)

                if rev_rows:
                    rev_col = ['품목명']
                    for c in REVENUE_CATEGORIES:
                        if any(r.get(f'{c}_수량', 0) for r in rev_rows):
                            rev_col += [f'{c}_수량', f'{c}_단가', f'{c}_매출']
                    rev_col.append('총매출')

                    rev_df = pd.DataFrame(rev_rows).fillna(0)
                    existing = [c for c in rev_col if c in rev_df.columns]
                    rev_df = rev_df[existing]
                    for col in rev_df.columns:
                        if col != '품목명':
                            rev_df[col] = pd.to_numeric(rev_df[col], errors='coerce').fillna(0).astype(int)

                    # 합산 행 추가 (수량/매출만 합산, 단가는 빈값)
                    sum_row = {'품목명': '합계'}
                    for col in rev_df.columns:
                        if col == '품목명':
                            continue
                        elif '_단가' in col:
                            sum_row[col] = ''
                        else:
                            sum_row[col] = int(rev_df[col].sum())
                    rev_df = pd.concat([rev_df, pd.DataFrame([sum_row])], ignore_index=True)

                    rev_path = os.path.join(output_dir, f"일일매출_{ts}.xlsx")
                    rev_df.to_excel(rev_path, index=False)
                    result['files'].append(rev_path)
                    self.log(f"일일매출: {len(rev_df) - 1}종, 총매출 {sum_row.get('총매출', 0):,}원")

            # 로그 출력 & summary 구성
            self.log(f"총 {len(final_df)}종 품목")
            categories_summary = {}
            for c in ALL_CATEGORIES:
                if c in final_df.columns:
                    cat_total = int(final_df[c].sum())
                    self.log(f"  {c}: {cat_total}개")
                    categories_summary[c] = cat_total
            total_qty = int(final_df['합산'].sum())
            self.log(f"  합산: {total_qty}개")

            result['summary'] = {
                'total_items': len(final_df),
                'total_qty': total_qty,
                'categories': categories_summary
            }

            done_msg = f"집계 완료! ({len(final_df)}종)\n전체: {os.path.basename(out_path)}"
            if wh_files:
                done_msg += f"\n분리: {', '.join(wh_files)}"
            if rev_path:
                done_msg += f"\n매출: {os.path.basename(rev_path)}"

            self.log(done_msg)
            result['success'] = True

        except Exception as e:
            self.log(f"❌ 시스템 오류: {e}")
            result['error'] = str(e)

        return result
