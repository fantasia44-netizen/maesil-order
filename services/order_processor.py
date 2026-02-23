import os, io, re, warnings, gc, unicodedata
from datetime import datetime
import pandas as pd
import msoffcrypto

warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')


class OrderProcessor:
    def __init__(self):
        self.logs = []

    def log(self, msg):
        t = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
        print(t)
        self.logs.append(t)

    def get_safe_val(self, row, idx):
        """[135 에러 방지] 인덱스 초과 시 빈 값 반환"""
        try:
            if idx < len(row):
                return str(row.iloc[idx]).strip()
            return ""
        except:
            return ""

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

    def run(self, mode, order_file, option_file, invoice_file, target_type, output_dir):
        """
        mode: '스마트스토어'|'자사몰'|'쿠팡'|'옥션/G마켓'|'오아시스'
        order_file: file-like object or path
        option_file: file-like object or path
        invoice_file: file-like object or path (optional, for 리얼패킹/외부일괄)
        target_type: '송장'|'리얼패킹'|'외부일괄'
        output_dir: directory for output files

        returns: {
            'success': bool,
            'files': [list of output file paths],
            'logs': [list of log messages],
            'error': str or None
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
            self.log(f"🚀 v16.10 [{mode}] {target_type} 가동")

            # [1] 옵션지 분석
            opt_df = self.load_generic(option_file)
            if opt_df is None or opt_df.empty:
                result['error'] = "옵션리스트 오류"
                return result
            opt_raw = opt_df.iloc[:, [0, 1, 2, 4, 5]].copy()
            opt_raw.columns = ['원문명', '품목명', '라인코드', '출력순서', '바코드']
            opt_raw['출력순서'] = pd.to_numeric(opt_raw['출력순서'], errors='coerce').fillna(999)

            # [검증] 같은 출력순서(E열)에 품목명(B열)이 다른 경우 체크
            line_groups = opt_raw.groupby('출력순서')['품목명'].apply(lambda x: list(x.unique())).to_dict()
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

            opt_list = opt_raw.to_dict('records')
            for o in opt_list:
                o['Key'] = str(o['원문명']).replace(" ", "").upper()

            # [2] 플랫폼별 독립 엔진 (v16.10 필터링 보강)
            res = []
            df = None
            if mode == "스마트스토어":
                df = self.load_smart_store_memory(order_file)
                cols = [unicodedata.normalize('NFC', str(c)).replace(" ", "") for c in df.columns]

                def get_c(names, d):
                    for n in names:
                        n_n = unicodedata.normalize('NFC', n)
                        for i, c in enumerate(cols):
                            if n_n in c:
                                return i
                    return d

                m = {
                    'n': get_c(['수취인명', '수하인명'], 9),
                    'a': get_c(['배송지', '주소'], 33),
                    'a2': get_c(['상세주소'], 34),
                    'p1': get_c(['수취인연락처1'], 31),
                    'msg': get_c(['배송메세지'], 37),
                    'opt': get_c(['옵션정보'], 14),
                    'prod': get_c(['상품명'], 12),
                    'qty': get_c(['수량'], 16),
                    'st': get_c(['주문상태', '배송상태'], 10),
                    'date': get_c(['주문일시', '결제일'], 1),
                    'no': get_c(['상품주문번호', '주문번호'], 0)
                }

                # 1차 필터링 (취소/반품 제외)
                target = df[~df.iloc[:, m['st']].astype(str).str.contains('취소|반품', na=False)].copy()

                # [핵심] AW열(Index 48) 'N배송' 필터링 로직 추가
                if target.shape[1] > 48:
                    n_ship_col = target.iloc[:, 48].astype(str)
                    target = target[~n_ship_col.str.contains('N배송', na=False)].copy()
                    self.log(f"🚫 N배송 상품 자동 제외 완료 (잔여: {len(target)}건)")

            elif mode == "자사몰":
                df = self.load_generic(order_file, header=0)
                m = {'n': 13, 'a': 15, 'p1': 18, 'msg': 19, 'opt': 6, 'prod': 4, 'qty': 12, 'p2': 17, 'date': 3, 'no': 2}
                target = df.copy()

            elif mode == "쿠팡":
                df = self.load_generic(order_file, header=0)
                m = {'n': 26, 'a': 29, 'p1': 27, 'msg': 30, 'opt': 11, 'prod': 10, 'qty': 22, 'p2': 25, 'date': 9, 'no': 2}
                target = df.copy()

            elif mode == "옥션/G마켓":
                df = self.load_generic(order_file, header=0)
                m = {'n': 10, 'a': 31, 'p1': 27, 'msg': 32, 'opt': 17, 'prod': 4, 'qty': 16, 'p2': 28, 'date': 9, 'no': 1}
                target = df.copy()

            elif mode == "오아시스":
                df = self.load_generic(order_file, header=0)
                m = {'n': 31, 'a': 36, 'p1': 32, 'msg': 37, 'opt': 10, 'prod': 10, 'qty': 14, 'p2': 32, 'date': 1, 'no': 2}
                target = df.copy()

            # [3] 매칭 프로세스
            unmatched = []  # 매칭 실패 항목 수집
            for i, r in target.iterrows():
                try:
                    if mode == "쿠팡":
                        v_r, v_e = self.get_safe_val(r, m['opt']), self.get_safe_val(r, m['prod'])
                        k = v_e if "단일상품" in v_r or v_r == '' else v_e + v_r
                    elif mode == "옥션/G마켓":
                        v_r = self.get_safe_val(r, m['opt'])
                        k = v_r.split('/')[0].strip() if v_r else self.get_safe_val(r, m['prod'])
                    else:
                        k = self.get_safe_val(r, m['opt']) if self.get_safe_val(r, m['opt']) != '' else self.get_safe_val(r, m['prod'])

                    c_k = k.replace(" ", "").upper()
                    match = next((o for o in opt_list if c_k == o['Key'] or o['Key'] in c_k), None)

                    if match:
                        # 스마트스토어: AH(앞주소) + AI(상세주소) 합산
                        addr_front = self.get_safe_val(r, m['a'])
                        addr_detail = self.get_safe_val(r, m.get('a2', m['a'])) if 'a2' in m else ''
                        full_addr = f"{addr_front} {addr_detail}".strip() if addr_detail else addr_front
                        clean_addr = re.sub(r'\s+', '', full_addr)
                        res.append({
                            'name': self.get_safe_val(r, m['n']),
                            'addr': full_addr,
                            'clean_addr': clean_addr,
                            'p1': re.sub(r'[^0-9]', '', self.get_safe_val(r, m['p1'])),
                            'qty': int(float(self.get_safe_val(r, m['qty']) or 1)),
                            'display_nm': match['품목명'],
                            'barcode': match['바코드'],
                            'code': int(pd.to_numeric(match['라인코드'], errors='coerce') or 0),
                            'msg': self.get_safe_val(r, m['msg']),
                            'p2': self.get_safe_val(r, m.get('p2', m['p1'])),
                            'sort': match['출력순서'],
                            'order_date': self.get_safe_val(r, m['date']),
                            'order_no': self.get_safe_val(r, m['no'])
                        })
                    else:
                        # 매칭 실패 → 실제 매칭 키(k) 수집 (옵션리스트 A열에 넣을 값)
                        if k and k not in unmatched:
                            unmatched.append(k)
                except:
                    continue

            # 미매칭 항목 → 처리 중단
            if unmatched:
                self.log(f"⚠️ 옵션 미등록 {len(unmatched)}건 발견 → 처리 중단")
                msg = f"옵션리스트에 등록되지 않은 상품 {len(unmatched)}건:\n\n"
                for nm in unmatched[:20]:
                    msg += f"  • {nm[:80]}\n"
                if len(unmatched) > 20:
                    msg += f"  ... 외 {len(unmatched) - 20}건\n"
                msg += f"\n옵션리스트 A열(원문명)에 위 상품명을 등록 후 다시 실행하세요."
                result['error'] = msg
                return result

            if not res:
                self.log("❌ 매칭 데이터 0건")
                result['error'] = "매칭 데이터 0건"
                return result

            res_df = pd.DataFrame(res)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_nm = mode.replace("/", "_")

            if target_type == "송장":
                # [집계표] 출력순서(E열) 기준 통계 - 같은 라인은 하나로 합산 + 출고지 포함
                sums = res_df.groupby(['sort', 'display_nm'])['qty'].sum().reset_index()
                sums_by_line = sums.groupby('sort').agg({'display_nm': 'first', 'qty': 'sum'}).reset_index()
                master_list = opt_raw[['출력순서', '품목명', '라인코드']].drop_duplicates('출력순서')
                qty_rep = pd.merge(master_list, sums_by_line, left_on='출력순서', right_on='sort', how='left')
                qty_rep['qty'] = qty_rep['qty'].fillna(0).astype(int)
                qty_rep['warehouse'] = qty_rep['라인코드'].apply(
                    lambda c: "해서" if int(pd.to_numeric(c, errors='coerce') or 0) == 5 else "넥스원"
                )
                summary_path = os.path.join(output_dir, f"{safe_nm}_집계표_{ts}.xlsx")
                qty_rep.sort_values('출력순서')[['품목명', 'qty', 'warehouse']].to_excel(summary_path, index=False)
                result['files'].append(summary_path)

                # [송장 생성 - 주소지 합포장]
                rosen = res_df[res_df['code'] != 5]
                ext = res_df[res_df['code'] == 5]
                s_nms = {0: "단없음", 1: "1단", 2: "2단", 3: "3단", 4: "기타", 5: "외부"}
                for d, nt in [(rosen, ""), (ext, "_외부")]:
                    if not d.empty:
                        inv = []
                        for a_c, gp in d.groupby(['clean_addr'], sort=False):
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
                    result['error'] = "3번 송장결과를 선택하세요."
                    return result
                inv_df = self.load_generic(invoice_file)
                inv_df.columns = [str(c).replace(" ", "") for c in inv_df.columns]
                inv_cols = list(inv_df.columns)
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
                    result['error'] = "매칭 데이터 없음"
                    return result

                rp_path = os.path.join(output_dir, f"리얼패킹_{safe_nm}_{ts}.xlsx")
                pd.DataFrame(rp_f, columns=[
                    "주문일자", "주문번호", "배송방법", "택배사", "송장번호",
                    "이름", "연락처", "제품명", "수량", "바코드"
                ]).to_excel(rp_path, index=False)
                result['files'].append(rp_path)

                if mode == "스마트스토어" and ss_bulk:
                    ss_path = os.path.join(output_dir, f"스마트스토어_일괄배송입력_{ts}.xlsx")
                    pd.DataFrame(ss_bulk, columns=[
                        "상품주문번호", "배송방법", "택배사", "송장번호", "수취인", "전화번호"
                    ]).to_excel(ss_path, index=False)
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
                    ss_ext_path = os.path.join(output_dir, f"스마트스토어_외부_일괄배송_{ts}.xlsx")
                    pd.DataFrame(ss_ext, columns=[
                        "상품주문번호", "배송방법", "택배사", "송장번호", "수취인", "전화번호"
                    ]).to_excel(ss_ext_path, index=False)
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
