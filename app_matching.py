import streamlit as st
import pandas as pd
from rapidfuzz import fuzz
from unidecode import unidecode
import io
import time
import concurrent.futures
import multiprocessing
import re
from datetime import datetime

# ==========================================
# 0. DESIGN SYSTEM & CONFIG
# ==========================================
st.set_page_config(page_title="Opella Matcher", page_icon="⚡", layout="wide")

# Theme Colors
PRIMARY_COLOR = "#042B0B"   
BG_COLOR = "#F7EFE6"        
ACCENT_COLOR = "#CED5CE"    
HIGHLIGHT_COLOR = "#FF78D2" 
INFO_BG = "#E8F5E9"         

st.markdown(f"""
    <style>
    /* 1. RESET & BASICS */
    .stApp {{ background-color: {BG_COLOR}; color: {PRIMARY_COLOR}; font-family: 'Inter', system-ui, sans-serif; }}
    h1, h2, h3, h4, p, span, div, label, li {{ color: {PRIMARY_COLOR} !important; }}

    /* 2. CONTAINERS */
    div[data-testid="stVerticalBlockBorderWrapper"], .opella-card {{
        background-color: white; border: 1px solid {ACCENT_COLOR}; border-radius: 12px;
        box-shadow: 0 4px 10px rgba(4, 43, 11, 0.05); padding: 24px; margin-bottom: 20px;
    }}
    
    /* 3. BUTTONS */
    div.stButton > button[kind="primary"] {{
        background-color: {PRIMARY_COLOR}; color: white !important; border: none;
        border-radius: 6px; font-weight: 600; height: 48px; transition: all 0.2s;
    }}
    div.stButton > button[kind="primary"]:hover {{ opacity: 0.9; box-shadow: 0 4px 12px rgba(4, 43, 11, 0.3); }}
    div.stButton > button[kind="secondary"] {{
        background-color: white; color: {PRIMARY_COLOR} !important; border: 1px solid {PRIMARY_COLOR};
        border-radius: 6px; height: 48px;
    }}

    /* 4. SLIDER & INPUTS */
    div[data-baseweb="slider"] {{ padding-bottom: 4px; }}
    div[data-baseweb="slider"] > div > div {{ background-color: {ACCENT_COLOR} !important; height: 4px !important; }}
    div[data-baseweb="slider"] > div > div > div {{ background-color: {PRIMARY_COLOR} !important; }}
    div[role="slider"] {{
        background-color: {HIGHLIGHT_COLOR} !important; border: 2px solid {BG_COLOR} !important;
        width: 18px !important; height: 18px !important; box-shadow: 0 2px 4px rgba(0,0,0,0.2);
    }}
    
    /* 5. BLOCKING INFO BOX */
    .blocking-box {{
        background-color: {INFO_BG}; border: 1px solid {PRIMARY_COLOR}; color: {PRIMARY_COLOR};
        padding: 0px 15px; border-radius: 6px; font-size: 13px; font-weight: 600;
        display: flex; align-items: center; height: 42px; white-space: nowrap;
        overflow: hidden; text-overflow: ellipsis;
    }}

    /* 6. STEP HEADERS */
    .step-header {{
        font-size: 18px; font-weight: 800; text-transform: uppercase; letter-spacing: 0.5px;
        margin-top: 30px; margin-bottom: 15px; display: flex; align-items: center;
        border-bottom: 2px solid {ACCENT_COLOR}; padding-bottom: 8px;
    }}
    .step-number {{
        background-color: {PRIMARY_COLOR}; color: white !important; width: 28px; height: 28px;
        border-radius: 6px; display: flex; align-items: center; justify-content: center;
        margin-right: 12px; font-size: 14px; font-weight: bold;
    }}
    </style>
""", unsafe_allow_html=True)

# ==========================================
# 1. HELPER FUNCTIONS
# ==========================================
@st.cache_data
def load_excel_file(uploaded_file, sheet_name):
    df = pd.read_excel(uploaded_file, sheet_name=sheet_name)
    df = df.reset_index(drop=True)
    df.columns = [str(c).strip() for c in df.columns]
    return df

# ==========================================
# 2. LOGIC CLASS
# ==========================================
class SpecializedMatcher:
    def __init__(self):
        # COMPILED REGEX FOR SPEED AND SAFETY
        self.true_noise = r'\b(viet nam|vn|address|dia chi|moi|cu)\b'
        
        # Bỏ đi các từ quá ngắn như 'nt', 'qt' để tránh xóa nhầm tên riêng
        biz_words = [
            r'cong ty', r'cty', r'tnhh', r'co phan', r'cp', r'chi nhanh', 
            r'ho kinh doanh', r'hkd', r'nha thuoc tu nhan', r'nha thuoc', 
            r'quay thuoc', r'duoc pham', r'pharmacy', r'medicine', r'clinic',
            r'quay', r'thuoc', r'private', r'enterprise', r'dr', r'duoc', r'tu nhan'
        ]
        self.biz_noise_pattern = r'\b(' + '|'.join(biz_words) + r')\b'
        
        self.blocking_regex = [
            r'\b(thanh pho|tp|t\.p|tinh)\b', r'\b(quan|huyen|thi xa|thanh pho|tp|tx|q\.|h\.)\b', 
            r'\b(phuong|xa|thi tran|p\.|x\.|tt\.)\b', r'[^\w\s]' 
        ]

    def base_clean(self, text):
        if not isinstance(text, str): text = str(text)
        if not text or str(text).lower() == 'nan': return ""
        text = unidecode(text).lower()
        # Chuyển các ký tự đặc biệt thành khoảng trắng
        text = re.sub(r'[,\.\-\;\(\)\[\]]', ' ', text)
        return " ".join(text.split())

    def clean_business_name(self, text):
        text = self.base_clean(text)
        # Xóa noise 1 lần duy nhất bằng regex tổng hợp
        text = re.sub(self.biz_noise_pattern, "", text)
        return " ".join(text.split())

    def clean_address_text(self, text):
        text = self.base_clean(text)
        admin_patterns = [
            r'\b(phuong|p\.|p)\s+[a-z0-9]+\s?[a-z0-9]*', r'\b(quan|q\.|q)\s+[a-z0-9]+\s?[a-z0-9]*',    
            r'\b(thanh pho|tp|t\.p)\s+[a-z\s]+', r'\b(tinh)\s+[a-z\s]+', r'\b(to|khu)\s+[0-9]+[a-z]?'                  
        ]
        for pattern in admin_patterns: 
            text = re.sub(pattern, "", text)
        text = re.sub(self.true_noise, "", text)
        text = re.sub(r'\b(duong|pho|so|lo|nha)\b', '', text)
        return " ".join(text.split())
    
    def normalize_for_blocking(self, text):
        text = self.base_clean(text)
        for regex in self.blocking_regex: 
            text = re.sub(regex, "", text)
        return " ".join(text.split())

    def calculate_score(self, val_a, val_b, algo_key):
        # Đã bỏ chặn độ dài cực đoan. Chỉ trả về 0 nếu chuỗi rỗng hoàn toàn.
        if not val_a or not val_b: return 0
        
        if algo_key == "vn_address":
            num_a = re.findall(r'\b\d+[a-z]?[\/-]?\d*', val_a)
            num_b = re.findall(r'\b\d+[a-z]?[\/-]?\d*', val_b)
            penalty = 1.0
            if num_a and num_b:
                if num_a[0] != num_b[0]: penalty = 0.5 
            s = fuzz.token_sort_ratio(val_a, val_b)
            return s * penalty
        elif algo_key == "token_sort": 
            return fuzz.token_sort_ratio(val_a, val_b)
        elif algo_key == "token_set": 
            return fuzz.token_set_ratio(val_a, val_b)
        return 0

matcher = SpecializedMatcher()

# ==========================================
# 3. WORKER KERNEL
# ==========================================
def worker_process_chunk(args):
    (chunk_a, b_data_dict, b_blocking_map, criteria, threshold, show_details, need_id_b, col_id_b, blocking_config, addr_override_threshold) = args
    results = []
    
    active_criteria = [c for c in criteria if not c['blocking']]
    tot_w = sum(c['weight'] for c in active_criteria) or 1
    
    use_blocking = len(blocking_config) > 0
    
    all_b_indices = []
    if not use_blocking and b_data_dict:
        first_key = list(b_data_dict.keys())[0]
        all_b_indices = range(len(b_data_dict[first_key]))

    for idx_a, row_a in chunk_a.iterrows():
        row_r = {'_index_': idx_a} 
        bst_sc = -1; bst_ib = -1; bst_details = {}
        candidate_indices = []
        debug_key = "FULL_SCAN"

        if use_blocking:
            key_parts = []
            for cfg in blocking_config:
                val = str(row_a[cfg['col_a']]) if pd.notna(row_a[cfg['col_a']]) else ""
                key_parts.append(matcher.normalize_for_blocking(val))
            full_key_a = "_".join(key_parts)
            debug_key = full_key_a
            candidate_indices = b_blocking_map.get(full_key_a, [])
        else:
            candidate_indices = all_b_indices
        
        if not candidate_indices:
            row_r['Matching_Score'] = 0
            if show_details: row_r['Blocking_Key_Trace'] = debug_key
            if need_id_b: row_r['Matched_ID_B'] = None
            for c in criteria: row_r[f"Matched_{c['col_b']}"] = None
            results.append(row_r)
            continue

        for ib in candidate_indices:
            current_weighted_sum = 0
            current_details = {}
            perfect_address_found = False
            
            for c in active_criteria:
                raw_a = str(row_a[c['col_a']]) if pd.notna(row_a[c['col_a']]) else ""
                raw_b = b_data_dict[c['col_b']][ib] 
                clean_type = c.get('clean_type', 'general')
                val_a, val_b = "", ""
                
                if clean_type == 'biz_name':
                    val_a = matcher.clean_business_name(raw_a)
                    val_b = matcher.clean_business_name(raw_b)
                elif clean_type == 'address':
                    val_a = matcher.clean_address_text(raw_a)
                    val_b = matcher.clean_address_text(raw_b)
                else:
                    val_a = matcher.base_clean(raw_a)
                    val_b = matcher.base_clean(raw_b)
                    
                s = matcher.calculate_score(val_a, val_b, c['algo'])
                
                if clean_type == 'address' and s >= addr_override_threshold: perfect_address_found = True
                current_weighted_sum += s * c['weight']
                
                if show_details: 
                    current_details[f"Score_{c['col_a']}"] = s
                    # Xuất text đã được clean ra để debug dễ dàng
                    current_details[f"Cleaned_A_{c['col_a']}"] = val_a
                    current_details[f"Cleaned_B_{c['col_b']}"] = val_b
            
            fin = 100 if perfect_address_found else current_weighted_sum / tot_w
            
            if fin > bst_sc:
                bst_sc = fin; bst_ib = ib
                if show_details: bst_details = current_details.copy()
                if bst_sc == 100: break

        row_r['Matching_Score'] = round(bst_sc, 2) if bst_sc >= threshold else 0
        if show_details: row_r['Blocking_Key_Trace'] = debug_key

        if bst_sc >= threshold and bst_ib != -1:
            if need_id_b and col_id_b: row_r['Matched_ID_B'] = b_data_dict[col_id_b][bst_ib]
            for c in criteria: row_r[f"Matched_{c['col_b']}"] = b_data_dict[c['col_b']][bst_ib]
            if show_details: row_r.update(bst_details)
        else:
            if need_id_b: row_r['Matched_ID_B'] = None
            for c in criteria: row_r[f"Matched_{c['col_b']}"] = None
        
        results.append(row_r)
    return results

# ==========================================
# 4. APP STATE & CONFIG
# ==========================================
ALGO_OPTIONS = {
    "VN Smart Logic (Gatekeeper Số)": "vn_address",
    "Fuzzy: Token Sort": "token_sort",
    "Fuzzy: Token Set": "token_set"
}

AUTO_MAP_KEYWORDS = {
    'name': ['name', 'ten', 'khach hang', 'kh', 'partner', 'company', 'doi tac', 'doanh nghiep', 'nguoi mua'], 
    'address': ['dia chi', 'address', 'dc', 'location', 'noi sinh', 'ho khau', 'tru so'], 
    'id': ['id', 'code', 'ma', 'mst', 'tax']
}

def get_auto_index(col_a, cols_b):
    if not col_a: return 0
    col_a = str(col_a).lower()
    detected_type = None
    for key_type, keywords in AUTO_MAP_KEYWORDS.items():
        if any(kw in col_a for kw in keywords):
            detected_type = key_type
            break
    if detected_type:
        target_keywords = AUTO_MAP_KEYWORDS[detected_type]
        for idx, cb in enumerate(cols_b):
            cb_lower = str(cb).lower()
            if any(kw in cb_lower for kw in target_keywords):
                return idx
    return 0

# STATE
if 'data_loaded' not in st.session_state: st.session_state.data_loaded = False
if 'cols_a' not in st.session_state: st.session_state.cols_a = []
if 'cols_b' not in st.session_state: st.session_state.cols_b = []
if 'match_criteria' not in st.session_state:
    st.session_state.match_criteria = [
        {'id': 1, 'col_a': None, 'col_b': None, 'clean_type': 'biz_name', 'algo': 'token_sort', 'weight': 1.0, 'blocking': False},
        {'id': 2, 'col_a': None, 'col_b': None, 'clean_type': 'address', 'algo': 'vn_address', 'weight': 1.0, 'blocking': False}
    ]

# ==========================================
# 5. MAIN APP LAYOUT
# ==========================================
def main():
    st.markdown("<h1>Opella. Matcher</h1>", unsafe_allow_html=True)
    st.caption("Professional Entity Resolution Engine | v27.3 (Optimized Core) by Dat Ngo")
    
    with st.expander("📖 HƯỚNG DẪN SỬ DỤNG (USER GUIDE)"):
        st.markdown("""
        ### Quy trình 3 bước chuẩn:
        **Bước 1: Tải dữ liệu (Data)**
        * Upload file cần tìm vào **File A** và file gốc vào **File B**.
        
        **Bước 2: Cấu hình (Config)**
        * ✅ **Key (Blocking):** Tích vào đây nếu muốn dùng cột này để lọc nhóm (Ví dụ: Tỉnh/Thành phố).
        * ⚖️ **Weight:** Kéo thanh trượt để chỉnh độ quan trọng (nếu không phải Blocking Key).
        
        **Bước 3: Chạy (Execution)**
        * Bấm **Start Matching** và chờ kết quả.
        * File kết quả giờ đây sẽ có thêm cột `Cleaned_A` và `Cleaned_B` để bạn kiểm tra thuật toán đã xử lý chuỗi như thế nào.
        """)

    # --- STEP 1: DATA SOURCE ---
    st.markdown('<div class="step-header"><div class="step-number">1</div>DATA SOURCE</div>', unsafe_allow_html=True)
    
    if not st.session_state.data_loaded:
        with st.container(border=True):
            c1, c2 = st.columns(2)
            with c1:
                f_a = st.file_uploader("File A (Target - Cần tìm)", type=["xlsx"], key="fa")
                if f_a: sh_a = st.selectbox("Sheet A", pd.ExcelFile(f_a).sheet_names)
            with c2:
                f_b = st.file_uploader("File B (Reference - Dữ liệu gốc)", type=["xlsx"], key="fb")
                if f_b: sh_b = st.selectbox("Sheet B", pd.ExcelFile(f_b).sheet_names)
            
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("LOAD DATA", type="primary", use_container_width=True, disabled=not (f_a and f_b)):
                try:
                    st.session_state.df_a = load_excel_file(f_a, sh_a)
                    st.session_state.df_b = load_excel_file(f_b, sh_b)
                    st.session_state.cols_a = list(st.session_state.df_a.columns)
                    st.session_state.cols_b = list(st.session_state.df_b.columns)
                    st.session_state.data_loaded = True
                    st.rerun()
                except Exception as e: st.error(str(e))
    else:
        with st.container(border=True):
            c_info, c_btn = st.columns([5, 1], vertical_alignment="center")
            with c_info:
                st.markdown(f"**File A:** {len(st.session_state.df_a):,} dòng | **File B:** {len(st.session_state.df_b):,} dòng")
            with c_btn:
                if st.button("Change Data", type="secondary"):
                    st.session_state.data_loaded = False
                    st.rerun()

    # --- STEP 2: CONFIGURATION ---
    if st.session_state.data_loaded:
        st.markdown('<div class="step-header"><div class="step-number">2</div>CONFIGURATION</div>', unsafe_allow_html=True)
        
        with st.container(border=True):
            headers = st.columns([0.6, 3, 3, 3, 0.5], vertical_alignment="bottom")
            headers[0].caption("KEY (BLOCK)")
            headers[1].caption("CỘT FILE A")
            headers[2].caption("CỘT FILE B (GỢI Ý)")
            headers[3].caption("THUẬT TOÁN & TRỌNG SỐ")
            
            criteria_to_remove = []
            for i, crit in enumerate(st.session_state.match_criteria):
                st.markdown("<hr style='margin:10px 0; border-top:1px dashed #CED5CE;'>", unsafe_allow_html=True)
                row = st.columns([0.6, 3, 3, 3, 0.5], vertical_alignment="center")
                
                is_blocking = row[0].checkbox("Key", key=f"bk_{i}", value=crit['blocking'], label_visibility="collapsed")
                crit['blocking'] = is_blocking
                
                idx_a = st.session_state.cols_a.index(crit['col_a']) if crit['col_a'] in st.session_state.cols_a else 0
                crit['col_a'] = row[1].selectbox(f"A{i}", st.session_state.cols_a, key=f"ca_{i}", index=idx_a, label_visibility="collapsed")
                
                s_idx = 0
                if crit['col_a']: s_idx = get_auto_index(crit['col_a'], st.session_state.cols_b)
                curr_b = crit['col_b'] if crit['col_b'] in st.session_state.cols_b else st.session_state.cols_b[s_idx]
                curr_idx = st.session_state.cols_b.index(curr_b) if curr_b in st.session_state.cols_b else 0
                crit['col_b'] = row[2].selectbox(f"B{i}", st.session_state.cols_b, key=f"cb_{i}", index=curr_idx, label_visibility="collapsed")
                
                with row[3]:
                    if is_blocking:
                        st.markdown(f"""
                        <div class="blocking-box">
                            🔒 BLOCKING KEY: Dùng để lọc nhóm, không tính điểm.
                        </div>
                        """, unsafe_allow_html=True)
                        crit['weight'] = 0.0
                    else:
                        sub_row = st.columns([1.2, 1.8], vertical_alignment="center")
                        raw_algo = sub_row[0].selectbox(f"Al{i}", list(ALGO_OPTIONS.keys()), key=f"al_{i}", 
                                                    index=list(ALGO_OPTIONS.values()).index(crit['algo']) if crit['algo'] in ALGO_OPTIONS.values() else 0,
                                                    label_visibility="collapsed")
                        crit['algo'] = ALGO_OPTIONS[raw_algo]
                        crit['weight'] = sub_row[1].slider(f"W{i}", 0.0, 5.0, float(crit['weight']), 0.5, key=f"w_{i}", label_visibility="collapsed")
                
                if row[4].button("✕", key=f"del_{i}"): criteria_to_remove.append(i)

            if criteria_to_remove:
                for idx in sorted(criteria_to_remove, reverse=True): st.session_state.match_criteria.pop(idx)
                st.rerun()
                
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("+ Thêm tiêu chí (Add Rule)", type="secondary"):
                st.session_state.match_criteria.append({'id': len(st.session_state.match_criteria)+1, 'col_a': None, 'col_b': None, 'clean_type': 'biz_name', 'algo': 'token_sort', 'weight': 1.0, 'blocking': False})
                st.rerun()

    # --- STEP 3: EXECUTION ---
    st.markdown('<div class="step-header"><div class="step-number">3</div>EXECUTION</div>', unsafe_allow_html=True)
    
    with st.container(border=True):
        col_exec_main, col_exec_adv = st.columns([3, 1])
        
        with col_exec_main:
            st.caption("**Sẵn sàng chạy**")
            start_btn = st.button("START MATCHING PROCESS 🚀", type="primary", use_container_width=True)
        
        with col_exec_adv:
            with st.expander("⚙️ Cài đặt nâng cao"):
                max_cpu = multiprocessing.cpu_count()
                num_workers = st.slider("Số nhân CPU (Cores)", 1, max_cpu, max(1, max_cpu-1))
                threshold = st.slider("Ngưỡng điểm chuẩn (Threshold)", 0, 100, 75)
                addr_override = st.slider("Ưu tiên địa chỉ (Override)", 90, 100, 100)
                need_id_b = st.checkbox("Lấy thêm cột ID từ B")
                col_id_b = st.selectbox("Chọn cột ID", st.session_state.cols_b) if need_id_b else None
                show_details = st.checkbox("Hiện chi tiết & Text Debug", value=True)
    
    # --- PROCESSING ---
    if start_btn:
        df_a = st.session_state.df_a
        df_b = st.session_state.df_b
        criteria = st.session_state.match_criteria
        
        blocking_config = [c for c in criteria if c['blocking']]
        cols_needed_b = [c['col_b'] for c in criteria]
        if need_id_b and col_id_b: cols_needed_b.append(col_id_b)

        status_box = st.status("Đang xử lý dữ liệu...", expanded=True)
        
        # 1. BLOCKING MAP
        b_blocking_map = {}
        if blocking_config:
            keys_info = " + ".join([f"{c['col_a']}" for c in blocking_config])
            status_box.write(f"⚡ Đang tạo chỉ mục Blocking theo: [{keys_info}]...")
            
            b_keys_series = []
            for cfg in blocking_config:
                col_b_data = df_b[cfg['col_b']].astype(str).fillna("").apply(matcher.normalize_for_blocking)
                b_keys_series.append(col_b_data)
            
            full_b_keys = ["_".join(parts) for parts in zip(*b_keys_series)]
            for idx, key in enumerate(full_b_keys):
                if key not in b_blocking_map: b_blocking_map[key] = []
                b_blocking_map[key].append(idx)
        else:
            status_box.warning("⚠️ Đang chạy chế độ Full Scan (Không có Key chặn).")
        
        # 2. DISTRIBUTE & RUN
        status_box.write(f"🚀 Phân phối tác vụ cho {num_workers} nhân CPU...")
        b_data_dict = {col: df_b[col].astype(str).fillna("").tolist() for col in set(cols_needed_b)}
        
        chunk_size = max(1, len(df_a) // (num_workers * 4))
        chunks = [df_a.iloc[i:i + chunk_size] for i in range(0, len(df_a), chunk_size)]
        
        final_results_list = []
        prog_bar = status_box.progress(0)
        
        map_args = []
        for chunk in chunks:
            map_args.append((chunk, b_data_dict, b_blocking_map, criteria, threshold, show_details, need_id_b, col_id_b, blocking_config, addr_override))
        
        start_time = time.time()
        
        with concurrent.futures.ProcessPoolExecutor(max_workers=num_workers) as executor:
            futures = [executor.submit(worker_process_chunk, arg) for arg in map_args]
            done = 0
            for future in concurrent.futures.as_completed(futures):
                try:
                    res = future.result()
                    final_results_list.extend(res)
                except Exception as e: st.error(f"Error: {e}")
                
                done += 1
                prog_bar.progress(done/len(chunks))

        status_box.update(label=f"✅ Hoàn thành trong {round(time.time() - start_time, 2)}s", state="complete", expanded=False)
        
        if final_results_list:
            res_df = pd.DataFrame(final_results_list)
            res_df = res_df.set_index('_index_').sort_index()
            final_df = df_a.join(res_df)
            
            st.markdown("### Kết quả (Preview)")
            
            with st.container(border=True):
                m1, m2, m3 = st.columns(3, vertical_alignment="center")
                matches = len(final_df[final_df['Matching_Score'] > 0])
                avg = final_df[final_df['Matching_Score'] > 0]['Matching_Score'].mean()
                
                m1.metric("Tìm thấy (Matches)", f"{matches:,}")
                m2.metric("Điểm trung bình", f"{avg:.1f}" if not pd.isna(avg) else "0")
                
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                buff = io.BytesIO()
                with pd.ExcelWriter(buff, engine='openpyxl') as writer: final_df.to_excel(writer, index=False)
                m3.download_button("📥 Tải kết quả (.xlsx)", buff.getvalue(), f"Opella_Result_{timestamp}.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", type="primary", use_container_width=True)
            
            st.dataframe(final_df.head(100), use_container_width=True, height=400)

if __name__ == "__main__":
    main()
