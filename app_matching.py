import streamlit as st
import pandas as pd
from rapidfuzz import fuzz
from unidecode import unidecode
import io
import time
import concurrent.futures
import sys
import multiprocessing
import re
import os

# ==========================================
# 0. HELPER FUNCTIONS
# ==========================================

@st.cache_data
def load_excel_file(uploaded_file, sheet_name):
    df = pd.read_excel(uploaded_file, sheet_name=sheet_name)
    df = df.reset_index(drop=True)
    return df

# ==========================================
# 1. LOGIC CLASS (CLEANING & SCORING)
# ==========================================
class SpecializedMatcher:
    def __init__(self):
        # 1. Từ điển chuẩn hóa địa chỉ
        self.addr_map = {
            r'\bp\.?\s': 'phuong ',      
            r'\bq\.?\s': 'quan ',        
            r'\btp\.?\s': 'thanh pho ',  
            r'\bt\.p\.?\s': 'thanh pho ',
            r'\btx\.?\s': 'thi xa ',
            r'\btt\.?\s': 'thi tran ',
            r'\bh\.?\s': 'huyen ',
            r'\btinh\s': 'tinh ',        
            r'\bduong\s': 'duong ',
            r'\bso\s': 'so ',
        }
        
        # Từ rác địa chỉ
        self.true_noise = [
            r'\bviet nam\b', r'\bvn\b', r'\baddress\b', r'\bdia chi\b',
            r'\bmoi\b', r'\bcu\b' 
        ]
        
        # 2. Từ rác Tên Doanh nghiệp
        self.biz_noise = [
            r'\bcong ty\b', r'\bcty\b', r'\btnhh\b', r'\bco phan\b', r'\bcp\b',
            r'\bchi nhanh\b', r'\bho kinh doanh\b', r'\bhkd\b',
            r'\bnha thuoc\b', r'\bquay thuoc\b', r'\bnt\b', r'\bqt\b',
            r'\bduoc pham\b', r'\bpharmacy\b', r'\bmedicine\b', r'\bclinic\b',
            r'\bquay\b', r'\bthuoc\b', r'\bprivate\b', r'\benterprise\b',
            r'\bdr\b', r'\bduoc\b', r'\btu nhan\b', r'\bprivate\b'
        ]

        # 3. Regex Blocking Key
        self.blocking_regex = [
            r'\b(thanh pho|tp|t\.p|tinh)\b', 
            r'\b(quan|huyen|thi xa|thanh pho|tp|tx|q\.|h\.)\b', 
            r'\b(phuong|xa|thi tran|p\.|x\.|tt\.)\b',
            r'[^\w\s]' 
        ]

    def base_clean(self, text):
        if not isinstance(text, str): text = str(text)
        if not text: return ""
        text = unidecode(text).lower()
        text = text.replace(",", " ").replace(".", " ").replace("-", " ").replace(";", " ")
        return " ".join(text.split())

    def clean_business_name(self, text):
        text = self.base_clean(text)
        for pattern in self.biz_noise:
            text = re.sub(pattern, "", text)
        return " ".join(text.split())

    def clean_address_text(self, text):
        text = self.base_clean(text)
        
        # Xóa hành chính kèm tên riêng (p.9, q.5...)
        admin_patterns = [
            r'\b(phuong|p\.|p)\s+[a-z0-9]+\s?[a-z0-9]*', 
            r'\b(quan|q\.|q)\s+[a-z0-9]+\s?[a-z0-9]*',   
            r'\b(thanh pho|tp|t\.p)\s+[a-z\s]+',         
            r'\b(tinh)\s+[a-z\s]+',                       
            r'\b(to|khu)\s+[0-9]+[a-z]?'                  
        ]
        for pattern in admin_patterns:
            text = re.sub(pattern, "", text)
            
        for pattern in self.true_noise:
            text = re.sub(pattern, "", text)
            
        # Xóa từ dẫn nhưng giữ số
        text = re.sub(r'\b(duong|pho|so|lo|nha)\b', '', text)

        return " ".join(text.split())
    
    def normalize_for_blocking(self, text):
        text = self.base_clean(text)
        for regex in self.blocking_regex:
            text = re.sub(regex, "", text)
        return " ".join(text.split())

    def calculate_score(self, val_a, val_b, algo_key):
        if not val_a or not val_b: return 0
        if len(val_a) < 2 or len(val_b) < 2: return 0

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
# 2. WORKER KERNEL (FLEXIBLE OVERRIDE)
# ==========================================
def worker_process_chunk(args):
    # Nhận thêm tham số addr_override_threshold
    (chunk_a, b_data_dict, b_blocking_map, criteria, threshold, show_details, need_id_b, col_id_b, blocking_config, addr_override_threshold) = args
    
    results = []
    active_criteria = [c for c in criteria if not c['blocking']]
    tot_w = sum(c['weight'] for c in active_criteria) or 1
    
    use_blocking = blocking_config is not None
    
    all_b_indices = []
    if not use_blocking and b_data_dict:
        first_key = list(b_data_dict.keys())[0]
        all_b_indices = range(len(b_data_dict[first_key]))

    for idx_a, row_a in chunk_a.iterrows():
        row_r = {'_index_': idx_a} 
        
        bst_sc = -1; bst_ib = -1; bst_details = {}
        candidate_indices = []
        debug_key = "FULL_SCAN"

        # 1. BLOCKING
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

        # 2. MATCHING
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
                
                # --- FLEXIBLE ADDRESS OVERRIDE ---
                # Nếu là cột Address và điểm >= Ngưỡng người dùng đặt -> Override
                if clean_type == 'address' and s >= addr_override_threshold:
                    perfect_address_found = True
                
                current_weighted_sum += s * c['weight']
                if show_details: current_details[f"Score_{c['col_a']}"] = s
            
            # --- FINAL SCORE ---
            if perfect_address_found:
                fin = 100
            else:
                fin = current_weighted_sum / tot_w
            
            if fin > bst_sc:
                bst_sc = fin; bst_ib = ib
                if show_details: bst_details = current_details
                
                # Nếu max score 100 rồi thì dừng loop nhóm
                if bst_sc == 100: break

        # 3. RESULT
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
# 3. APP UI SETUP
# ==========================================
st.set_page_config(page_title="Matcher", page_icon="🟢", layout="wide")

st.markdown("""
    <style>
    .stApp { background-color: #F7EFE6; color: #042B0B; font-family: 'Arial', sans-serif; }
    h1, h2, h3, h4, p, label, .stMarkdown { color: #042B0B !important; }
    div[data-testid="stExpander"], .opella-card {
        background-color: #FFFFFF; border-radius: 12px; border: 1px solid #CED5CE; 
        box-shadow: 0 4px 10px rgba(4, 43, 11, 0.05); margin-bottom: 20px; padding: 20px;
    }
    div[data-baseweb="slider"] { background-color: transparent !important; padding-bottom: 4px; }
    div[data-baseweb="slider"] > div > div { background-color: #CED4DA !important; height: 4px !important; }
    div[data-baseweb="slider"] > div > div > div { background-color: #042B0B !important; }
    div[role="slider"] {
        background-color: #FF78D2 !important; border: 2px solid #F7EFE6 !important;
        width: 18px !important; height: 18px !important; border-radius: 50% !important;
    }
    .stProgress > div > div > div > div { background-color: #042B0B; }
    </style>
""", unsafe_allow_html=True)

# CONFIG
ALGO_OPTIONS = {
    "VN Smart Logic (Gatekeeper Số)": "vn_address",
    "Fuzzy: Token Sort": "token_sort",
    "Fuzzy: Token Set": "token_set"
}
AUTO_MAP_KEYWORDS = {'name': ['name', 'ten'], 'address': ['dia chi', 'address'], 'id': ['id', 'code']}

def get_auto_index(col_a, cols_b):
    if not col_a: return 0
    col_a = str(col_a).lower()
    for k, v in AUTO_MAP_KEYWORDS.items():
        if any(x in col_a for x in v):
            for idx, cb in enumerate(cols_b):
                if any(x in str(cb).lower() for x in v): return idx
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

st.markdown("Matcher")

# 1. UPLOAD
with st.expander("📂 1. UPLOAD DATA", expanded=not st.session_state.data_loaded):
    c1, c2 = st.columns(2)
    with c1:
        f_a = st.file_uploader("File A", type=["xlsx"], key="fa")
        if f_a: sh_a = st.selectbox("Sheet A", pd.ExcelFile(f_a).sheet_names)
    with c2:
        f_b = st.file_uploader("File B", type=["xlsx"], key="fb")
        if f_b: sh_b = st.selectbox("Sheet B", pd.ExcelFile(f_b).sheet_names)
    
    if st.button("LOAD DATA", type="primary", disabled=not (f_a and f_b)):
        try:
            st.session_state.df_a = load_excel_file(f_a, sh_a)
            st.session_state.df_b = load_excel_file(f_b, sh_b)
            st.session_state.cols_a = list(st.session_state.df_a.columns)
            st.session_state.cols_b = list(st.session_state.df_b.columns)
            st.session_state.data_loaded = True
            st.rerun()
        except Exception as e: st.error(str(e))

# 2. CONFIGURATION
if st.session_state.data_loaded:
    st.markdown("---")
    st.markdown('<div class="opella-card">', unsafe_allow_html=True)
    st.markdown("### 2. Configuration")
    
    h1, h2, h3, h4, h5 = st.columns([0.5, 2.5, 2.5, 3, 0.5])
    h1.caption("KEY")
    h2.caption("COLUMN A")
    h3.caption("COLUMN B")
    h4.caption("ALGORITHM & WEIGHT")
    h5.caption("DEL")
    
    criteria_to_remove = []
    for i, crit in enumerate(st.session_state.match_criteria):
        st.markdown("<hr style='margin:5px 0; border-top:1px solid #CED5CE;'>", unsafe_allow_html=True)
        r1, r2, r3, r4, r5 = st.columns([0.5, 2.5, 2.5, 3, 0.5])
        
        if 'clean_type' not in crit: crit['clean_type'] = 'general'
        row_label = ""
        if i == 0: row_label = "NAME"
        elif i == 1: row_label = "ADDRESS"
        
        with r1:
            st.write("")
            crit['blocking'] = st.checkbox("Key", key=f"bk_{i}", value=crit['blocking'], label_visibility="collapsed")
        with r2:
            if row_label: st.caption(f"**{row_label}**")
            idx_a = st.session_state.cols_a.index(crit['col_a']) if crit['col_a'] in st.session_state.cols_a else 0
            crit['col_a'] = st.selectbox(f"A{i}", st.session_state.cols_a, key=f"ca_{i}", index=idx_a, label_visibility="collapsed")
        with r3:
            if row_label: st.caption("Matching")
            s_idx = 0
            if crit['col_a']: s_idx = get_auto_index(crit['col_a'], st.session_state.cols_b)
            curr_b = crit['col_b'] if crit['col_b'] in st.session_state.cols_b else st.session_state.cols_b[s_idx]
            curr_idx = st.session_state.cols_b.index(curr_b)
            crit['col_b'] = st.selectbox(f"B{i}", st.session_state.cols_b, key=f"cb_{i}", index=curr_idx, label_visibility="collapsed")
        with r4:
            if row_label: st.caption(f"Logic: {crit['clean_type']}")
            if crit['blocking']:
                st.info("🔒 Blocking Key")
            else:
                c_alg, c_w = st.columns([1.5, 1])
                with c_alg:
                    crit['algo'] = st.selectbox(f"Al{i}", list(ALGO_OPTIONS.keys()), key=f"al_{i}", 
                                              index=list(ALGO_OPTIONS.values()).index(crit['algo']) if crit['algo'] in ALGO_OPTIONS.values() else 0,
                                              label_visibility="collapsed")
                    crit['algo'] = ALGO_OPTIONS[crit['algo']]
                with c_w:
                    crit['weight'] = st.slider(f"W{i}", 0.0, 5.0, float(crit['weight']), 0.1, key=f"w_{i}", label_visibility="collapsed")
        with r5:
            if st.button("✕", key=f"del_{i}"): criteria_to_remove.append(i)

    if criteria_to_remove:
        for idx in sorted(criteria_to_remove, reverse=True): st.session_state.match_criteria.pop(idx)
        st.rerun()
        
    st.markdown("<hr style='margin:5px 0; border-top:1px solid #CED5CE;'>", unsafe_allow_html=True)
    if st.button("+ Add Criteria"):
        st.session_state.match_criteria.append({'id': len(st.session_state.match_criteria)+1, 'col_a': None, 'col_b': None, 'clean_type': 'general', 'algo': 'token_sort', 'weight': 1.0, 'blocking': False})
        st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)

    # 3. EXECUTION
    st.markdown("---")
    st.markdown('<div class="opella-card">', unsafe_allow_html=True)
    st.markdown("### 3. Execution")
    
    max_cpu = multiprocessing.cpu_count()
    st.caption(f"**CPU CORES (Max: {max_cpu})**")
    num_workers = st.slider("Cores", 1, max_cpu, max_cpu, 1, label_visibility="collapsed")
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    c1, c2, c3 = st.columns([1.5, 2, 2])
    with c1:
        threshold = st.slider("Match Threshold (%)", 0, 100, 75)
    with c2:
        # THANH TRƯỢT OVERRIDE ĐỊA CHỈ
        addr_override = st.slider("Address Override At (%)", 90, 100, 100, help="Nếu điểm địa chỉ >= mức này, tự động coi là Match 100% (bỏ qua Tên).")
    with c3:
        need_id_b = st.checkbox("Get ID from File B")
        col_id_b = None
        if need_id_b: col_id_b = st.selectbox("ID Col", st.session_state.cols_b)
        show_details = st.checkbox("Show Details", value=True)

    st.markdown("<br>", unsafe_allow_html=True)
    
    if st.button("START MATCHING", type="primary", use_container_width=True):
        df_a = st.session_state.df_a
        df_b = st.session_state.df_b
        criteria = st.session_state.match_criteria
        
        # 1. BLOCKING
        blocking_config = [c for c in criteria if c['blocking']]
        b_blocking_map = {}
        cols_needed_b = [c['col_b'] for c in criteria]
        if need_id_b and col_id_b: cols_needed_b.append(col_id_b)

        if blocking_config:
            keys_info = " + ".join([f"{c['col_a']}" for c in blocking_config])
            st.info(f"⚡ RAM Blocking Active: Grouping by [{keys_info}]")
            
            b_keys_series = []
            for cfg in blocking_config:
                col_b_data = df_b[cfg['col_b']].astype(str).fillna("").apply(matcher.normalize_for_blocking)
                b_keys_series.append(col_b_data)
            
            full_b_keys = ["_".join(parts) for parts in zip(*b_keys_series)]
            
            for idx, key in enumerate(full_b_keys):
                if key not in b_blocking_map: b_blocking_map[key] = []
                b_blocking_map[key].append(idx)
        else:
            st.warning("⚠️ Full Scan Mode (Slower).")
        
        # 2. EXECUTE
        b_data_dict = {col: df_b[col].astype(str).fillna("").tolist() for col in set(cols_needed_b)}
        
        chunk_size = max(1, len(df_a) // (num_workers * 4))
        chunks = [df_a.iloc[i:i + chunk_size] for i in range(0, len(df_a), chunk_size)]
        
        final_results_list = []
        prog_bar = st.progress(0)
        status = st.empty()
        
        # TRUYỀN THAM SỐ ADDR_OVERRIDE VÀO WORKER
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
                pct = int(done/len(futures)*100)
                prog_bar.progress(pct/100)
                status.markdown(f"**Processing on {num_workers} Cores... {pct}%**")

        st.success(f"Done in {round(time.time() - start_time, 2)}s")
        
        if final_results_list:
            res_df = pd.DataFrame(final_results_list)
            res_df = res_df.set_index('_index_').sort_index()
            final_df = df_a.join(res_df)
            
            buff = io.BytesIO()
            with pd.ExcelWriter(buff, engine='openpyxl') as writer: final_df.to_excel(writer, index=False)
            st.download_button("Download Result", buff.getvalue(), "Matched.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", type="primary")


    st.markdown('</div>', unsafe_allow_html=True)
