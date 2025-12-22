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
# 0. PAGE CONFIG & STYLING
# ==========================================
st.set_page_config(
    page_title="Opella Matcher",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Professional & Clean CSS
st.markdown("""
    <style>
    /* Global Font & Background */
    .stApp {
        background-color: #f8f9fa;
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }
    
    /* Headings */
    h1, h2, h3 {
        color: #1a1a1a !important;
        font-weight: 600 !important;
        letter-spacing: -0.5px;
    }
    
    /* Containers/Cards */
    div[data-testid="stVerticalBlockBorderWrapper"] {
        background-color: #ffffff;
        border-radius: 12px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        padding: 24px;
        border: 1px solid #e9ecef;
    }
    
    /* Buttons */
    div.stButton > button {
        border-radius: 6px;
        font-weight: 500;
        height: 42px;
        transition: all 0.2s;
    }
    div.stButton > button:hover {
        transform: translateY(-1px);
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    
    /* Sidebar */
    section[data-testid="stSidebar"] {
        background-color: #ffffff;
        border-right: 1px solid #e9ecef;
    }
    
    /* Metrics */
    div[data-testid="stMetricValue"] {
        font-size: 24px;
        font-weight: 700;
        color: #2563eb;
    }
    </style>
""", unsafe_allow_html=True)

# ==========================================
# 1. HELPER FUNCTIONS
# ==========================================
@st.cache_data
def load_excel_file(uploaded_file, sheet_name):
    try:
        df = pd.read_excel(uploaded_file, sheet_name=sheet_name)
        df = df.reset_index(drop=True)
        # Clean column names (strip whitespace)
        df.columns = [str(c).strip() for c in df.columns] 
        return df
    except Exception as e:
        return None

# ==========================================
# 2. LOGIC CORE (ALGORITHMS)
# ==========================================
class SpecializedMatcher:
    def __init__(self):
        # Standardize administrative units
        self.addr_map = {
            r'\bp\.?\s': 'phuong ', r'\bq\.?\s': 'quan ', r'\btp\.?\s': 'thanh pho ',  
            r'\bt\.p\.?\s': 'thanh pho ', r'\btx\.?\s': 'thi xa ', r'\btt\.?\s': 'thi tran ',
            r'\bh\.?\s': 'huyen ', r'\btinh\s': 'tinh ', r'\bduong\s': 'duong ', r'\bso\s': 'so ',
        }
        # True Noise (Removed completely)
        self.true_noise = [
            r'\bviet nam\b', r'\bvn\b', r'\baddress\b', r'\bdia chi\b', r'\bmoi\b', r'\bcu\b' 
        ]
        # Business Entity Noise
        self.biz_noise = [
            r'\bcong ty\b', r'\bcty\b', r'\btnhh\b', r'\bco phan\b', r'\bcp\b',
            r'\bchi nhanh\b', r'\bho kinh doanh\b', r'\bhkd\b', r'\bnha thuoc\b', 
            r'\bquay thuoc\b', r'\bnt\b', r'\bqt\b', r'\bduoc pham\b', r'\bpharmacy\b', 
            r'\bmedicine\b', r'\bclinic\b', r'\bquay\b', r'\bthuoc\b', r'\bprivate\b', 
            r'\benterprise\b', r'\bdr\b', r'\bduoc\b', r'\btu nhan\b', r'\bprivate\b'
        ]
        # Blocking Key Normalization Regex
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
        # Remove admin units accompanied by names to focus on street/number
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
        # Remove prefixes
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
            # Number Gatekeeper Logic
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
# 3. WORKER PROCESS (PARALLEL EXECUTION)
# ==========================================
def worker_process_chunk(args):
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

        # Blocking Step
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
        
        # If no candidates found in block
        if not candidate_indices:
            row_r['Matching_Score'] = 0
            if show_details: row_r['Blocking_Trace'] = debug_key
            results.append(row_r)
            continue

        # Matching Step
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
                
                # Address Override Logic
                if clean_type == 'address' and s >= addr_override_threshold:
                    perfect_address_found = True
                
                current_weighted_sum += s * c['weight']
                if show_details: current_details[f"Score_{c['col_a']}"] = s
            
            # Final Score Calculation
            if perfect_address_found:
                fin = 100
            else:
                fin = current_weighted_sum / tot_w
            
            if fin > bst_sc:
                bst_sc = fin; bst_ib = ib
                if show_details: bst_details = current_details
                if bst_sc == 100: break

        # Result construction
        row_r['Matching_Score'] = round(bst_sc, 2) if bst_sc >= threshold else 0
        if show_details: row_r['Blocking_Trace'] = debug_key

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
# 4. APP STATE & CONSTANTS
# ==========================================
ALGO_OPTIONS = {
    "VN Smart Address": "vn_address",
    "Fuzzy Token Sort": "token_sort", 
    "Fuzzy Token Set": "token_set"
}
AUTO_MAP_KEYWORDS = {'name': ['name', 'ten'], 'address': ['dia chi', 'address'], 'id': ['id', 'code']}

if 'data_loaded' not in st.session_state: st.session_state.data_loaded = False
if 'cols_a' not in st.session_state: st.session_state.cols_a = []
if 'cols_b' not in st.session_state: st.session_state.cols_b = []
if 'match_criteria' not in st.session_state:
    st.session_state.match_criteria = [
        {'id': 1, 'col_a': None, 'col_b': None, 'clean_type': 'biz_name', 'algo': 'token_sort', 'weight': 1.0, 'blocking': False},
        {'id': 2, 'col_a': None, 'col_b': None, 'clean_type': 'address', 'algo': 'vn_address', 'weight': 1.0, 'blocking': False}
    ]

def get_auto_index(col_a, cols_b):
    if not col_a: return 0
    col_a = str(col_a).lower()
    for k, v in AUTO_MAP_KEYWORDS.items():
        if any(x in col_a for x in v):
            for idx, cb in enumerate(cols_b):
                if any(x in str(cb).lower() for x in v): return idx
    return 0

# ==========================================
# 5. MAIN APPLICATION
# ==========================================
def main():
    # --- SIDEBAR ---
    with st.sidebar:
        st.title("⚡ Opella Matcher")
        st.caption("Professional Fuzzy Matching Engine")
        st.markdown("---")
        
        st.subheader("Data Input")
        f_a = st.file_uploader("File A (Target)", type=["xlsx"])
        sh_a = st.selectbox("Sheet A", pd.ExcelFile(f_a).sheet_names) if f_a else None
        
        st.markdown("<div style='height: 10px'></div>", unsafe_allow_html=True)
        
        f_b = st.file_uploader("File B (Reference)", type=["xlsx"])
        sh_b = st.selectbox("Sheet B", pd.ExcelFile(f_b).sheet_names) if f_b else None
        
        st.markdown("---")
        
        if f_a and f_b:
            if st.button("Load Data", type="primary", use_container_width=True):
                try:
                    df_a = load_excel_file(f_a, sh_a)
                    df_b = load_excel_file(f_b, sh_b)
                    
                    if df_a is not None and df_b is not None:
                        st.session_state.df_a = df_a
                        st.session_state.df_b = df_b
                        st.session_state.cols_a = list(df_a.columns)
                        st.session_state.cols_b = list(df_b.columns)
                        st.session_state.data_loaded = True
                        st.success("Loaded successfully.")
                        st.rerun()
                    else:
                        st.error("Could not read files.")
                except Exception as e:
                    st.error(f"Error: {str(e)}")
        
        st.markdown(f"<div style='text-align: center; color: #666; font-size: 12px; margin-top: 20px;'>System Active<br>Cores: {multiprocessing.cpu_count()}</div>", unsafe_allow_html=True)

    # --- MAIN UI ---
    if not st.session_state.data_loaded:
        st.info("👋 Please upload your Excel files in the sidebar to begin.")
    else:
        # 1. DASHBOARD
        c1, c2, c3 = st.columns(3)
        with c1: st.metric("Target Rows (A)", f"{len(st.session_state.df_a):,}")
        with c2: st.metric("Reference Rows (B)", f"{len(st.session_state.df_b):,}")
        with c3: st.metric("Fields", f"{len(st.session_state.cols_a)} | {len(st.session_state.cols_b)}")

        st.markdown("### Configuration")
        
        # 2. CRITERIA MAPPING
        with st.container(border=True):
            st.markdown("**Matching Rules**")
            
            # Header
            cols = st.columns([0.5, 3, 3, 3, 0.5])
            cols[0].caption("BLOCK")
            cols[1].caption("COLUMN A")
            cols[2].caption("COLUMN B")
            cols[3].caption("LOGIC / WEIGHT")
            cols[4].caption("")

            criteria_to_remove = []
            
            for i, crit in enumerate(st.session_state.match_criteria):
                c_row = st.columns([0.5, 3, 3, 3, 0.5])
                
                with c_row[0]:
                    st.write("")
                    crit['blocking'] = st.checkbox("Key", key=f"bk_{i}", value=crit['blocking'], help="Use as Blocking Key for performance")
                
                with c_row[1]:
                    idx_a = st.session_state.cols_a.index(crit['col_a']) if crit['col_a'] in st.session_state.cols_a else 0
                    crit['col_a'] = st.selectbox(f"A_{i}", st.session_state.cols_a, key=f"ca_{i}", index=idx_a, label_visibility="collapsed")
                
                with c_row[2]:
                    # Auto mapping logic
                    s_idx = 0
                    if crit['col_a']: s_idx = get_auto_index(crit['col_a'], st.session_state.cols_b)
                    curr_b = crit['col_b'] if crit['col_b'] in st.session_state.cols_b else st.session_state.cols_b[s_idx]
                    curr_idx = st.session_state.cols_b.index(curr_b)
                    crit['col_b'] = st.selectbox(f"B_{i}", st.session_state.cols_b, key=f"cb_{i}", index=curr_idx, label_visibility="collapsed")
                
                with c_row[3]:
                    if crit['blocking']:
                        st.info("🔒 Blocking Key")
                    else:
                        c_sub1, c_sub2 = st.columns([1.5, 1])
                        with c_sub1:
                            raw_algo = st.selectbox(f"Alg_{i}", list(ALGO_OPTIONS.keys()), key=f"al_{i}", label_visibility="collapsed")
                            crit['algo'] = ALGO_OPTIONS[raw_algo]
                        with c_sub2:
                            crit['weight'] = st.number_input(f"W_{i}", 0.0, 10.0, float(crit['weight']), 0.5, key=f"w_{i}", label_visibility="collapsed")
                
                with c_row[4]:
                    if st.button("✕", key=f"del_{i}"): criteria_to_remove.append(i)

            if criteria_to_remove:
                for idx in sorted(criteria_to_remove, reverse=True): st.session_state.match_criteria.pop(idx)
                st.rerun()

            st.button("＋ Add Rule", on_click=lambda: st.session_state.match_criteria.append({'id': len(st.session_state.match_criteria)+1, 'col_a': None, 'col_b': None, 'clean_type': 'general', 'algo': 'token_sort', 'weight': 1.0, 'blocking': False}))

        # 3. SETTINGS & EXECUTION
        with st.expander("Advanced Settings", expanded=True):
            col_s1, col_s2, col_s3 = st.columns(3)
            
            with col_s1:
                st.markdown("**Thresholds**")
                threshold = st.slider("Match Acceptance (%)", 0, 100, 75)
                addr_override = st.slider("Address Override (%)", 90, 100, 100, help="Force 100% match if Address Score exceeds this value")
            
            with col_s2:
                st.markdown("**System**")
                max_cpu = multiprocessing.cpu_count()
                num_workers = st.slider("CPU Cores", 1, max_cpu, max(1, max_cpu - 1))
            
            with col_s3:
                st.markdown("**Output Config**")
                need_id_b = st.checkbox("Retrieve ID from File B")
                col_id_b = st.selectbox("ID Column", st.session_state.cols_b) if need_id_b else None
                show_details = st.checkbox("Include Score Details", value=True)

            st.markdown("<br>", unsafe_allow_html=True)
            start_btn = st.button("Run Matching Process", type="primary", use_container_width=True)

        # 4. PROCESSING LOGIC
        if start_btn:
            df_a = st.session_state.df_a
            df_b = st.session_state.df_b
            criteria = st.session_state.match_criteria
            
            # Setup
            blocking_config = [c for c in criteria if c['blocking']]
            cols_needed_b = [c['col_b'] for c in criteria]
            if need_id_b and col_id_b: cols_needed_b.append(col_id_b)
            
            status_container = st.status("Initializing...", expanded=True)
            
            # 1. Indexing
            status_container.write("Building Blocking Index...")
            b_blocking_map = {}
            if blocking_config:
                b_keys_series = []
                for cfg in blocking_config:
                    col_b_data = df_b[cfg['col_b']].astype(str).fillna("").apply(matcher.normalize_for_blocking)
                    b_keys_series.append(col_b_data)
                full_b_keys = ["_".join(parts) for parts in zip(*b_keys_series)]
                for idx, key in enumerate(full_b_keys):
                    if key not in b_blocking_map: b_blocking_map[key] = []
                    b_blocking_map[key].append(idx)
                status_container.write(f"✅ Index built ({len(b_blocking_map)} groups).")
            else:
                status_container.warning("⚠️ Full Scan Mode (No Blocking Key).")

            # 2. Parallel Processing
            status_container.write("Distributing workloads...")
            b_data_dict = {col: df_b[col].astype(str).fillna("").tolist() for col in set(cols_needed_b)}
            
            chunk_size = max(1, len(df_a) // (num_workers * 4))
            chunks = [df_a.iloc[i:i + chunk_size] for i in range(0, len(df_a), chunk_size)]
            
            map_args = [(chunk, b_data_dict, b_blocking_map, criteria, threshold, show_details, need_id_b, col_id_b, blocking_config, addr_override) for chunk in chunks]
            
            final_results_list = []
            progress_bar = status_container.progress(0)
            
            start_time = time.time()
            
            with concurrent.futures.ProcessPoolExecutor(max_workers=num_workers) as executor:
                futures = [executor.submit(worker_process_chunk, arg) for arg in map_args]
                for i, future in enumerate(concurrent.futures.as_completed(futures)):
                    try:
                        final_results_list.extend(future.result())
                        progress_bar.progress((i + 1) / len(chunks))
                    except Exception as e:
                        st.error(f"Worker Error: {e}")
            
            status_container.update(label=f"✅ Completed in {round(time.time() - start_time, 2)}s", state="complete", expanded=False)
            
            # 3. Final Output & Export
            if final_results_list:
                res_df = pd.DataFrame(final_results_list)
                res_df = res_df.set_index('_index_').sort_index()
                final_df = df_a.join(res_df)
                
                st.markdown("### Results")
                
                # Metrics
                matches_found = len(final_df[final_df['Matching_Score'] > 0])
                avg_score = final_df[final_df['Matching_Score'] > 0]['Matching_Score'].mean()
                
                m1, m2, m3 = st.columns(3)
                with m1: st.metric("Matches Found", f"{matches_found:,}")
                with m2: st.metric("Average Score", f"{avg_score:.1f}" if not pd.isna(avg_score) else "0")
                with m3: 
                    # Timestamped Filename Generation
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    file_name = f"Match_Result_{timestamp}.xlsx"
                    
                    buff = io.BytesIO()
                    with pd.ExcelWriter(buff, engine='openpyxl') as writer: final_df.to_excel(writer, index=False)
                    st.download_button(
                        label="📥 Download Excel Report", 
                        data=buff.getvalue(), 
                        file_name=file_name, 
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", 
                        type="primary", 
                        use_container_width=True
                    )
                
                st.dataframe(final_df.head(100), use_container_width=True, height=400)

if __name__ == "__main__":
    main()
