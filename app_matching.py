import sys
import streamlit as st
import pandas as pd
from rapidfuzz import fuzz
from unidecode import unidecode
import concurrent.futures
import multiprocessing
import re
from collections import defaultdict
import io
import time

# ================================
# Ensure Streamlit runtime
# ================================

def _ensure_streamlit():
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx
        if get_script_run_ctx() is None:
            print("\nRun with:\nstreamlit run matcher_app.py\n")
            sys.exit()
    except:
        pass

_ensure_streamlit()

# ================================
# PAGE CONFIG
# ================================

st.set_page_config(
    page_title="Professional Entity Resolution Engine",
    page_icon="⚡",
    layout="wide"
)

# ================================
# TEXT NORMALIZATION
# ================================

def base_clean(text):
    if not isinstance(text,str):
        text=str(text)
    text=unidecode(text).lower()
    text=re.sub(r"[^\w\s]"," ",text)
    return " ".join(text.split())

def clean_series(series):
    return (
        series
        .astype(str)
        .apply(base_clean)
    )

# ================================
# MATCH SCORE
# ================================

def score_pair(a,b):
    if not a or not b:
        return 0
    return fuzz.token_sort_ratio(a,b)

# ================================
# WORKER
# ================================

def worker(args):
    chunk_a, b_clean, blocking_map, threshold = args
    results = []
    
    for idx, row in chunk_a.iterrows():
        key = row["_block"]
        candidates = blocking_map.get(key, [])
        best_score = 0
        best_idx = -1
        val_a = row["_clean"]
        
        for i in candidates:
            val_b = b_clean[i]
            s = score_pair(val_a, val_b)
            if s > best_score:
                best_score = s
                best_idx = i
                
        if best_score >= threshold:
            results.append((idx, best_idx, best_score))
        else:
            results.append((idx, None, 0))
            
    return results

# ================================
# LOAD EXCEL
# ================================

@st.cache_data(show_spinner=False)
def load_excel(uploaded,sheet):
    df=pd.read_excel(uploaded,sheet_name=sheet,engine="openpyxl")
    df.columns=df.columns.astype(str)
    return df

# ================================
# UI
# ================================

st.title("⚡ Professional Entity Resolution Engine")
st.caption("Trình Khớp Dữ Liệu Chuyên Nghiệp - Version: v27.4")

col1,col2=st.columns(2)

with col1:
    file_a=st.file_uploader("File A")

with col2:
    file_b=st.file_uploader("File B")

if file_a and file_b:
    sheet_a=st.selectbox("Sheet A",pd.ExcelFile(file_a).sheet_names)
    sheet_b=st.selectbox("Sheet B",pd.ExcelFile(file_b).sheet_names)

    if st.button("Load Data",width="stretch"):
        df_a=load_excel(file_a,sheet_a)
        df_b=load_excel(file_b,sheet_b)

        st.session_state.df_a=df_a
        st.session_state.df_b=df_b
        st.success("Data loaded")

# ================================
# CONFIG
# ================================

if "df_a" in st.session_state:
    df_a=st.session_state.df_a
    df_b=st.session_state.df_b

    st.subheader("Configuration")

    colA=st.selectbox("Column A",df_a.columns)
    colB=st.selectbox("Column B",df_b.columns)

    blockA=st.selectbox("Blocking Column A",df_a.columns)
    blockB=st.selectbox("Blocking Column B",df_b.columns)

    threshold=st.slider("Match Threshold",0,100,80)

    cpu=st.slider(
        "CPU Workers",
        1,
        multiprocessing.cpu_count(),
        max(1,multiprocessing.cpu_count()-1)
    )

# ================================
# RUN MATCH
# ================================

    if st.button("START MATCHING",width="stretch"):
        start=time.time()

        st.write("Cleaning data...")
        df_a["_clean"]=clean_series(df_a[colA])
        df_b["_clean"]=clean_series(df_b[colB])

        df_a["_block"]=clean_series(df_a[blockA])
        df_b["_block"]=clean_series(df_b[blockB])

        st.write("Building blocking index...")
        blocking_map=defaultdict(list)

        for i,key in enumerate(df_b["_block"]):
            blocking_map[key].append(i)

        b_clean=df_b["_clean"].tolist()

        st.write("Splitting workload...")
        chunk_size=max(1000,len(df_a)//(cpu*8))
        chunks=[df_a.iloc[i:i+chunk_size] for i in range(0,len(df_a),chunk_size)]
        
        args=[
            (chunk,b_clean,blocking_map,threshold)
            for chunk in chunks
        ]

        results=[]
        progress=st.progress(0)

        with concurrent.futures.ThreadPoolExecutor(max_workers=cpu) as exe:
            futures=[exe.submit(worker,arg) for arg in args]
            done=0
            for f in concurrent.futures.as_completed(futures):
                results.extend(f.result())
                done+=1
                progress.progress(done/len(futures))

        st.write("Building result...")
        match_idx=[]
        scores=[]

        for idx,bidx,score in results:
            match_idx.append(bidx)
            scores.append(score)

        df_a["Matched_Index"]=match_idx
        df_a["Matching_Score"]=scores

        df_a["Matched_Value"]=df_a["Matched_Index"].apply(
            lambda x: df_b[colB].iloc[x] if x is not None else None
        )

        elapsed=round(time.time()-start,2)
        st.success(f"Done in {elapsed}s")

        st.dataframe(df_a.head(100),width="stretch")

# ================================
# EXPORT
# ================================

        buffer=io.BytesIO()
        with pd.ExcelWriter(buffer,engine="openpyxl") as writer:
            df_a.to_excel(writer,index=False)

        st.download_button(
            "Download Result",
            buffer.getvalue(),
            "matching_result_v27_4.xlsx",
            width="stretch"
        )
