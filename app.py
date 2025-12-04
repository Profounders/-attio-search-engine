import streamlit as st
import re
from supabase import create_client

# --- PAGE CONFIG ---
st.set_page_config(page_title="Attio Search", page_icon="🔍", layout="centered")

# --- CUSTOM CSS ---
st.markdown("""
<style>
    .block-container { padding-top: 2rem; }
    hr { margin-top: 0.5rem; margin-bottom: 0.5rem; opacity: 0.2; }
    a { text-decoration: none; color: #007bff !important; }
    a:hover { text-decoration: underline; }
    .snippet-text { font-size: 14px; color: #333; line-height: 1.5; margin-bottom: 8px; }
    
    /* Hide the default label for the multiselect */
    div[data-testid="stMultiSelect"] label { display: none; }
    
    /* Green Tags */
    .stMultiSelect span[data-baseweb="tag"] {
        background-color: #d1e7dd !important; 
        border: 1px solid #a3cfbb !important; 
    }
    .stMultiSelect span[data-baseweb="tag"] span { color: #0a3622 !important; }
    .stMultiSelect span[data-baseweb="tag"] svg { fill: #0a3622 !important; color: #0a3622 !important; }
</style>
""", unsafe_allow_html=True)

# --- DATABASE CONNECTION ---
@st.cache_resource
def init_connection():
    try:
        url = st.secrets["SUPABASE_URL"]
        key = st.secrets["SUPABASE_KEY"]
        return create_client(url, key)
    except Exception as e:
        return None

supabase = init_connection()

if not supabase:
    st.error("❌ Could not connect to Supabase. Check Secrets.")
    st.stop()

# --- HELPER: SNIPPET GENERATOR ---
def get_context_snippet(text, query, window=200):
    if not text: return ""
    text = " ".join(text.split())
    
    # Clean query for regex highlighting
    clean_query = re.sub(r'[^\w\s]', '', query).strip()
    words = clean_query.split()
    
    # Try to find the first matching word from the query
    flags = re.IGNORECASE
    match = None
    
    for word in words:
        if len(word) > 1: # Ignore single letters
            match = re.search(re.escape(word), text, flags)
            if match: break
            
    if match:
        start_idx = match.start()
        end_idx = match.end()
        start_cut = max(0, start_idx - window)
        end_cut = min(len(text), end_idx + win
