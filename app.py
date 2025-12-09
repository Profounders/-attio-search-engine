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
    .snippet-text { font-size: 14px; color: #333; line-height: 1.6; margin-bottom: 8px; font-family: sans-serif; }
    
    div[data-testid="stMultiSelect"] label { display: none; }
    
    .stMultiSelect span[data-baseweb="tag"] {
        background-color: #d1e7dd !important; 
        border: 1px solid #a3cfbb !important; 
    }
    .stMultiSelect span[data-baseweb="tag"] span { color: #0a3622 !important; }
    .stMultiSelect span[data-baseweb="tag"] svg { fill: #0a3622 !important; color: #0a3622 !important; }
    
    /* Pagination Buttons */
    div[data-testid="stButton"] button {
        width: 100%;
    }
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
    
    clean_query = re.sub(r'[^\w\s]', '', query).strip()
    words = clean_query.split()
    
    flags = re.IGNORECASE
    match = None
    matched_word = ""

    for word in words:
        if len(word) > 1:
            match = re.search(re.escape(word), text, flags)
            if match: 
                matched_word = word
                break
    
    if not match:
        suffixes = ['ation', 'tional', 'tion', 'sion', 'ment', 'ing', 'ed', 'es', 's', 'al']
        for word in words:
            if len(word) > 4: 
                root = word
                for suffix in suffixes:
                    if root.endswith(suffix):
                        root = root[:-len(suffix)]
                        break
                if len(root) >= 3:
                    match = re.search(re.escape(root), text, flags)
                    if match:
                        matched_word = root
                        break

    if match:
        start_idx = match.start()
        end_idx = match.end()
        start_cut = max(0, start_idx - window)
        end_cut = min(len(text), end_idx + window)
        
        snippet = text[start_cut:end_cut]
        if start_cut > 0: snippet = "..." + snippet
        if end_cut < len(text): snippet = snippet + "..."
        
        highlight_terms = [re.escape(w) for w in words if len(w) > 1]
        if matched_word and matched_word not in words:
            highlight_terms.append(re.escape(matched_word))
            
        pattern = "|".join(highlight_terms)
        highlight_style = "background-color: #ffd700; color: black; padding: 0 4px; border-radius: 3px; font-weight: bold; box-shadow: 0 1px 2px rgba(0,0,0,0.1);"
        
        snippet = re.sub(
            f"({pattern}\w*)", 
            fr'<span style="{highlight_style}">\1</span>', 
            snippet, 
            flags=re.IGNORECASE
        )
        return snippet
    else:
        return text[:(window*2)] + "..."

# --- SESSION STATE FOR PAGINATION ---
if 'page' not in st.session_state:
    st.session_state.page = 0
if 'last_query' not in st.session_state:
    st.session_state.last_query = ""

# --- UI START ---
st.title("🔍 Search Attio")

# 1. SEARCH BAR
query = st.text_input("Search", placeholder='Try "Client Name" -draft ...', label_visibility="collapsed")
st.caption("Tip: Use quotes for \"exact phrases\", minus for -exclusion, and OR for multiple options.")

# 2. FILTER TOGGLES
available_types = ["person", "company", "note", "task", "call_recording", "comment", "list", "email"]
selected_types = st.multiselect("Filter Types", options=available_types, default=available_types)

# RESET PAGINATION IF QUERY CHANGES
if query != st.session_state.last_query:
    st.session_state.page = 0
    st.session_state.last_query = query

PAGE_SIZE = 50

if query:
    if not selected_types:
        st.warning("⚠️ Please select at least one filter.")
    else:
        results = []
        count = 0
        search_error = None
        
        # Calculate Range
        start = st.session_state.page * PAGE_SIZE
        end = start + PAGE_SIZE - 1
        
        # --- QUERY EXECUTION ---
        try:
            # We use select("*", count="exact") to get the total number of matches
            req = supabase.table("attio_index").select("*", count="exact")
            req = req.in_("type", selected_types)
            
            # Hybrid Search Logic (Websearch -> Plain Fallback)
            try:
                # Attempt 1: Advanced
                base_req = req.text_search("fts", query, options={"type": "websearch", "config": "english"})
                response = base_req.range(start, end).execute()
            except:
                # Attempt 2: Plain Fallback
                base_req = req.text_search("fts", query, options={"type": "plain", "config": "english"})
                response = base_req.range(start, end).execute()
            
            results = response.data
            count = response.count # Total matches in DB
            
        except Exception as e:
            search_error = e

        # --- DISPLAY RESULTS ---
        if search_error:
            st.error(f"Search failed: {search_error}")
        elif count == 0:
            st.warning(f"No results found for '{query}'")
        else:
            # Stats Header
            st.info(f"Found **{count}** matches. Showing {start+1}-{min(end+1, count)}.")
            
            for item in results:
                t = item.get('type', 'unknown')
                
                icon = "📄"
                if t == 'person': icon = "👤"
                elif t == 'company': icon = "🏢"
                elif t == 'note': icon = "📝"
                elif t == 'task': icon = "✅"
                elif t == 'comment': icon = "💬"
                elif t == 'call_recording': icon = "📞"
                elif t == 'email': icon = "📧"

                with st.container():
                    url = item.get('url', '#')
                    title = item.get('title') or "Untitled"
                    
                    st.markdown(
                        f"""
                        <div style="font-size: 18px; font-weight: 600; margin-bottom: 2px;">
                            <a href="{url}" target="_blank">{icon} {title}</a>
                        </div>
                        """, 
                        unsafe_allow_html=True
                    )
                    
                    meta_info = t.upper()
                    if item.get("metadata") and item["metadata"].get("created_at"):
                        date_str = item["metadata"]["created_at"][:10]
                        meta_info += f" • {date_str}"
                    st.caption(meta_info)

                    content = item.get('content') or ""
                    clean_query = re.sub(r'[^\w\s]', '', query).strip() 
                    snippet = get_context_snippet(content, clean_query, window=200)

                    if content.startswith("{'") or content.startswith('{"'):
                            st.info("Match found in Record Metadata")
                    else:
                            st.markdown(f'<div class="snippet-text">{snippet}</div>', unsafe_allow_html=True)

                    with st.expander("View Full Content", expanded=False):
                        if content.startswith("{'") or content.startswith('{"'):
                                st.code(content, language="json")
                        else:
                                st.markdown(f"""<div style="font-size: 14px; white-space: pre-wrap;">{content}</div>""", unsafe_allow_html=True)
                    
                    st.divider()

            # --- PAGINATION CONTROLS ---
            if count > PAGE_SIZE:
                col1, col2, col3 = st.columns([1, 2, 1])
                
                with col1:
                    if st.session_state.page > 0:
                        if st.button("⬅️ Previous"):
                            st.session_state.page -= 1
                            st.rerun()
                
                with col3:
                    if end < count:
                        if st.button("Next ➡️"):
                            st.session_state.page += 1
                            st.rerun()
