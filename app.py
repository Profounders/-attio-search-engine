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
        # This was the line that caused the error:
        end_cut = min(len(text), end_idx + window)
        
        snippet = text[start_cut:end_cut]
        if start_cut > 0: snippet = "..." + snippet
        if end_cut < len(text): snippet = snippet + "..."
        
        # Highlight all query terms
        for word in words:
            if len(word) > 1:
                snippet = re.sub(f"({re.escape(word)})", r"**\1**", snippet, flags=re.IGNORECASE)
        return snippet
    else:
        return text[:(window*2)] + "..."

# --- UI START ---
st.title("🔍 Search Attio")

# 1. SEARCH BAR
query = st.text_input("Search", placeholder='Try "Client Name" -draft ...', label_visibility="collapsed")
st.caption("Tip: Use quotes for \"exact phrases\", minus for -exclusion, and OR for multiple options.")

# 2. FILTER TOGGLES
available_types = ["person", "company", "note", "task", "call_recording", "comment", "list", "email"]
selected_types = st.multiselect("Filter Types", options=available_types, default=available_types)

if query:
    if not selected_types:
        st.warning("⚠️ Please select at least one filter.")
    else:
        results = []
        search_error = None
        
        # --- ROBUST SEARCH LOGIC (HYBRID) ---
        try:
            # ATTEMPT 1: ADVANCED (Google-Style)
            req = supabase.table("attio_index").select("*")
            req = req.in_("type", selected_types)
            req = req.limit(100)
            req = req.text_search("fts", query, options={"type": "websearch", "config": "english"})
            results = req.execute().data
            
        except Exception:
            # ATTEMPT 2: FALLBACK (Simple/Plain)
            try:
                req = supabase.table("attio_index").select("*")
                req = req.in_("type", selected_types)
                req = req.limit(100)
                req = req.text_search("fts", query, options={"type": "plain", "config": "english"})
                results = req.execute().data
            except Exception as e:
                search_error = e

        # --- DISPLAY RESULTS ---
        if search_error:
            st.error(f"Search failed: {search_error}")
        elif not results:
            st.warning(f"No results found for '{query}'")
        else:
            st.caption(f"Found {len(results)} matches")
            
            for item in results:
                t = item.get('type', 'unknown')
                
                # Icons
                icon = "📄"
                if t == 'person': icon = "👤"
                elif t == 'company': icon = "🏢"
                elif t == 'note': icon = "📝"
                elif t == 'task': icon = "✅"
                elif t == 'comment': icon = "💬"
                elif t == 'call_recording': icon = "📞"
                elif t == 'email': icon = "📧"

                with st.container():
                    # 1. Title
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
                    
                    # 2. Metadata
                    meta_info = t.upper()
                    if item.get("metadata") and item["metadata"].get("created_at"):
                        date_str = item["metadata"]["created_at"][:10]
                        meta_info += f" • {date_str}"
                    st.caption(meta_info)

                    # 3. Content
                    content = item.get('content') or ""
                    
                    # Generate Snippet
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
