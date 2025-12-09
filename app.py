import streamlit as st
import re
from datetime import datetime
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
    
    .stMultiSelect span[data-baseweb="tag"] { background-color: #d1e7dd !important; border: 1px solid #a3cfbb !important; }
    .stMultiSelect span[data-baseweb="tag"] span { color: #0a3622 !important; }
    .stMultiSelect span[data-baseweb="tag"] svg { fill: #0a3622 !important; color: #0a3622 !important; }
</style>
""", unsafe_allow_html=True)

# --- DATABASE ---
@st.cache_resource
def init_connection():
    try:
        return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])
    except: return None

supabase = init_connection()
if not supabase: st.stop()

# --- HELPER: RELEVANCE SORTING ---
def calculate_relevance(item, query):
    content = (item.get('content') or "").lower()
    title = (item.get('title') or "").lower()
    q = query.lower().strip()
    score = title.count(q) * 10 + content.count(q)
    return score

# --- HELPER: SNIPPET ---
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
        highlight_style = "background-color: #ffd700; color: black; padding: 0 4px; border-radius: 3px; font-weight: bold;"
        
        snippet = re.sub(f"({pattern}\w*)", fr'<span style="{highlight_style}">\1</span>', snippet, flags=re.IGNORECASE)
        return snippet
    else:
        return text[:(window*2)] + "..."

# --- UI ---
st.title("🔍 Search Attio")

query = st.text_input("Search", placeholder='Try "Client Name" -draft ...', label_visibility="collapsed")
st.caption("Tip: Use quotes for \"exact phrases\", minus for -exclusion, and OR for multiple options.")

# --- FILTERS ROW ---
col1, col2 = st.columns([3, 1])
with col1:
    available_types = ["person", "company", "note", "task", "call_recording", "comment", "list", "email"]
    selected_types = st.multiselect("Filter Types", options=available_types, default=available_types)

# --- DATE FILTER ---
with st.expander("📅 Filter by Date"):
    d_col1, d_col2 = st.columns(2)
    start_date = d_col1.date_input("Start Date", value=None)
    end_date = d_col2.date_input("End Date", value=None)

if query:
    if not selected_types:
        st.warning("⚠️ Please select at least one filter.")
    else:
        try:
            # 1. Fetch Candidates
            req = supabase.table("attio_index").select("*")
            req = req.in_("type", selected_types)
            req = req.limit(500) # Fetch pool
            
            try:
                # Advanced
                req_web = req.text_search("fts", query, options={"type": "websearch", "config": "english"})
                results = req_web.execute().data
            except:
                # Fallback
                req_plain = req.text_search("fts", query, options={"type": "plain", "config": "english"})
                results = req_plain.execute().data
            
            # 2. DATE FILTERING (Python Side)
            filtered_results = []
            if start_date or end_date:
                for item in results:
                    meta = item.get("metadata") or {}
                    date_str = meta.get("created_at") or meta.get("start")
                    
                    if date_str:
                        try:
                            # Parse ISO format (YYYY-MM-DD...)
                            item_date = datetime.fromisoformat(date_str.replace("Z", "+00:00")).date()
                            
                            # Apply Filter
                            if start_date and item_date < start_date: continue
                            if end_date and item_date > end_date: continue
                            
                            filtered_results.append(item)
                        except:
                            filtered_results.append(item) # Keep if date parse fails
                    else:
                        filtered_results.append(item) # Keep if no date
                results = filtered_results

            # 3. SORTING
            if results:
                clean_q = re.sub(r'[^\w\s]', '', query)
                results.sort(key=lambda x: calculate_relevance(x, clean_q), reverse=True)

            # --- DISPLAY ---
            st.caption(f"Found {len(results)} matches")
            
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
                        """, unsafe_allow_html=True
                    )
                    
                    meta_info = t.upper()
                    if item.get("metadata") and item["metadata"].get("created_at"):
                        date_str = item["metadata"]["created_at"][:10]
                        meta_info += f" • {date_str}"
                    st.caption(meta_info)

                    content = item.get('content') or ""
                    clean_query_display = re.sub(r'[^\w\s]', '', query).strip() 
                    snippet = get_context_snippet(content, clean_query_display, window=200)

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

        except Exception as e:
            st.error(f"Search failed: {e}")
