import streamlit as st
import re
from supabase import create_client

# --- PAGE CONFIG ---
st.set_page_config(page_title="Attio Search", page_icon="🔍", layout="centered")

# --- CUSTOM CSS ---
st.markdown("""
<style>
    /* Spacing tweaks */
    .block-container { padding-top: 2rem; }
    hr { margin-top: 0.5rem; margin-bottom: 0.5rem; opacity: 0.2; }
    
    /* Link styling */
    a { text-decoration: none; color: #007bff !important; }
    a:hover { text-decoration: underline; }
    
    /* Snippet text style */
    .snippet-text { font-size: 14px; color: #333; line-height: 1.5; margin-bottom: 8px; }
    
    /* Hide the default label for the multiselect */
    div[data-testid="stMultiSelect"] label { display: none; }

    /* --- GREEN TAGS CSS --- */
    
    /* 1. Target the Tag Background & Border */
    .stMultiSelect span[data-baseweb="tag"] {
        background-color: #d1e7dd !important; /* Light Green Background */
        border: 1px solid #a3cfbb !important; /* Green Border */
    }

    /* 2. Target the Text inside the tag */
    .stMultiSelect span[data-baseweb="tag"] span {
        color: #0a3622 !important; /* Dark Green Text */
    }

    /* 3. Target the 'X' icon */
    .stMultiSelect span[data-baseweb="tag"] svg {
        fill: #0a3622 !important; /* Dark Green Icon */
        color: #0a3622 !important;
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
    text = " ".join(text.split()) # Clean whitespace
    
    flags = re.IGNORECASE
    match = re.search(re.escape(query), text, flags)
    
    if match:
        start_idx = match.start()
        end_idx = match.end()
        start_cut = max(0, start_idx - window)
        end_cut = min(len(text), end_idx + window)
        
        snippet = text[start_cut:end_cut]
        if start_cut > 0: snippet = "..." + snippet
        if end_cut < len(text): snippet = snippet + "..."
        
        # Highlight term
        snippet = re.sub(f"({re.escape(query)})", r"**\1**", snippet, flags=re.IGNORECASE)
        return snippet
    else:
        return text[:(window*2)] + "..."

# --- UI START ---
st.title("🔍 Search Attio")

# 1. SEARCH BAR
query = st.text_input("Search", placeholder="Type keywords...", label_visibility="collapsed")

# 2. FILTER TOGGLES (Green Tags)
available_types = ["person", "company", "note", "task", "comment", "list"]

selected_types = st.multiselect(
    "Filter Types", 
    options=available_types, 
    default=available_types,
    help="Remove tags to hide specific data types."
)

if query:
    if not selected_types:
        st.warning("⚠️ Please select at least one data type to search.")
    else:
        try:
            # 3. BUILD QUERY
            req = supabase.table("attio_index").select("*")
            req = req.in_("type", selected_types) # Filter
            req = req.limit(100) # Limit
            req = req.text_search("fts", query) # Search
            
            results = req.execute().data

            if not results:
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

                    with st.container():
                        # Title
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
                        
                        # Metadata
                        meta_info = t.upper()
                        if item.get("metadata") and item["metadata"].get("created_at"):
                            date_str = item["metadata"]["created_at"][:10]
                            meta_info += f" • {date_str}"
                        st.caption(meta_info)

                        # Content Snippet
                        content = item.get('content') or ""
                        snippet = get_context_snippet(content, query, window=200)

                        if content.startswith("{'") or content.startswith('{"'):
                             st.info("Match found in Record Metadata")
                        else:
                             st.markdown(f'<div class="snippet-text">{snippet}</div>', unsafe_allow_html=True)

                        # Full Content Expander
                        with st.expander("View Full Content", expanded=False):
                            if content.startswith("{'") or content.startswith('{"'):
                                 st.code(content, language="json")
                            else:
                                 st.markdown(
                                    f"""<div style="font-size: 14px; white-space: pre-wrap;">{content}</div>""", 
                                    unsafe_allow_html=True
                                 )
                        
                        st.divider()

        except Exception as e:
            st.error(f"Search failed: {e}")
