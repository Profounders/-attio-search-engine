import streamlit as st
import re # Import Regex for smart highlighting
from supabase import create_client

# --- PAGE CONFIG ---
st.set_page_config(page_title="Attio Search", page_icon="🔍", layout="centered")

# --- CUSTOM CSS ---
st.markdown("""
<style>
    hr { margin-top: 0.5rem; margin-bottom: 0.5rem; opacity: 0.2; }
    a { text-decoration: none; color: #007bff !important; }
    a:hover { text-decoration: underline; }
    .streamlit-expanderHeader { font-size: 14px; color: #555; }
    /* specific style for the snippet text */
    .snippet-text { font-size: 14px; color: #333; line-height: 1.5; margin-bottom: 8px; }
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

# --- HELPER: GENERATE SNIPPET ---
def get_context_snippet(text, query, window=200):
    if not text: return ""
    
    # Clean text slightly (remove massive whitespaces)
    text = " ".join(text.split())
    
    # Find the search term (case insensitive)
    flags = re.IGNORECASE
    match = re.search(re.escape(query), text, flags)
    
    if match:
        start_idx = match.start()
        end_idx = match.end()
        
        # Calculate window
        start_cut = max(0, start_idx - window)
        end_cut = min(len(text), end_idx + window)
        
        snippet = text[start_cut:end_cut]
        
        # Add ellipses
        if start_cut > 0: snippet = "..." + snippet
        if end_cut < len(text): snippet = snippet + "..."
        
        # Highlight the term in the snippet
        # We replace the found term with **term**
        snippet = re.sub(f"({re.escape(query)})", r"**\1**", snippet, flags=re.IGNORECASE)
        
        return snippet
    else:
        # Fallback: If exact word not found (maybe fuzzy match), return start
        return text[:(window*2)] + "..."

# --- SEARCH INTERFACE ---
st.title("🔍 Search Attio")

query = st.text_input("Search", placeholder="Search people, companies, notes...", label_visibility="collapsed")

if query:
    try:
        response = supabase.table("attio_index") \
            .select("*") \
            .limit(100) \
            .text_search("fts", query) \
            .execute()
        
        results = response.data

        if not results:
            st.warning(f"No results found for '{query}'")
        else:
            st.caption(f"Found {len(results)} matches")
            
            for item in results:
                t = item.get('type', 'unknown')
                icon = "📄"
                if t == 'person': icon = "👤"
                elif t == 'company': icon = "🏢"
                elif t == 'note': icon = "📝"
                elif t == 'task': icon = "✅"
                elif t == 'comment': icon = "💬"

                with st.container():
                    # 1. TITLE
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
                    
                    # 2. METADATA
                    meta_info = t.upper()
                    if item.get("metadata") and item["metadata"].get("created_at"):
                        date_str = item["metadata"]["created_at"][:10]
                        meta_info += f" • {date_str}"
                    st.caption(meta_info)

                    # 3. CONTENT LOGIC
                    content = item.get('content') or ""
                    
                    # A. Generate the Snippet (200 chars before/after)
                    snippet = get_context_snippet(content, query, window=200)
                    
                    # Check if it's raw data (ugly) or text (nice)
                    if content.startswith("{'") or content.startswith('{"'):
                        st.info("Match found in Record Metadata")
                    else:
                        # Display the snippet directly
                        st.markdown(f'<div class="snippet-text">{snippet}</div>', unsafe_allow_html=True)

                    # B. Expander for Full Content
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

    except Exception as e:
        st.error(f"Search failed: {e}")
