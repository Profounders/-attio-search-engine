import streamlit as st
from supabase import create_client

# --- PAGE CONFIG ---
st.set_page_config(page_title="Attio Search", page_icon="🔍", layout="centered")

# --- CUSTOM CSS FOR COMPACT RESULTS ---
st.markdown("""
<style>
    /* Make the divider lines subtle */
    hr { margin-top: 0.5rem; margin-bottom: 0.5rem; opacity: 0.2; }
    /* Tighten container spacing */
    .block-container { padding-top: 2rem; }
    /* Link styling */
    a { text-decoration: none; color: #007bff !important; }
    a:hover { text-decoration: underline; }
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

# --- SEARCH INTERFACE ---
st.title("🔍 Search Attio")

query = st.text_input("Search", placeholder="Search people, companies, notes...", label_visibility="collapsed")

if query:
    try:
        # 1. LIMIT INCREASED TO 500
        # Note: We apply limit() BEFORE text_search() to prevent library errors.
        response = supabase.table("attio_index") \
            .select("*") \
            .limit(500) \
            .text_search("fts", query) \
            .execute()
        
        results = response.data

        if not results:
            st.warning(f"No results found for '{query}'")
        else:
            st.caption(f"Found {len(results)} matches")
            
            for item in results:
                # Assign Icons
                t = item.get('type', 'unknown')
                icon = "📄"
                if t == 'person': icon = "👤"
                elif t == 'company': icon = "🏢"
                elif t == 'note': icon = "📝"
                elif t == 'task': icon = "✅"
                elif t == 'comment': icon = "💬"
                elif t in ['call', 'meeting', 'call_recording']: icon = "📞"

                with st.container():
                    # --- 1. TITLE (18px Bold Link) ---
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
                    
                    # --- 2. METADATA (Small Caption) ---
                    meta_info = t.upper()
                    if item.get("metadata") and item["metadata"].get("created_at"):
                        date_str = item["metadata"]["created_at"][:10] # Grab YYYY-MM-DD
                        meta_info += f" • {date_str}"
                    
                    st.caption(meta_info)

                    # --- 3. CONTENT (14px Readable Body) ---
                    content = item.get('content') or ""
                    
                    # Clean up raw data dumps
                    if content.startswith("{'") or content.startswith('{"'):
                        content = "Record details matched."
                    
                    # Truncate to 300 chars
                    if len(content) > 300:
                        content = content[:300] + "..."
                    
                    st.markdown(
                        f"""
                        <div style="font-size: 14px; line-height: 1.5; color: inherit; opacity: 0.9;">
                            {content}
                        </div>
                        """, 
                        unsafe_allow_html=True
                    )
                    
                    st.divider()

    except Exception as e:
        st.error(f"Search failed: {e}")
