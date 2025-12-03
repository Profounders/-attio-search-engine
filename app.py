import streamlit as st
from supabase import create_client

# --- PAGE CONFIG ---
st.set_page_config(page_title="Attio Search", page_icon="🔍", layout="centered")

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
    st.error("❌ Could not connect to Supabase. Please check your Secrets.")
    st.stop()

# --- SEARCH INTERFACE ---
st.title("🔍 Search Attio")

# Simple, big search bar (Label hidden for clean look)
query = st.text_input("Search", placeholder="Search people, companies, notes, tasks...", label_visibility="collapsed")

if query:
    try:
        # 1. Run Global Search (Limit to top 50 matches for speed)
        response = supabase.table("attio_index") \
            .select("*") \
            .text_search("fts", query) \
            .limit(50) \
            .execute()
        
        results = response.data

        # 2. Display Results
        if not results:
            st.warning(f"No results found for '{query}'")
        else:
            st.caption(f"Found {len(results)} matches")
            
            for item in results:
                # Assign Icons based on type
                t = item.get('type', 'unknown')
                icon = "📄"
                if t == 'person': icon = "👤"
                elif t == 'company': icon = "🏢"
                elif t == 'note': icon = "📝"
                elif t == 'task': icon = "✅"
                elif t == 'comment': icon = "💬"
                elif t in ['call', 'meeting', 'call_recording']: icon = "📞"

                # Render Result Card
                with st.container():
                    # Title & Link
                    url = item.get('url', '#')
                    title = item.get('title') or "Untitled"
                    
                    st.markdown(f"### [{icon} {title}]({url})")
                    
                    # Content Snippet
                    content = item.get('content') or ""
                    # Clean up raw dictionary strings if they look messy
                    if content.startswith("{'"): 
                        content = "Record Data match" 
                    
                    if len(content) > 280:
                        content = content[:280] + "..."
                    
                    st.markdown(f"**{t.upper()}** • {content}")
                    st.divider()

    except Exception as e:
        st.error(f"Search failed: {e}")
