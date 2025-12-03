import streamlit as st
from supabase import create_client

# --- 1. PAGE CONFIG ---
st.set_page_config(page_title="Attio Search", layout="wide")

# --- 2. CONNECT TO SUPABASE ---
@st.cache_resource
def init_connection():
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)

try:
    # Initialize the client BEFORE using it
    supabase = init_connection()
except Exception as e:
    st.error(f"❌ Could not connect to database. Check Streamlit Secrets. Error: {e}")
    st.stop()

# --- 3. TITLE & DEBUG SECTION ---
st.title("🔍 Attio Search Engine")

# This section checks if data exists. 
# It will disappear automatically if you want, or you can keep it to monitor sync status.
try:
    # Get a quick count
    count_response = supabase.table("attio_index").select("id", count="exact").execute()
    total_count = count_response.count
    
    if total_count == 0:
        st.error("⚠️ Database is connected but EMPTY. The Sync Script hasn't uploaded any data yet.")
    else:
        st.caption(f"✅ Database Status: Online | {total_count} records indexed.")
        
except Exception as e:
    st.warning(f"Could not verify database stats: {e}")


# --- 4. SIDEBAR FILTERS ---
st.sidebar.title("Filters")
type_filter = st.sidebar.multiselect(
    "Data Types",
    # These are common Attio types. The sync script might add more dynamically.
    ["person", "company", "note", "task", "comment", "call_recording", "list", "deal", "meeting"],
    default=["person", "company", "note"]
)

# --- 5. SEARCH LOGIC ---
query = st.text_input("Search...", placeholder="Type query here (e.g. 'Project Alpha')")

if query:
    # A. Start Query
    req = supabase.table("attio_index").select("*")
    
    # B. Apply Filters FIRST
    if type_filter:
        req = req.in_("type", type_filter)
    
    # C. Apply Text Search LAST
    req = req.text_search("fts", query)
        
    # D. Execute
    try:
        results = req.execute().data
        
        st.markdown(f"**Found {len(results)} results**")
        
        # E. Render Results
        for item in results:
            with st.container():
                # Dynamic Icons
                t = item['type']
                emoji = "📄"
                if t == 'person': emoji = "👤"
                elif t == 'company': emoji = "🏢"
                elif t == 'note': emoji = "📝"
                elif t == 'task': emoji = "✅"
                elif t in ['call_recording', 'meeting', 'call']: emoji = "📞"
                elif t == 'deal': emoji = "💰"
                
                # Title & Link
                st.subheader(f"{emoji} {item['title']}")
                url = item.get('url', '#')
                st.markdown(f"**Type:** {t.upper()} | [View in Attio]({url})")
                
                # Content Preview
                content = item.get('content') or ""
                # clean up brackets if it's raw data
                if len(content) > 300:
                    preview = content[:300] + "..."
                else:
                    preview = content
                
                st.info(preview)
                st.divider()

    except Exception as e:
        st.error(f"Search failed: {e}")
