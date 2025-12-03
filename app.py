import streamlit as st
from supabase import create_client

# Page Setup
st.set_page_config(page_title="Attio Search", layout="wide")
st.title("🔍 Attio Search Engine")

# ... imports ...
st.title("🔍 Attio Search Engine")

# --- DEBUG SECTION ---
try:
    # Get a simple count of all rows
    response = supabase.table("attio_index").select("id", count="exact").execute()
    total_count = response.count
    st.metric(label="Total Records in Database", value=total_count)
    
    if total_count > 0:
        # Show the most recent 3 items to verify data quality
        st.caption("Most recent updates:")
        recent = supabase.table("attio_index").select("title, type, created_at").order("created_at", desc=True).limit(3).execute()
        st.table(recent.data)
    else:
        st.error("The database is empty. The Sync Script ran, but uploaded 0 items.")
except Exception as e:
    st.error(f"Database Check Failed: {e}")
# --- END DEBUG ---

# ... rest of your code ...

# Connect DB
@st.cache_resource
def init_connection():
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)

try:
    supabase = init_connection()
except Exception as e:
    st.error(f"Could not connect to database. Check Secrets. Error: {e}")
    st.stop()

# Sidebar
st.sidebar.title("Filters")
type_filter = st.sidebar.multiselect(
    "Data Types",
    ["person", "company", "note", "task", "comment", "call_recording", "list"],
    default=["person", "company", "note"]
)

# Search Input
query = st.text_input("Search...", placeholder="Type query here (e.g. 'Project Alpha')")

if query:
    # 1. Start the query builder
    req = supabase.table("attio_index").select("*")
    
    # 2. Apply Filters FIRST (Chain safer)
    if type_filter:
        req = req.in_("type", type_filter)
    
    # 3. Apply Search LAST
    req = req.text_search("fts", query)
        
    # 4. Execute
    try:
        results = req.execute().data
        
        st.markdown(f"**Found {len(results)} results**")
        
        # 5. Display Results (Indented correctly now)
        for item in results:
            with st.container():
                emoji = "📄"
                if item['type'] == 'person': emoji = "👤"
                if item['type'] == 'company': emoji = "🏢"
                if item['type'] == 'call_recording': emoji = "📞"
                if item['type'] == 'task': emoji = "✅"
                if item['type'] == 'note': emoji = "📝"
                
                # Title and Link
                st.subheader(f"{emoji} {item['title']}")
                url = item.get('url', '#')
                st.markdown(f"**Type:** {item['type'].upper()} | [View in Attio]({url})")
                
                # Content Preview (Handle None values safely)
                content = item.get('content') or ""
                preview = content[:300] + "..." if len(content) > 300 else content
                st.info(preview)
                
                st.divider()

    except Exception as e:
        st.error(f"Search failed: {e}")
