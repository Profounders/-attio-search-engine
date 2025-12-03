import streamlit as st
from supabase import create_client

# Page Setup
st.set_page_config(page_title="Attio Search", layout="wide")
st.title("🔍 Attio Search Engine")

# Connect DB
@st.cache_resource
def init_connection():
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)

supabase = init_connection()

# Sidebar
st.sidebar.title("Filters")
type_filter = st.sidebar.multiselect(
    "Data Types",
    ["person", "company", "note", "task", "comment", "call_recording", "list"],
    default=["person", "company", "note"]
)

# Search
query = st.text_input("Search...", placeholder="Type query here")

if query:
    # 1. Start the query builder
    req = supabase.table("attio_index").select("*")
    
    # 2. Apply Filters FIRST (Chain safer)
    if type_filter:
        req = req.in_("type", type_filter)
    
    # 3. Apply Search LAST
    req = req.text_search("fts", query)
        
    # 4. Execute
    results = req.execute().data
    
    st.markdown(f"**Found {len(results)} results**")
    
    for item in results:
        # ... rest of the code is fine ...
    st.markdown(f"**Found {len(results)} results**")
    
    for item in results:
        with st.container():
            emoji = "📄"
            if item['type'] == 'person': emoji = "👤"
            if item['type'] == 'company': emoji = "🏢"
            if item['type'] == 'call_recording': emoji = "📞"
            if item['type'] == 'task': emoji = "✅"
            
            st.subheader(f"{emoji} {item['title']}")
            st.markdown(f"**Type:** {item['type']} | [View in Attio]({item['url']})")
            
            # Show preview of content
            preview = item['content'][:300] + "..." if len(item['content']) > 300 else item['content']
            st.info(preview)
            st.divider()
