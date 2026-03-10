import streamlit as st
import re
from supabase import create_client

# --- PAGE CONFIG ---
st.set_page_config(page_title="Notes Search", page_icon="📝", layout="centered")

st.markdown("""
<style>
    .block-container { padding-top: 2rem; }
    hr { margin-top: 0.5rem; margin-bottom: 0.5rem; opacity: 0.2; }
    a { text-decoration: none; color: #007bff !important; font-size: 18px; font-weight: 600;}
    a:hover { text-decoration: underline; }
    .snippet-text { font-size: 14px; color: #333; line-height: 1.6; margin-bottom: 8px; }
</style>
""", unsafe_allow_html=True)

# --- DB CONNECTION ---
@st.cache_resource
def init_connection():
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])

supabase = init_connection()

# --- HIGHLIGHT HELPER (FUZZY VISUALS) ---
def get_highlighted_snippet(text, query, window=200):
    if not text: return ""
    text = " ".join(text.split())
    
    clean_query = re.sub(r'[^\w\s]', '', query).strip()
    words =[w for w in clean_query.split() if len(w) > 2]
    if not words: return text[:window] + "..."

    # Find first match using a root/stem
    match = None
    for word in words:
        root = word
        # Strip common suffixes for the visual highlighter
        for s in['s', 'es', 'ed', 'ing', 'tion']:
            if root.endswith(s) and len(root) > 4:
                root = root[:-len(s)]
                break
        
        match = re.search(re.escape(root), text, re.IGNORECASE)
        if match: break

    # Create Snippet Window
    if match:
        start = max(0, match.start() - window)
        end = min(len(text), match.end() + window)
        snippet = ("..." if start > 0 else "") + text[start:end] + ("..." if end < len(text) else "")
    else:
        snippet = text[:window*2] + "..."

    # Highlight Words (Yellow)
    roots = []
    for w in words:
        r = w
        for s in['s', 'es', 'ed', 'ing', 'tion']:
            if r.endswith(s) and len(r) > 4:
                r = r[:-len(s)]
                break
        roots.append(r)
        
    pattern = "|".join([re.escape(r) for r in roots])
    style = "background-color: #ffd700; color: black; padding: 0 2px; border-radius: 3px; font-weight: bold;"
    snippet = re.sub(f"({pattern}\w*)", fr'<span style="{style}">\1</span>', snippet, flags=re.IGNORECASE)
    
    return snippet

# --- UI ---
st.title("📝 Attio Notes Search")
query = st.text_input("Search", placeholder="Search your notes...", label_visibility="collapsed")

if query:
    try:
        # THE FIX: .limit() is now BEFORE .text_search()
        try:
            # 1. Try Advanced Google-Style Search
            res = supabase.table("attio_notes").select("*", count="exact") \
                .limit(100) \
                .text_search("fts", query, options={"type": "websearch", "config": "english"}) \
                .execute()
        except:
            # 2. Fallback to standard fuzzy search if advanced syntax fails
            res = supabase.table("attio_notes").select("*", count="exact") \
                .limit(100) \
                .text_search("fts", query, options={"type": "plain", "config": "english"}) \
                .execute()
                
        results = res.data
        count = res.count

        if count == 0:
            st.warning(f"No notes found for '{query}'")
        else:
            st.caption(f"Found {count} notes matching your search.")
            
            for item in results:
                with st.container():
                    # Title & Link
                    st.markdown(f"""<a href="{item['url']}" target="_blank">📄 {item['title']}</a>""", unsafe_allow_html=True)
                    
                    # Date
                    if item.get('created_at'):
                        st.caption(item['created_at'][:10])

                    # Snippet
                    content = item.get('content', '')
                    snippet = get_highlighted_snippet(content, query)
                    st.markdown(f'<div class="snippet-text">{snippet}</div>', unsafe_allow_html=True)

                    # Full Content Expander
                    with st.expander("View Full Note"):
                        st.markdown(f"""<div style="font-size: 14px; white-space: pre-wrap;">{content}</div>""", unsafe_allow_html=True)
                    
                    st.divider()

    except Exception as e:
        # This will now print the exact error if it ever fails again
        st.error(f"Search failed: {e}")
