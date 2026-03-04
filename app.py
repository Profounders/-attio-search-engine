import streamlit as st
import re
import html
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
    except Exception:
        return None

supabase = init_connection()
if not supabase:
    st.error("❌ Could not connect to Supabase. Check Secrets.")
    st.stop()

# --- HELPER: RELEVANCE SORTING ---
def calculate_relevance(item, query):
    content = (item.get("content") or "").lower()
    title = (item.get("title") or "").lower()
    q = query.lower().strip()
    if not q:
        return 0
    score = title.count(q) * 10 + content.count(q)
    return score

# --- HELPER: SNIPPET ---
def get_context_snippet(text, query, window=200):
    if not text:
        return ""

    # Normalize whitespace
    text = " ".join(text.split())
    clean_query = re.sub(r"[^\w\s]", "", query).strip()
    words = clean_query.split()

    flags = re.IGNORECASE
    match = None
    matched_word = ""

    # Try exact word match first
    for word in words:
        if len(word) > 1:
            match = re.search(re.escape(word), text, flags)
            if match:
                matched_word = word
                break

    # Try crude stemming fallback
    if not match:
        suffixes = ["ation", "tional", "tion", "sion", "ment", "ing", "ed", "es", "s", "al"]
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

    if not match:
        return html.escape(text[: (window * 2)]) + ("..." if len(text) > (window * 2) else "")

    start_idx = match.start()
    end_idx = match.end()
    start_cut = max(0, start_idx - window)
    end_cut = min(len(text), end_idx + window)

    snippet = text[start_cut:end_cut]
    if start_cut > 0:
        snippet = "..." + snippet
    if end_cut < len(text):
        snippet = snippet + "..."

    # Escape HTML first (prevents transcripts or note text from breaking the UI)
    snippet = html.escape(snippet)

    highlight_terms = [re.escape(w) for w in words if len(w) > 1]
    if matched_word and matched_word not in words:
        highlight_terms.append(re.escape(matched_word))

    if not highlight_terms:
        return snippet

    pattern = "|".join(highlight_terms)
    highlight_style = "background-color: #ffd700; color: black; padding: 0 4px; border-radius: 3px; font-weight: bold;"

    snippet = re.sub(
        f"({pattern}\\w*)",
        fr'<span style="{highlight_style}">\1</span>',
        snippet,
        flags=re.IGNORECASE,
    )
    return snippet

# --- UI ---
st.title("🔍 Search Attio")

query = st.text_input("Search", placeholder='Try "Client Name" -draft ...', label_visibility="collapsed")
st.caption('Tip: Use quotes for "exact phrases", minus for -exclusion, and OR for multiple options.')

# --- FILTERS ROW ---
col1, col2 = st.columns([3, 1])
with col1:
    available_types = [
        "person",
        "company",
        "note",
        "task",
        "call_recording",
        "call_transcript",
        "comment",
        "list",
        "email",
        "meeting",
    ]
    selected_types = st.multiselect("Filter Types", options=available_types, default=available_types)

# --- DATE FILTER ---
with st.expander("📅 Filter by Date"):
    use_date = st.checkbox("Enable date filter", value=False)
    start_date = None
    end_date = None
    if use_date:
        d_col1, d_col2 = st.columns(2)
        start_date = d_col1.date_input("Start Date")
        end_date = d_col2.date_input("End Date")

def build_base_req():
    r = supabase.table("attio_index").select("*")
    r = r.in_("type", selected_types)
    return r

if query:
    if not selected_types:
        st.warning("⚠️ Please select at least one filter.")
        st.stop()

    try:
        # Text search first, then limit (prevents weird truncation behaviour)
        base = build_base_req()

        # Try websearch mode, fallback to plain
        results = None
        search_mode = None

        try:
            req_web = base.text_search("fts", query, options={"type": "websearch", "config": "english"})
            results = req_web.limit(500).execute().data
            search_mode = "websearch"
        except Exception:
            base = build_base_req()
            req_plain = base.text_search("fts", query, options={"type": "plain", "config": "english"})
            results = req_plain.limit(500).execute().data
            search_mode = "plain"

        if results is None:
            results = []

        # DATE FILTERING (Python-side)
        if use_date and (start_date or end_date):
            filtered = []
            for item in results:
                meta = item.get("metadata") or {}
                # Try a few common keys
                date_str = (
                    meta.get("created_at")
                    or meta.get("start_datetime")
                    or meta.get("start")
                    or meta.get("meeting_start")
                )

                if not date_str:
                    filtered.append(item)
                    continue

                try:
                    item_date = datetime.fromisoformat(str(date_str).replace("Z", "+00:00")).date()
                    if start_date and item_date < start_date:
                        continue
                    if end_date and item_date > end_date:
                        continue
                    filtered.append(item)
                except Exception:
                    # Keep item if parse fails
                    filtered.append(item)
            results = filtered

        # SORTING
        clean_q = re.sub(r"[^\w\s]", "", query).strip()
        if results and clean_q:
            results.sort(key=lambda x: calculate_relevance(x, clean_q), reverse=True)

        st.caption(f"Found {len(results)} matches (mode: {search_mode})")

        for item in results:
            t = item.get("type", "unknown")
            icon = "📄"
            if t == "person":
                icon = "👤"
            elif t == "company":
                icon = "🏢"
            elif t == "note":
                icon = "📝"
            elif t == "task":
                icon = "✅"
            elif t == "comment":
                icon = "💬"
            elif t == "call_recording":
                icon = "📞"
            elif t == "call_transcript":
                icon = "🗣️"
            elif t == "email":
                icon = "📧"
            elif t == "meeting":
                icon = "📅"

            url = item.get("url") or "#"
            title = item.get("title") or "Untitled"

            meta = item.get("metadata") or {}
            meta_bits = [t.upper()]

            # Add parent name when present
            parent_name = meta.get("parent_name")
            if parent_name:
                meta_bits.append(f"Parent: {parent_name}")

            # Prefer created_at for display
            created_at = meta.get("created_at")
            if created_at and isinstance(created_at, str) and len(created_at) >= 10:
                meta_bits.append(created_at[:10])

            st.markdown(
                f"""
                <div style="font-size: 18px; font-weight: 600; margin-bottom: 2px;">
                    <a href="{html.escape(url)}" target="_blank">{icon} {html.escape(title)}</a>
                </div>
                """,
                unsafe_allow_html=True,
            )
            st.caption(" • ".join(meta_bits))

            content = item.get("content") or ""
            snippet = get_context_snippet(content, clean_q, window=200)

            if content.strip().startswith("{") or content.strip().startswith("["):
                st.info("Match found in structured content / metadata")
            else:
                st.markdown(f'<div class="snippet-text">{snippet}</div>', unsafe_allow_html=True)

            with st.expander("View Full Content", expanded=False):
                # Always safe rendering for full text (no HTML)
                st.code(content or "", language="text")

            st.divider()

    except Exception as e:
        st.error(f"Search failed: {e}")
