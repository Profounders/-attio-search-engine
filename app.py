# ... (Imports and CSS remain the same) ...

# --- UI START ---
st.title("🔍 Search Attio")

query = st.text_input("Search", placeholder='Try "Client Name" -draft ...', label_visibility="collapsed")
st.caption("Tip: Use quotes for \"exact phrases\", minus for -exclusion, and OR for multiple options.")

available_types = ["person", "company", "note", "task", "call_recording", "comment", "list", "email"]
selected_types = st.multiselect("Filter Types", options=available_types, default=available_types)

if query:
    if not selected_types:
        st.warning("⚠️ Please select at least one filter.")
    else:
        results = []
        search_error = None
        
        # --- ROBUST SEARCH LOGIC ---
        try:
            # ATTEMPT 1: ADVANCED (Google-Style)
            # This supports "quotes", -minus, and OR.
            req = supabase.table("attio_index").select("*")
            req = req.in_("type", selected_types)
            req = req.limit(100)
            req = req.text_search("fts", query, options={"type": "websearch", "config": "english"})
            results = req.execute().data
            
        except Exception:
            # ATTEMPT 2: FALLBACK (Simple/Plain)
            # If the above crashes (e.g. "noxus ai" syntax error), we switch to 'plain'.
            # 'plain' automatically converts "noxus ai" -> "noxus & ai" (Crash Proof)
            try:
                req = supabase.table("attio_index").select("*")
                req = req.in_("type", selected_types)
                req = req.limit(100)
                req = req.text_search("fts", query, options={"type": "plain", "config": "english"})
                results = req.execute().data
            except Exception as e:
                search_error = e

        # --- DISPLAY RESULTS ---
        if search_error:
            st.error(f"Search failed: {search_error}")
        elif not results:
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
                elif t == 'call_recording': icon = "📞"
                elif t == 'email': icon = "📧"

                with st.container():
                    # 1. Title
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
                    
                    # 2. Metadata
                    meta_info = t.upper()
                    if item.get("metadata") and item["metadata"].get("created_at"):
                        date_str = item["metadata"]["created_at"][:10]
                        meta_info += f" • {date_str}"
                    st.caption(meta_info)

                    # 3. Content
                    content = item.get('content') or ""
                    
                    # Clean snippet generation
                    clean_query = re.sub(r'[^\w\s]', '', query).strip() # Remove symbols for highlighting
                    snippet = get_context_snippet(content, clean_query, window=200)

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
