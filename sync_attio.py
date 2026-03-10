# --- MAIN SYNC: ALL NOTES ---
def sync_all_notes():
    print("\n🔎 Fetching all notes globally from Attio...")
    
    # EXACT ALIGNMENT WITH API DOCS: Max limit for notes is 50
    limit = 50 
    offset = 0
    total_synced = 0
    
    while True:
        # We only pass limit and offset. Omitting parent_object fetches globally.
        params = {"limit": limit, "offset": offset}
        res = requests.get("https://api.attio.com/v2/notes", headers=HEADERS, params=params)
        
        if res.status_code != 200:
            print(f"❌ API Error {res.status_code}: {res.text}")
            break
            
        notes = res.json().get("data",[])
        if not notes: 
            break # Reached the end
            
        batch =[]
        for n in notes:
            # 1. Extract raw data (Aligns with the response schema in docs)
            note_id = n['id']['note_id']
            parent_id = n.get('parent_record_id')
            parent_slug = n.get('parent_object')
            
            content = n.get('content_plaintext', '').strip()
            raw_title = n.get('title', '').strip()
            
            # 2. Get the name of the Company/Person using the cache
            parent_name = get_parent_name(parent_slug, parent_id)
            
            # 3. Build a beautiful title
            if raw_title and raw_title != "Untitled":
                final_title = f"Note: {raw_title} ({parent_name})"
            elif content:
                snippet = content[:50].replace('\n', ' ')
                final_title = f"Note: {snippet}... ({parent_name})"
            else:
                final_title = f"Empty Note ({parent_name})"

            # 4. Append to database batch
            batch.append({
                "id": note_id,
                "title": final_title,
                "content": content,
                "url": f"https://app.attio.com/w/workspace/note/{note_id}",
                "created_at": n.get("created_at")
            })
        
        # 5. Save to Supabase
        if batch:
            supabase.table("attio_notes").upsert(batch).execute()
            total_synced += len(batch)
            print(f"   💾 Saved batch of {len(batch)}. Total so far: {total_synced}")
            
        # Pagination check based on the limit
        if len(notes) < limit: break
        offset += limit
        
    print(f"\n✅ Sync Complete! Total Notes Synced: {total_synced}")
