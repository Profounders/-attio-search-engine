import os
import requests
from supabase import create_client

print("🚀 Starting Clean Sync: Notes & Transcripts...")

# --- CONFIG ---
ATTIO_API_KEY = os.environ.get("ATTIO_API_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not ATTIO_API_KEY or not SUPABASE_URL:
    print("❌ Error: Secrets missing.")
    exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
HEADERS = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Accept": "application/json"}
NAME_CACHE = {} 

# --- HELPER: GET PARENT NAME ---
def get_parent_name(slug, record_id):
    if not record_id or not slug: return "Unknown"
    
    cache_key = f"{slug}:{record_id}"
    if cache_key in NAME_CACHE: return NAME_CACHE[cache_key]

    try:
        res = requests.get(f"https://api.attio.com/v2/objects/{slug}/records/{record_id}", headers=HEADERS)
        if res.status_code != 200: return "Unknown"
        
        vals = res.json().get("data", {}).get("values", {})
        name = "Unknown"
        for key in['name', 'full_name', 'title', 'company_name', 'deal_name']:
            if key in vals and vals[key]:
                name = vals[key][0]['value']
                break
        if name == "Unknown" and 'email_addresses' in vals and vals['email_addresses']:
            name = vals['email_addresses'][0]['value']
            
        NAME_CACHE[cache_key] = name
        return name
    except: return "Unknown"

# --- 1. SYNC: ALL NOTES ---
def sync_all_notes():
    print("\n🔎 Fetching all notes globally from Attio...")
    limit = 50 
    offset = 0
    total_synced = 0
    
    while True:
        params = {"limit": limit, "offset": offset}
        res = requests.get("https://api.attio.com/v2/notes", headers=HEADERS, params=params)
        
        if res.status_code != 200:
            print(f"   ❌ API Error {res.status_code}: {res.text}")
            break
            
        notes = res.json().get("data",[])
        if not notes: break 
            
        batch =[]
        for n in notes:
            try:
                note_id = n['id']['note_id']
                parent_id = n.get('parent_record_id')
                parent_slug = n.get('parent_object') 
                
                content = n.get('content_plaintext', '').strip()
                raw_title = n.get('title', '').strip()
                parent_name = get_parent_name(parent_slug, parent_id)
                
                if raw_title and raw_title != "Untitled":
                    final_title = f"Note: {raw_title} ({parent_name})"
                elif content:
                    snippet = content[:50].replace('\n', ' ')
                    final_title = f"Note: {snippet}... ({parent_name})"
                else:
                    final_title = f"Empty Note ({parent_name})"

                batch.append({
                    "id": note_id,
                    "title": final_title,
                    "content": content,
                    "url": f"https://app.attio.com/w/workspace/note/{note_id}",
                    "created_at": n.get("created_at")
                })
            except: pass
        
        if batch:
            supabase.table("attio_notes").upsert(batch).execute()
            total_synced += len(batch)
            print(f"   💾 Saved {len(batch)} notes. Total: {total_synced}")
            
        if len(notes) < limit: break
        offset += limit
        
    print(f"✅ Notes Sync Complete.")

# --- 2. SYNC: RECENT TRANSCRIPTS ---
def sync_all_transcripts():
    print("\n📞 Fetching recent meeting transcripts...")
    
    limit = 100
    offset = 0
    total_synced = 0
    
    # CUTOFF: Only check meetings from 2024 onwards to prevent infinite 2013 loops
    TARGET_YEARS =["2024", "2025", "2026"] 
    
    while True:
        res = requests.get("https://api.attio.com/v2/meetings", headers=HEADERS, params={"limit": limit, "offset": offset})
        if res.status_code != 200: break
        meetings = res.json().get("data",[])
        if not meetings: break
        
        batch =[]
        skipped = 0
        
        for m in meetings:
            try:
                # 1. Date check (Skip old history instantly)
                start_date = m.get("start", {}).get("datetime", "")
                if not any(y in start_date for y in TARGET_YEARS):
                    skipped += 1
                    continue
                
                mid = m['id'].get('meeting_id') or m['id'].get('record_id')
                title = m.get('title') or m.get('subject') or "Untitled Meeting"
                
                # 2. Check for recordings
                r_res = requests.get(f"https://api.attio.com/v2/meetings/{mid}/call_recordings", headers=HEADERS)
                if r_res.status_code != 200: continue
                recordings = r_res.json().get("data",[])
                
                for r in recordings:
                    rid = r['id']['call_recording_id']
                    
                    # 3. Get transcript
                    t_res = requests.get(f"https://api.attio.com/v2/meetings/{mid}/call_recordings/{rid}/transcript", headers=HEADERS)
                    if t_res.status_code == 200:
                        data = t_res.json()
                        txt = data.get("content_plaintext") or data.get("text") or data.get("subtitles")
                        
                        if txt:
                            print(f"   ✅ Transcript Found: {title}")
                            batch.append({
                                "id": rid,
                                "title": f"Transcript: {title}",
                                "content": txt,
                                "url": "https://app.attio.com", # Deep links to meetings are complex, routing to root
                                "created_at": start_date
                            })
            except Exception as e: 
                pass
                
        if batch:
            supabase.table("attio_notes").upsert(batch).execute()
            total_synced += len(batch)
            
        if len(meetings) < limit: break
        offset += limit
        
    print(f"✅ Transcripts Sync Complete! Total found: {total_synced}")

if __name__ == "__main__":
    sync_all_notes()
    sync_all_transcripts()
