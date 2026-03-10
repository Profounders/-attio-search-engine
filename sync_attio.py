import os
import time
import requests
from supabase import create_client

print("🚀 Starting Clean Reset: Notes Only Sync...")

# --- CONFIG ---
ATTIO_API_KEY = os.environ.get("ATTIO_API_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not ATTIO_API_KEY or not SUPABASE_URL:
    print("❌ Error: Secrets missing.")
    exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
HEADERS = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Accept": "application/json"}
NAME_CACHE = {} # Speeds up parent lookups

# --- HELPER: GET PARENT NAME ---
# Finds out WHO the note is about (e.g. "Acme Corp")
def get_parent_name(slug, record_id):
    if not record_id: return "Unknown"
    cache_key = f"{slug}:{record_id}"
    if cache_key in NAME_CACHE: return NAME_CACHE[cache_key]

    try:
        res = requests.get(f"https://api.attio.com/v2/objects/{slug}/records/{record_id}", headers=HEADERS)
        if res.status_code != 200: return "Unknown"
        
        vals = res.json().get("data", {}).get("values", {})
        name = "Unknown"
        for key in['name', 'full_name', 'title', 'company_name']:
            if key in vals and vals[key]:
                name = vals[key][0]['value']
                break
        if name == "Unknown" and 'email_addresses' in vals and vals['email_addresses']:
            name = vals['email_addresses'][0]['value']
            
        NAME_CACHE[cache_key] = name
        return name
    except: return "Unknown"

# --- MAIN SYNC: NOTES ---
def sync_all_notes():
    buckets = ["people", "companies", "deals"]
    total_synced = 0
    
    for slug in buckets:
        print(f"\n🔎 Searching for notes attached to {slug}...")
        limit = 500
        offset = 0
        
        while True:
            params = {"limit": limit, "offset": offset, "parent_object": slug}
            res = requests.get("https://api.attio.com/v2/notes", headers=HEADERS, params=params)
            
            if res.status_code != 200: break
            notes = res.json().get("data",[])
            if not notes: break
            
            batch =[]
            for n in notes:
                # 1. Extract Data
                note_id = n['id']['note_id']
                parent_id = n.get('parent_record_id')
                content = n.get('content_plaintext', '').strip()
                raw_title = n.get('title', '').strip()
                
                # 2. Build a useful Title
                parent_name = get_parent_name(slug, parent_id)
                
                if raw_title and raw_title != "Untitled":
                    final_title = f"{raw_title} ({parent_name})"
                elif content:
                    snippet = content[:50].replace('\n', ' ')
                    final_title = f"Note: {snippet}... ({parent_name})"
                else:
                    final_title = f"Empty Note ({parent_name})"

                # 3. Add to Batch
                batch.append({
                    "id": note_id,
                    "title": final_title,
                    "content": content,
                    "url": f"https://app.attio.com/w/workspace/note/{note_id}",
                    "created_at": n.get("created_at")
                })
            
            # 4. Save to DB
            if batch:
                supabase.table("attio_notes").upsert(batch).execute()
                total_synced += len(batch)
                print(f"   💾 Saved {len(batch)} notes...")
                
            if len(notes) < limit: break
            offset += limit
            
    print(f"\n✅ Sync Complete! Total Notes Synced: {total_synced}")

if __name__ == "__main__":
    sync_all_notes()
