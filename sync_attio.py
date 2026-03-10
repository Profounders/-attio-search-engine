import os
import requests
from supabase import create_client
from dotenv import load_dotenv

# Load variables if an env file exists (useful for local testing)
load_dotenv('env')
load_dotenv('.env')

print("🚀 Starting Clean Reset: Notes Only Sync (Global)...", flush=True)

# --- CONFIG ---
ATTIO_API_KEY = os.environ.get("ATTIO_API_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not ATTIO_API_KEY or not SUPABASE_URL:
    print("❌ Error: Secrets missing. Ensure they are set in GitHub Actions Secrets.", flush=True)
    exit(1)

try:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("   🔌 DB Connected.", flush=True)
except Exception as e:
    print(f"   ❌ DB Connection Failed: {e}", flush=True)
    exit(1)

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
        for key in ['name', 'full_name', 'title', 'company_name', 'deal_name']:
            if key in vals and vals[key]:
                name = vals[key][0]['value']
                break
        if name == "Unknown" and 'email_addresses' in vals and vals['email_addresses']:
            name = vals['email_addresses'][0]['value']
            
        NAME_CACHE[cache_key] = name
        return name
    except: return "Unknown"

# --- MAIN SYNC: ALL NOTES ---
def sync_all_notes():
    print("\n🔎 Fetching all notes globally from Attio...", flush=True)
    
    # EXACT ALIGNMENT WITH API DOCS: Max limit is 50
    limit = 50 
    offset = 0
    total_synced = 0
    
    while True:
        params = {"limit": limit, "offset": offset}
        res = requests.get("https://api.attio.com/v2/notes", headers=HEADERS, params=params)
        
        if res.status_code != 200:
            print(f"   ❌ API Error {res.status_code}: {res.text}", flush=True)
            break
            
        notes = res.json().get("data",[])
        if not notes: 
            break # Reached the end
            
        batch =[]
        for n in notes:
            try:
                # 1. Extract raw data
                note_id = n['id']['note_id']
                parent_id = n.get('parent_record_id')
                parent_slug = n.get('parent_object') 
                
                content = n.get('content_plaintext', '').strip()
                raw_title = n.get('title', '').strip()
                
                # 2. Get the name of the Company/Person
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
            except Exception as e:
                print(f"   ⚠️ Error parsing note: {e}", flush=True)
        
        # 5. Save to Supabase
        if batch:
            try:
                supabase.table("attio_notes").upsert(batch).execute()
                total_synced += len(batch)
                print(f"   💾 Saved batch of {len(batch)}. Total so far: {total_synced}", flush=True)
            except Exception as e:
                print(f"   ❌ Database Upsert Error: {e}", flush=True)
            
        if len(notes) < limit: break
        offset += limit
        
    print(f"\n✅ Sync Complete! Total Notes Synced: {total_synced}", flush=True)

if __name__ == "__main__":
    sync_all_notes()
