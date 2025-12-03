import os
import time
import requests
from supabase import create_client, Client

# --- CONFIG ---
ATTIO_API_KEY = os.environ.get("ATTIO_API_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# --- CUSTOMIZE THIS ---
# If your calls are stored in an object named "meetings" or "calls", type it here.
# If you leave it empty, we will try to find it automatically.
CUSTOM_CALLS_OBJECT = "meetings" 

print(f"DEBUG: Starting Sync Script...")

# 1. Setup Supabase
if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ Error: Supabase Secrets missing.")
    exit(1)

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception as e:
    print(f"❌ Supabase Connection Error: {e}")
    exit(1)

# 2. API Helper
def attio_get(endpoint, params=None):
    url = f"https://api.attio.com/v2/{endpoint}"
    headers = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Accept": "application/json"}
    try:
        time.sleep(0.2) 
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 403:
            print(f"🚫 PERMISSION DENIED for '{endpoint}'. Check API Scopes!")
            return []
        if response.status_code != 200:
            print(f"⚠️ API Error {response.status_code} on {endpoint}")
            return []
            
        return response.json().get("data", [])
    except Exception as e:
        print(f"❌ Network Error on {endpoint}: {e}")
        return []

def upsert_item(data):
    try:
        if "metadata" in data and data["metadata"]:
             data["metadata"] = {k: v for k, v in data["metadata"].items() if v is not None}
        supabase.table("attio_index").upsert(data).execute()
        # Print a dot for progress, or the title for debugging
        print(f"✅ Synced: {data['type']} - {data['title'][:20]}")
    except Exception as e:
        print(f"❌ DB Error: {e}")

# --- SYNC LOGIC ---

def sync_specific_object(slug):
    """Syncs a specific object type (people, companies, etc)"""
    print(f"\n📂 FORCE SYNCING: {slug}...")
    
    # Get Records
    records = attio_get(f"objects/{slug}/records", params={"limit": 200})
    
    if not records:
        print(f"⚠️ No records found for '{slug}'. Either empty or permission denied.")
        return

    print(f"   Found {len(records)} records. Processing...")

    for rec in records:
        try:
            rec_id = rec['id']['record_id']
            vals = rec.get('values', {})
            
            # Name Finder
            name = "Untitled"
            for k in ['name', 'title', 'email_addresses', 'domains']:
                if k in vals and vals[k]:
                    name = vals[k][0]['value']
                    break
            
            # 1. Sync the Record itself
            upsert_item({
                "id": rec_id,
                "type": slug,
                "title": name,
                "content": str(vals),
                "url": f"https://app.attio.com/w/workspace/record/{slug}/{rec_id}",
                "metadata": {}
            })

            # 2. Sync Notes
            notes = attio_get("notes", params={"parent_record_id": rec_id})
            for n in notes:
                upsert_item({
                    "id": n['id']['note_id'],
                    "parent_id": rec_id,
                    "type": "note",
                    "title": f"Note: {n.get('title', 'Untitled')}",
                    "content": n.get('content_plaintext', ''),
                    "url": f"https://app.attio.com/w/workspace/note/{n['id']['note_id']}",
                    "metadata": {}
                })
            
            # 3. Sync Tasks
            tasks = attio_get(f"objects/{slug}/records/{rec_id}/tasks")
            for t in tasks:
                 upsert_item({
                    "id": t['id']['task_id'],
                    "parent_id": rec_id,
                    "type": "task",
                    "title": f"Task: {t.get('content_plaintext', '')}",
                    "content": f"Due: {t.get('deadline_at')}",
                    "url": "https://app.attio.com/w/workspace/tasks",
                    "metadata": {}
                })

        except Exception as e:
            print(f"   Error processing record: {e}")

def sync_everything():
    # 1. Lists (You said this works)
    print("\n--- Syncing Lists ---")
    lists = attio_get("lists")
    for l in lists:
        upsert_item({
            "id": l['id']['list_id'],
            "type": "list",
            "title": f"List: {l['name']}",
            "content": str(l),
            "url": f"https://app.attio.com/w/workspace/lists/{l['id']['list_id']}",
            "metadata": {}
        })

    # 2. Force Sync Standard Objects (Ignores dynamic object errors)
    sync_specific_object("people")
    sync_specific_object("companies")
    sync_specific_object("deals")
    
    # 3. Force Sync Custom Calls/Meetings
    if CUSTOM_CALLS_OBJECT:
        sync_specific_object(CUSTOM_CALLS_OBJECT)

if __name__ == "__main__":
    sync_everything()
    print("\n🏁 Sync Job Finished.")
