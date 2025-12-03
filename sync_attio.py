import os
import time
import requests
from supabase import create_client, Client

# --- CONFIG ---
ATTIO_API_KEY = os.environ.get("ATTIO_API_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# --- DEBUG CHECKS (Run before connecting) ---
print(f"DEBUG: Checking credentials...")

if not SUPABASE_URL:
    print("❌ Error: SUPABASE_URL is missing/empty.")
    exit(1)

if not SUPABASE_URL.startswith("https://"):
    print(f"❌ Error: SUPABASE_URL is invalid. It must start with 'https://'. Current value starts with: {SUPABASE_URL[:4]}...")
    exit(1)

if not SUPABASE_KEY:
    print("❌ Error: SUPABASE_KEY is missing.")
    exit(1)

# Now it is safe to connect
try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception as e:
    print(f"❌ Critical Error: Could not connect to Supabase. Reason: {e}")
    exit(1)


# --- API HELPER ---
def attio_get(endpoint, params=None):
    url = f"https://api.attio.com/v2/{endpoint}"
    headers = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Accept": "application/json"}
    try:
        # Simple rate limit prevention
        time.sleep(0.2) 
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code != 200:
            print(f"⚠️ Warning: {endpoint} returned {response.status_code}")
            return []
            
        return response.json().get("data", [])
    except Exception as e:
        print(f"❌ Error fetching {endpoint}: {e}")
        return []

# --- MAIN SYNC ---
def upsert_item(data):
    try:
        # Clean metadata (Supabase hates nulls in JSONB sometimes)
        if "metadata" in data and data["metadata"]:
             data["metadata"] = {k: v for k, v in data["metadata"].items() if v is not None}
        
        supabase.table("attio_index").upsert(data).execute()
        print(f"✅ Synced: {data['title'][:40]}")
    except Exception as e:
        print(f"❌ DB Error on {data.get('title')}: {e}")

def sync_everything():
    print("🚀 Starting Sync...")

    # 1. LISTS
    lists = attio_get("lists")
    for l in lists:
        upsert_item({
            "id": l['id']['list_id'],
            "type": "list",
            "title": f"List: {l['name']}",
            "content": f"Workspace list {l.get('api_slug')}",
            "url": f"https://app.attio.com/w/workspace/lists/{l['id']['list_id']}",
            "metadata": {}
        })

    # 2. OBJECTS
    objects = attio_get("objects")
    for obj in objects:
        slug = obj['api_slug']
        print(f"📂 Processing Object: {slug}...")
        
        # Fetch records
        records = attio_get(f"objects/{slug}/records", params={"limit": 100})
        
        for rec in records:
            try:
                rec_id = rec['id']['record_id']
                vals = rec.get('values', {})
                
                # Try to find a name
                name = "Untitled"
                # Check common name fields
                for k in ['name', 'title', 'email_addresses', 'domain']:
                    if k in vals and vals[k]:
                        name = vals[k][0]['value']
                        break

                upsert_item({
                    "id": rec_id,
                    "type": slug,
                    "title": name,
                    "content": str(vals),
                    "url": f"https://app.attio.com/w/workspace/record/{slug}/{rec_id}",
                    "metadata": {}
                })
                
                # Fetch Notes for this record
                notes = attio_get("notes", params={"parent_record_id": rec_id})
                for n in notes:
                    upsert_item({
                        "id": n['id']['note_id'],
                        "parent_id": rec_id,
                        "type": "note",
                        "title": n.get('title', 'Note'),
                        "content": n.get('content_plaintext', 'No content'),
                        "url": f"https://app.attio.com/w/workspace/note/{n['id']['note_id']}",
                        "metadata": {}
                    })

            except Exception as e:
                print(f"Skipping broken record: {e}")
                continue

if __name__ == "__main__":
    sync_everything()
