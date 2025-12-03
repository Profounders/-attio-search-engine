import os
import time
import json
import requests
from supabase import create_client, Client

# --- CONFIG ---
ATTIO_API_KEY = os.environ.get("ATTIO_API_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# Check if secrets are loaded
if not ATTIO_API_KEY or not SUPABASE_URL:
    print("❌ Error: Secrets not found. Please check GitHub Actions Secrets.")
    exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def attio_get(endpoint, params=None):
    url = f"https://api.attio.com/v2/{endpoint}"
    headers = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Accept": "application/json"}
    try:
        time.sleep(0.2) # Increased sleep to prevent Rate Limits (429)
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 429:
            print(f"⚠️ Rate limit hit on {endpoint}. Waiting 5 seconds...")
            time.sleep(5)
            return attio_get(endpoint, params) # Retry
            
        if response.status_code != 200:
            print(f"⚠️ Warning: {endpoint} returned {response.status_code} - {response.text}")
            return []
            
        return response.json().get("data", [])
    except Exception as e:
        print(f"❌ Error fetching {endpoint}: {e}")
        return []

def upsert_item(data):
    try:
        # Convert Metadata to string if it's too complex, ensuring JSON compatibility
        if "metadata" in data and isinstance(data["metadata"], dict):
            # Clean metadata to remove nulls or complex objects Supabase might reject
            data["metadata"] = {k: v for k, v in data["metadata"].items() if v is not None}
            
        supabase.table("attio_index").upsert(data).execute()
        print(f"✅ Synced: {data['title'][:30]}")
    except Exception as e:
        print(f"❌ Database Error on {data.get('title')}: {e}")

# --- HELPER: Safely find a name ---
def get_safe_name(values):
    """Tries multiple ways to find a name/title in a record"""
    try:
        # Try standard slugs
        if 'name' in values and values['name']: return values['name'][0]['value']
        if 'title' in values and values['title']: return values['title'][0]['value']
        if 'email_addresses' in values and values['email_addresses']: return values['email_addresses'][0]['value']
        if 'domains' in values and values['domains']: return values['domains'][0]['value']
        
        # Fallback: Just take the first available text value
        for key, val in values.items():
            if isinstance(val, list) and len(val) > 0 and 'value' in val[0]:
                return val[0]['value']
                
        return "Untitled Record"
    except:
        return "Unknown"

# --- SYNC FUNCTIONS ---
def sync_everything():
    print("🚀 Starting Sync...")

    # 1. LISTS
    lists = attio_get("lists")
    for l in lists:
        try:
            upsert_item({
                "id": l['id']['list_id'],
                "type": "list",
                "title": f"List: {l['name']}",
                "content": f"Workspace list {l.get('api_slug')}",
                "url": f"https://app.attio.com/w/workspace/lists/{l['id']['list_id']}",
                "metadata": {}
            })
        except Exception as e:
            print(f"⚠️ Skipped List {l.get('name')}: {e}")

    # 2. OBJECTS & RECORDS
    objects = attio_get("objects")
    for obj in objects:
        slug = obj['api_slug']
        print(f"📂 Processing Object: {slug}...")
        
        records = attio_get(f"objects/{slug}/records", params={"limit": 100}) # Lower limit for safety
        
        for rec in records:
            try:
                rec_id = rec['id']['record_id']
                vals = rec.get('values', {})
                name = get_safe_name(vals)

                # Sync Record
                upsert_item({
                    "id": rec_id,
                    "type": slug,
                    "title": name,
                    "content": str(vals),
                    "url": f"https://app.attio.com/w/workspace/record/{slug}/{rec_id}",
                    "metadata": {} # Keep metadata empty to reduce complexity errors for now
                })

                # Sync Notes (V2)
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
                
                # Sync Tasks
                tasks = attio_get(f"objects/{slug}/records/{rec_id}/tasks")
                for t in tasks:
                    upsert_item({
                        "id": t['id']['task_id'],
                        "parent_id": rec_id,
                        "type": "task",
                        "title": f"Task: {t.get('content_plaintext', 'Untitled')}",
                        "content": f"Completed: {t.get('is_completed')}",
                        "url": "https://app.attio.com/w/workspace/tasks",
                        "metadata": {}
                    })

            except Exception as e:
                print(f"⚠️ Failed to process record {rec.get('id', 'unknown')}: {e}")
                continue # Skip this record, move to next

    print("🏁 Sync Complete.")

if __name__ == "__main__":
    sync_everything()