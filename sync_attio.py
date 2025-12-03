import os
import time
import requests
from supabase import create_client, Client

# --- CONFIG ---
ATTIO_API_KEY = os.environ.get("ATTIO_API_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not ATTIO_API_KEY or not SUPABASE_URL:
    print("❌ Error: Secrets missing.")
    exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- API HELPERS ---
def attio_post_query(endpoint, payload=None):
    """Used for listing records (Attio V2 requires POST /query)"""
    url = f"https://api.attio.com/v2/{endpoint}"
    headers = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Content-Type": "application/json", "Accept": "application/json"}
    if payload is None: payload = {}
    
    try:
        time.sleep(0.2)
        response = requests.post(url, headers=headers, json=payload)
        
        if response.status_code != 200:
            print(f"   ⚠️ POST Error {response.status_code} on {endpoint}: {response.text[:100]}")
            return []
            
        return response.json().get("data", [])
    except Exception as e:
        print(f"   ❌ Network Error: {e}")
        return []

def attio_get(endpoint, params=None):
    """Used for simple gets (Lists, Tasks, Objects)"""
    url = f"https://api.attio.com/v2/{endpoint}"
    headers = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Accept": "application/json"}
    try:
        time.sleep(0.2)
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code != 200:
            # Silence 404s/400s slightly to keep logs clean unless critical
            return []
            
        return response.json().get("data", [])
    except Exception as e:
        return []

def upsert_item(data):
    try:
        # Clean metadata
        if "metadata" in data and data["metadata"]:
             data["metadata"] = {k: v for k, v in data["metadata"].items() if v is not None}
        
        supabase.table("attio_index").upsert(data).execute()
        # Log success occasionally
        if int(time.time()) % 5 == 0: 
             print(f"   ✅ Synced: {data['type']} - {data['title'][:30]}")
    except Exception as e:
        print(f"   ❌ DB Error: {e}")

# --- SYNC LOGIC ---

def sync_tasks_globally():
    # You confirmed this works!
    print("✅ Syncing Tasks...")
    tasks = attio_get("tasks") 
    for t in tasks:
        upsert_item({
            "id": t['id']['task_id'],
            "type": "task",
            "title": f"Task: {t.get('content_plaintext', 'Untitled')}",
            "content": f"Status: {t.get('is_completed')}",
            "url": "https://app.attio.com/w/workspace/tasks",
            "metadata": {"deadline": t.get("deadline_at")}
        })

def sync_objects_and_records():
    print("\n🔍 Discovering Objects...")
    objects = attio_get("objects")
    
    for obj in objects:
        slug = obj['api_slug'] # e.g. "people", "companies"
        singular = obj['singular_noun']
        print(f"   📂 Processing {singular} ({slug})...")
        
        # FIX 1: Use POST /query to get records (Fixes the 404)
        records = attio_post_query(f"objects/{slug}/records/query", payload={"limit": 1000})
        
        if not records:
            print(f"      (0 records found or permission denied)")
            continue
            
        print(f"      Found {len(records)} records. Fetching their notes...")

        for rec in records:
            rec_id = rec['id']['record_id']
            vals = rec.get('values', {})
            
            # Find Name
            name = "Untitled"
            for k in ['name', 'title', 'company_name', 'email_addresses', 'domains']:
                if k in vals and vals[k]:
                    name = vals[k][0]['value']
                    break
            
            # 1. Sync Record
            upsert_item({
                "id": rec_id,
                "type": slug,
                "title": name,
                "content": str(vals),
                "url": f"https://app.attio.com/w/workspace/record/{slug}/{rec_id}",
                "metadata": {} 
            })

            # FIX 2: Fetch Notes PER RECORD (Fixes the 400)
            notes = attio_get("notes", params={"parent_record_id": rec_id})
            for n in notes:
                upsert_item({
                    "id": n['id']['note_id'],
                    "parent_id": rec_id,
                    "type": "note",
                    "title": f"Note on {name}",
                    "content": n.get('content_plaintext', ''),
                    "url": f"https://app.attio.com/w/workspace/note/{n['id']['note_id']}",
                    "metadata": {"created_at": n.get("created_at")}
                })
            
            # 3. Fetch Comments (Often where "notes" are actually hidden)
            comments = attio_get(f"objects/{slug}/records/{rec_id}/comments")
            for c in comments:
                upsert_item({
                    "id": c['id']['comment_id'],
                    "parent_id": rec_id,
                    "type": "comment",
                    "title": f"Comment on {name}",
                    "content": c.get('content_plaintext', ''),
                    "url": f"https://app.attio.com/w/workspace/record/{slug}/{rec_id}",
                    "metadata": {"author": c.get('author')}
                })

if __name__ == "__main__":
    # 1. Lists
    print("--- 1. Lists ---")
    lists = attio_get("lists")
    for l in lists:
        upsert_item({"id": l['id']['list_id'], "type": "list", "title": l['name'], "content": "", "url": "", "metadata": {}})

    # 2. Tasks (Global)
    sync_tasks_globally()

    # 3. Records & Notes (Nested)
    sync_objects_and_records()
    
    print("\n🏁 Sync Job Finished.")
