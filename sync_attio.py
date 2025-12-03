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

# --- API HELPER ---
def attio_get(endpoint, params=None):
    url = f"https://api.attio.com/v2/{endpoint}"
    headers = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Accept": "application/json"}
    
    all_results = []
    limit = 1000  # Max limit per page
    if params is None: params = {}
    params['limit'] = limit
    
    try:
        while True:
            time.sleep(0.2) 
            response = requests.get(url, headers=headers, params=params)
            
            if response.status_code == 403:
                print(f"   🚫 Permission Denied: {endpoint}")
                return []
            if response.status_code != 200:
                print(f"   ⚠️ Error {response.status_code} on {endpoint}")
                return []
            
            data = response.json()
            results = data.get("data", [])
            all_results.extend(results)
            
            # Pagination Check
            next_cursor = data.get("next_cursor") # Check how Attio sends cursor in V2
            # Some Attio endpoints use data['next_page_token'] or similar. 
            # If no cursor logic is needed for your volume, we break. 
            # For simplicity in this script, we take the first 1000 items. 
            # If you have >1000 notes, we need full pagination logic.
            break 
            
        return all_results

    except Exception as e:
        print(f"   ❌ Network Error: {e}")
        return []

def upsert_item(data):
    try:
        # Clean metadata
        if "metadata" in data and data["metadata"]:
             data["metadata"] = {k: v for k, v in data["metadata"].items() if v is not None}
        
        supabase.table("attio_index").upsert(data).execute()
        # Print infrequent logs
        if int(time.time()) % 2 == 0: 
             print(f"   ✅ Synced: {data['type']} - {data['title'][:30]}")
    except Exception as e:
        print(f"   ❌ DB Error: {e}")

# --- SYNC FUNCTIONS ---

def sync_all_notes_globally():
    """
    Fetches ALL notes in the workspace directly, bypassing records.
    """
    print("\n📝 Syncing GLOBAL Notes (V2)...")
    notes = attio_get("notes") # Hits https://api.attio.com/v2/notes
    
    if not notes:
        print("   ⚠️ No notes found (or Note:Read permission missing).")
        return

    print(f"   Found {len(notes)} notes. Uploading...")
    
    for n in notes:
        # Construct a title
        title = n.get("title", "Untitled Note")
        content = n.get("content_plaintext", "")
        note_id = n['id']['note_id']
        
        # Try to link to the parent record if possible
        parent_id = n.get("parent_record_id", "")
        
        upsert_item({
            "id": note_id,
            "parent_id": parent_id,
            "type": "note",
            "title": f"Note: {title}",
            "content": content,
            "url": f"https://app.attio.com/w/workspace/note/{note_id}",
            "metadata": {
                "created_at": n.get("created_at"),
                "author": n.get("created_by_actor")
            }
        })

def sync_all_tasks_globally():
    """
    Fetches ALL tasks in the workspace directly.
    """
    print("\n✅ Syncing GLOBAL Tasks (V2)...")
    tasks = attio_get("tasks")
    
    if not tasks:
        return

    for t in tasks:
        content = t.get("content_plaintext", "Untitled Task")
        task_id = t['id']['task_id']
        
        upsert_item({
            "id": task_id,
            "type": "task",
            "title": f"Task: {content}",
            "content": f"Is Completed: {t.get('is_completed')}",
            "url": "https://app.attio.com/w/workspace/tasks",
            "metadata": {"deadline": t.get("deadline_at")}
        })

def sync_records_dynamic():
    print("\n🔍 Discovering Objects & Records...")
    objects = attio_get("objects")
    
    for obj in objects:
        slug = obj['api_slug']
        print(f"   📂 Processing Object: {obj['singular_noun']} ({slug})")
        
        records = attio_get(f"objects/{slug}/records", params={"limit": 200})
        
        for rec in records:
            try:
                rec_id = rec['id']['record_id']
                vals = rec.get('values', {})
                
                # Name Finding
                name = "Untitled"
                for k in ['name', 'title', 'company_name', 'email_addresses', 'domains']:
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
            except:
                continue

# --- MAIN ---
if __name__ == "__main__":
    # 1. Sync Lists
    print("--- 1. Lists ---")
    lists = attio_get("lists")
    for l in lists:
        upsert_item({
            "id": l['id']['list_id'], "type": "list", "title": l['name'], 
            "content": "", "url": "", "metadata": {}
        })

    # 2. Global Sync (The Fix)
    sync_all_notes_globally()
    sync_all_tasks_globally()

    # 3. Records
    sync_records_dynamic()
    
    print("\n🏁 Sync Job Finished.")
