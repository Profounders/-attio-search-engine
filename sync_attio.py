import os
import time
import requests
import traceback
from supabase import create_client, Client

# --- CONFIG ---
ATTIO_API_KEY = os.environ.get("ATTIO_API_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# Track if we managed to get notes the fast way
NOTES_ALREADY_SYNCED = False

print("🚀 Starting Sync V10 (Notes First + Anti-Timeout)...")

if not ATTIO_API_KEY or not SUPABASE_URL:
    print("❌ Error: Secrets missing.")
    exit(1)

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception as e:
    print(f"❌ Supabase Connection Error: {e}")
    exit(1)

# --- API HELPER ---
def make_request(method, endpoint, payload=None, params=None):
    url = f"https://api.attio.com/v2/{endpoint}"
    headers = {
        "Authorization": f"Bearer {ATTIO_API_KEY}", 
        "Content-Type": "application/json", 
        "Accept": "application/json"
    }
    
    for attempt in range(3):
        try:
            time.sleep(0.1) # Brief pause
            
            if method == "POST":
                response = requests.post(url, headers=headers, json=payload, timeout=60)
            else:
                response = requests.get(url, headers=headers, params=params, timeout=60)

            if response.status_code == 429:
                print(f"   ⚠️ API Rate Limit. Sleeping 5s...")
                time.sleep(5)
                continue 
            
            if response.status_code != 200:
                if attempt == 2 and response.status_code != 404:
                     print(f"   ⚠️ API {response.status_code} on {endpoint}")
                return None

            data = response.json().get("data", [])
            return data

        except Exception as e:
            time.sleep(1)
    return None

def safe_upsert(items, batch_size=50):
    """
    Saves items in small chunks to prevent Supabase Timeouts (57014).
    If a chunk fails, it retries with a smaller chunk.
    """
    if not items: return

    total = len(items)
    
    # Process in chunks
    for i in range(0, total, batch_size):
        chunk = items[i:i+batch_size]
        
        try:
            # Clean metadata
            for item in chunk:
                if "metadata" in item and item["metadata"]:
                    item["metadata"] = {k: v for k, v in item["metadata"].items() if v is not None}
            
            # Attempt Upsert
            supabase.table("attio_index").upsert(chunk).execute()
            print(f"   💾 Saved {len(chunk)} items...")
            
        except Exception as e:
            error_msg = str(e)
            # Check for Timeout (57014) or Connection Error
            if "57014" in error_msg or "timeout" in error_msg.lower():
                print(f"   ⚠️ DB Timeout. Retrying with smaller batch (10 items)...")
                time.sleep(2)
                # Recursive retry with safe size
                safe_upsert(chunk, batch_size=10)
            else:
                print(f"   ❌ DB Error: {e}")

# --- FAST SYNC FUNCTIONS ---
def sync_notes_first():
    """
    PRIORITY 1: Fetch ALL notes globally before doing anything else.
    """
    global NOTES_ALREADY_SYNCED
    print("\n📝 PRIORITY: Syncing Notes...")
    
    # Try getting notes without ANY parameters (Global List)
    notes = make_request("GET", "notes")
    
    if notes:
        print(f"   ✅ Found {len(notes)} notes globally. Saving...")
        batch = []
        for n in notes:
            batch.append({
                "id": n['id']['note_id'], 
                "parent_id": n.get('parent_record_id'), 
                "type": "note",
                "title": f"Note: {n.get('title', 'Untitled')}",
                "content": n.get('content_plaintext', ''),
                "url": f"https://app.attio.com/w/workspace/note/{n['id']['note_id']}",
                "metadata": {"created_at": n.get("created_at")}
            })
        
        safe_upsert(batch, batch_size=50) # Use safe upsert
        NOTES_ALREADY_SYNCED = True
    else:
        print("   🔸 Global Note Sync failed (likely permissions). Will sync notes per-record later.")

# --- STREAMING LOGIC ---
def process_object_streaming(slug, singular_name):
    limit = 1000
    offset = 0
    print(f"\n📂 Streaming {singular_name} ({slug})...")
    
    while True:
        # 1. Fetch Page
        payload = {"limit": limit, "offset": offset}
        records = make_request("POST", f"objects/{slug}/records/query", payload=payload)
        
        if not records:
            break 
            
        print(f"   📥 Downloaded {len(records)} records (Offset: {offset})...")

        # 2. Process Records
        record_batch = []
        for rec in records:
            try:
                rec_id = rec['id']['record_id']
                vals = rec.get('values', {})
                
                # Name Finding
                name = "Untitled"
                for k in ['name', 'title', 'company_name', 'email_addresses', 'domains', 'deal_name']:
                    if k in vals and vals[k] and isinstance(vals[k], list) and len(vals[k]) > 0:
                        name = vals[k][0]['value']
                        break
                
                record_batch.append({
                    "id": rec_id, "type": slug, "title": name,
                    "content": str(vals),
                    "url": f"https://app.attio.com/w/workspace/record/{slug}/{rec_id}",
                    "metadata": {} 
                })
            except: pass
        
        # Save Records (Batch 50)
        safe_upsert(record_batch, batch_size=50)

        # 3. Fetch Notes/Comments (SLOW PATH)
        if not NOTES_ALREADY_SYNCED:
            note_batch = []
            for rec in records:
                try:
                    rec_id = rec['id']['record_id']
                    
                    # Notes
                    notes = make_request("GET", "notes", params={"parent_record_id": rec_id, "parent_object": slug})
                    if notes:
                        for n in notes:
                            note_batch.append({
                                "id": n['id']['note_id'], "parent_id": rec_id, "type": "note",
                                "title": f"Note on {name}",
                                "content": n.get('content_plaintext', ''),
                                "url": f"https://app.attio.com/w/workspace/note/{n['id']['note_id']}",
                                "metadata": {"created_at": n.get("created_at")}
                            })
                    
                    # Comments
                    comments = make_request("GET", f"objects/{slug}/records/{rec_id}/comments")
                    if comments:
                        for c in comments:
                            note_batch.append({
                                "id": c['id']['comment_id'], "parent_id": rec_id, "type": "comment",
                                "title": f"Comment on {name}",
                                "content": c.get('content_plaintext', ''),
                                "url": f"https://app.attio.com/w/workspace/record/{slug}/{rec_id}",
                                "metadata": {"author": c.get("author")}
                            })
                except: pass
            
            # Save notes (Batch 50)
            if note_batch:
                print(f"   📝 Saving {len(note_batch)} notes/comments...")
                safe_upsert(note_batch, batch_size=50)

        # Next Page
        if len(records) < limit:
            break
        offset += limit

# --- MAIN SYNC ---
def sync_everything():
    
    # 1. NOTES (FIRST!!)
    sync_notes_first()

    # 2. TASKS
    print("\n--- Syncing Tasks ---")
    tasks = make_request("GET", "tasks") or []
    task_items = []
    for t in tasks:
        task_items.append({
            "id": t['id']['task_id'], "type": "task", 
            "title": f"Task: {t.get('content_plaintext', 'Untitled')}",
            "content": f"Status: {t.get('is_completed')}",
            "url": "https://app.attio.com/w/workspace/tasks",
            "metadata": {"deadline": t.get("deadline_at")}
        })
    safe_upsert(task_items, batch_size=50)

    # 3. LISTS
    print("\n--- Syncing Lists ---")
    lists = make_request("GET", "lists") or []
    list_items = []
    for l in lists:
        list_items.append({
            "id": l['id']['list_id'], "type": "list", "title": l['name'], 
            "content": "", "url": "", "metadata": {}
        })
    safe_upsert(list_items, batch_size=50)

    # 4. OBJECTS
    print("\n--- Syncing Objects ---")
    objects = make_request("GET", "objects") or []
    for obj in objects:
        slug = obj['api_slug']
        singular = obj['singular_noun']
        process_object_streaming(slug, singular)

if __name__ == "__main__":
    try:
        sync_everything()
        print("\n🏁 Sync Job Finished.")
    except Exception as e:
        print("\n❌ CRITICAL FAILURE")
        traceback.print_exc()
        exit(1)
