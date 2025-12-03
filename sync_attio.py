import os
import time
import requests
import traceback
from supabase import create_client, Client

# --- CONFIG ---
ATTIO_API_KEY = os.environ.get("ATTIO_API_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# Global flag to track if we successfully got notes the fast way
NOTES_ALREADY_SYNCED = False

print("🚀 Starting Sync V9 (Streaming + Fast Notes)...")

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
            # Reduced sleep time for speed (0.1s)
            time.sleep(0.1)
            
            if method == "POST":
                response = requests.post(url, headers=headers, json=payload)
            else:
                response = requests.get(url, headers=headers, params=params)

            if response.status_code == 429:
                print(f"   ⚠️ Rate Limit. Sleeping 5s...")
                time.sleep(5)
                continue 
            
            if response.status_code != 200:
                # Return None so we know it failed, rather than empty list
                if attempt == 2: 
                     # Only print error on final attempt to reduce noise
                     # check if it is just a 404 (not found) which is fine
                     if response.status_code != 404:
                         print(f"   ⚠️ API {response.status_code} on {endpoint}")
                return None

            data = response.json().get("data", [])
            return data

        except Exception as e:
            time.sleep(1)
    return None

def upsert_batch(items):
    if not items: return
    try:
        # Clean metadata
        for item in items:
            if "metadata" in item and item["metadata"]:
                item["metadata"] = {k: v for k, v in item["metadata"].items() if v is not None}
        
        supabase.table("attio_index").upsert(items).execute()
        # Print a dot for progress
        print(f"   💾 Saved batch of {len(items)} items.")
    except Exception as e:
        print(f"   ❌ DB Error: {e}")

# --- FAST SYNC FUNCTIONS ---
def try_global_notes_sync():
    """
    Attempts to fetch ALL notes globally. 
    If this works, we save hours of time.
    """
    global NOTES_ALREADY_SYNCED
    print("\n⚡ Attempting Fast Note Sync...")
    
    # Try getting notes without ANY parameters (Global List)
    notes = make_request("GET", "notes")
    
    if notes is not None:
        print(f"   ✅ Fast Sync worked! Found {len(notes)} notes globally.")
        batch = []
        for n in notes:
            batch.append({
                "id": n['id']['note_id'], 
                "parent_id": n.get('parent_record_id'), # Might be None in global view
                "type": "note",
                "title": f"Note: {n.get('title', 'Untitled')}",
                "content": n.get('content_plaintext', ''),
                "url": f"https://app.attio.com/w/workspace/note/{n['id']['note_id']}",
                "metadata": {"created_at": n.get("created_at")}
            })
            if len(batch) >= 100:
                upsert_batch(batch)
                batch = []
        upsert_batch(batch)
        NOTES_ALREADY_SYNCED = True
    else:
        print("   🔸 Fast Sync failed (API requires filtering). Switching to Slow Mode for Notes.")

# --- STREAMING LOGIC ---
def process_object_streaming(slug, singular_name):
    """
    Fetches records page by page and SAVES them immediately.
    """
    limit = 1000
    offset = 0
    print(f"\n📂 Streaming {singular_name} ({slug})...")
    
    while True:
        # 1. Fetch Page
        payload = {"limit": limit, "offset": offset}
        records = make_request("POST", f"objects/{slug}/records/query", payload=payload)
        
        if not records:
            break # End of data
            
        print(f"   📥 Downloaded records {offset} to {offset + len(records)}...")

        # 2. Process & Save Records IMMEDIATELY
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
        
        # Save Records now
        upsert_batch(record_batch)

        # 3. Fetch Notes/Comments (SLOW PATH)
        # Only do this if Fast Sync failed OR if we need comments (comments are always local)
        if not NOTES_ALREADY_SYNCED:
            print(f"   🐢 Slow-Syncing notes for this batch...")
            note_batch = []
            for i, rec in enumerate(records):
                try:
                    rec_id = rec['id']['record_id']
                    
                    # Fetch Notes (Safe filtering)
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
                    
                    # Fetch Comments (Always needed as they aren't "Notes")
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
            
            # Save the notes for this page
            upsert_batch(note_batch)

        # Move to next page
        if len(records) < limit:
            break
        offset += limit

# --- MAIN SYNC ---
def sync_everything():
    
    # 1. LISTS
    print("--- 1. Syncing Lists ---")
    lists = make_request("GET", "lists") or []
    list_items = []
    for l in lists:
        list_items.append({
            "id": l['id']['list_id'], "type": "list", "title": l['name'], 
            "content": "", "url": "", "metadata": {}
        })
    upsert_batch(list_items)

    # 2. TASKS
    print("--- 2. Syncing Tasks ---")
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
    upsert_batch(task_items)

    # 3. TRY FAST NOTES
    try_global_notes_sync()

    # 4. OBJECTS & RECORDS
    print("\n--- 3. Syncing Objects ---")
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
