import os
import time
import requests
import traceback
from supabase import create_client, Client

# --- CONFIG ---
ATTIO_API_KEY = os.environ.get("ATTIO_API_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

print("🚀 Starting Sync V8 (Save-First Architecture)...")

if not ATTIO_API_KEY or not SUPABASE_URL:
    print("❌ Error: Secrets missing.")
    exit(1)

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception as e:
    print(f"❌ Error connecting to Supabase: {e}")
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
            time.sleep(0.3)
            if method == "POST":
                response = requests.post(url, headers=headers, json=payload)
            else:
                response = requests.get(url, headers=headers, params=params)

            if response.status_code == 429:
                print(f"   ⚠️ Rate Limit. Sleeping 10s...")
                time.sleep(10)
                continue 
            
            # Validation errors (usually means no notes exist for this object type)
            if response.status_code == 400:
                return []

            if response.status_code != 200:
                return []

            return response.json().get("data", [])
        except:
            time.sleep(1)
    return []

def upsert_batch(items):
    if not items: return
    try:
        # Clean metadata
        for item in items:
            if "metadata" in item and item["metadata"]:
                item["metadata"] = {k: v for k, v in item["metadata"].items() if v is not None}
        
        supabase.table("attio_index").upsert(items).execute()
        print(f"   ✅ Saved batch of {len(items)} items.")
    except Exception as e:
        print(f"   ❌ DB Error: {e}")

# --- PAGINATION ---
def fetch_all_records_paginated(slug):
    all_records = []
    limit = 1000
    offset = 0
    print(f"   🔄 Fetching '{slug}' from Attio...")
    
    while True:
        payload = {"limit": limit, "offset": offset}
        batch = make_request("POST", f"objects/{slug}/records/query", payload=payload)
        
        if not batch: break
        
        all_records.extend(batch)
        print(f"      - Downloaded {len(batch)} records (Total: {len(all_records)})")
        
        if len(batch) < limit: break
        offset += limit
        
    return all_records

# --- MAIN SYNC ---
def sync_everything():
    
    # 1. LISTS
    print("--- 1. Syncing Lists ---")
    lists = make_request("GET", "lists")
    list_items = []
    for l in lists:
        list_items.append({
            "id": l['id']['list_id'], "type": "list", "title": l['name'], 
            "content": "", "url": "", "metadata": {}
        })
    upsert_batch(list_items)

    # 2. TASKS
    print("--- 2. Syncing Tasks ---")
    tasks = make_request("GET", "tasks")
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

    # 3. OBJECTS (The Fix)
    print("--- 3. Syncing Objects & Records ---")
    objects = make_request("GET", "objects")
    
    for obj in objects:
        slug = obj['api_slug']
        singular = obj['singular_noun']
        print(f"\n📂 Processing Object: {singular} ({slug})")
        
        # A. DOWNLOAD ALL RECORDS
        records = fetch_all_records_paginated(slug)
        if not records: continue

        # B. PREPARE & SAVE RECORDS IMMEDIATELY
        print(f"   💾 Saving {len(records)} {singular} records to Database...")
        record_batch = []
        
        for rec in records:
            try:
                rec_id = rec['id']['record_id']
                vals = rec.get('values', {})
                
                # Name Finding
                name = "Untitled Record"
                # Try standard name fields
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
            except: pass # Skip bad data
        
        # Upsert Records in chunks of 100
        for i in range(0, len(record_batch), 100):
            upsert_batch(record_batch[i:i+100])

        # C. NOW FETCH NOTES/COMMENTS
        print(f"   📝 Fetching Notes & Comments for {len(records)} records...")
        note_batch = []
        
        for i, rec in enumerate(records):
            try:
                rec_id = rec['id']['record_id']
                
                # 1. Notes
                notes = make_request("GET", "notes", params={
                    "parent_record_id": rec_id,
                    "parent_object": slug
                })
                for n in notes:
                    note_batch.append({
                        "id": n['id']['note_id'], "parent_id": rec_id, "type": "note",
                        "title": f"Note on {singular}",
                        "content": n.get('content_plaintext', ''),
                        "url": f"https://app.attio.com/w/workspace/note/{n['id']['note_id']}",
                        "metadata": {"created_at": n.get("created_at")}
                    })

                # 2. Comments
                comments = make_request("GET", f"objects/{slug}/records/{rec_id}/comments")
                for c in comments:
                    note_batch.append({
                        "id": c['id']['comment_id'], "parent_id": rec_id, "type": "comment",
                        "title": f"Comment on {singular}",
                        "content": c.get('content_plaintext', ''),
                        "url": f"https://app.attio.com/w/workspace/record/{slug}/{rec_id}",
                        "metadata": {"author": c.get("author")}
                    })

                # Batch Save Notes every 50 records
                if len(note_batch) >= 50:
                    upsert_batch(note_batch)
                    note_batch = []
                    # Print progress so you know it's not frozen
                    if i % 100 == 0:
                        print(f"      ...Checked {i}/{len(records)} records for notes...")

            except Exception: pass

        # Final Save of Notes
        upsert_batch(note_batch)

if __name__ == "__main__":
    try:
        sync_everything()
        print("\n🏁 Sync Job Finished.")
    except Exception as e:
        print("\n❌ CRITICAL FAILURE")
        traceback.print_exc()
        exit(1)
