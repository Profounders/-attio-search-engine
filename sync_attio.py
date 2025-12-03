import os
import time
import requests
import traceback
from supabase import create_client, Client

# --- CONFIG ---
ATTIO_API_KEY = os.environ.get("ATTIO_API_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

print("🚀 Starting Sync V6 (Pagination Enabled)...")

if not ATTIO_API_KEY or not SUPABASE_URL:
    print("❌ Error: Secrets missing.")
    exit(1)

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception as e:
    print(f"❌ Error connecting to Supabase: {e}")
    exit(1)

# --- API HELPER WITH RETRY & BACKOFF ---
def make_request(method, endpoint, payload=None, params=None):
    url = f"https://api.attio.com/v2/{endpoint}"
    headers = {
        "Authorization": f"Bearer {ATTIO_API_KEY}", 
        "Content-Type": "application/json", 
        "Accept": "application/json"
    }
    
    retries = 3
    for attempt in range(retries):
        try:
            # Sleep slightly to be nice to the API
            time.sleep(0.3) 
            
            if method == "POST":
                response = requests.post(url, headers=headers, json=payload)
            else:
                response = requests.get(url, headers=headers, params=params)

            # Handle Rate Limits (429)
            if response.status_code == 429:
                print(f"   ⚠️ Rate Limit hit. Sleeping for 10 seconds... (Attempt {attempt+1}/{retries})")
                time.sleep(10)
                continue # Retry loop
            
            if response.status_code == 404:
                # Object doesn't exist, not critical, just return empty
                return []
                
            if response.status_code != 200:
                print(f"   ⚠️ API Error {response.status_code} on {endpoint}: {response.text[:100]}")
                return []

            return response.json().get("data", [])

        except Exception as e:
            print(f"   ❌ Network Exception: {e}")
            time.sleep(2)
    return []

def upsert_batch(table_name, items):
    """Sends a batch of items to Supabase to save time"""
    if not items: return
    try:
        # Clean metadata
        for item in items:
            if "metadata" in item and item["metadata"]:
                item["metadata"] = {k: v for k, v in item["metadata"].items() if v is not None}
        
        supabase.table(table_name).upsert(items).execute()
        print(f"   ✅ Saved batch of {len(items)} items.")
    except Exception as e:
        print(f"   ❌ DB Batch Error: {e}")

# --- PAGINATION LOGIC ---
def fetch_all_records_paginated(slug):
    """
    Fetches ALL records for a slug using offset pagination.
    """
    all_records = []
    limit = 1000
    offset = 0
    
    print(f"   🔄 Fetching '{slug}' (Batch size: {limit})...")
    
    while True:
        payload = {
            "limit": limit,
            "offset": offset
        }
        
        batch = make_request("POST", f"objects/{slug}/records/query", payload=payload)
        
        if not batch:
            break # No more results
            
        all_records.extend(batch)
        print(f"      - Fetched {len(batch)} records (Total so far: {len(all_records)})")
        
        if len(batch) < limit:
            break # We reached the end
            
        offset += limit
        
    return all_records

# --- MAIN SYNC LOGIC ---
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
    upsert_batch("attio_index", list_items)

    # 2. TASKS (Global)
    print("--- 2. Syncing Tasks ---")
    tasks = make_request("GET", "tasks")
    task_items = []
    for t in tasks:
        task_items.append({
            "id": t['id']['task_id'],
            "type": "task",
            "title": f"Task: {t.get('content_plaintext', 'Untitled')}",
            "content": f"Status: {t.get('is_completed')}",
            "url": "https://app.attio.com/w/workspace/tasks",
            "metadata": {"deadline": t.get("deadline_at")}
        })
    upsert_batch("attio_index", task_items)

    # 3. OBJECTS & RECORDS (The Big Loop)
    print("--- 3. Syncing Objects & Records ---")
    objects = make_request("GET", "objects")
    
    for obj in objects:
        slug = obj['api_slug']
        print(f"\n📂 Processing Object: {obj['singular_noun']} ({slug})")
        
        # A. GET ALL RECORDS (Paginated)
        records = fetch_all_records_paginated(slug)
        
        if not records:
            print("      (Skipping - No records found)")
            continue

        # Prepare batches for database
        record_batch = []
        note_batch = []
        
        # B. PROCESS RECORDS
        for i, rec in enumerate(records):
            try:
                rec_id = rec['id']['record_id']
                vals = rec.get('values', {})
                
                # Name Finding
                name = "Untitled"
                for k in ['name', 'title', 'company_name', 'email_addresses', 'domains', 'deal_name']:
                    if k in vals and vals[k] and isinstance(vals[k], list) and len(vals[k]) > 0:
                        name = vals[k][0]['value']
                        break
                
                # Add to Batch
                record_batch.append({
                    "id": rec_id,
                    "type": slug,
                    "title": name,
                    "content": str(vals),
                    "url": f"https://app.attio.com/w/workspace/record/{slug}/{rec_id}",
                    "metadata": {} 
                })

                # C. FETCH NOTES FOR THIS RECORD
                # We fetch this individually. It is slow but accurate.
                notes = make_request("GET", "notes", params={"parent_record_id": rec_id})
                for n in notes:
                    note_batch.append({
                        "id": n['id']['note_id'],
                        "parent_id": rec_id,
                        "type": "note",
                        "title": f"Note on {name}",
                        "content": n.get('content_plaintext', ''),
                        "url": f"https://app.attio.com/w/workspace/note/{n['id']['note_id']}",
                        "metadata": {"created_at": n.get("created_at")}
                    })
                
                # D. SAVE BATCHES EVERY 50 RECORDS (To prevent memory issues)
                if len(record_batch) >= 50:
                    upsert_batch("attio_index", record_batch)
                    upsert_batch("attio_index", note_batch)
                    record_batch = []
                    note_batch = []
                    print(f"      ...Processed {i+1}/{len(records)} records...")

            except Exception as e:
                pass # Skip broken record

        # Save remaining
        upsert_batch("attio_index", record_batch)
        upsert_batch("attio_index", note_batch)

if __name__ == "__main__":
    try:
        sync_everything()
        print("\n🏁 Sync Job Finished.")
    except Exception as e:
        print("\n❌ CRITICAL FAILURE")
        traceback.print_exc()
        exit(1)
