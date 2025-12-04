import os
import time
import requests
import traceback
from supabase import create_client, Client

# --- CONFIG ---
ATTIO_API_KEY = os.environ.get("ATTIO_API_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

print("🚀 Starting Sync V12 (Lightweight People + Full Notes)...")

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
            time.sleep(0.1)
            
            if method == "POST":
                # Timeout set to 60s
                response = requests.post(url, headers=headers, json=payload, timeout=60)
            else:
                response = requests.get(url, headers=headers, params=params, timeout=60)

            if response.status_code == 429:
                print(f"   ⚠️ Rate Limit. Sleeping 5s...")
                time.sleep(5)
                continue 
            
            if response.status_code != 200:
                if attempt == 2 and response.status_code != 404:
                     # Silence non-critical errors
                     pass
                return None

            return response.json().get("data", [])

        except Exception as e:
            time.sleep(1)
    return None

def safe_upsert(items, batch_size=100):
    if not items: return
    
    # Process in chunks
    for i in range(0, len(items), batch_size):
        chunk = items[i:i+batch_size]
        try:
            # Clean metadata
            for item in chunk:
                if "metadata" in item and item["metadata"]:
                    item["metadata"] = {k: v for k, v in item["metadata"].items() if v is not None}
            
            supabase.table("attio_index").upsert(chunk).execute()
            print(f"   💾 Saved {len(chunk)} items...")
            
        except Exception as e:
            if "57014" in str(e) or "timeout" in str(e).lower():
                print(f"   ⚠️ DB Timeout. Retrying small batch...")
                time.sleep(2)
                safe_upsert(chunk, batch_size=10) # Recursive retry
            else:
                print(f"   ❌ DB Error: {e}")

# --- 1. GLOBAL NOTES (FULL CONTENT) ---
def sync_notes_globally_paginated():
    print("\n📝 1. Syncing ALL Notes (Full Content)...")
    
    limit = 1000
    offset = 0
    total_notes = 0
    
    while True:
        params = {"limit": limit, "offset": offset}
        notes = make_request("GET", "notes", params=params)
        
        if not notes:
            break
            
        batch = []
        for n in notes:
            # We KEEP full content for notes
            content = n.get('content_plaintext', '')
            title = n.get('title', 'Untitled Note')
            
            batch.append({
                "id": n['id']['note_id'], 
                "parent_id": n.get('parent_record_id'), 
                "type": "note",
                "title": f"Note: {title}",
                "content": content, # Full text search enabled here
                "url": f"https://app.attio.com/w/workspace/note/{n['id']['note_id']}",
                "metadata": {"created_at": n.get("created_at")}
            })
        
        # Save Notes
        safe_upsert(batch, batch_size=50)
        
        total_notes += len(notes)
        print(f"      - Processed {len(notes)} notes (Total: {total_notes})")
        
        if len(notes) < limit:
            break
        offset += limit

# --- 2. PEOPLE (LIGHTWEIGHT - NAMES ONLY) ---
def sync_people_lightweight():
    print("\n👤 2. Syncing People (Names Only)...")
    slug = "people"
    
    limit = 1000 # We can increase batch size now because data is light
    offset = 0
    total_people = 0
    
    while True:
        payload = {"limit": limit, "offset": offset}
        records = make_request("POST", f"objects/{slug}/records/query", payload=payload)
        
        if not records:
            break 

        record_batch = []
        for rec in records:
            try:
                rec_id = rec['id']['record_id']
                vals = rec.get('values', {})
                
                # Extract Name Only
                name = "Untitled Person"
                if 'name' in vals and vals['name']: 
                     name = vals['name'][0]['value']
                elif 'email_addresses' in vals and vals['email_addresses']:
                     name = vals['email_addresses'][0]['value']
                
                record_batch.append({
                    "id": rec_id, 
                    "type": "person", 
                    "title": name,
                    "content": "", # <--- EMPTY CONTENT (Optimized)
                    "url": f"https://app.attio.com/w/workspace/record/people/{rec_id}",
                    "metadata": {} 
                })
            except: pass
        
        # Save People
        safe_upsert(record_batch, batch_size=200) # Fast save
        
        total_people += len(records)
        print(f"      - Processed {len(records)} people (Total: {total_people})")

        if len(records) < limit:
            break
        offset += limit

# --- 3. COMPANIES & OTHERS ---
def sync_other_objects():
    print("\n🏢 3. Syncing Companies & Others...")
    objects = make_request("GET", "objects") or []
    
    for obj in objects:
        slug = obj['api_slug']
        if slug == "people": continue # Already done
        
        print(f"   📂 Processing {obj['singular_noun']} ({slug})...")
        
        limit = 1000
        offset = 0
        while True:
            payload = {"limit": limit, "offset": offset}
            records = make_request("POST", f"objects/{slug}/records/query", payload=payload)
            if not records: break
            
            record_batch = []
            for rec in records:
                try:
                    rec_id = rec['id']['record_id']
                    vals = rec.get('values', {})
                    
                    name = "Untitled"
                    # Try to find a name attribute
                    for k in ['name', 'company_name', 'title', 'deal_name']:
                         if k in vals and vals[k]:
                             name = vals[k][0]['value']
                             break
                    
                    record_batch.append({
                        "id": rec_id, "type": slug, "title": name,
                        "content": str(vals), # Keep context for companies/deals
                        "url": f"https://app.attio.com/w/workspace/record/{slug}/{rec_id}",
                        "metadata": {} 
                    })
                except: pass
            
            safe_upsert(record_batch, batch_size=100)
            
            if len(records) < limit: break
            offset += limit

# --- 4. TASKS ---
def sync_tasks():
    print("\n✅ 4. Syncing Tasks...")
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
    safe_upsert(task_items, batch_size=100)

if __name__ == "__main__":
    try:
        sync_notes_globally_paginated()
        sync_people_lightweight()
        sync_other_objects()
        sync_tasks()
        print("\n🏁 Sync Job Finished.")
    except Exception as e:
        print("\n❌ CRITICAL FAILURE")
        traceback.print_exc()
        exit(1)
