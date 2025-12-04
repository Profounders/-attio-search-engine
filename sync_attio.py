import os
import time
import requests
import traceback
from supabase import create_client, Client

# --- CONFIG ---
ATTIO_API_KEY = os.environ.get("ATTIO_API_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

print("🚀 Starting Sync V14 (Notes Specialist)...")

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
                response = requests.post(url, headers=headers, json=payload, timeout=60)
            else:
                response = requests.get(url, headers=headers, params=params, timeout=60)

            if response.status_code == 429:
                print(f"   ⚠️ Rate Limit. Sleeping 10s...")
                time.sleep(10)
                continue 
            
            if response.status_code == 403:
                print(f"   🚫 PERMISSION DENIED for '{endpoint}'. Check API Scopes!")
                return None

            if response.status_code != 200:
                if attempt == 2 and response.status_code != 404:
                     print(f"   ⚠️ API Error {response.status_code} on {endpoint}")
                return None

            return response.json().get("data", [])

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
        print(f"   💾 Saved batch of {len(items)} items.")
    except Exception as e:
        # Retry logic for DB Timeouts
        if "57014" in str(e) or "timeout" in str(e).lower():
            time.sleep(2)
            try:
                supabase.table("attio_index").upsert(items).execute()
            except: 
                print("   ❌ DB Write Failed after retry.")
        else:
            print(f"   ❌ DB Error: {e}")

# --- 1. SYNC NOTES (THE PRIORITY) ---
def sync_notes_globally():
    """
    Connects to the dedicated GET /v2/notes endpoint.
    Iterates through all pages to ensure no note is left behind.
    """
    print("\n📝 1. Syncing Attio Notes (Class: Note)...")
    
    limit = 1000
    offset = 0
    total_notes = 0
    
    while True:
        # Fetch a page of notes
        params = {"limit": limit, "offset": offset}
        notes = make_request("GET", "notes", params=params)
        
        if not notes:
            if total_notes == 0:
                print("   ⚠️ No notes returned. (Check 'Note:Read' permission if you know notes exist).")
            break
            
        print(f"   📥 Downloaded {len(notes)} notes...")
        
        batch = []
        for n in notes:
            # Extract content carefully
            content = n.get('content_plaintext', '')
            title = n.get('title', 'Untitled Note')
            note_id = n['id']['note_id']
            parent_id = n.get('parent_record_id')
            
            # Map to Database
            batch.append({
                "id": note_id, 
                "parent_id": parent_id, 
                "type": "note",
                "title": f"Note: {title}",
                "content": content, # <--- This contains the text body
                "url": f"https://app.attio.com/w/workspace/note/{note_id}",
                "metadata": {
                    "created_at": n.get("created_at"),
                    "created_by": n.get("created_by_actor")
                }
            })
        
        # Save to Supabase immediately
        upsert_batch(batch)
        
        total_notes += len(notes)
        
        if len(notes) < limit:
            break
        offset += limit
    
    print(f"   ✅ Total Notes Synced: {total_notes}")

# --- 2. SYNC PEOPLE (Context for Notes) ---
def sync_people_names():
    print("\n👤 2. Syncing People (Names for Search Context)...")
    slug = "people"
    limit = 1000
    offset = 0
    
    while True:
        payload = {"limit": limit, "offset": offset}
        records = make_request("POST", f"objects/{slug}/records/query", payload=payload)
        
        if not records: break 

        batch = []
        for rec in records:
            try:
                rec_id = rec['id']['record_id']
                vals = rec.get('values', {})
                
                # Simple Name Extraction
                name = "Unknown Person"
                if 'name' in vals and vals['name']: name = vals['name'][0]['value']
                elif 'email_addresses' in vals and vals['email_addresses']: name = vals['email_addresses'][0]['value']
                
                batch.append({
                    "id": rec_id, "type": "person", "title": name, "content": "", 
                    "url": f"https://app.attio.com/w/workspace/record/people/{rec_id}", "metadata": {} 
                })
            except: pass
        
        upsert_batch(batch)
        if len(records) < limit: break
        offset += limit

# --- 3. SYNC COMPANIES ---
def sync_companies():
    print("\n🏢 3. Syncing Companies...")
    slug = "companies"
    limit = 1000
    offset = 0
    
    while True:
        payload = {"limit": limit, "offset": offset}
        records = make_request("POST", f"objects/{slug}/records/query", payload=payload)
        
        if not records: break 

        batch = []
        for rec in records:
            try:
                rec_id = rec['id']['record_id']
                vals = rec.get('values', {})
                
                name = "Unknown Company"
                if 'name' in vals and vals['name']: name = vals['name'][0]['value']
                
                batch.append({
                    "id": rec_id, "type": "company", "title": name, "content": str(vals), 
                    "url": f"https://app.attio.com/w/workspace/record/companies/{rec_id}", "metadata": {} 
                })
            except: pass
        
        upsert_batch(batch)
        if len(records) < limit: break
        offset += limit

# --- 4. SYNC TASKS ---
def sync_tasks():
    print("\n✅ 4. Syncing Tasks...")
    tasks = make_request("GET", "tasks") or []
    batch = []
    for t in tasks:
        batch.append({
            "id": t['id']['task_id'], "type": "task", 
            "title": f"Task: {t.get('content_plaintext', 'Untitled')}",
            "content": f"Status: {t.get('is_completed')}",
            "url": "https://app.attio.com/w/workspace/tasks",
            "metadata": {"deadline": t.get("deadline_at")}
        })
    upsert_batch(batch)

if __name__ == "__main__":
    try:
        sync_notes_globally()
        sync_people_names()
        sync_companies()
        sync_tasks()
        print("\n🏁 Sync Job Finished.")
    except Exception as e:
        print("\n❌ CRITICAL FAILURE")
        traceback.print_exc()
        exit(1)
