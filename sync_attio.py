import os
import time
import requests
import traceback
from supabase import create_client, Client

# --- CONFIG ---
ATTIO_API_KEY = os.environ.get("ATTIO_API_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

print("🚀 Starting Sync V15 (The 'Mimic' Script)...")

if not ATTIO_API_KEY or not SUPABASE_URL:
    print("❌ Error: Secrets missing.")
    exit(1)

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception as e:
    print(f"❌ Supabase Connection Error: {e}")
    exit(1)

# --- SAFE DB SAVE ---
def safe_upsert(items):
    if not items: return
    try:
        # 1. Clean Metadata (Remove Nulls)
        for item in items:
            if "metadata" in item and isinstance(item["metadata"], dict):
                item["metadata"] = {k: v for k, v in item["metadata"].items() if v is not None}
        
        # 2. Save
        supabase.table("attio_index").upsert(items).execute()
        print(f"   💾 Saved batch of {len(items)} items.")
    except Exception as e:
        print(f"   ❌ DB Error: {e}")

# --- HELPER: SAFE NAME EXTRACTION ---
def get_safe_name(vals):
    """
    Prevents the 'value' error you saw in the logs.
    It carefully checks if data exists before grabbing it.
    """
    try:
        if not vals: return "Untitled"
        
        # Priority 1: Name
        if 'name' in vals and isinstance(vals['name'], list) and len(vals['name']) > 0:
            return vals['name'][0].get('value', 'Untitled')
            
        # Priority 2: Email
        if 'email_addresses' in vals and isinstance(vals['email_addresses'], list) and len(vals['email_addresses']) > 0:
            return vals['email_addresses'][0].get('value', 'Untitled')
            
        # Priority 3: Company Name / Title
        for k in ['company_name', 'title', 'deal_name']:
             if k in vals and isinstance(vals[k], list) and len(vals[k]) > 0:
                 return vals[k][0].get('value', 'Untitled')
                 
        return "Untitled Record"
    except:
        return "Error Parsing Name"

# --- 1. SYNC NOTES (EXACT FORENSIC METHOD) ---
def sync_notes_simple():
    print("\n📝 1. Syncing Notes (Simple Method)...")
    
    url = "https://api.attio.com/v2/notes"
    headers = {
        "Authorization": f"Bearer {ATTIO_API_KEY}", 
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    # We use a simple limit of 200 to be safe (default page size)
    params = {"limit": 200, "offset": 0}
    
    total_synced = 0
    
    while True:
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            
            if response.status_code != 200:
                print(f"   ⚠️ API Error {response.status_code}: {response.text}")
                break
                
            data = response.json().get("data", [])
            if not data:
                break # No more notes
                
            print(f"   📥 Downloaded {len(data)} notes...")
            
            batch = []
            for n in data:
                content = n.get('content_plaintext', '')
                title = n.get('title', 'Untitled Note')
                note_id = n['id']['note_id']
                
                # Check for parent
                parent_id = n.get('parent_record_id')
                
                batch.append({
                    "id": note_id, 
                    "parent_id": parent_id, 
                    "type": "note",
                    "title": f"Note: {title}",
                    "content": content, 
                    "url": f"https://app.attio.com/w/workspace/note/{note_id}",
                    "metadata": {"created_at": n.get("created_at")}
                })
            
            safe_upsert(batch)
            total_synced += len(data)
            
            # Pagination Logic
            if len(data) < 200:
                break
            params["offset"] += 200
            
        except Exception as e:
            print(f"   ❌ Note Sync Crashed: {e}")
            break

    print(f"   ✅ Total Notes Synced: {total_synced}")

# --- 2. SYNC PEOPLE (SAFE MODE) ---
def sync_people_safe():
    print("\n👤 2. Syncing People (Safe Mode)...")
    slug = "people"
    
    url = f"https://api.attio.com/v2/objects/{slug}/records/query"
    headers = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Content-Type": "application/json"}
    
    limit = 500
    offset = 0
    
    while True:
        payload = {"limit": limit, "offset": offset}
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=60)
            if response.status_code != 200: break
            
            records = response.json().get("data", [])
            if not records: break
            
            batch = []
            for rec in records:
                rec_id = rec['id']['record_id']
                vals = rec.get('values', {})
                
                # USE SAFE NAME EXTRACTOR
                name = get_safe_name(vals)
                
                batch.append({
                    "id": rec_id, "type": "person", "title": name, "content": "", 
                    "url": f"https://app.attio.com/w/workspace/record/people/{rec_id}", "metadata": {} 
                })
            
            safe_upsert(batch)
            
            if len(records) < limit: break
            offset += limit
            
        except Exception as e:
            print(f"   ⚠️ Error getting people page: {e}")
            break

# --- 3. SYNC COMPANIES ---
def sync_companies_safe():
    print("\n🏢 3. Syncing Companies...")
    slug = "companies"
    
    url = f"https://api.attio.com/v2/objects/{slug}/records/query"
    headers = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Content-Type": "application/json"}
    
    limit = 500
    offset = 0
    while True:
        payload = {"limit": limit, "offset": offset}
        try:
            response = requests.post(url, headers=headers, json=payload)
            records = response.json().get("data", [])
            if not records: break
            
            batch = []
            for rec in records:
                rec_id = rec['id']['record_id']
                vals = rec.get('values', {})
                name = get_safe_name(vals)
                
                batch.append({
                    "id": rec_id, "type": "company", "title": name, "content": str(vals), 
                    "url": f"https://app.attio.com/w/workspace/record/companies/{rec_id}", "metadata": {} 
                })
            safe_upsert(batch)
            
            if len(records) < limit: break
            offset += limit
        except: break

# --- 4. TASKS ---
def sync_tasks_simple():
    print("\n✅ 4. Syncing Tasks...")
    try:
        response = requests.get("https://api.attio.com/v2/tasks", 
                              headers={"Authorization": f"Bearer {ATTIO_API_KEY}"})
        tasks = response.json().get("data", [])
        batch = []
        for t in tasks:
            batch.append({
                "id": t['id']['task_id'], "type": "task", 
                "title": f"Task: {t.get('content_plaintext', 'Untitled')}",
                "content": f"Status: {t.get('is_completed')}",
                "url": "https://app.attio.com/w/workspace/tasks",
                "metadata": {"deadline": t.get("deadline_at")}
            })
        safe_upsert(batch)
    except: pass

if __name__ == "__main__":
    try:
        sync_notes_simple()
        sync_people_safe()
        sync_companies_safe()
        sync_tasks_simple()
        print("\n🏁 Sync Job Finished.")
    except Exception as e:
        print("\n❌ CRITICAL FAILURE")
        traceback.print_exc()
        exit(1)
