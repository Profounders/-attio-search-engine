import os
import time
import requests
import traceback
from supabase import create_client, Client

# --- CONFIG ---
ATTIO_API_KEY = os.environ.get("ATTIO_API_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

print("🚀 Starting Sync V18 (Smart Names + Note Detective)...")

if not ATTIO_API_KEY or not SUPABASE_URL:
    print("❌ Error: Secrets missing.")
    exit(1)

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception as e:
    print(f"❌ Supabase Connection Error: {e}")
    exit(1)

# --- DB HELPER ---
def safe_upsert(items):
    if not items: return
    try:
        # Clean metadata
        for item in items:
            if "metadata" in item and isinstance(item["metadata"], dict):
                item["metadata"] = {k: v for k, v in item["metadata"].items() if v is not None}
        
        supabase.table("attio_index").upsert(items).execute()
        print(f"   💾 Saved batch of {len(items)}.")
    except Exception as e:
        print(f"   ❌ DB Error: {e}")

# --- HELPER: SMART NAME EXTRACTOR (FROM V16) ---
def extract_smart_name_and_content(vals):
    """
    Scans raw data for any name-like field to ensure
    People are not blank in the database.
    """
    name = "Untitled Person"
    email = ""
    
    try:
        # 1. Try Find Name (Iterate common keys)
        # This handles 'full_name', 'name', 'first_name', etc.
        for key in ['name', 'full_name', 'first_name', 'title', 'company_name']:
            if key in vals and vals[key]:
                item = vals[key][0]
                if 'value' in item:
                    name = item['value']
                elif 'first_name' in item: # Handle composite names
                    name = f"{item.get('first_name', '')} {item.get('last_name', '')}"
                break
        
        # 2. Try Find Email
        if 'email_addresses' in vals and vals['email_addresses']:
            email = vals['email_addresses'][0].get('value', '')
        elif 'email' in vals and vals['email']:
            email = vals['email'][0].get('value', '')

        # 3. Fallback: If name is still "Untitled", use Email
        if name == "Untitled Person" and email:
            name = email

        # 4. Content String (For Search)
        # This ensures the search bar finds this person by Name OR Email
        content = f"Name: {name} | Email: {email}"
            
    except Exception:
        content = "Error parsing person"

    return name, content

# --- 1. GLOBAL NOTES (VERBOSE LOGGING) ---
def sync_notes_global():
    print("\n📝 1. Syncing GLOBAL Notes...")
    
    url = "https://api.attio.com/v2/notes"
    headers = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Accept": "application/json"}
    
    limit = 1000
    offset = 0
    total_found = 0
    
    while True:
        params = {"limit": limit, "offset": offset}
        try:
            response = requests.get(url, headers=headers, params=params, timeout=45)
            data = response.json().get("data", [])
            count = len(data)
            
            if count == 0: break 
            
            batch = []
            for n in data:
                batch.append({
                    "id": n['id']['note_id'], 
                    "parent_id": n.get('parent_record_id'), 
                    "type": "note",
                    "title": f"Note: {n.get('title', 'Untitled')}",
                    "content": n.get('content_plaintext', ''), 
                    "url": f"https://app.attio.com/w/workspace/note/{n['id']['note_id']}",
                    "metadata": {"created_at": n.get("created_at")}
                })
            
            safe_upsert(batch)
            total_found += count
            print(f"   📥 Synced {count} notes (Total: {total_found})")
            
            if count < limit: break
            offset += limit
            
        except Exception as e:
            print(f"   ❌ Note Sync Error: {e}")
            break

# --- 2. NOTES VIA COMPANIES (BACKDOOR) ---
def sync_notes_via_companies():
    print("\n🏢 2. Syncing Notes attached to Companies...")
    slug = "companies"
    limit = 1000
    offset = 0
    
    while True:
        payload = {"limit": limit, "offset": offset}
        try:
            res = requests.post(f"https://api.attio.com/v2/objects/{slug}/records/query", 
                                headers={"Authorization": f"Bearer {ATTIO_API_KEY}"}, json=payload)
            companies = res.json().get("data", [])
            if not companies: break
            
            note_batch = []
            for comp in companies:
                cid = comp['id']['record_id']
                # Check for notes on this specific company
                n_res = requests.get("https://api.attio.com/v2/notes", 
                                     headers={"Authorization": f"Bearer {ATTIO_API_KEY}"},
                                     params={"parent_record_id": cid, "parent_object": slug})
                notes = n_res.json().get("data", [])
                
                for n in notes:
                    note_batch.append({
                        "id": n['id']['note_id'], 
                        "parent_id": cid, "type": "note",
                        "title": f"Note: {n.get('title', 'Untitled')}",
                        "content": n.get('content_plaintext', ''), 
                        "url": f"https://app.attio.com/w/workspace/note/{n['id']['note_id']}",
                        "metadata": {"created_at": n.get("created_at")}
                    })
            
            safe_upsert(note_batch)
            if len(companies) < limit: break
            offset += limit
        except: break

# --- 3. PEOPLE (SMART PARSING) ---
def sync_people_smart():
    print("\n👤 3. Syncing People (Smart Parsing)...")
    slug = "people"
    limit = 1000
    offset = 0
    
    while True:
        try:
            payload = {"limit": limit, "offset": offset}
            res = requests.post(f"https://api.attio.com/v2/objects/{slug}/records/query", 
                                headers={"Authorization": f"Bearer {ATTIO_API_KEY}"}, json=payload)
            people = res.json().get("data", [])
            
            if not people: break
            
            batch = []
            for p in people:
                pid = p['id']['record_id']
                vals = p.get('values', {})
                
                # USE SMART EXTRACTOR to ensure name/content is not blank
                name, content = extract_smart_name_and_content(vals)
                
                batch.append({
                    "id": pid, "type": "person", 
                    "title": name, 
                    "content": content, # Populated with Name + Email
                    "url": f"https://app.attio.com/w/workspace/record/people/{pid}", 
                    "metadata": {} 
                })
            
            safe_upsert(batch)
            print(f"   📥 Synced {len(people)} people...")
            
            if len(people) < limit: break
            offset += limit
        except Exception as e:
            print(f"   ⚠️ People Error: {e}")
            break

# --- 4. TASKS ---
def sync_tasks():
    print("\n✅ 4. Syncing Tasks...")
    try:
        res = requests.get("https://api.attio.com/v2/tasks", headers={"Authorization": f"Bearer {ATTIO_API_KEY}"})
        tasks = res.json().get("data", [])
        batch = []
        for t in tasks:
            batch.append({
                "id": t['id']['task_id'], "type": "task", 
                "title": f"Task: {t.get('content_plaintext', 'Untitled')}",
                "content": f"Status: {t.get('is_completed')}",
                "url": "https://app.attio.com/w/workspace/tasks", "metadata": {}
            })
        safe_upsert(batch)
    except: pass

if __name__ == "__main__":
    try:
        sync_notes_global()
        sync_notes_via_companies() 
        sync_people_smart()
        sync_tasks()
        print("\n🏁 Sync Job Finished.")
    except Exception as e:
        print("\n❌ CRITICAL FAILURE")
        traceback.print_exc()
        exit(1)
