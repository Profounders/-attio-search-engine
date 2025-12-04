import os
import time
import requests
import traceback
import json
from supabase import create_client, Client

# --- CONFIG ---
ATTIO_API_KEY = os.environ.get("ATTIO_API_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

print("🚀 Starting Sync V20 (Full Sync + Call Transcripts)...")

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

# --- HELPER: NAME EXTRACTOR ---
def extract_smart_name_and_content(vals):
    name = "Untitled Person"
    email = ""
    try:
        for key in ['name', 'full_name', 'first_name', 'title', 'company_name']:
            if key in vals and vals[key]:
                item = vals[key][0]
                if 'value' in item: name = item['value']
                elif 'first_name' in item: name = f"{item.get('first_name', '')} {item.get('last_name', '')}"
                break
        
        if 'email_addresses' in vals and vals['email_addresses']:
            email = vals['email_addresses'][0].get('value', '')
        
        if name == "Untitled Person" and email: name = email
        content = f"Name: {name} | Email: {email}"
    except:
        content = "Error parsing person"
    return name, content

# --- 1. GLOBAL NOTES (FULL SCAN) ---
def sync_notes_global():
    print("\n📝 1. Syncing ALL Notes...")
    url = "https://api.attio.com/v2/notes"
    headers = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Accept": "application/json"}
    limit = 1000
    offset = 0
    
    while True:
        params = {"limit": limit, "offset": offset}
        try:
            response = requests.get(url, headers=headers, params=params, timeout=60)
            data = response.json().get("data", [])
            
            if not data: break 
            
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
            if len(data) < limit: break
            offset += limit
            
        except Exception as e:
            print(f"   ❌ Note Sync Error: {e}")
            break

# --- 2. CALL RECORDINGS & TRANSCRIPTS (NEW) ---
def sync_call_recordings():
    print("\n📞 2. Hunting for Call Recordings/Transcripts...")
    
    # 1. Find the object (It might be called 'calls', 'meetings', 'call_recordings')
    headers = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Accept": "application/json"}
    objects = requests.get("https://api.attio.com/v2/objects", headers=headers).json().get("data", [])
    
    target_slug = None
    for obj in objects:
        slug = obj['api_slug']
        if "call" in slug or "recording" in slug or "meeting" in slug:
            print(f"   🔎 Found candidate object: {slug}")
            target_slug = slug
            # We don't break, we sync ALL matching candidates just in case
            
            print(f"   ...Syncing {target_slug}...")
            
            limit = 200
            offset = 0
            while True:
                # Fetch Records
                payload = {"limit": limit, "offset": offset}
                res = requests.post(f"https://api.attio.com/v2/objects/{target_slug}/records/query", 
                                    headers=headers, json=payload)
                records = res.json().get("data", [])
                
                if not records: break
                
                batch = []
                for rec in records:
                    rec_id = rec['id']['record_id']
                    vals = rec.get('values', {})
                    
                    # A. Find Title
                    title = "Untitled Call"
                    if 'name' in vals: title = vals['name'][0]['value']
                    elif 'title' in vals: title = vals['title'][0]['value']
                    elif 'topic' in vals: title = vals['topic'][0]['value']
                    
                    # B. Find Transcript (The Heavy Text)
                    transcript = ""
                    # Look for likely transcript keys
                    for key in ['transcript', 'description', 'notes', 'summary', 'body', 'text']:
                        if key in vals and vals[key]:
                            val_item = vals[key][0]
                            # Attio rich text often comes as 'content_plaintext' inside the value, or just 'value'
                            if 'value' in val_item: transcript = val_item['value']
                            elif 'content_plaintext' in val_item: transcript = val_item['content_plaintext']
                            break
                    
                    # Only sync if we have something useful
                    batch.append({
                        "id": rec_id,
                        "type": "call_recording", # Special type for search icon
                        "title": f"Call: {title}",
                        "content": transcript, # <--- The large text goes here
                        "url": f"https://app.attio.com/w/workspace/record/{target_slug}/{rec_id}",
                        "metadata": {"object_slug": target_slug}
                    })
                
                # C. Save in TINY batches (Transcripts are large!)
                # Sending 50 transcripts at once will timeout. We send 5 at a time.
                chunk_size = 5
                for i in range(0, len(batch), chunk_size):
                    safe_upsert(batch[i:i+chunk_size])
                
                if len(records) < limit: break
                offset += limit

# --- 3. PEOPLE (FULL SYNC) ---
def sync_people_full():
    print("\n👤 3. Syncing People (Full Names)...")
    slug = "people"
    limit = 1000
    offset = 0
    
    while True:
        payload = {"limit": limit, "offset": offset}
        try:
            res = requests.post(f"https://api.attio.com/v2/objects/{slug}/records/query", 
                                headers={"Authorization": f"Bearer {ATTIO_API_KEY}"}, json=payload)
            people = res.json().get("data", [])
            
            if not people: break
            
            batch = []
            for p in people:
                pid = p['id']['record_id']
                vals = p.get('values', {})
                name, content = extract_smart_name_and_content(vals)
                
                batch.append({
                    "id": pid, "type": "person", 
                    "title": name, "content": content,
                    "url": f"https://app.attio.com/w/workspace/record/people/{pid}", 
                    "metadata": {} 
                })
            
            safe_upsert(batch)
            if len(people) < limit: break
            offset += limit
        except Exception as e:
            print(f"   ⚠️ People Error: {e}")
            break

# --- 4. COMPANIES (FULL SYNC) ---
def sync_companies_full():
    print("\n🏢 4. Syncing Companies...")
    slug = "companies"
    limit = 1000
    offset = 0
    while True:
        try:
            payload = {"limit": limit, "offset": offset}
            res = requests.post(f"https://api.attio.com/v2/objects/{slug}/records/query", 
                                headers={"Authorization": f"Bearer {ATTIO_API_KEY}"}, json=payload)
            records = res.json().get("data", [])
            if not records: break
            
            batch = []
            for rec in records:
                rec_id = rec['id']['record_id']
                vals = rec.get('values', {})
                name = "Untitled"
                if 'name' in vals: name = vals['name'][0]['value']
                elif 'company_name' in vals: name = vals['company_name'][0]['value']
                
                batch.append({
                    "id": rec_id, "type": "company", "title": name, "content": str(vals), 
                    "url": f"https://app.attio.com/w/workspace/record/companies/{rec_id}", "metadata": {} 
                })
            safe_upsert(batch)
            if len(records) < limit: break
            offset += limit
        except: break

# --- 5. TASKS ---
def sync_tasks():
    print("\n✅ 5. Syncing Tasks...")
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
        sync_call_recordings() # <--- NEW
        sync_people_full()
        sync_companies_full()
        sync_tasks()
        print("\n🏁 Sync Job Finished.")
    except Exception as e:
        print("\n❌ CRITICAL FAILURE")
        traceback.print_exc()
        exit(1)
