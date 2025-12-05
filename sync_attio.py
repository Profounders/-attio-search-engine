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

print("🚀 Starting Sync V23 (The Exhaustive Crawler)...")

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
        for item in items:
            if "metadata" in item and isinstance(item["metadata"], dict):
                item["metadata"] = {k: v for k, v in item["metadata"].items() if v is not None}
        
        supabase.table("attio_index").upsert(items).execute()
        print(f"   💾 Saved batch of {len(items)}.")
    except Exception as e:
        print(f"   ❌ DB Error: {e}")

# --- HELPER: SMART NAME EXTRACTOR ---
def extract_smart_name_and_content(vals):
    name = "Untitled"
    email = ""
    try:
        # Scan all values for a string that looks like a name
        for key, field_list in vals.items():
            if not field_list: continue
            item = field_list[0]
            
            # Priority Name Fields
            if key in ['name', 'title', 'deal_name', 'project_name', 'full_name']:
                if 'value' in item: 
                    name = item['value']
                    break
            
            # Fallback: Capture Email
            if key in ['email_addresses', 'email']:
                email = item.get('value', '')

        if name == "Untitled" and email: name = email
        content = f"Name: {name} | Email: {email}"
    except:
        content = "Error parsing record"
    return name, content

# --- 1. GLOBAL NOTES (Standard Baseline) ---
def sync_notes_global():
    print("\n📝 1. Syncing Global Notes Endpoint...")
    url = "https://api.attio.com/v2/notes"
    headers = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Accept": "application/json"}
    limit = 1000
    offset = 0
    total = 0
    
    while True:
        try:
            res = requests.get(url, headers=headers, params={"limit": limit, "offset": offset}, timeout=60)
            data = res.json().get("data", [])
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
            total += len(data)
            if len(data) < limit: break
            offset += limit
        except: break
    print(f"   ✅ Global Endpoint found {total} notes.")

# --- 2. EXHAUSTIVE OBJECT CRAWLER ---
def sync_exhaustive_objects():
    print("\n🕵️ 2. Starting Exhaustive Object Crawl (Checking Deals, Projects, etc)...")
    
    # A. Get ALL Object Types
    headers = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Accept": "application/json"}
    obj_res = requests.get("https://api.attio.com/v2/objects", headers=headers)
    objects = obj_res.json().get("data", [])
    
    for obj in objects:
        slug = obj['api_slug']
        singular = obj['singular_noun']
        
        # Skip People/Companies (We do them separately to avoid timeout, or you can include them)
        # Note: If you are missing notes on People, comment out the next line.
        if slug in ["people", "companies"]: 
            continue 
            
        print(f"\n📂 Crawling Object: {singular} ({slug})...")
        
        limit = 500
        offset = 0
        
        while True:
            # B. Get Records for this Object
            payload = {"limit": limit, "offset": offset}
            res = requests.post(f"https://api.attio.com/v2/objects/{slug}/records/query", 
                                headers=headers, json=payload)
            records = res.json().get("data", [])
            
            if not records: break
            
            print(f"   ...Checking {len(records)} {slug} records for attached notes...")
            
            note_batch = []
            record_batch = []
            
            for rec in records:
                rec_id = rec['id']['record_id']
                vals = rec.get('values', {})
                name, content = extract_smart_name_and_content(vals)
                
                # Save the Record itself (Deal, Project, etc)
                record_batch.append({
                    "id": rec_id, "type": slug, "title": name, "content": str(vals),
                    "url": f"https://app.attio.com/w/workspace/record/{slug}/{rec_id}", "metadata": {}
                })
                
                # C. EXPLICITLY FETCH NOTES FOR THIS RECORD
                # This bypasses the Global Filter logic
                try:
                    n_res = requests.get("https://api.attio.com/v2/notes", headers=headers,
                                         params={"parent_record_id": rec_id, "parent_object": slug})
                    notes = n_res.json().get("data", [])
                    
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
                except: pass
                
                # D. FETCH COMMENTS (Often used as notes)
                try:
                    c_res = requests.get(f"https://api.attio.com/v2/objects/{slug}/records/{rec_id}/comments", headers=headers)
                    comments = c_res.json().get("data", [])
                    for c in comments:
                        note_batch.append({
                            "id": c['id']['comment_id'], "parent_id": rec_id, "type": "comment",
                            "title": f"Comment on {name}",
                            "content": c.get('content_plaintext', ''), 
                            "url": f"https://app.attio.com/w/workspace/record/{slug}/{rec_id}",
                            "metadata": {"author": c.get("author")}
                        })
                except: pass

            # Save
            safe_upsert(record_batch)
            if note_batch:
                print(f"      found {len(note_batch)} hidden notes/comments!")
                safe_upsert(note_batch)

            if len(records) < limit: break
            offset += limit

# --- 3. PEOPLE & COMPANIES (Standard Sync) ---
def sync_standard_entities():
    print("\n👤 3. Syncing People & Companies (Standard)...")
    for slug in ["people", "companies"]:
        offset = 0
        while True:
            try:
                res = requests.post(f"https://api.attio.com/v2/objects/{slug}/records/query", 
                                    headers={"Authorization": f"Bearer {ATTIO_API_KEY}"}, 
                                    json={"limit": 1000, "offset": offset})
                data = res.json().get("data", [])
                if not data: break
                
                batch = []
                for rec in data:
                    rec_id = rec['id']['record_id']
                    name, content = extract_smart_name_and_content(rec.get('values', {}))
                    batch.append({
                        "id": rec_id, "type": slug, "title": name, "content": content,
                        "url": f"https://app.attio.com/w/workspace/record/{slug}/{rec_id}", "metadata": {}
                    })
                safe_upsert(batch)
                if len(data) < 1000: break
                offset += 1000
            except: break

if __name__ == "__main__":
    try:
        sync_notes_global()
        sync_exhaustive_objects() # <--- THE NEW CRAWLER
        sync_standard_entities()
        print("\n🏁 Sync Job Finished.")
    except Exception as e:
        print("\n❌ CRITICAL FAILURE")
        traceback.print_exc()
        exit(1)
