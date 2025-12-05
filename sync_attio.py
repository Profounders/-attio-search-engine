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

print("🚀 Starting Sync V25 (Immediate Note Recovery)...")

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

# --- HELPER: NAME EXTRACTOR ---
def extract_smart_name_and_content(vals):
    name = "Untitled"
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
        
        if name == "Untitled" and email: name = email
        content = f"Name: {name} | Email: {email}"
    except:
        content = "Error parsing"
    return name, content

# --- 1. GLOBAL NOTES RECOVERY (The Proven Method) ---
def sync_notes_global_recovery():
    print("\n📝 1. Recovering Global Notes...")
    
    url = "https://api.attio.com/v2/notes"
    headers = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Accept": "application/json"}
    
    limit = 1000
    offset = 0
    total = 0
    
    while True:
        # We revert to the standard GET /v2/notes which we know works for at least 400 records
        params = {"limit": limit, "offset": offset}
        try:
            response = requests.get(url, headers=headers, params=params, timeout=60)
            data = response.json().get("data", [])
            
            if not data: 
                print(f"   ℹ️ No more notes found at offset {offset}")
                break
            
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
            print(f"   📥 Recovered {total} notes so far...")
            
            if len(data) < limit: break
            offset += limit
            
        except Exception as e:
            print(f"   ❌ Note Sync Error: {e}")
            break

# --- 2. NOTES VIA PARENT OBJECTS (The "Hidden" Note Hunter) ---
def sync_notes_via_parents():
    print("\n🕵️ 2. Hunting for 'Hidden' Notes on Companies & Deals...")
    
    # We iterate through Companies and Deals specifically, as these often hold the "missing" notes
    target_objects = ["companies", "deals"]
    headers = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Accept": "application/json"}
    
    for slug in target_objects:
        print(f"   ...Scanning {slug} for attached notes...")
        limit = 500
        offset = 0
        
        while True:
            # Get Records
            payload = {"limit": limit, "offset": offset}
            try:
                res = requests.post(f"https://api.attio.com/v2/objects/{slug}/records/query", 
                                    headers=headers, json=payload)
                records = res.json().get("data", [])
                
                if not records: break
                
                note_batch = []
                for rec in records:
                    rec_id = rec['id']['record_id']
                    
                    # Fetch Notes SPECIFICALLY for this record
                    # This bypasses global filters
                    n_res = requests.get("https://api.attio.com/v2/notes", headers=headers,
                                         params={"parent_record_id": rec_id, "parent_object": slug})
                    
                    notes = n_res.json().get("data", [])
                    for n in notes:
                        note_batch.append({
                            "id": n['id']['note_id'], 
                            "parent_id": rec_id, 
                            "type": "note",
                            "title": f"Note on {slug}",
                            "content": n.get('content_plaintext', ''), 
                            "url": f"https://app.attio.com/w/workspace/note/{n['id']['note_id']}",
                            "metadata": {"created_at": n.get("created_at")}
                        })
                
                if note_batch:
                    safe_upsert(note_batch)
                    print(f"      -> Found {len(note_batch)} notes attached to {slug} batch.")
                
                if len(records) < limit: break
                offset += limit
            except: 
                break

# --- 3. RE-SYNC PEOPLE/COMPANIES (Just to be safe) ---
def sync_entities():
    print("\n👤 3. Verifying People & Companies...")
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
        # Run in this specific order to prioritize Notes
        sync_notes_global_recovery()
        sync_notes_via_parents()
        sync_entities()
        print("\n🏁 Sync Job Finished.")
    except Exception as e:
        print("\n❌ CRITICAL FAILURE")
        traceback.print_exc()
        exit(1)
