import os
import time
import requests
import traceback
from supabase import create_client, Client

# --- CONFIG ---
ATTIO_API_KEY = os.environ.get("ATTIO_API_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

print("🚀 Starting Sync V24 (The Bucket Strategy)...")

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

# --- 1. SYNC NOTES BY BUCKET (THE FIX) ---
def sync_notes_by_object_type():
    print("\n📝 1. Syncing Notes by Object Type (Bucket Strategy)...")
    
    # 1. Get list of all object types (people, companies, deals, etc.)
    headers = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Accept": "application/json"}
    obj_res = requests.get("https://api.attio.com/v2/objects", headers=headers)
    objects = obj_res.json().get("data", [])
    
    total_notes_found = 0
    
    for obj in objects:
        slug = obj['api_slug']
        singular = obj['singular_noun']
        
        print(f"\n   🔎 Checking for notes attached to: {singular} ({slug})...")
        
        # 2. Fetch Notes specifically for this object type
        # Endpoint: GET /v2/notes?parent_object={slug}
        url = "https://api.attio.com/v2/notes"
        limit = 1000
        offset = 0
        
        while True:
            params = {
                "limit": limit, 
                "offset": offset,
                "parent_object": slug # <--- THIS IS THE KEY FILTER
            }
            
            try:
                response = requests.get(url, headers=headers, params=params, timeout=60)
                data = response.json().get("data", [])
                
                if not data: 
                    if offset == 0:
                        print(f"      - No notes found for {slug}.")
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
                        "metadata": {"created_at": n.get("created_at"), "parent_object": slug}
                    })
                
                safe_upsert(batch)
                total_notes_found += len(data)
                print(f"      - Synced {len(data)} notes for {slug} (Total: {total_notes_found})")
                
                if len(data) < limit: break
                offset += limit
                
            except Exception as e:
                print(f"   ❌ Error syncing notes for {slug}: {e}")
                break

    print(f"   ✅ Total Notes Synced via Buckets: {total_notes_found}")

# --- 2. TRANSCRIPTS ---
def sync_transcripts():
    print("\n📞 2. Syncing Transcripts...")
    # (Simplified logic from previous working version)
    # Find Meeting Object
    slug = "meetings" # Default
    headers = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Accept": "application/json"}
    
    # Quick check if 'meetings' exists, otherwise find 'calls'
    obj_check = requests.get("https://api.attio.com/v2/objects", headers=headers).json().get("data", [])
    slugs = [o['api_slug'] for o in obj_check]
    if "meetings" not in slugs and "calls" in slugs: slug = "calls"
    elif "meetings" not in slugs and "call_recordings" in slugs: slug = "call_recordings"
    
    print(f"   Using object: {slug}")
    
    limit = 200
    offset = 0
    while True:
        try:
            res = requests.post(f"https://api.attio.com/v2/objects/{slug}/records/query", 
                                headers=headers, json={"limit": limit, "offset": offset})
            meetings = res.json().get("data", [])
            if not meetings: break
            
            trans_batch = []
            for m in meetings:
                mid = m['id']['record_id']
                # Check for recordings
                rec_res = requests.get(f"https://api.attio.com/v2/meetings/{mid}/call_recordings", headers=headers)
                recs = rec_res.json().get("data", [])
                
                for r in recs:
                    rid = r['id']['call_recording_id']
                    t_res = requests.get(f"https://api.attio.com/v2/meetings/{mid}/call_recordings/{rid}/transcript", headers=headers)
                    if t_res.status_code == 200:
                        txt = t_res.json().get("content_plaintext", "")
                        if txt:
                            # Try to find a title
                            title = "Transcript"
                            if 'name' in m['values']: title = m['values']['name'][0]['value']
                            
                            trans_batch.append({
                                "id": rid, "parent_id": mid, "type": "call_recording",
                                "title": f"Transcript: {title}", "content": txt,
                                "url": f"https://app.attio.com/w/workspace/record/{slug}/{mid}", "metadata": {}
                            })
            safe_upsert(trans_batch)
            if len(meetings) < limit: break
            offset += limit
        except: break

# --- 3. COMPANIES (Only update if needed) ---
def sync_companies():
    print("\n🏢 3. Syncing Companies...")
    slug = "companies"
    limit = 1000
    offset = 0
    while True:
        try:
            res = requests.post(f"https://api.attio.com/v2/objects/{slug}/records/query", 
                                headers={"Authorization": f"Bearer {ATTIO_API_KEY}"}, json={"limit": limit, "offset": offset})
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

# --- 4. PEOPLE (Only Names) ---
def sync_people_names():
    print("\n👤 4. Syncing People...")
    slug = "people"
    limit = 1000
    offset = 0
    while True:
        try:
            res = requests.post(f"https://api.attio.com/v2/objects/{slug}/records/query", 
                                headers={"Authorization": f"Bearer {ATTIO_API_KEY}"}, json={"limit": limit, "offset": offset})
            people = res.json().get("data", [])
            if not people: break
            
            batch = []
            for p in people:
                pid = p['id']['record_id']
                vals = p.get('values', {})
                name = "Untitled"
                email = ""
                # Smart extraction
                if 'name' in vals and vals['name']: name = vals['name'][0]['value']
                elif 'email_addresses' in vals and vals['email_addresses']: name = vals['email_addresses'][0]['value']
                
                if 'email_addresses' in vals and vals['email_addresses']: email = vals['email_addresses'][0]['value']
                
                batch.append({
                    "id": pid, "type": "person", "title": name, "content": f"Name: {name} | Email: {email}",
                    "url": f"https://app.attio.com/w/workspace/record/people/{pid}", "metadata": {} 
                })
            safe_upsert(batch)
            if len(people) < limit: break
            offset += limit
        except: break

if __name__ == "__main__":
    try:
        sync_notes_by_object_type() # <--- THE FIX
        sync_transcripts()
        sync_companies()
        sync_people_names()
        print("\n🏁 Sync Job Finished.")
    except Exception as e:
        print("\n❌ CRITICAL FAILURE")
        traceback.print_exc()
        exit(1)
