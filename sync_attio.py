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

print("🚀 Starting Sync V38 (Recent-Activity Priority)...", flush=True)

if not ATTIO_API_KEY or not SUPABASE_URL:
    print("❌ Error: Secrets missing.", flush=True)
    exit(1)

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("   🔌 DB Connected.", flush=True)
except Exception as e:
    print(f"   ❌ DB Connection Failed: {e}", flush=True)
    exit(1)

# --- GLOBAL CACHE ---
NAME_CACHE = {}

# --- API HELPER ---
def make_request(method, url, params=None, json_data=None):
    headers = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Accept": "application/json"}
    try:
        if method == "GET":
            res = requests.get(url, headers=headers, params=params, timeout=30)
        else:
            res = requests.post(url, headers=headers, json=json_data, timeout=30)
        
        if res.status_code == 429:
            print("   ⚠️ Rate Limit. Sleeping 5s...", flush=True)
            time.sleep(5)
            return make_request(method, url, params, json_data)
            
        return res
    except:
        return None

# --- DB HELPER ---
def safe_upsert(items):
    if not items: return
    try:
        for item in items:
            if "metadata" in item and isinstance(item["metadata"], dict):
                item["metadata"] = {k: v for k, v in item["metadata"].items() if v is not None}
        supabase.table("attio_index").upsert(items).execute()
        print(f"   💾 Saved batch of {len(items)} items.", flush=True)
    except Exception as e:
        print(f"   ❌ DB Error: {e}", flush=True)

# --- HELPER: CACHED PARENT LOOKUP ---
def get_parent_name(object_slug, record_id):
    if not record_id: return "Unknown"
    cache_key = f"{object_slug}:{record_id}"
    if cache_key in NAME_CACHE: return NAME_CACHE[cache_key]

    try:
        url = f"https://api.attio.com/v2/objects/{object_slug}/records/{record_id}"
        res = make_request("GET", url)
        if not res or res.status_code != 200: return "Unknown"
        vals = res.json().get("data", {}).get("values", {})
        name = "Unknown"
        for key in ['name', 'full_name', 'title', 'company_name']:
            if key in vals and vals[key]:
                name = vals[key][0]['value']
                break
        if name == "Unknown" and 'email_addresses' in vals:
             if vals['email_addresses']: name = vals['email_addresses'][0]['value']
        NAME_CACHE[cache_key] = name
        return name
    except:
        return "Unknown"

# --- 1. SMART TRANSCRIPT HUNTER (VIA ACTIVE PEOPLE) ---
def sync_transcripts_smart():
    print("\n📞 1. Syncing Transcripts (Targeting Active People)...", flush=True)
    
    # We query people, sorted by Last Interaction.
    # This brings 2025 activity to the top and ignores 2013.
    url = "https://api.attio.com/v2/objects/people/records/query"
    
    # If 'last_interaction' isn't available in your tier, we fallback to default sort
    payload = {
        "limit": 100, 
        "sort": {
            "direction": "desc",
            "attribute": "last_interaction_active_at" 
        }
    }
    
    offset = 0
    total_people_scanned = 0
    
    # We will scan the top 1000 active people
    MAX_PEOPLE_SCAN = 1000
    
    while total_people_scanned < MAX_PEOPLE_SCAN:
        payload["offset"] = offset
        
        # 1. Get Batch of Active People
        res = make_request("POST", url, json_data=payload)
        
        if not res: 
            print("   ❌ Error fetching people query. trying fallback sort...", flush=True)
            # Fallback: remove sort if it fails (API tier restriction)
            del payload["sort"]
            res = make_request("POST", url, json_data=payload)
            if not res: break

        people = res.json().get("data", [])
        if not people: break
        
        print(f"   🔎 Scanning {len(people)} active people for recent meetings...", flush=True)
        
        for p in people:
            pid = p['id']['record_id']
            
            # 2. Get Meetings for this Person
            # GET /v2/objects/people/records/{record_id}/meetings
            m_res = make_request("GET", f"https://api.attio.com/v2/objects/people/records/{pid}/meetings")
            
            if m_res and m_res.status_code == 200:
                meetings = m_res.json().get("data", [])
                
                # Check these meetings for recordings
                transcript_batch = []
                for m in meetings:
                    mid = m['id'].get('meeting_id') or m['id'].get('record_id')
                    title = m.get('title', 'Untitled Meeting')
                    
                    # 3. Check for Recordings
                    r_res = make_request("GET", f"https://api.attio.com/v2/meetings/{mid}/call_recordings")
                    recordings = r_res.json().get("data", []) if r_res else []
                    
                    for r in recordings:
                        rid = r['id']['call_recording_id']
                        t_res = make_request("GET", f"https://api.attio.com/v2/meetings/{mid}/call_recordings/{rid}/transcript")
                        
                        if t_res and t_res.status_code == 200:
                            data = t_res.json()
                            txt = data.get("content_plaintext") or data.get("subtitles") or data.get("text")
                            
                            if txt:
                                print(f"      ✅ Found Transcript: {title}", flush=True)
                                transcript_batch.append({
                                    "id": rid, "type": "call_recording",
                                    "title": f"Transcript: {title}", 
                                    "content": txt,
                                    "url": "https://app.attio.com", 
                                    "metadata": {"meeting_id": mid}
                                })
                
                safe_upsert(transcript_batch)

        total_people_scanned += len(people)
        offset += len(people)
        
    print("   🏁 Smart Transcript Sync Complete.", flush=True)

# --- 2. SYNC NOTES (CACHED) ---
def sync_notes_cached():
    print("\n📝 2. Syncing Notes...", flush=True)
    targets = ["people", "companies", "deals"]
    for slug in targets:
        limit = 1000
        offset = 0
        while True:
            params = {"limit": limit, "offset": offset, "parent_object": slug}
            res = make_request("GET", "https://api.attio.com/v2/notes", params=params)
            if not res or res.status_code != 200: break
            data = res.json().get("data", [])
            if not data: break
            
            batch = []
            for n in data:
                try:
                    nid = n['id']['note_id']
                    pid = n.get('parent_record_id')
                    pname = get_parent_name(slug, pid)
                    raw_title = n.get('title', 'Untitled')
                    if not raw_title or raw_title == "Untitled":
                        final_title = f"Note on {pname}"
                    else:
                        final_title = f"Note: {raw_title} ({pname})"
                    
                    batch.append({
                        "id": nid, "parent_id": pid, "type": "note",
                        "title": final_title,
                        "content": n.get('content_plaintext', ''),
                        "url": f"https://app.attio.com/w/workspace/note/{nid}",
                        "metadata": {"created_at": n.get("created_at"), "parent": pname}
                    })
                except: pass
            safe_upsert(batch)
            if len(data) < limit: break
            offset += limit

# --- 3. PEOPLE/COMPANIES ---
def sync_standard():
    print("\n📦 3. Syncing People & Companies (Singular)...", flush=True)
    for slug in ["people", "companies"]:
        db_type = "person" if slug == "people" else "company"
        limit = 1000
        offset = 0
        while True:
            url = f"https://api.attio.com/v2/objects/{slug}/records/query"
            res = make_request("POST", url, json_data={"limit": limit, "offset": offset})
            if not res: break
            data = res.json().get("data", [])
            if not data: break
            
            batch = []
            for d in data:
                try:
                    rid = d['id']['record_id']
                    name = "Untitled"
                    vals = d.get('values', {})
                    if 'name' in vals and vals['name']: name = vals['name'][0]['value']
                    elif 'company_name' in vals: name = vals['company_name'][0]['value']
                    elif 'email_addresses' in vals: name = vals['email_addresses'][0]['value']
                    
                    batch.append({
                        "id": rid, "type": db_type, "title": name, "content": str(vals),
                        "url": f"https://app.attio.com/w/workspace/record/{slug}/{rid}", "metadata": {}
                    })
                except: pass
            safe_upsert(batch)
            if len(data) < limit: break
            offset += limit

# --- 4. TASKS ---
def sync_tasks():
    print("\n✅ 4. Syncing Tasks...", flush=True)
    res = make_request("GET", "https://api.attio.com/v2/tasks")
    if not res: return
    batch = []
    for t in res.json().get("data", []):
        batch.append({
            "id": t['id']['task_id'], "type": "task", 
            "title": f"Task: {t.get('content_plaintext', 'Untitled')}",
            "content": f"Status: {t.get('is_completed')}",
            "url": "https://app.attio.com/w/workspace/tasks", "metadata": {}
        })
    safe_upsert(batch)

if __name__ == "__main__":
    try:
        sync_transcripts_smart()
        sync_notes_cached()
        sync_standard()
        sync_tasks()
        print("\n🏁 Sync Job Finished.", flush=True)
    except Exception as e:
        print(f"\n❌ CRITICAL: {e}", flush=True)
        traceback.print_exc()
        exit(1)
