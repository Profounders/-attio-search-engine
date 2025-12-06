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

print("🚀 Starting Sync V39 (Anti-Timeout Mode)...", flush=True)

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

# --- DB HELPER (SMART RETRY) ---
def safe_upsert(items):
    """
    Recursive retry logic:
    If a batch fails due to timeout (57014), split it in half and try again.
    """
    if not items: return
    
    try:
        # Clean metadata
        for item in items:
            if "metadata" in item and isinstance(item["metadata"], dict):
                item["metadata"] = {k: v for k, v in item["metadata"].items() if v is not None}
        
        # Attempt Save
        supabase.table("attio_index").upsert(items).execute()
        print(f"   💾 Saved {len(items)} items.", flush=True)
        
    except Exception as e:
        error_str = str(e)
        # Check for Timeout (57014) or Connection Error
        if "57014" in error_str or "timeout" in error_str.lower() or "502" in error_str:
            if len(items) <= 1:
                print(f"   ❌ Failed to save single item: {error_str}", flush=True)
                return
            
            # Split batch and retry
            mid = len(items) // 2
            print(f"   ⚠️ DB Timeout. Retrying with split batches ({mid} items)...", flush=True)
            time.sleep(1) # Cool down
            safe_upsert(items[:mid])
            safe_upsert(items[mid:])
        else:
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

# --- 1. SMART TRANSCRIPT HUNTER ---
def sync_transcripts_smart():
    print("\n📞 1. Syncing Transcripts (Scanning Active People)...", flush=True)
    
    url = "https://api.attio.com/v2/objects/people/records/query"
    
    # Try sorting by recent activity
    payload = {
        "limit": 100, 
        "sort": {"direction": "desc", "attribute": "last_interaction_active_at"}
    }
    
    offset = 0
    total_people_scanned = 0
    MAX_PEOPLE_SCAN = 1000
    total_transcripts = 0
    
    while total_people_scanned < MAX_PEOPLE_SCAN:
        payload["offset"] = offset
        res = make_request("POST", url, json_data=payload)
        
        # Fallback if sort isn't supported
        if not res: 
            del payload["sort"]
            res = make_request("POST", url, json_data=payload)
            if not res: break

        people = res.json().get("data", [])
        if not people: break
        
        print(f"   🔎 Scanning batch of {len(people)} active people...", flush=True)
        
        transcript_batch = []
        
        for p in people:
            pid = p['id']['record_id']
            
            # Check Meetings
            m_res = make_request("GET", f"https://api.attio.com/v2/objects/people/records/{pid}/meetings")
            if not m_res: continue
            
            meetings = m_res.json().get("data", [])
            
            for m in meetings:
                mid = m['id'].get('meeting_id') or m['id'].get('record_id')
                title = m.get('title', 'Untitled Meeting')
                
                # Check Recordings
                r_res = make_request("GET", f"https://api.attio.com/v2/meetings/{mid}/call_recordings")
                if not r_res: continue
                
                recordings = r_res.json().get("data", [])
                
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
                                "title": f"Transcript: {title}", "content": txt,
                                "url": "https://app.attio.com", "metadata": {"meeting_id": mid}
                            })
                            total_transcripts += 1
                    elif t_res:
                        # Print failure reason if not 200 OK (e.g. 403 Forbidden)
                        if t_res.status_code not in [404, 200]:
                             print(f"      ⚠️ Failed to get transcript for {title}: {t_res.status_code}", flush=True)

        safe_upsert(transcript_batch)
        total_people_scanned += len(people)
        offset += len(people)
        
    print(f"   🏁 Transcripts Complete. Found: {total_transcripts}", flush=True)

# --- 2. SYNC NOTES ---
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

# --- 3. PEOPLE/COMPANIES (ANTI-TIMEOUT) ---
def sync_standard():
    print("\n📦 3. Syncing People & Companies (Small Batches)...", flush=True)
    for slug in ["people", "companies"]:
        db_type = "person" if slug == "people" else "company"
        
        # REDUCED LIMIT to prevent massive JSON payloads
        limit = 200 
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
            
            # This calls the smart retry logic
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
