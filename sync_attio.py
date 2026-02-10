import os
import time
import requests
import traceback
import json
from datetime import datetime
from supabase import create_client, Client

# --- IMMEDIATE ALIVENESS CHECK ---
print("------------------------------------------------", flush=True)
print("✅ SCRIPT IS ALIVE. V48 (Smart Auto-Titling) Starting...", flush=True)
print("------------------------------------------------", flush=True)

# --- CONFIG ---
ATTIO_API_KEY = os.environ.get("ATTIO_API_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

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
        # print(f"   💾 Saved {len(items)} items.", flush=True)
    except Exception as e:
        err = str(e)
        if "57014" in err or "timeout" in err.lower():
            if len(items) > 1:
                mid = len(items) // 2
                print("   ⚠️ DB Timeout. Splitting batch...", flush=True)
                time.sleep(1)
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

# --- 1. SYNC TRANSCRIPTS ---
def sync_transcripts():
    print("\n📞 1. Syncing Transcripts...", flush=True)
    url = "https://api.attio.com/v2/meetings"
    limit = 100
    offset = 0
    TARGET_YEARS = ["2024", "2025"] 
    
    while True:
        res = make_request("GET", url, params={"limit": limit, "offset": offset})
        if not res or res.status_code != 200: break
        meetings = res.json().get("data", [])
        if not meetings: break
        
        # Log progress
        first_date = meetings[0].get("start", {}).get("datetime", "")
        if offset % 1000 == 0:
             print(f"   🔎 Scanning batch {offset}... ({first_date})", flush=True)

        batch = []
        for m in meetings:
            try:
                m_date = m.get("start", {}).get("datetime", "")
                if not any(y in m_date for y in TARGET_YEARS): continue 

                mid = m['id'].get('meeting_id') or m['id'].get('record_id')
                title = m.get('title') or m.get('subject') or "Untitled Meeting"
                
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
                            batch.append({
                                "id": rid, "type": "call_recording",
                                "title": f"Transcript: {title}", "content": str(txt),
                                "url": "https://app.attio.com", 
                                "metadata": {"meeting_id": mid, "created_at": m_date}
                            })
            except: pass
        safe_upsert(batch)
        if len(meetings) < limit: break
        offset += limit
    print("   ✅ Transcripts Complete.", flush=True)

# --- 2. SYNC NOTES (SMART TITLES) ---
def sync_notes_cached():
    print("\n📝 2. Syncing Notes (Smart Titles)...", flush=True)
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
                    
                    # 1. Get Parent Name (Company/Person)
                    pname = get_parent_name(slug, pid)
                    
                    # 2. Get Raw Title & Content
                    raw_title = n.get('title', '').strip()
                    content = n.get('content_plaintext', '').strip()
                    
                    # 3. SMART TITLE LOGIC
                    if raw_title and raw_title != "Untitled":
                        # If Attio has a real title, use it
                        final_title = f"Note: {raw_title} ({pname})"
                    elif content:
                        # If no title, take first 60 chars of content
                        # Replace newlines with spaces for a clean title
                        snippet = content[:60].replace('\n', ' ')
                        final_title = f"Note: {snippet}... ({pname})"
                    else:
                        # Fallback if both are empty
                        final_title = f"Empty Note ({pname})"
                    
                    batch.append({
                        "id": nid, "parent_id": pid, "type": "note",
                        "title": final_title, # <--- IMPROVED TITLE
                        "content": content,
                        "url": f"https://app.attio.com/w/workspace/note/{nid}",
                        "metadata": {"created_at": n.get("created_at"), "parent": pname}
                    })
                except: pass
            safe_upsert(batch)
            if len(data) < limit: break
            offset += limit
    print("   ✅ Notes Complete.", flush=True)

# --- 3. PEOPLE/COMPANIES ---
def sync_standard():
    print("\n📦 3. Syncing People & Companies...", flush=True)
    for slug in ["people", "companies"]:
        db_type = "person" if slug == "people" else "company"
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
                    vals = d.get('values', {})
                    name = "Untitled"
                    if 'name' in vals: name = vals['name'][0]['value']
                    elif 'company_name' in vals: name = vals['company_name'][0]['value']
                    elif 'email_addresses' in vals: name = vals['email_addresses'][0]['value']
                    
                    batch.append({
                        "id": rid, "type": db_type, "title": name, "content": str(vals),
                        "url": f"https://app.attio.com/w/workspace/record/{slug}/{rid}", 
                        "metadata": {"created_at": d.get("created_at")}
                    })
                except: pass
            safe_upsert(batch)
            if len(data) < limit: break
            offset += limit
    print("   ✅ Records Complete.", flush=True)

# --- 4. TASKS ---
def sync_tasks():
    print("\n✅ 4. Syncing Tasks...", flush=True)
    res = make_request("GET", "https://api.attio.com/v2/tasks")
    if not res: return
    batch = []
    
    for t in res.json().get("data", []):
        date_ref = t.get('deadline_at') or t.get('created_at')
        batch.append({
            "id": t['id']['task_id'], "type": "task", 
            "title": f"Task: {t.get('content_plaintext', 'Untitled')}",
            "content": f"Status: {t.get('is_completed')}",
            "url": "https://app.attio.com/w/workspace/tasks", 
            "metadata": {"created_at": t.get('created_at'), "deadline": t.get('deadline_at')}
        })
    safe_upsert(batch)
    print("   ✅ Tasks Complete.", flush=True)

if __name__ == "__main__":
    try:
        sync_transcripts()
        sync_notes_cached()
        sync_standard()
        sync_tasks()
        print("\n🏁 Sync Job Finished.", flush=True)
    except Exception as e:
        print(f"\n❌ CRITICAL: {e}", flush=True)
        traceback.print_exc()
        exit(1)
