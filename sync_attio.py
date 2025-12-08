import os
import time
import requests
import traceback
import json
from supabase import create_client, Client

# --- IMMEDIATE ALIVENESS CHECK ---
print("------------------------------------------------", flush=True)
print("✅ SCRIPT IS ALIVE. V45 (Stable Core - No Transcripts) Starting...", flush=True)
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

# --- DB HELPER (ANTI-TIMEOUT) ---
def safe_upsert(items):
    if not items: return
    try:
        for item in items:
            if "metadata" in item and isinstance(item["metadata"], dict):
                item["metadata"] = {k: v for k, v in item["metadata"].items() if v is not None}
        
        supabase.table("attio_index").upsert(items).execute()
        # Commented out success print to reduce log noise
        # print(f"   💾 Saved {len(items)} items.", flush=True)
    except Exception as e:
        err = str(e)
        if "57014" in err or "timeout" in err.lower() or "502" in err:
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

# --- 1. SYNC NOTES (CACHED) ---
def sync_notes_cached():
    print("\n📝 1. Syncing Notes...", flush=True)
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
    print("   ✅ Notes Complete.", flush=True)

# --- 2. PEOPLE/COMPANIES ---
def sync_standard():
    print("\n📦 2. Syncing People & Companies...", flush=True)
    for slug in ["people", "companies"]:
        db_type = "person" if slug == "people" else "company"
        # Reduced limit to prevent timeouts on heavy records
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
            
            safe_upsert(batch)
            if len(data) < limit: break
            offset += limit
    print("   ✅ Records Complete.", flush=True)

# --- 3. TASKS ---
def sync_tasks():
    print("\n✅ 3. Syncing Tasks...", flush=True)
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
    print("   ✅ Tasks Complete.", flush=True)

if __name__ == "__main__":
    try:
        # Transcript sync removed to restore stability
        sync_notes_cached()
        sync_standard()
        sync_tasks()
        print("\n🏁 Sync Job Finished.", flush=True)
    except Exception as e:
        print(f"\n❌ CRITICAL: {e}", flush=True)
        traceback.print_exc()
        exit(1)
