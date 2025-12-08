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

print("🚀 Starting Sync V41 (Stable Limits + Crash Proofing)...", flush=True)

if not ATTIO_API_KEY or not SUPABASE_URL:
    print("❌ Error: Secrets missing.", flush=True)
    exit(1)

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("   🔌 DB Connected.", flush=True)
except Exception as e:
    print(f"   ❌ DB Connection Failed: {e}", flush=True)
    exit(1)

# --- HELPER: SAFE DATA EXTRACTION ---
def get_safe_value(values_dict, key):
    """Safely extracts a value from Attio's list structure without crashing."""
    try:
        if key in values_dict and values_dict[key] and isinstance(values_dict[key], list):
            if len(values_dict[key]) > 0:
                return values_dict[key][0].get('value', '')
    except: pass
    return ""

def get_record_name(vals):
    """Finds the best name for a record."""
    name = get_safe_value(vals, 'name')
    if not name: name = get_safe_value(vals, 'title')
    if not name: name = get_safe_value(vals, 'company_name')
    if not name: name = get_safe_value(vals, 'email_addresses')
    return name or "Untitled"

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

# --- 1. SYNC TRANSCRIPTS (TIME MACHINE) ---
def sync_transcripts():
    print("\n📞 1. Syncing Transcripts...", flush=True)
    
    url = "https://api.attio.com/v2/meetings"
    headers = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Accept": "application/json"}
    
    # FIXED: Reduced to 200 to avoid 400 Bad Request Error
    limit = 200 
    offset = 0
    total_found = 0
    
    # Only check meetings newer than this year to speed up processing
    CUTOFF_YEAR = 2023 
    
    while True:
        params = {"limit": limit, "offset": offset}
        try:
            res = requests.get(url, headers=headers, params=params, timeout=45)
            
            if res.status_code != 200:
                print(f"   ❌ API Error {res.status_code}: {res.text}", flush=True)
                break
                
            meetings = res.json().get("data", [])
            if not meetings: 
                print("   ℹ️ No more meetings found.", flush=True)
                break
            
            # Date Check for logs
            start_date = "Unknown"
            if meetings and "start" in meetings[0]:
                 start_date = meetings[0]["start"].get("datetime", "Unknown")
            print(f"   🔎 Batch {offset}-{offset+len(meetings)} | Date: {start_date}", flush=True)
            
            transcript_batch = []
            skipped_old = 0
            
            for m in meetings:
                # 1. TIME FILTER
                date_str = m.get("start", {}).get("datetime", "")
                if date_str:
                    try:
                        year = int(date_str[:4])
                        if year < CUTOFF_YEAR:
                            skipped_old += 1
                            continue
                    except: pass

                # 2. CHECK TRANSCRIPT
                mid = m['id'].get('meeting_id') or m['id'].get('record_id')
                title = m.get('title') or "Untitled Meeting"
                
                # Check Recordings
                rec_res = requests.get(f"https://api.attio.com/v2/meetings/{mid}/call_recordings", headers=headers)
                recordings = rec_res.json().get("data", []) if rec_res.status_code == 200 else []
                
                for r in recordings:
                    rid = r['id']['call_recording_id']
                    # Get Text
                    t_res = requests.get(f"https://api.attio.com/v2/meetings/{mid}/call_recordings/{rid}/transcript", headers=headers)
                    if t_res.status_code == 200:
                        data = t_res.json()
                        txt = data.get("content_plaintext") or data.get("subtitles") or data.get("text")
                        
                        if txt:
                            print(f"      ✅ FOUND: {title}", flush=True)
                            transcript_batch.append({
                                "id": rid, "type": "call_recording",
                                "title": f"Transcript: {title}", "content": str(txt),
                                "url": "https://app.attio.com", "metadata": {"meeting_id": mid}
                            })
                            total_found += 1

            if skipped_old > 0:
                print(f"      ⏩ Skipped {skipped_old} old meetings.", flush=True)
                
            safe_upsert(transcript_batch)
            
            if len(meetings) < limit: break
            offset += limit
            
        except Exception as e:
            print(f"   ⚠️ Loop Error: {e}", flush=True)
            break

    print(f"   🏁 Transcripts Complete. Found: {total_found}", flush=True)

# --- 2. STANDARD SYNC (CRASH PROOF) ---
def sync_standard_items():
    print("\n📦 2. Syncing People & Companies...", flush=True)
    
    # 1. COMPANIES
    offset = 0
    while True:
        res = requests.post("https://api.attio.com/v2/objects/companies/records/query", 
                            headers={"Authorization": f"Bearer {ATTIO_API_KEY}"}, 
                            json={"limit": 500, "offset": offset})
        data = res.json().get("data", []) if res.status_code == 200 else []
        if not data: break
        
        batch = []
        for r in data:
            rid = r['id']['record_id']
            vals = r.get('values', {})
            # SAFE EXTRACTION
            name = get_record_name(vals)
            
            batch.append({
                "id": rid, "type": "company", "title": name, "content": str(vals), 
                "url": f"https://app.attio.com/w/workspace/record/companies/{rid}", "metadata": {}
            })
        safe_upsert(batch)
        if len(data) < 500: break
        offset += 500

    # 2. PEOPLE
    offset = 0
    while True:
        res = requests.post("https://api.attio.com/v2/objects/people/records/query", 
                            headers={"Authorization": f"Bearer {ATTIO_API_KEY}"}, 
                            json={"limit": 500, "offset": offset})
        data = res.json().get("data", []) if res.status_code == 200 else []
        if not data: break
        
        batch = []
        for p in data:
            try:
                pid = p['id']['record_id']
                vals = p.get('values', {})
                # SAFE EXTRACTION (Fixes the crash)
                name = get_record_name(vals)
                email = get_safe_value(vals, 'email_addresses')
                
                batch.append({
                    "id": pid, "type": "person", "title": name, 
                    "content": f"Name: {name} | Email: {email}", 
                    "url": f"https://app.attio.com/w/workspace/record/people/{pid}", "metadata": {} 
                })
            except: pass
            
        safe_upsert(batch)
        if len(data) < 500: break
        offset += 500

# --- 3. NOTES ---
def sync_notes_global():
    print("\n📝 3. Syncing Notes...", flush=True)
    offset = 0
    while True:
        res = requests.get("https://api.attio.com/v2/notes", 
                           headers={"Authorization": f"Bearer {ATTIO_API_KEY}"}, 
                           params={"limit": 1000, "offset": offset})
        data = res.json().get("data", []) if res.status_code == 200 else []
        if not data: break
        
        batch = []
        for n in data:
            batch.append({
                "id": n['id']['note_id'], "parent_id": n.get('parent_record_id'), "type": "note",
                "title": f"Note: {n.get('title', 'Untitled')}",
                "content": n.get('content_plaintext', ''), 
                "url": f"https://app.attio.com/w/workspace/note/{n['id']['note_id']}", "metadata": {}
            })
        safe_upsert(batch)
        if len(data) < 1000: break
        offset += 1000

if __name__ == "__main__":
    try:
        sync_transcripts()
        sync_standard_items()
        sync_notes_global()
        print("\n🏁 Sync Job Finished.", flush=True)
    except Exception as e:
        print(f"\n❌ CRITICAL: {e}", flush=True)
        traceback.print_exc()
        exit(1)
