import os
import time
import requests
import traceback
import json
from datetime import datetime
from supabase import create_client, Client

# --- CONFIG ---
ATTIO_API_KEY = os.environ.get("ATTIO_API_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

print("🚀 Starting Sync V40 (The Time Machine)...", flush=True)

if not ATTIO_API_KEY or not SUPABASE_URL:
    print("❌ Error: Secrets missing.", flush=True)
    exit(1)

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("   🔌 DB Connected.", flush=True)
except Exception as e:
    print(f"   ❌ DB Connection Failed: {e}", flush=True)
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
        print(f"   💾 Saved batch of {len(items)} items.", flush=True)
    except Exception as e:
        print(f"   ❌ DB Error: {e}", flush=True)

# --- 1. SYNC TRANSCRIPTS (DATE FILTERED) ---
def sync_transcripts_fast_forward():
    print("\n📞 1. Syncing Transcripts (Skipping Old History)...", flush=True)
    
    url = "https://api.attio.com/v2/meetings"
    headers = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Accept": "application/json"}
    
    # We increase limit to 1000 to fly through history faster
    limit = 1000 
    offset = 0
    total_scanned = 0
    total_found = 0
    
    # CUTOFF DATE: Only check meetings after Jan 1, 2024
    # Adjust this if you need older transcripts
    CUTOFF_YEAR = 2024
    
    while True:
        params = {"limit": limit, "offset": offset}
        try:
            res = requests.get(url, headers=headers, params=params, timeout=45)
            if res.status_code != 200:
                print(f"   ❌ API Error: {res.status_code}", flush=True)
                break
                
            meetings = res.json().get("data", [])
            if not meetings: 
                print("   ℹ️ No more meetings.", flush=True)
                break
            
            # --- DATE CHECK ---
            # We look at the first and last meeting in the batch to guess the range
            start_date_str = meetings[0].get("start", {}).get("datetime", "Unknown")
            
            print(f"   🔎 Batch {offset}-{offset+len(meetings)} | Start Date: {start_date_str}", flush=True)
            
            # Process this batch
            transcript_batch = []
            skipped_count = 0
            
            for m in meetings:
                # 1. CHECK YEAR
                date_str = m.get("start", {}).get("datetime", "")
                if date_str:
                    try:
                        # Extract Year (e.g. "2023-01-01...")
                        year = int(date_str[:4])
                        if year < CUTOFF_YEAR:
                            skipped_count += 1
                            continue # SKIP OLD MEETING
                    except: pass

                # 2. IF RECENT: CHECK FOR TRANSCRIPT
                mid = m['id'].get('meeting_id') or m['id'].get('record_id')
                title = m.get('title') or "Untitled Meeting"
                
                # Check Recordings
                rec_res = requests.get(f"https://api.attio.com/v2/meetings/{mid}/call_recordings", headers=headers)
                recordings = rec_res.json().get("data", []) if rec_res.status_code == 200 else []
                
                for r in recordings:
                    rid = r['id']['call_recording_id']
                    # Get Transcript
                    t_res = requests.get(f"https://api.attio.com/v2/meetings/{mid}/call_recordings/{rid}/transcript", headers=headers)
                    
                    if t_res.status_code == 200:
                        data = t_res.json()
                        txt = data.get("content_plaintext") or data.get("subtitles") or data.get("text")
                        
                        if txt:
                            print(f"      ✅ FOUND TRANSCRIPT: {title}", flush=True)
                            transcript_batch.append({
                                "id": rid, 
                                "type": "call_recording",
                                "title": f"Transcript: {title}", 
                                "content": txt,
                                "url": "https://app.attio.com", 
                                "metadata": {"meeting_id": mid}
                            })
                            total_found += 1

            if skipped_count > 0:
                print(f"      ⏩ Skipped {skipped_count} old meetings (<{CUTOFF_YEAR}).", flush=True)
            
            if transcript_batch:
                safe_upsert(transcript_batch)
            
            if len(meetings) < limit: break
            offset += limit
            total_scanned += len(meetings)
            
        except Exception as e:
            print(f"   ⚠️ Loop Error: {e}", flush=True)
            break

    print(f"   🏁 Transcript Sync Complete. Found: {total_found}", flush=True)

# --- 2. STANDARD SYNC (Notes, People, Companies) ---
# Keeping this lightweight so the script finishes
def sync_standard_items():
    print("\n📦 2. Syncing Notes & People...", flush=True)
    
    # A. NOTES
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

    # B. PEOPLE (Singular)
    offset = 0
    while True:
        res = requests.post("https://api.attio.com/v2/objects/people/records/query", 
                            headers={"Authorization": f"Bearer {ATTIO_API_KEY}"}, 
                            json={"limit": 1000, "offset": offset})
        people = res.json().get("data", []) if res.status_code == 200 else []
        if not people: break
        
        batch = []
        for p in people:
            pid = p['id']['record_id']
            vals = p.get('values', {})
            name = "Untitled"
            if 'name' in vals: name = vals['name'][0]['value']
            elif 'email_addresses' in vals: name = vals['email_addresses'][0]['value']
            
            batch.append({
                "id": pid, "type": "person", "title": name, 
                "content": f"Name: {name}", 
                "url": f"https://app.attio.com/w/workspace/record/people/{pid}", "metadata": {} 
            })
        safe_upsert(batch)
        if len(people) < 1000: break
        offset += 1000

    # C. COMPANIES
    offset = 0
    while True:
        res = requests.post("https://api.attio.com/v2/objects/companies/records/query", 
                            headers={"Authorization": f"Bearer {ATTIO_API_KEY}"}, 
                            json={"limit": 1000, "offset": offset})
        recs = res.json().get("data", []) if res.status_code == 200 else []
        if not recs: break
        
        batch = []
        for r in recs:
            rid = r['id']['record_id']
            name = "Untitled"
            if 'name' in r['values']: name = r['values']['name'][0]['value']
            elif 'company_name' in r['values']: name = r['values']['company_name'][0]['value']
            
            batch.append({
                "id": rid, "type": "company", "title": name, "content": str(r['values']), 
                "url": f"https://app.attio.com/w/workspace/record/companies/{rid}", "metadata": {} 
            })
        safe_upsert(batch)
        if len(recs) < 1000: break
        offset += 1000

if __name__ == "__main__":
    try:
        sync_transcripts_fast_forward()
        sync_standard_items()
        print("\n🏁 Sync Job Finished.", flush=True)
    except Exception as e:
        print(f"\n❌ CRITICAL: {e}", flush=True)
        traceback.print_exc()
        exit(1)
