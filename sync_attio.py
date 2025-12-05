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

print("🚀 Starting Sync V28 (The Complete Picture)...")

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

# --- 1. SYNC GLOBAL MEETINGS & TRANSCRIPTS (NEW!) ---
def sync_meetings_global():
    print("\n📞 1. Syncing Global Meetings & Transcripts...")
    url = "https://api.attio.com/v2/meetings"
    headers = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Accept": "application/json"}
    
    limit = 200
    offset = 0
    total_transcripts = 0
    
    while True:
        try:
            params = {"limit": limit, "offset": offset}
            res = requests.get(url, headers=headers, params=params, timeout=60)
            
            if res.status_code != 200:
                print(f"   ❌ API Error {res.status_code} fetching meetings")
                break
                
            meetings = res.json().get("data", [])
            if not meetings: break
            
            print(f"   ...Processing batch of {len(meetings)} meetings...")
            
            transcript_batch = []
            
            for m in meetings:
                # 1. Safe ID Extraction
                # Try 'meeting_id' first, fall back to 'record_id'
                mid = m['id'].get('meeting_id') or m['id'].get('record_id')
                
                # Title
                title = m.get('title', 'Untitled Meeting')
                
                # 2. Get Recordings
                rec_url = f"https://api.attio.com/v2/meetings/{mid}/call_recordings"
                rec_res = requests.get(rec_url, headers=headers)
                recordings = rec_res.json().get("data", [])
                
                if not recordings:
                    continue # No recording = No transcript
                
                for rec in recordings:
                    rid = rec['id']['call_recording_id']
                    
                    # 3. Get Transcript
                    trans_url = f"https://api.attio.com/v2/meetings/{mid}/call_recordings/{rid}/transcript"
                    trans_res = requests.get(trans_url, headers=headers)
                    
                    if trans_res.status_code == 200:
                        t_data = trans_res.json()
                        
                        # Try multiple keys for text
                        text = t_data.get("content_plaintext", "")
                        if not text: text = t_data.get("subtitles", "")
                        if not text: text = t_data.get("text", "")
                        
                        if text:
                            print(f"      ✅ Transcript Found: {title}")
                            transcript_batch.append({
                                "id": rid,
                                "parent_id": mid,
                                "type": "call_recording",
                                "title": f"Transcript: {title}",
                                "content": text, # The heavy text
                                "url": f"https://app.attio.com", # Generic link as deep links vary
                                "metadata": {"meeting_id": mid, "duration": rec.get("duration")}
                            })
                            total_transcripts += 1
            
            # Save Transcripts (Small chunks)
            if transcript_batch:
                safe_upsert(transcript_batch)
                
            if len(meetings) < limit: break
            offset += limit
            
        except Exception as e:
            print(f"   ⚠️ Meeting Sync Error: {e}")
            break
            
    print(f"   ✅ Total Transcripts Synced: {total_transcripts}")

# --- 2. GLOBAL NOTES ---
def sync_notes_global():
    print("\n📝 2. Syncing Notes...")
    url = "https://api.attio.com/v2/notes"
    headers = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Accept": "application/json"}
    limit = 1000
    offset = 0
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
            if len(data) < limit: break
            offset += limit
        except: break

# --- 3. PEOPLE & COMPANIES ---
def sync_entities():
    print("\n👤 3. Syncing People & Companies...")
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
        sync_meetings_global() # Runs FIRST
        sync_notes_global()
        sync_entities()
        print("\n🏁 Sync Job Finished.")
    except Exception as e:
        print("\n❌ CRITICAL FAILURE")
        traceback.print_exc()
        exit(1)
