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

print("🚀 Starting Sync V21 (Notes + Call Transcripts)...")

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
    name = "Untitled Person"
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
        
        if name == "Untitled Person" and email: name = email
        content = f"Name: {name} | Email: {email}"
    except:
        content = "Error parsing person"
    return name, content

# --- 1. GLOBAL NOTES ---
def sync_notes_global():
    print("\n📝 1. Syncing ALL Notes...")
    url = "https://api.attio.com/v2/notes"
    headers = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Accept": "application/json"}
    limit = 1000
    offset = 0
    
    while True:
        params = {"limit": limit, "offset": offset}
        try:
            response = requests.get(url, headers=headers, params=params, timeout=60)
            data = response.json().get("data", [])
            
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
            
        except Exception as e:
            print(f"   ❌ Note Sync Error: {e}")
            break

# --- 2. TRANSCRIPTS (THE NEW LOGIC) ---
def sync_transcripts():
    print("\n📞 2. Syncing Meeting Transcripts...")
    
    # Step A: Find all Meetings
    # We assume the object slug is "meetings" (Standard V2)
    slug = "meetings"
    headers = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Accept": "application/json"}
    
    print(f"   🔎 Listing all records in object: '{slug}'...")
    
    limit = 200
    offset = 0
    
    while True:
        # Get batch of meetings
        payload = {"limit": limit, "offset": offset}
        try:
            # Try to list meetings using Object Query
            res = requests.post(f"https://api.attio.com/v2/objects/{slug}/records/query", 
                                headers=headers, json=payload)
            
            if res.status_code == 404:
                print("   ⚠️ Object 'meetings' not found. Trying 'calls'...")
                slug = "calls" # Fallback
                continue
                
            meetings = res.json().get("data", [])
            if not meetings: break
            
            print(f"   ...Checking {len(meetings)} meetings for recordings...")

            # Step B: Check each meeting for recordings
            transcript_batch = []
            
            for m in meetings:
                meeting_id = m['id']['record_id']
                
                # Get Meeting Title for context
                vals = m.get('values', {})
                title = "Untitled Meeting"
                if 'title' in vals: title = vals['title'][0]['value']
                elif 'name' in vals: title = vals['name'][0]['value']

                # 1. Get Recordings for this meeting
                # Endpoint: GET /v2/meetings/{meeting_id}/call_recordings
                rec_res = requests.get(f"https://api.attio.com/v2/meetings/{meeting_id}/call_recordings", 
                                     headers=headers)
                
                # Attio sometimes returns a list, sometimes a single object depending on version
                # We handle the list format
                recordings = rec_res.json().get("data", [])
                
                # If the endpoint returns 404/403, we skip
                if not recordings: continue

                for rec in recordings:
                    call_id = rec['id']['call_recording_id']
                    
                    # 2. Get Transcript for this recording
                    # Endpoint: GET /v2/meetings/{meeting_id}/call_recordings/{call_recording_id}/transcript
                    trans_res = requests.get(f"https://api.attio.com/v2/meetings/{meeting_id}/call_recordings/{call_id}/transcript",
                                           headers=headers)
                    
                    if trans_res.status_code == 200:
                        transcript_text = trans_res.json().get("content_plaintext", "") # Or 'subtitles' depending on format
                        
                        # Sometimes text is inside 'data' -> 'text'
                        if not transcript_text and "text" in trans_res.json():
                             transcript_text = trans_res.json()['text']

                        if transcript_text:
                            print(f"      ✅ Found transcript for: {title}")
                            transcript_batch.append({
                                "id": call_id,
                                "parent_id": meeting_id,
                                "type": "call_recording", # Special type for search
                                "title": f"Transcript: {title}",
                                "content": transcript_text, # The heavy text
                                "url": f"https://app.attio.com/w/workspace/record/meetings/{meeting_id}",
                                "metadata": {"meeting_id": meeting_id}
                            })
                            
            # Save Transcripts (Small batches because text is huge)
            if transcript_batch:
                safe_upsert(transcript_batch)

            if len(meetings) < limit: break
            offset += limit

        except Exception as e:
            print(f"   ⚠️ Transcript Loop Error: {e}")
            break

# --- 3. PEOPLE (FULL SYNC) ---
def sync_people_full():
    print("\n👤 3. Syncing People...")
    slug = "people"
    limit = 1000
    offset = 0
    
    while True:
        payload = {"limit": limit, "offset": offset}
        try:
            res = requests.post(f"https://api.attio.com/v2/objects/{slug}/records/query", 
                                headers={"Authorization": f"Bearer {ATTIO_API_KEY}"}, json=payload)
            people = res.json().get("data", [])
            
            if not people: break
            
            batch = []
            for p in people:
                pid = p['id']['record_id']
                vals = p.get('values', {})
                name, content = extract_smart_name_and_content(vals)
                
                batch.append({
                    "id": pid, "type": "person", 
                    "title": name, "content": content,
                    "url": f"https://app.attio.com/w/workspace/record/people/{pid}", 
                    "metadata": {} 
                })
            
            safe_upsert(batch)
            if len(people) < limit: break
            offset += limit
        except Exception as e:
            break

# --- 4. COMPANIES ---
def sync_companies():
    print("\n🏢 4. Syncing Companies...")
    slug = "companies"
    limit = 1000
    offset = 0
    while True:
        try:
            payload = {"limit": limit, "offset": offset}
            res = requests.post(f"https://api.attio.com/v2/objects/{slug}/records/query", 
                                headers={"Authorization": f"Bearer {ATTIO_API_KEY}"}, json=payload)
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

# --- 5. TASKS ---
def sync_tasks():
    print("\n✅ 5. Syncing Tasks...")
    try:
        res = requests.get("https://api.attio.com/v2/tasks", headers={"Authorization": f"Bearer {ATTIO_API_KEY}"})
        tasks = res.json().get("data", [])
        batch = []
        for t in tasks:
            batch.append({
                "id": t['id']['task_id'], "type": "task", 
                "title": f"Task: {t.get('content_plaintext', 'Untitled')}",
                "content": f"Status: {t.get('is_completed')}",
                "url": "https://app.attio.com/w/workspace/tasks", "metadata": {}
            })
        safe_upsert(batch)
    except: pass

if __name__ == "__main__":
    try:
        sync_notes_global()
        sync_transcripts() # <--- NEW FUNCTION
        sync_people_full()
        sync_companies()
        sync_tasks()
        print("\n🏁 Sync Job Finished.")
    except Exception as e:
        print("\n❌ CRITICAL FAILURE")
        traceback.print_exc()
        exit(1)
