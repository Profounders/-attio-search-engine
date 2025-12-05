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

print("🚀 Starting Sync V26 (Transcript Deep Dive)...")

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
        # Clean metadata to prevent JSON errors
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

# --- 1. TRANSCRIPT DEEP DIVE (THE NEW LOGIC) ---
def sync_transcripts_deep():
    print("\n📞 1. Starting Transcript Deep Dive...")
    
    headers = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Accept": "application/json"}
    
    # We iterate common names for the Meeting object. 
    # 'meetings' is the standard V2 object, but some workspaces use 'calls'.
    potential_slugs = ["meetings", "calls", "call_recordings"]
    
    for slug in potential_slugs:
        print(f"   🔎 Scanning object '{slug}' for recordings...")
        
        limit = 200
        offset = 0
        total_transcripts = 0
        
        while True:
            # A. Get a batch of Meetings
            payload = {"limit": limit, "offset": offset}
            try:
                # 1. Fetch Meetings
                res = requests.post(f"https://api.attio.com/v2/objects/{slug}/records/query", 
                                    headers=headers, json=payload)
                
                # If object doesn't exist, skip to next slug
                if res.status_code == 404:
                    print(f"      (Object '{slug}' not found, skipping)")
                    break
                
                meetings = res.json().get("data", [])
                if not meetings: break
                
                print(f"      - Checking {len(meetings)} meetings in batch...")

                transcript_batch = []
                
                # B. Loop through every meeting
                for m in meetings:
                    meeting_id = m['id']['record_id']
                    
                    # Get Title for context
                    vals = m.get('values', {})
                    title = "Untitled Meeting"
                    if 'title' in vals: title = vals['title'][0]['value']
                    elif 'name' in vals: title = vals['name'][0]['value']

                    # 2. Check for Call Recordings (Nested Endpoint)
                    # GET /v2/meetings/{meeting_id}/call_recordings
                    rec_url = f"https://api.attio.com/v2/meetings/{meeting_id}/call_recordings"
                    rec_res = requests.get(rec_url, headers=headers)
                    
                    # If this endpoint fails (404), it means this object isn't a "Meeting" type
                    if rec_res.status_code != 200: continue
                    
                    recordings = rec_res.json().get("data", [])
                    
                    for rec in recordings:
                        call_id = rec['id']['call_recording_id']
                        
                        # 3. Get Transcript (Deepest Nesting)
                        # GET /v2/meetings/{meeting_id}/call_recordings/{call_id}/transcript
                        trans_url = f"https://api.attio.com/v2/meetings/{meeting_id}/call_recordings/{call_id}/transcript"
                        trans_res = requests.get(trans_url, headers=headers)
                        
                        if trans_res.status_code == 200:
                            t_data = trans_res.json().get("data", {})
                            
                            # Try to find the text
                            text = t_data.get("content_plaintext", "")
                            if not text: text = t_data.get("subtitles", "") # Sometimes raw
                            
                            if text:
                                print(f"         ✅ Found Transcript: {title[:30]}...")
                                transcript_batch.append({
                                    "id": call_id,
                                    "parent_id": meeting_id,
                                    "type": "call_recording", # This ensures the 📞 icon shows up
                                    "title": f"Transcript: {title}",
                                    "content": str(text), # Ensure string format
                                    "url": f"https://app.attio.com/w/workspace/record/{slug}/{meeting_id}",
                                    "metadata": {"meeting_id": meeting_id, "duration": rec.get("duration")}
                                })

                # C. Save Transcripts (Small batches, text is heavy)
                if transcript_batch:
                    # We send 1 item at a time if the text is huge to avoid timeouts
                    for t_item in transcript_batch:
                        safe_upsert([t_item])
                        total_transcripts += 1

                if len(meetings) < limit: break
                offset += limit

            except Exception as e:
                print(f"   ⚠️ Error scanning {slug}: {e}")
                break
        
        if total_transcripts > 0:
            print(f"   ✅ Finished {slug}. Synced {total_transcripts} transcripts.")
            return # We found the correct object, no need to check others

# --- 2. NOTES (Working) ---
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
                    "id": n['id']['note_id'], "parent_id": n.get('parent_record_id'), "type": "note",
                    "title": f"Note: {n.get('title', 'Untitled')}",
                    "content": n.get('content_plaintext', ''), 
                    "url": f"https://app.attio.com/w/workspace/note/{n['id']['note_id']}",
                    "metadata": {"created_at": n.get("created_at")}
                })
            safe_upsert(batch)
            if len(data) < limit: break
            offset += limit
        except: break

# --- 3. ENTITIES (Working) ---
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
        sync_transcripts_deep() # Run this FIRST to see the logs
        sync_notes_global()
        sync_entities()
        print("\n🏁 Sync Job Finished.")
    except Exception as e:
        print("\n❌ CRITICAL FAILURE")
        traceback.print_exc()
        exit(1)
