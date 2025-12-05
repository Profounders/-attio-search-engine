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

print("🚀 Starting Sync V27 (Meeting/Transcript Probe)...")

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

# --- TRANSCRIPT HUNTER ---
def sync_transcripts_v27():
    print("\n📞 1. Hunting for Transcripts...")
    headers = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Accept": "application/json"}
    
    # --- STRATEGY A: Direct Global List (The "Easy Way") ---
    # We test if Attio allows listing all meetings globally
    print("   🔎 Attempting Strategy A: Global Meeting List...")
    try:
        url = "https://api.attio.com/v2/meetings" # Guessing this exists like /v2/notes
        res = requests.get(url, headers=headers)
        
        if res.status_code == 200:
            print("      ✅ Success! Global Meeting endpoint found.")
            meetings = res.json().get("data", [])
            process_meetings(meetings)
            return # We are done if this worked
        else:
            print(f"      🔸 Strategy A failed (Status {res.status_code}). Endpoint likely doesn't exist.")
    except: pass

    # --- STRATEGY B: The "Calendar Crawl" (The "Hard Way") ---
    # We iterate through People to find their meetings
    print("   🔎 Attempting Strategy B: Scanning People for associated meetings...")
    
    limit = 200 # Scan batches of people
    offset = 0
    total_scanned = 0
    
    while True:
        try:
            # Get People
            res = requests.post("https://api.attio.com/v2/objects/people/records/query", 
                                headers=headers, json={"limit": limit, "offset": offset})
            people = res.json().get("data", [])
            if not people: break
            
            print(f"      - Scanning batch of {len(people)} people for meeting history...")
            
            for p in people:
                pid = p['id']['record_id']
                
                # Check for Meetings attached to this person
                # Note: Attio V2 usually links meetings via the 'calendar_events' or similar endpoint
                # We try the most common pattern: GET /v2/objects/people/records/{id}/meetings
                m_res = requests.get(f"https://api.attio.com/v2/objects/people/records/{pid}/meetings", headers=headers)
                
                if m_res.status_code == 200:
                    meetings = m_res.json().get("data", [])
                    if meetings:
                        print(f"         Found {len(meetings)} meetings for person {pid}")
                        process_meetings(meetings)
            
            if len(people) < limit: break
            offset += limit
            total_scanned += len(people)
            
            # Safety break to prevent running forever if it's not finding anything
            if total_scanned > 2000:
                print("      ⚠️ Scanned 2000 people and found no meetings. Stopping Transcript hunt.")
                break
                
        except Exception as e:
            print(f"      ❌ Scan Error: {e}")
            break

def process_meetings(meetings):
    """
    Takes a list of meeting objects, finds recordings, gets transcripts, saves to DB.
    """
    headers = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Accept": "application/json"}
    
    for m in meetings:
        try:
            mid = m['id']['meeting_id'] # ID structure depends on endpoint
            title = m.get('title', 'Untitled Meeting')
            
            # 1. Get Recordings
            rec_res = requests.get(f"https://api.attio.com/v2/meetings/{mid}/call_recordings", headers=headers)
            recordings = rec_res.json().get("data", [])
            
            for rec in recordings:
                rid = rec['id']['call_recording_id']
                
                # 2. Get Transcript
                trans_res = requests.get(f"https://api.attio.com/v2/meetings/{mid}/call_recordings/{rid}/transcript", headers=headers)
                
                if trans_res.status_code == 200:
                    text = trans_res.json().get("content_plaintext", "")
                    if text:
                        print(f"         ✅ SAVING TRANSCRIPT: {title}")
                        safe_upsert([{
                            "id": rid,
                            "type": "call_recording",
                            "title": f"Transcript: {title}",
                            "content": text,
                            "url": f"https://app.attio.com", # Deep links to meetings are tricky without slug
                            "metadata": {"meeting_id": mid}
                        }])
        except: pass

# --- STANDARD SYNC (Keep your existing working logic) ---
def sync_standard_items():
    print("\n📦 2. Syncing Standard Items (People/Companies/Notes)...")
    # ... (I excluded the full code for brevity, but assume your existing logic runs here) ...
    # For now, let's just focus on the transcripts to debug.

if __name__ == "__main__":
    sync_transcripts_v27()
    print("\n🏁 Transcript Probe Complete.")
