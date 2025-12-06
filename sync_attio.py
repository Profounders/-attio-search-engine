import os
import requests
import json

ATTIO_API_KEY = os.environ.get("ATTIO_API_KEY")

print("🕵️ STARTING TRANSCRIPT X-RAY...")

if not ATTIO_API_KEY:
    print("❌ Error: Secrets missing.")
    exit(1)

def probe_transcripts():
    headers = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Accept": "application/json"}
    
    # 1. CHECK GLOBAL MEETINGS
    print("\n1️⃣ Checking GET /v2/meetings...")
    url = "https://api.attio.com/v2/meetings"
    res = requests.get(url, headers=headers, params={"limit": 10})
    
    print(f"   Status: {res.status_code}")
    if res.status_code != 200:
        print(f"   ❌ Error: {res.text}")
        return

    meetings = res.json().get("data", [])
    print(f"   ✅ Found {len(meetings)} meetings in this batch.")
    
    if not meetings:
        print("   ⚠️ STOPPING: No meetings returned by API. (Are you using 'Attio Meetings' or just 'Calendar Events'?)")
        return

    # 2. CHECK RECORDINGS FOR THE FIRST 5 MEETINGS
    print("\n2️⃣ Checking for Call Recordings...")
    
    found_recording = False
    
    for m in meetings[:5]:
        # Try both ID formats just in case
        mid = m['id'].get('meeting_id') or m['id'].get('record_id')
        title = "Untitled"
        if 'title' in m['values']: title = m['values']['title'][0]['value']
        
        print(f"   --- Checking Meeting: '{title}' (ID: {mid}) ---")
        
        rec_url = f"https://api.attio.com/v2/meetings/{mid}/call_recordings"
        rec_res = requests.get(rec_url, headers=headers)
        
        if rec_res.status_code == 403:
            print("      🚫 403 FORBIDDEN: You do not have 'Call Recording: Read' permission.")
            continue
            
        recordings = rec_res.json().get("data", [])
        
        if not recordings:
            print("      🔸 No recordings found for this meeting.")
            continue
            
        print(f"      ✅ FOUND {len(recordings)} RECORDING(S)!")
        found_recording = True
        
        # 3. CHECK TRANSCRIPT FOR THE FIRST RECORDING
        for rec in recordings:
            rid = rec['id']['call_recording_id']
            print(f"      🔎 Requesting Transcript for Recording ID: {rid}...")
            
            trans_url = f"https://api.attio.com/v2/meetings/{mid}/call_recordings/{rid}/transcript"
            trans_res = requests.get(trans_url, headers=headers)
            
            print(f"      Status: {trans_res.status_code}")
            
            if trans_res.status_code == 200:
                data = trans_res.json()
                # Print keys to see where text is hiding
                print(f"      🔑 Keys in response: {list(data.keys())}")
                
                # Check for content
                if "content_plaintext" in data:
                    print(f"      📄 content_plaintext length: {len(data['content_plaintext'])}")
                elif "subtitles" in data:
                    print(f"      📄 subtitles found (Raw format)")
                else:
                    print(f"      ⚠️ No text field found! Raw dump: {str(data)[:200]}")
            else:
                print(f"      ❌ Error getting transcript: {trans_res.text}")

    if not found_recording:
        print("\n⚠️ SUMMARY: We found meetings, but ZERO recordings.")
        print("Possible causes:")
        print("1. Your team uses Google/Outlook Calendar events (not 'Attio Meetings').")
        print("2. 'Meeting Intelligence' is not enabled/recording.")
        print("3. Permissions allow seeing Meetings but not Recordings.")

if __name__ == "__main__":
    probe_transcripts()
