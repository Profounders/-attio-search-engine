import os
import requests
import json

ATTIO_API_KEY = os.environ.get("ATTIO_API_KEY")

print("🕵️ STARTING TRANSCRIPT X-RAY (CRASH PROOF)...")

if not ATTIO_API_KEY:
    print("❌ Error: Secrets missing.")
    exit(1)

def probe_transcripts():
    headers = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Accept": "application/json"}
    
    print("\n1️⃣ Checking GET /v2/meetings...")
    url = "https://api.attio.com/v2/meetings"
    
    try:
        res = requests.get(url, headers=headers, params={"limit": 5})
        if res.status_code != 200:
            print(f"   ❌ API Error: {res.status_code} - {res.text}")
            return

        meetings = res.json().get("data", [])
        print(f"   ✅ Found {len(meetings)} meetings.")
        
        if not meetings: return

        # --- DEBUG: PRINT RAW STRUCTURE ---
        print("\n🔎 DEBUG: Raw JSON of the first meeting:")
        print(json.dumps(meetings[0], indent=2)[:500] + "...") 
        print("------------------------------------------------")

        # 2. Iterate safely
        for m in meetings:
            # Safe ID extraction
            mid = m['id'].get('meeting_id') or m['id'].get('record_id')
            
            # Safe Title extraction (The Fix)
            # We check top-level keys first, then values as fallback
            title = m.get('title') or m.get('name') or m.get('subject') or "Untitled"
            
            print(f"\n   --- Checking Meeting: '{title}' (ID: {mid}) ---")
            
            # Check Recordings
            rec_url = f"https://api.attio.com/v2/meetings/{mid}/call_recordings"
            rec_res = requests.get(rec_url, headers=headers)
            
            recordings = rec_res.json().get("data", [])
            
            if not recordings:
                print("      🔸 No recordings.")
                continue
                
            print(f"      ✅ FOUND {len(recordings)} RECORDING(S)!")
            
            # Check Transcripts
            for rec in recordings:
                rid = rec['id']['call_recording_id']
                trans_url = f"https://api.attio.com/v2/meetings/{mid}/call_recordings/{rid}/transcript"
                trans_res = requests.get(trans_url, headers=headers)
                
                if trans_res.status_code == 200:
                    data = trans_res.json()
                    keys = list(data.keys())
                    print(f"      📝 Transcript Response Keys: {keys}")
                    
                    # Check for text content
                    text = data.get("content_plaintext") or data.get("text") or data.get("subtitles")
                    if text:
                        print(f"      ✅ Text Length: {len(str(text))} chars")
                    else:
                        print("      ⚠️ Response 200 OK, but text field is empty/missing.")
                else:
                    print(f"      ❌ Transcript Error: {trans_res.status_code}")

    except Exception as e:
        print(f"❌ Crash: {e}")

if __name__ == "__main__":
    probe_transcripts()
