import os
import requests
import json

ATTIO_API_KEY = os.environ.get("ATTIO_API_KEY")

# --- FROM YOUR URL ---
# URL: https://app.attio.com/.../calls/[ID_1]/[ID_2]/meeting
ID_1 = "b3ab5aae-0857-432f-908e-04c23e03dc04"
ID_2 = "11543579-cb48-4e55-8a63-7742f1857358"

print(f"🎯 STARTING SNIPER PROBE...")
print(f"   Target 1: {ID_1}")
print(f"   Target 2: {ID_2}")

if not ATTIO_API_KEY:
    print("❌ Error: Secrets missing.")
    exit(1)

headers = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Accept": "application/json"}

def test_endpoint(name, url):
    print(f"\nTesting: {name}")
    print(f"   URL: {url}")
    try:
        res = requests.get(url, headers=headers)
        print(f"   Status: {res.status_code}")
        
        if res.status_code == 200:
            data = res.json()
            print("   ✅ SUCCESS!")
            # Check for text keys
            if isinstance(data, dict):
                keys = list(data.keys())
                # Dig deeper if nested
                if "data" in keys: 
                    keys = list(data['data'].keys())
                    
                print(f"   🔑 Found Keys: {keys}")
                
                # Check for transcript text
                dump = json.dumps(data)
                if "content_plaintext" in dump or "subtitles" in dump:
                    print("   📄 TRANSCRIPT TEXT FOUND IN PAYLOAD!")
                else:
                    print("   ⚠️ Valid response, but no obvious text field found.")
        elif res.status_code == 404:
            print("   ❌ Not Found (Invalid ID or Wrong Endpoint)")
        elif res.status_code == 403:
            print("   🚫 Forbidden (Permission Issue)")
        else:
            print(f"   ⚠️ Error: {res.text}")
            
    except Exception as e:
        print(f"   ❌ Crash: {e}")

# --- TEST 1: Assume ID_1 is the Meeting, ID_2 is the Recording ---
test_endpoint(
    "Scenario A: ID_1=Meeting, ID_2=Recording (Transcript Endpoint)",
    f"https://api.attio.com/v2/meetings/{ID_1}/call_recordings/{ID_2}/transcript"
)

# --- TEST 2: Assume ID_2 is the Meeting, ID_1 is the Recording ---
test_endpoint(
    "Scenario B: ID_2=Meeting, ID_1=Recording (Transcript Endpoint)",
    f"https://api.attio.com/v2/meetings/{ID_2}/call_recordings/{ID_1}/transcript"
)

# --- TEST 3: Check if ID_1 is a generic 'Object Record' ---
# Maybe 'calls' is a custom object and ID_1 is just a record ID?
test_endpoint(
    "Scenario C: ID_1 is a Record in a 'calls' object",
    f"https://api.attio.com/v2/objects/calls/records/{ID_1}"
)

# --- TEST 4: Check if ID_1 is a global Meeting ---
test_endpoint(
    "Scenario D: ID_1 is a Global Meeting",
    f"https://api.attio.com/v2/meetings/{ID_1}"
)
