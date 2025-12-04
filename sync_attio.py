import os
import requests
import json

# --- CONFIG ---
ATTIO_API_KEY = os.environ.get("ATTIO_API_KEY")

print("🕵️ STARTING FORENSIC DATA SEARCH...")

if not ATTIO_API_KEY:
    print("❌ Critical: ATTIO_API_KEY is missing from Secrets.")
    exit(1)

def check_endpoint(name, url, method="GET", payload=None):
    headers = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Content-Type": "application/json"}
    try:
        if method == "POST":
            response = requests.post(url, headers=headers, json=payload)
        else:
            response = requests.get(url, headers=headers)
            
        print(f"\n🔎 CHECKING: {name} ({url})")
        print(f"   Status: {response.status_code}")
        
        if response.status_code == 403:
            print("   🚫 PERMISSION DENIED (This is your problem!)")
            return
            
        if response.status_code != 200:
            print(f"   ⚠️ API Error: {response.text[:200]}")
            return

        data = response.json().get("data", [])
        count = len(data)
        print(f"   ✅ Found: {count} items")
        
        if count > 0:
            # Print the first item to see what it looks like
            sample = data[0]
            if "content_plaintext" in sample:
                print(f"   📝 SAMPLE CONTENT: {sample['content_plaintext'][:100]}...")
            elif "values" in sample:
                print(f"   📄 SAMPLE VALUES: {str(sample['values'])[:100]}...")
            else:
                print(f"   📦 SAMPLE RAW: {str(sample)[:100]}...")
    except Exception as e:
        print(f"   ❌ Crashed: {e}")

# --- 1. CHECK "OFFICIAL" NOTES ---
check_endpoint("Official Notes Object", "https://api.attio.com/v2/notes")

# --- 2. CHECK "TIMELINE COMMENTS" (Where most people write notes) ---
# We need to find a person first to check their timeline
headers = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Content-Type": "application/json"}
print("\n🔎 CHECKING: Timeline Comments on random people...")

try:
    # Get 5 people
    people_resp = requests.post("https://api.attio.com/v2/objects/people/records/query", 
                                headers=headers, json={"limit": 5})
    people = people_resp.json().get("data", [])
    
    if not people:
        print("   ⚠️ No people found to check comments on.")
    else:
        for p in people:
            pid = p['id']['record_id']
            # Get name for context
            name = "Unknown"
            vals = p.get('values', {})
            if 'email_addresses' in vals and vals['email_addresses']:
                name = vals['email_addresses'][0]['value']
                
            print(f"   ...Checking timeline for: {name}")
            
            # Check Comments Endpoint
            check_endpoint(f"Comments for {name}", 
                           f"https://api.attio.com/v2/objects/people/records/{pid}/comments")
            
            # Check Notes Endpoint with Parent Filter
            check_endpoint(f"Notes attached to {name}", 
                           f"https://api.attio.com/v2/notes?parent_record_id={pid}&parent_object=people")

except Exception as e:
    print(f"   ❌ Error checking timelines: {e}")

# --- 3. CHECK FOR CUSTOM "NOTE" OBJECTS ---
# Sometimes people create a custom object called "Meeting Note" instead of using the feature
print("\n🔎 CHECKING: Custom Objects (Did you build your own Notes object?)")
check_endpoint("All Object Types", "https://api.attio.com/v2/objects")

print("\n🕵️ FORENSIC SEARCH COMPLETE.")
