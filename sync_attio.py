import os
import requests
import json

ATTIO_API_KEY = os.environ.get("ATTIO_API_KEY")

print("🕵️ STARTING OBJECT INVENTORY...")

if not ATTIO_API_KEY:
    print("❌ Error: Secrets missing.")
    exit(1)

def list_all_objects():
    url = "https://api.attio.com/v2/objects"
    headers = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Accept": "application/json"}
    
    try:
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            print(f"❌ API Error {response.status_code}: {response.text}")
            return

        objects = response.json().get("data", [])
        print(f"\n✅ Found {len(objects)} Objects in your Workspace:\n")
        print(f"{'OBJECT NAME':<25} | {'API SLUG (Use this!)':<25}")
        print("-" * 55)
        
        for obj in objects:
            name = obj.get('singular_noun', 'Unknown')
            slug = obj.get('api_slug', 'Unknown')
            print(f"{name:<25} | {slug:<25}")
            
    except Exception as e:
        print(f"❌ Script crashed: {e}")

if __name__ == "__main__":
    list_all_objects()
