import os
import requests
import json

ATTIO_API_KEY = os.environ.get("ATTIO_API_KEY")

print("🕵️ STARTING ATTIO SCHEMA SCAN...")

if not ATTIO_API_KEY:
    print("❌ Error: ATTIO_API_KEY is missing.")
    exit(1)

def scan_workspace():
    headers = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Accept": "application/json"}
    
    # 1. Get All Objects
    print("\n1️⃣ FETCHING OBJECTS...")
    objects = requests.get("https://api.attio.com/v2/objects", headers=headers).json().get("data", [])
    
    for obj in objects:
        slug = obj['api_slug']
        name = obj['singular_noun']
        print(f"\n------------------------------------------------")
        print(f"📂 OBJECT: {name} (API Slug: '{slug}')")
        
        # 2. Fetch 1 Record to see its structure
        payload = {"limit": 1}
        rec_res = requests.post(f"https://api.attio.com/v2/objects/{slug}/records/query", 
                                headers=headers, json=payload)
        records = rec_res.json().get("data", [])
        
        if not records:
            print("   ⚠️ No records found (Empty object)")
            continue
            
        # 3. List all Attributes (Fields)
        rec = records[0]
        vals = rec.get('values', {})
        print(f"   🔎 AVAILABLE ATTRIBUTES (Field Slugs):")
        for key in vals.keys():
            # Check if this looks like a transcript (Long text)
            data_sample = str(vals[key])[:50]
            print(f"      - '{key}' (Sample: {data_sample}...)")

scan_workspace()
