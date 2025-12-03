import os
import time
import requests
from supabase import create_client, Client

# --- CONFIG ---
ATTIO_API_KEY = os.environ.get("ATTIO_API_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not ATTIO_API_KEY or not SUPABASE_URL:
    print("❌ Error: Secrets missing.")
    exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def attio_get(endpoint, params=None):
    url = f"https://api.attio.com/v2/{endpoint}"
    headers = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Accept": "application/json"}
    try:
        time.sleep(0.2) 
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 404:
            print(f"   ⚠️ 404 Not Found: '{endpoint}' (Object might not exist)")
            return None
        if response.status_code == 403:
            print(f"   🚫 403 Forbidden: '{endpoint}' (Check API Scopes)")
            return None
        if response.status_code != 200:
            print(f"   ⚠️ Error {response.status_code}: {response.text}")
            return None
            
        return response.json().get("data", [])
    except Exception as e:
        print(f"   ❌ Network Error: {e}")
        return None

def upsert_item(data):
    try:
        if "metadata" in data and data["metadata"]:
             data["metadata"] = {k: v for k, v in data["metadata"].items() if v is not None}
        supabase.table("attio_index").upsert(data).execute()
        # Only print every 10th item to keep logs clean
        if int(time.time()) % 2 == 0: 
            print(f"   ✅ Synced: {data['title'][:30]}")
    except Exception as e:
        print(f"   ❌ DB Error: {e}")

# --- MAIN LOGIC ---
def run_dynamic_sync():
    print("🚀 Starting Dynamic Sync...")

    # 1. Discover Objects
    print("\n🔍 Discovering Objects in your Workspace...")
    objects = attio_get("objects")
    
    if not objects:
        print("❌ Critical: Could not fetch Object List. Check 'Object Configuration' scope.")
        return

    print(f"   Found {len(objects)} Objects.")

    # 2. Iterate through every object found
    for obj in objects:
        slug = obj['api_slug']
        obj_id = obj['id']['object_id']
        singular_name = obj['singular_noun']
        
        print(f"\n📂 Processing Object: {singular_name} (Slug: {slug})")
        
        # Sync the Object Definition
        upsert_item({
            "id": obj_id,
            "type": "object_config",
            "title": f"Object: {singular_name}",
            "content": obj.get('description', 'No description'),
            "url": f"https://app.attio.com/w/settings/objects/{slug}",
            "metadata": {"slug": slug}
        })

        # 3. Fetch Records using the SLUG found
        records = attio_get(f"objects/{slug}/records", params={"limit": 500})
        
        if not records:
            print(f"   ⚠️ No records found for {slug} (or permission denied).")
            continue

        print(f"   Processing {len(records)} records for {slug}...")

        for rec in records:
            try:
                rec_id = rec['id']['record_id']
                vals = rec.get('values', {})
                
                # Intelligent Name Finding
                name = "Untitled"
                # Look for standard name fields
                possible_keys = ['name', 'title', 'company_name', 'deal_name', 'topic', 'email_addresses', 'domains']
                for k in possible_keys:
                    if k in vals and vals[k]:
                        name = vals[k][0]['value']
                        break
                
                # 4. Upsert Record
                upsert_item({
                    "id": rec_id,
                    "type": slug,
                    "title": name,
                    "content": str(vals), # Store all data as text for search
                    "url": f"https://app.attio.com/w/workspace/record/{slug}/{rec_id}",
                    "metadata": {} 
                })

                # 5. Fetch Notes for this Record
                notes = attio_get("notes", params={"parent_record_id": rec_id})
                if notes:
                    for n in notes:
                        upsert_item({
                            "id": n['id']['note_id'],
                            "parent_id": rec_id,
                            "type": "note",
                            "title": f"Note on {name}",
                            "content": n.get('content_plaintext', ''),
                            "url": f"https://app.attio.com/w/workspace/note/{n['id']['note_id']}",
                            "metadata": {}
                        })
                
                # 6. Fetch Tasks for this Record
                tasks = attio_get(f"objects/{slug}/records/{rec_id}/tasks")
                if tasks:
                    for t in tasks:
                        upsert_item({
                            "id": t['id']['task_id'],
                            "parent_id": rec_id,
                            "type": "task",
                            "title": f"Task: {t.get('content_plaintext', 'Task')}",
                            "content": f"Status: {t.get('is_completed')}",
                            "url": "https://app.attio.com/w/workspace/tasks",
                            "metadata": {}
                        })

            except Exception as e:
                print(f"   ⚠️ Error processing a record: {e}")

    print("\n🏁 Sync Complete.")

if __name__ == "__main__":
    run_dynamic_sync()
