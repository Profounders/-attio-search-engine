import os
import time
import requests
import traceback # Added for detailed error logs
from supabase import create_client, Client

# --- CONFIG ---
ATTIO_API_KEY = os.environ.get("ATTIO_API_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

print("🚀 Starting Sync Script V5 (Defensive Mode)...")

if not ATTIO_API_KEY or not SUPABASE_URL:
    print("❌ Error: Secrets missing. Check GitHub Actions Secrets.")
    exit(1)

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception as e:
    print(f"❌ Error connecting to Supabase: {e}")
    exit(1)

# --- API HELPERS ---
def attio_post_query(endpoint, payload=None):
    url = f"https://api.attio.com/v2/{endpoint}"
    headers = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Content-Type": "application/json", "Accept": "application/json"}
    if payload is None: payload = {}
    
    try:
        time.sleep(0.2)
        response = requests.post(url, headers=headers, json=payload)
        
        if response.status_code != 200:
            print(f"   ⚠️ POST Error {response.status_code} on {endpoint}: {response.text[:100]}")
            return []
            
        return response.json().get("data", [])
    except Exception as e:
        print(f"   ❌ Network Error on {endpoint}: {e}")
        return []

def attio_get(endpoint, params=None):
    url = f"https://api.attio.com/v2/{endpoint}"
    headers = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Accept": "application/json"}
    try:
        time.sleep(0.2)
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code != 200:
            return []
            
        return response.json().get("data", [])
    except Exception as e:
        return []

def upsert_item(data):
    try:
        # Clean metadata to prevent JSON errors
        if "metadata" in data and data["metadata"]:
             data["metadata"] = {k: v for k, v in data["metadata"].items() if v is not None}
        
        supabase.table("attio_index").upsert(data).execute()
        # Log success occasionally
        if int(time.time()) % 5 == 0: 
             print(f"   ✅ Synced: {data['type']} - {data['title'][:30]}")
    except Exception as e:
        print(f"   ❌ DB Error saving {data.get('title')}: {e}")

# --- SYNC LOGIC ---

def sync_tasks_globally():
    print("✅ Syncing Tasks...")
    tasks = attio_get("tasks") 
    for t in tasks:
        try:
            upsert_item({
                "id": t['id']['task_id'],
                "type": "task",
                "title": f"Task: {t.get('content_plaintext', 'Untitled')}",
                "content": f"Status: {t.get('is_completed')}",
                "url": "https://app.attio.com/w/workspace/tasks",
                "metadata": {"deadline": t.get("deadline_at")}
            })
        except Exception as e:
            print(f"Skipping broken task: {e}")

def sync_objects_and_records():
    print("\n🔍 Discovering Objects...")
    objects = attio_get("objects")
    
    if not objects:
        print("   ⚠️ No objects found. Check 'Object Configuration' permissions.")
        return

    for obj in objects:
        try:
            slug = obj['api_slug']
            singular = obj['singular_noun']
            print(f"   📂 Processing {singular} ({slug})...")
            
            # Use POST /query to get records
            records = attio_post_query(f"objects/{slug}/records/query", payload={"limit": 1000})
            
            if not records:
                print(f"      (0 records found or permission denied)")
                continue
                
            print(f"      Found {len(records)} records. Processing...")

            for rec in records:
                # --- SAFETY BLOCK START ---
                try:
                    rec_id = rec['id']['record_id']
                    vals = rec.get('values', {})
                    
                    # Safe Name Finding
                    name = "Untitled"
                    for k in ['name', 'title', 'company_name', 'email_addresses', 'domains', 'deal_name']:
                        if k in vals and vals[k] and isinstance(vals[k], list) and len(vals[k]) > 0:
                            name = vals[k][0]['value']
                            break
                    
                    # 1. Sync Record
                    upsert_item({
                        "id": rec_id,
                        "type": slug,
                        "title": name,
                        "content": str(vals),
                        "url": f"https://app.attio.com/w/workspace/record/{slug}/{rec_id}",
                        "metadata": {} 
                    })

                    # 2. Fetch Notes (Nested)
                    notes = attio_get("notes", params={"parent_record_id": rec_id})
                    for n in notes:
                        upsert_item({
                            "id": n['id']['note_id'],
                            "parent_id": rec_id,
                            "type": "note",
                            "title": f"Note on {name}",
                            "content": n.get('content_plaintext', ''),
                            "url": f"https://app.attio.com/w/workspace/note/{n['id']['note_id']}",
                            "metadata": {"created_at": n.get("created_at")}
                        })
                    
                    # 3. Fetch Comments
                    comments = attio_get(f"objects/{slug}/records/{rec_id}/comments")
                    for c in comments:
                        upsert_item({
                            "id": c['id']['comment_id'],
                            "parent_id": rec_id,
                            "type": "comment",
                            "title": f"Comment on {name}",
                            "content": c.get('content_plaintext', ''),
                            "url": f"https://app.attio.com/w/workspace/record/{slug}/{rec_id}",
                            "metadata": {"author": c.get('author')}
                        })
                except Exception as e:
                    # This catches crashes on individual records so the script keeps going
                    # print(f"      ⚠️ Skipped broken record: {e}") 
                    pass 
                # --- SAFETY BLOCK END ---

        except Exception as e:
            print(f"   ❌ Error processing object {slug}: {e}")

if __name__ == "__main__":
    try:
        # 1. Lists
        print("--- 1. Lists ---")
        lists = attio_get("lists")
        for l in lists:
            try:
                upsert_item({"id": l['id']['list_id'], "type": "list", "title": l['name'], "content": "", "url": "", "metadata": {}})
            except: pass

        # 2. Tasks
        sync_tasks_globally()

        # 3. Records & Notes
        sync_objects_and_records()
        
        print("\n🏁 Sync Job Finished Successfully.")
    except Exception as e:
        print("\n❌ CRITICAL SCRIPT FAILURE")
        traceback.print_exc()
        exit(1)
