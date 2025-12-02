import os
import time
import requests
from supabase import create_client, Client

# --- CONFIG ---
ATTIO_API_KEY = os.environ.get("ATTIO_API_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def attio_get(endpoint, params=None):
    url = f"https://api.attio.com/v2/{endpoint}"
    headers = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Accept": "application/json"}
    try:
        time.sleep(0.1) # Prevent rate limits
        response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            print(f"Skipping {endpoint}: {response.text}")
            return []
        return response.json().get("data", [])
    except Exception as e:
        print(f"Error {endpoint}: {e}")
        return []

def upsert_item(data):
    try:
        supabase.table("attio_index").upsert(data).execute()
    except Exception as e:
        print(f"Error saving {data['title']}: {e}")

# --- 1. LISTS & CONFIG ---
def sync_lists():
    print("--- Syncing Lists ---")
    lists = attio_get("lists")
    for l in lists:
        upsert_item({
            "id": l['id']['list_id'],
            "type": "list",
            "title": f"List: {l['name']}",
            "content": f"Workspace list. Slug: {l.get('api_slug')}",
            "url": f"https://app.attio.com/w/workspace/lists/{l['id']['list_id']}",
            "metadata": {}
        })
        # Sync entries inside this list
        entries = attio_get(f"lists/{l['id']['list_id']}/entries")
        for entry in entries:
             upsert_item({
                "id": entry['id']['entry_id'],
                "parent_id": l['id']['list_id'],
                "type": "list_entry",
                "title": f"Entry in {l['name']}",
                "content": f"Entry ID {entry['id']['entry_id']}", # List entries are often just pointers to records
                "url": f"https://app.attio.com/w/workspace/lists/{l['id']['list_id']}",
                "metadata": {}
            })

# --- 2. OBJECTS, RECORDS, TASKS, NOTES, COMMENTS ---
def sync_objects_full():
    print("--- Syncing Objects & Children ---")
    objects = attio_get("objects")
    
    for obj in objects:
        slug = obj['api_slug']
        print(f"Processing Object: {slug}...")
        
        # Sync Object Config
        upsert_item({
            "id": obj['id']['object_id'],
            "type": "object_config",
            "title": f"Object: {obj['singular_noun']}",
            "content": obj.get('description', ''),
            "url": f"https://app.attio.com/w/settings/objects/{slug}",
            "metadata": {"slug": slug}
        })

        # Sync Records
        records = attio_get(f"objects/{slug}/records", params={"limit": 500})
        for rec in records:
            rec_id = rec['id']['record_id']
            vals = rec.get('values', {})
            
            # Smart Name Extraction
            name = "Untitled"
            if 'name' in vals: name = vals['name'][0]['value']
            elif 'email_addresses' in vals: name = vals['email_addresses'][0]['value']
            elif 'title' in vals: name = vals['title'][0]['value']

            upsert_item({
                "id": rec_id,
                "type": slug, 
                "title": name,
                "content": str(vals), 
                "url": f"https://app.attio.com/w/workspace/record/{slug}/{rec_id}",
                "metadata": vals
            })

            # Sync Tasks
            tasks = attio_get(f"objects/{slug}/records/{rec_id}/tasks")
            for t in tasks:
                upsert_item({
                    "id": t['id']['task_id'],
                    "parent_id": rec_id,
                    "type": "task",
                    "title": f"Task: {t['content_plaintext']}",
                    "content": f"Completed: {t['is_completed']}",
                    "url": "https://app.attio.com/w/workspace/tasks",
                    "metadata": {"deadline": t.get('deadline_at')}
                })

            # Sync Comments
            comments = attio_get(f"objects/{slug}/records/{rec_id}/comments")
            for c in comments:
                upsert_item({
                    "id": c['id']['comment_id'],
                    "parent_id": rec_id,
                    "type": "comment",
                    "title": f"Comment on {name}",
                    "content": c['content_plaintext'],
                    "url": f"https://app.attio.com/w/workspace/record/{slug}/{rec_id}",
                    "metadata": {"author": c.get('author')}
                })
                
            # Sync Notes (V2 Endpoint)
            notes = attio_get("notes", params={"parent_record_id": rec_id})
            for n in notes:
                upsert_item({
                    "id": n['id']['note_id'],
                    "parent_id": rec_id,
                    "type": "note",
                    "title": n.get('title', 'Untitled Note'),
                    "content": n.get('content_plaintext', ''),
                    "url": f"https://app.attio.com/w/workspace/note/{n['id']['note_id']}",
                    "metadata": {}
                })

# --- 3. CALL RECORDINGS (CUSTOM OBJECT) ---
def sync_call_recordings():
    print("--- Syncing Calls/Transcripts ---")
    # CHANGE 'calls' TO YOUR ATTIO OBJECT SLUG (e.g., 'meetings', 'zoom_calls')
    calls_slug = "calls" 
    calls = attio_get(f"objects/{calls_slug}/records")
    
    for call in calls:
        vals = call.get('values', {})
        # CHANGE 'transcript' TO YOUR ATTIO ATTRIBUTE SLUG
        transcript = ""
        if 'transcript' in vals: transcript = vals['transcript'][0]['value']
        
        title = "Call"
        if 'title' in vals: title = vals['title'][0]['value']

        upsert_item({
            "id": call['id']['record_id'],
            "type": "call_recording",
            "title": title,
            "content": transcript, 
            "url": f"https://app.attio.com/w/workspace/record/{calls_slug}/{call['id']['record_id']}",
            "metadata": vals
        })

if __name__ == "__main__":
    sync_lists()
    sync_objects_full()
    sync_call_recordings()
    print("Sync Complete.")