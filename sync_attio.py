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

print("🚀 Starting Sync V29 (Context & Title Fixer)...")

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

# --- HELPER: SMART NAME EXTRACTOR ---
def extract_smart_name_and_content(vals):
    name = "Untitled"
    email = ""
    try:
        # Priority scan for names
        for key in ['name', 'full_name', 'first_name', 'title', 'company_name', 'deal_name']:
            if key in vals and vals[key]:
                item = vals[key][0]
                if 'value' in item: name = item['value']
                elif 'first_name' in item: name = f"{item.get('first_name', '')} {item.get('last_name', '')}"
                break
        
        if 'email_addresses' in vals and vals['email_addresses']:
            email = vals['email_addresses'][0].get('value', '')
        
        if name == "Untitled" and email: name = email
        content = f"Name: {name} | Email: {email}"
    except:
        content = "Error parsing"
    return name, content

# --- 1. SYNC NOTES WITH CONTEXT (THE FIX) ---
def sync_enriched_notes():
    print("\n📝 1. Syncing Notes with Rich Titles...")
    
    # We scan the objects where notes usually live to get the Parent Name
    target_slugs = ["people", "companies", "deals"] 
    headers = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Accept": "application/json"}
    
    for slug in target_slugs:
        print(f"\n   🔎 Scanning {slug} to fix note titles...")
        
        limit = 500
        offset = 0
        
        while True:
            # A. Get Records (People/Companies)
            payload = {"limit": limit, "offset": offset}
            try:
                res = requests.post(f"https://api.attio.com/v2/objects/{slug}/records/query", 
                                    headers=headers, json=payload)
                
                # If object doesn't exist (e.g. deals), skip
                if res.status_code == 404: break
                
                records = res.json().get("data", [])
                if not records: break
                
                note_batch = []
                
                for rec in records:
                    rec_id = rec['id']['record_id']
                    
                    # B. Get the REAL Name (e.g., "Acme Corp")
                    parent_name, _ = extract_smart_name_and_content(rec.get('values', {}))
                    
                    # C. Fetch Notes for this specific record
                    n_res = requests.get("https://api.attio.com/v2/notes", headers=headers,
                                         params={"parent_record_id": rec_id, "parent_object": slug})
                    notes = n_res.json().get("data", [])
                    
                    for n in notes:
                        # D. Construct Better Title
                        # Original note title (e.g. "Meeting Minutes")
                        raw_title = n.get('title', 'Untitled')
                        
                        # New Rich Title: "Note: Meeting Minutes - Acme Corp"
                        if raw_title == "Untitled" or raw_title == "":
                            final_title = f"Note on {parent_name}"
                        else:
                            final_title = f"Note: {raw_title} ({parent_name})"

                        note_batch.append({
                            "id": n['id']['note_id'], 
                            "parent_id": rec_id, 
                            "type": "note",
                            "title": final_title, # <--- FIXED TITLE
                            "content": n.get('content_plaintext', ''), 
                            "url": f"https://app.attio.com/w/workspace/note/{n['id']['note_id']}",
                            "metadata": {"created_at": n.get("created_at"), "parent": parent_name}
                        })
                
                # Save batch
                if note_batch:
                    safe_upsert(note_batch)
                    print(f"      -> Updated {len(note_batch)} notes for {slug}")

                if len(records) < limit: break
                offset += limit
                
            except Exception as e:
                print(f"   ⚠️ Error in loop: {e}")
                break

# --- 2. TRANSCRIPTS (Keep existing logic) ---
def sync_transcripts():
    print("\n📞 2. Syncing Meeting Transcripts...")
    # (Same logic as V28 - Auto Detect Meeting Object)
    headers = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Accept": "application/json"}
    
    # 1. Find Meeting Object
    slug = "meetings" # Default
    try:
        obj_check = requests.get("https://api.attio.com/v2/objects", headers=headers).json().get("data", [])
        slugs = [o['api_slug'] for o in obj_check]
        if "meetings" not in slugs:
            if "calls" in slugs: slug = "calls"
            elif "sales_calls" in slugs: slug = "sales_calls"
    except: pass
    
    limit = 200
    offset = 0
    while True:
        try:
            res = requests.post(f"https://api.attio.com/v2/objects/{slug}/records/query", 
                                headers=headers, json={"limit": limit, "offset": offset})
            if res.status_code == 404: break
            meetings = res.json().get("data", [])
            if not meetings: break
            
            trans_batch = []
            for m in meetings:
                # Safe ID extraction
                mid = m['id'].get('meeting_id') or m['id'].get('record_id')
                
                # Get Title
                title = "Untitled Meeting"
                if 'title' in m['values']: title = m['values']['title'][0]['value']
                elif 'name' in m['values']: title = m['values']['name'][0]['value']
                
                # Get Recordings
                rec_res = requests.get(f"https://api.attio.com/v2/meetings/{mid}/call_recordings", headers=headers)
                recs = rec_res.json().get("data", [])
                
                for r in recs:
                    rid = r['id']['call_recording_id']
                    t_res = requests.get(f"https://api.attio.com/v2/meetings/{mid}/call_recordings/{rid}/transcript", headers=headers)
                    if t_res.status_code == 200:
                        txt = t_res.json().get("content_plaintext", "") or t_res.json().get("text", "")
                        if txt:
                            trans_batch.append({
                                "id": rid, "parent_id": mid, "type": "call_recording",
                                "title": f"Transcript: {title}", # Good title
                                "content": txt,
                                "url": f"https://app.attio.com",
                                "metadata": {"meeting_id": mid}
                            })
            safe_upsert(trans_batch)
            if len(meetings) < limit: break
            offset += limit
        except: break

# --- 3. ENTITIES (Keep existing logic) ---
def sync_entities():
    print("\n👤 3. Syncing People & Companies...")
    for slug in ["people", "companies"]:
        offset = 0
        while True:
            try:
                res = requests.post(f"https://api.attio.com/v2/objects/{slug}/records/query", 
                                    headers={"Authorization": f"Bearer {ATTIO_API_KEY}"}, 
                                    json={"limit": 1000, "offset": offset})
                data = res.json().get("data", [])
                if not data: break
                
                batch = []
                for rec in data:
                    rec_id = rec['id']['record_id']
                    name, content = extract_smart_name_and_content(rec.get('values', {}))
                    batch.append({
                        "id": rec_id, "type": slug, "title": name, "content": content,
                        "url": f"https://app.attio.com/w/workspace/record/{slug}/{rec_id}", "metadata": {}
                    })
                safe_upsert(batch)
                if len(data) < 1000: break
                offset += 1000
            except: break

# --- 4. TASKS ---
def sync_tasks():
    print("\n✅ 4. Syncing Tasks...")
    try:
        res = requests.get("https://api.attio.com/v2/tasks", headers={"Authorization": f"Bearer {ATTIO_API_KEY}"})
        tasks = res.json().get("data", [])
        batch = []
        for t in tasks:
            batch.append({
                "id": t['id']['task_id'], "type": "task", 
                "title": f"Task: {t.get('content_plaintext', 'Untitled')}",
                "content": f"Status: {t.get('is_completed')}",
                "url": "https://app.attio.com/w/workspace/tasks", "metadata": {}
            })
        safe_upsert(batch)
    except: pass

if __name__ == "__main__":
    try:
        # Run Enriched Notes first to fix the titles
        sync_enriched_notes()
        sync_transcripts()
        sync_entities()
        sync_tasks()
        print("\n🏁 Sync Job Finished.")
    except Exception as e:
        print("\n❌ CRITICAL FAILURE")
        traceback.print_exc()
        exit(1)
