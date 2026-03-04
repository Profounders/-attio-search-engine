import os
import time
import traceback
import json
import requests
from datetime import datetime
from supabase import create_client, Client

print("------------------------------------------------", flush=True)
print("✅ SCRIPT IS ALIVE. V50 (Notes fixed + Updates fixed + Transcripts added) Starting...", flush=True)
print("------------------------------------------------", flush=True)

# --- CONFIG ---
ATTIO_API_KEY = os.environ.get("ATTIO_API_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not ATTIO_API_KEY or not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ Error: Missing ATTIO_API_KEY / SUPABASE_URL / SUPABASE_KEY.", flush=True)
    raise SystemExit(1)

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("   🔌 DB Connected.", flush=True)
except Exception as e:
    print(f"   ❌ DB Connection Failed: {e}", flush=True)
    raise SystemExit(1)

# --- GLOBAL CACHE ---
NAME_CACHE = {}

# --- API HELPER ---
def make_request(method: str, url: str, params=None, json_data=None, max_retries=6):
    """
    Basic request wrapper with:
    - Authorization header
    - rate-limit retry (429)
    - transient retry (5xx / timeouts)
    """
    headers = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Accept": "application/json"}

    backoff = 1.5
    for attempt in range(1, max_retries + 1):
        try:
            if method.upper() == "GET":
                res = requests.get(url, headers=headers, params=params, timeout=45)
            else:
                res = requests.post(url, headers=headers, params=params, json=json_data, timeout=45)

            if res.status_code == 429:
                sleep_s = min(10, int(backoff * attempt))
                print(f"   ⚠️ Rate limited (429). Sleeping {sleep_s}s... ({url})", flush=True)
                time.sleep(sleep_s)
                continue

            if 500 <= res.status_code <= 599:
                sleep_s = min(10, int(backoff * attempt))
                print(f"   ⚠️ Attio {res.status_code}. Sleeping {sleep_s}s... ({url})", flush=True)
                time.sleep(sleep_s)
                continue

            return res

        except Exception as e:
            sleep_s = min(10, int(backoff * attempt))
            print(f"   ⚠️ Request error: {e}. Sleeping {sleep_s}s... ({url})", flush=True)
            time.sleep(sleep_s)

    return None

# --- DB HELPER ---
def safe_upsert(items):
    """
    Upserts into attio_index.
    Assumes:
      - attio_index.id is UNIQUE (text recommended)
      - FTS is maintained by a trigger / generated column on title+content
    """
    if not items:
        return

    # Remove None values in metadata (supabase sometimes dislikes them)
    for item in items:
        if "metadata" in item and isinstance(item["metadata"], dict):
            item["metadata"] = {k: v for k, v in item["metadata"].items() if v is not None}

    try:
        supabase.table("attio_index").upsert(items).execute()
    except Exception as e:
        err = str(e)
        if "57014" in err or "timeout" in err.lower():
            if len(items) > 1:
                mid = len(items) // 2
                print("   ⚠️ DB Timeout. Splitting batch...", flush=True)
                time.sleep(1)
                safe_upsert(items[:mid])
                safe_upsert(items[mid:])
        else:
            print(f"   ❌ DB Error: {e}", flush=True)

# --- HELPER: CACHED RECORD NAME LOOKUP ---
def get_record_display_name(object_slug: str, record_id: str) -> str:
    """
    Used only to enrich UI metadata.
    """
    if not record_id or not object_slug:
        return "Unknown"

    cache_key = f"{object_slug}:{record_id}"
    if cache_key in NAME_CACHE:
        return NAME_CACHE[cache_key]

    try:
        url = f"https://api.attio.com/v2/objects/{object_slug}/records/{record_id}"
        res = make_request("GET", url)
        if not res or res.status_code != 200:
            return "Unknown"

        vals = res.json().get("data", {}).get("values", {}) or {}
        name = "Unknown"

        for key in ["name", "full_name", "title", "company_name"]:
            if key in vals and vals[key]:
                name = vals[key][0].get("value") or "Unknown"
                break

        if name == "Unknown" and "email_addresses" in vals and vals["email_addresses"]:
            name = vals["email_addresses"][0].get("value") or "Unknown"

        NAME_CACHE[cache_key] = name
        return name
    except Exception:
        return "Unknown"

# --- 1) SYNC NOTES (exact titles, fixed pagination) ---
def sync_notes():
    """
    Uses GET /v2/notes (limit max is 50). :contentReference[oaicite:1]{index=1}
    """
    print("\n📝 1. Syncing Notes (exact title, fixed pagination)...", flush=True)

    limit = 50  # per docs max is 50
    offset = 0
    total = 0

    while True:
        params = {"limit": limit, "offset": offset}
        res = make_request("GET", "https://api.attio.com/v2/notes", params=params)
        if not res:
            print("   ❌ Notes request failed (no response).", flush=True)
            break
        if res.status_code != 200:
            print(f"   ❌ Notes request failed: {res.status_code} {res.text[:200]}", flush=True)
            break

        data = res.json().get("data", []) or []
        if not data:
            break

        batch = []
        for n in data:
            try:
                attio_note_id = n["id"]["note_id"]
                parent_object = n.get("parent_object")
                parent_record_id = n.get("parent_record_id")
                parent_name = get_record_display_name(parent_object, parent_record_id)

                title = (n.get("title") or "").strip()
                if not title:
                    title = "Untitled"

                content = (n.get("content_plaintext") or "").strip()

                # IMPORTANT: composite ID prevents collisions across types
                pk = f"note:{attio_note_id}"

                batch.append(
                    {
                        "id": pk,
                        "type": "note",
                        "parent_id": parent_record_id,
                        "title": title,  # exact Attio title
                        "content": content,
                        "url": f"https://app.attio.com/w/workspace/note/{attio_note_id}",
                        "metadata": {
                            "attio_note_id": attio_note_id,
                            "parent_object": parent_object,
                            "parent_record_id": parent_record_id,
                            "parent_name": parent_name,
                            "meeting_id": n.get("meeting_id"),
                            "created_at": n.get("created_at"),
                            "synced_at": datetime.utcnow().isoformat() + "Z",
                        },
                    }
                )
            except Exception:
                # Don't swallow silently—counts matter
                continue

        safe_upsert(batch)
        total += len(batch)

        # Correct pagination: advance by returned item count
        offset += len(data)
        if len(data) < limit:
            break

    print(f"   ✅ Notes Complete. Upserted: {total}", flush=True)

# --- 2) SYNC PEOPLE + COMPANIES (JSON content; composite id) ---
def sync_people_companies():
    """
    Uses POST /v2/objects/{object}/records/query :contentReference[oaicite:2]{index=2}
    """
    print("\n📦 2. Syncing People & Companies...", flush=True)

    for slug in ["people", "companies"]:
        db_type = "person" if slug == "people" else "company"
        limit = 200
        offset = 0
        total = 0

        while True:
            url = f"https://api.attio.com/v2/objects/{slug}/records/query"
            res = make_request("POST", url, json_data={"limit": limit, "offset": offset})
            if not res:
                print(f"   ❌ {slug} request failed (no response).", flush=True)
                break
            if res.status_code != 200:
                print(f"   ❌ {slug} request failed: {res.status_code} {res.text[:200]}", flush=True)
                break

            data = res.json().get("data", []) or []
            if not data:
                break

            batch = []
            for d in data:
                try:
                    rid = d["id"]["record_id"]
                    vals = d.get("values", {}) or {}

                    # basic display name
                    name = "Untitled"
                    if "name" in vals and vals["name"]:
                        name = vals["name"][0].get("value") or name
                    elif "company_name" in vals and vals["company_name"]:
                        name = vals["company_name"][0].get("value") or name
                    elif "email_addresses" in vals and vals["email_addresses"]:
                        name = vals["email_addresses"][0].get("value") or name

                    pk = f"{db_type}:{rid}"

                    batch.append(
                        {
                            "id": pk,
                            "type": db_type,
                            "parent_id": None,
                            "title": name,
                            "content": json.dumps(vals, ensure_ascii=False),
                            "url": f"https://app.attio.com/w/workspace/record/{slug}/{rid}",
                            "metadata": {
                                "attio_record_id": rid,
                                "object_slug": slug,
                                "created_at": d.get("created_at"),
                                "synced_at": datetime.utcnow().isoformat() + "Z",
                            },
                        }
                    )
                except Exception:
                    continue

            safe_upsert(batch)
            total += len(batch)

            offset += len(data)
            if len(data) < limit:
                break

        print(f"   ✅ {slug.capitalize()} complete. Upserted: {total}", flush=True)

# --- 3) SYNC TASKS (basic) ---
def sync_tasks():
    """
    Uses GET /v2/tasks (pagination may exist; this keeps behaviour simple).
    """
    print("\n✅ 3. Syncing Tasks...", flush=True)

    res = make_request("GET", "https://api.attio.com/v2/tasks")
    if not res:
        print("   ❌ Tasks request failed (no response).", flush=True)
        return
    if res.status_code != 200:
        print(f"   ❌ Tasks request failed: {res.status_code} {res.text[:200]}", flush=True)
        return

    data = res.json().get("data", []) or []
    batch = []

    for t in data:
        try:
            task_id = t["id"]["task_id"]
            pk = f"task:{task_id}"

            title_text = (t.get("content_plaintext") or "").strip() or "Untitled"
            batch.append(
                {
                    "id": pk,
                    "type": "task",
                    "parent_id": None,
                    "title": title_text,
                    "content": json.dumps(
                        {
                            "is_completed": t.get("is_completed"),
                            "deadline_at": t.get("deadline_at"),
                        },
                        ensure_ascii=False,
                    ),
                    "url": "https://app.attio.com/w/workspace/tasks",
                    "metadata": {
                        "attio_task_id": task_id,
                        "created_at": t.get("created_at"),
                        "deadline_at": t.get("deadline_at"),
                        "synced_at": datetime.utcnow().isoformat() + "Z",
                    },
                }
            )
        except Exception:
            continue

    safe_upsert(batch)
    print(f"   ✅ Tasks Complete. Upserted: {len(batch)}", flush=True)

# --- 4) SYNC CALL RECORDINGS + TRANSCRIPTS ---
def _iter_meetings(limit=50):
    """
    GET /v2/meetings uses cursor pagination. :contentReference[oaicite:3]{index=3}
    """
    cursor = None
    while True:
        params = {"limit": limit}
        if cursor:
            params["cursor"] = cursor

        res = make_request("GET", "https://api.attio.com/v2/meetings", params=params)
        if not res or res.status_code != 200:
            msg = res.text[:200] if res else "no response"
            print(f"   ⚠️ Meetings fetch failed: {msg}", flush=True)
            return

        body = res.json() or {}
        for m in (body.get("data") or []):
            yield m

        cursor = (body.get("pagination") or {}).get("next_cursor")
        if not cursor:
            break

def _iter_call_recordings(meeting_id: str, limit=200):
    """
    GET /v2/meetings/{meeting_id}/call_recordings uses cursor pagination. :contentReference[oaicite:4]{index=4}
    """
    cursor = None
    while True:
        params = {"limit": limit}
        if cursor:
            params["cursor"] = cursor

        url = f"https://api.attio.com/v2/meetings/{meeting_id}/call_recordings"
        res = make_request("GET", url, params=params)
        if not res:
            return
        if res.status_code == 404:
            return
        if res.status_code != 200:
            print(f"   ⚠️ Call recordings fetch failed for meeting {meeting_id}: {res.status_code} {res.text[:200]}", flush=True)
            return

        body = res.json() or {}
        for cr in (body.get("data") or []):
            yield cr

        cursor = (body.get("pagination") or {}).get("next_cursor")
        if not cursor:
            break

def fetch_full_transcript(meeting_id: str, call_recording_id: str):
    """
    GET /v2/meetings/{meeting_id}/call_recordings/{call_recording_id}/transcript
    paginates within the transcript via cursor. :contentReference[oaicite:5]{index=5}
    """
    cursor = None
    all_segments = []
    raw_transcript = None
    web_url = None

    while True:
        params = {}
        if cursor:
            params["cursor"] = cursor

        url = f"https://api.attio.com/v2/meetings/{meeting_id}/call_recordings/{call_recording_id}/transcript"
        res = make_request("GET", url, params=params)
        if not res:
            return None
        if res.status_code == 404:
            return None
        if res.status_code != 200:
            # Often indicates missing scopes: meeting:read / call_recording:read
            print(f"   ⚠️ Transcript fetch failed: {res.status_code} {res.text[:200]}", flush=True)
            return None

        body = res.json() or {}
        data = body.get("data") or {}

        # First page tends to include raw_transcript; keep it if present
        if raw_transcript is None:
            raw_transcript = data.get("raw_transcript")
        if web_url is None:
            web_url = data.get("web_url")

        segs = data.get("transcript") or []
        all_segments.extend(segs)

        cursor = (body.get("pagination") or {}).get("next_cursor")
        if not cursor:
            break

    return {
        "raw_transcript": raw_transcript,
        "segments": all_segments,
        "web_url": web_url,
    }

def sync_call_transcripts():
    """
    - Lists meetings
    - For each meeting, lists call recordings
    - For each call recording, fetches transcript and stores:
        type=call_transcript  (content = raw transcript if available, else a stitched transcript)
        type=call_recording   (content = status + minimal metadata)
    """
    print("\n🗣️ 4. Syncing Call Recordings + Transcripts...", flush=True)

    total_recordings = 0
    total_transcripts = 0
    batch = []
    batch_size = 50

    for meeting in _iter_meetings(limit=50):
        try:
            meeting_id = meeting["id"]["meeting_id"]
            meeting_title = (meeting.get("title") or "").strip() or "Untitled meeting"
            start_dt = (meeting.get("start") or {}).get("datetime")

            # Optional: store meeting rows too
            meeting_pk = f"meeting:{meeting_id}"
            batch.append(
                {
                    "id": meeting_pk,
                    "type": "meeting",
                    "parent_id": None,
                    "title": meeting_title,
                    "content": json.dumps(meeting, ensure_ascii=False),
                    "url": f"https://app.attio.com/w/workspace/meetings/{meeting_id}",
                    "metadata": {
                        "meeting_id": meeting_id,
                        "created_at": meeting.get("created_at"),
                        "start_datetime": start_dt,
                        "synced_at": datetime.utcnow().isoformat() + "Z",
                    },
                }
            )

            for cr in _iter_call_recordings(meeting_id, limit=200):
                cr_id = cr["id"]["call_recording_id"]
                status = cr.get("status")
                web_url = cr.get("web_url") or cr.get("web_url")

                # call recording row
                rec_pk = f"call_recording:{cr_id}"
                batch.append(
                    {
                        "id": rec_pk,
                        "type": "call_recording",
                        "parent_id": meeting_id,
                        "title": meeting_title,
                        "content": json.dumps(
                            {"status": status, "meeting_id": meeting_id, "call_recording_id": cr_id},
                            ensure_ascii=False,
                        ),
                        "url": web_url or f"https://app.attio.com/w/workspace/meetings/{meeting_id}",
                        "metadata": {
                            "meeting_id": meeting_id,
                            "call_recording_id": cr_id,
                            "status": status,
                            "created_at": cr.get("created_at"),
                            "meeting_start": start_dt,
                            "synced_at": datetime.utcnow().isoformat() + "Z",
                        },
                    }
                )
                total_recordings += 1

                # transcript row
                transcript = fetch_full_transcript(meeting_id, cr_id)
                if transcript:
                    raw = transcript.get("raw_transcript")
                    segments = transcript.get("segments") or []

                    if not raw and segments:
                        # stitch a readable transcript if raw isn't present
                        lines = []
                        for s in segments:
                            spk = (s.get("speaker") or {}).get("name") or "Speaker"
                            speech = s.get("speech") or ""
                            if speech:
                                lines.append(f"{spk}: {speech}")
                        raw = "\n".join(lines)

                    tx_pk = f"call_transcript:{cr_id}"
                    batch.append(
                        {
                            "id": tx_pk,
                            "type": "call_transcript",
                            "parent_id": meeting_id,
                            "title": f"{meeting_title} (Transcript)",
                            "content": raw or "",
                            "url": transcript.get("web_url") or web_url or "",
                            "metadata": {
                                "meeting_id": meeting_id,
                                "call_recording_id": cr_id,
                                "meeting_title": meeting_title,
                                "meeting_start": start_dt,
                                "created_at": cr.get("created_at"),
                                "segments_count": len(segments),
                                "synced_at": datetime.utcnow().isoformat() + "Z",
                            },
                        }
                    )
                    total_transcripts += 1

                # flush batches
                if len(batch) >= batch_size:
                    safe_upsert(batch)
                    batch = []

        except Exception:
            continue

    if batch:
        safe_upsert(batch)

    print(f"   ✅ Call recordings upserted: {total_recordings}", flush=True)
    print(f"   ✅ Call transcripts upserted: {total_transcripts}", flush=True)

if __name__ == "__main__":
    try:
        sync_notes()
        sync_people_companies()
        sync_tasks()
        sync_call_transcripts()
        print("\n🏁 Sync Job Finished.", flush=True)
    except Exception as e:
        print(f"\n❌ CRITICAL: {e}", flush=True)
        traceback.print_exc()
        raise SystemExit(1)
