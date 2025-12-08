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

print("🚀 Starting Sync V42 (Reverse-Order Strategy)...", flush=True)

if not ATTIO_API_KEY or not SUPABASE_URL:
    print("❌ Error: Secrets missing.", flush=True)
    exit(1)

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("   🔌 DB Connected.", flush=True)
except Exception as e:
    print(f"   ❌ DB Connection Failed: {e}", flush=True)
    exit(1)

# --- DB HELPER (Anti-Timeout) ---
def safe_upsert(items):
    if not items: return
    try:
        # Clean metadata
        for item in items:
            if "metadata" in item and isinstance(item["metadata"], dict):
                item["metadata"] = {k: v for k, v in item["metadata"].items() if v is not None}
        supabase.table("attio_index").upsert(items).execute()
        print(f"   💾 Saved {len(items)} items.", flush=True)
    except Exception as e:
        print(f"   ❌ DB Error: {e}", flush=True)

# --- 1. SYNC TRANSCRIPTS (VIA ACTIVE PEOPLE) ---
def sync_transcripts_rever
