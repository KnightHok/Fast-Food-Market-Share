"""
Scheduled script to automatically export completed Apify runs.
Run this with cron/Task Scheduler to periodically check for new completed runs.

Example cron: */15 * * * * python scheduled_export.py
(runs every 15 minutes)
"""
import os
import asyncio
import json
from datetime import datetime, timezone
from typing import Set
import httpx
import asyncpg
from dotenv import load_dotenv

from processor import fetch_and_export_run

load_dotenv(".env")

# Config
APIFY_TOKEN = os.getenv("APIFY_TOKEN")
APIFY_ACTOR_ID = os.getenv("APIFY_ACTOR_ID")
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")
STATE_FILE = "exported_runs.json"

def load_exported_runs() -> Set[str]:
    """Load set of run IDs we've already exported"""
    if not os.path.exists(STATE_FILE):
        return set()

    try:
        with open(STATE_FILE, "r") as f:
            data = json.load(f)
            return set(data.get("exported_runs", []))
    except Exception as e:
        print(f"Warning: Could not load state file: {e}")
        return set()

def save_exported_runs(run_ids: Set[str]) -> None:
    """Save set of exported run IDs to disk"""
    try:
        with open(STATE_FILE, "w") as f:
            json.dump({
                "exported_runs": list(run_ids),
                "last_updated": datetime.now(timezone.utc).isoformat()
            }, f, indent=4)
    except Exception as e:
        print(f"Warning: Could not save state file: {e}")

async def fetch_recent_runs(limit: int=50) -> list:
    """
    Fetch recent runs for this actor from Apify API

    Args:
        limit: Number of recent runs to check (default: 50)

    Returns:
        List of run objects from Apify
    """
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(
            f"https://api.apify.com/v2/acts/{APIFY_ACTOR_ID}/runs",
            headers={"Authorization": f"Bearer {APIFY_TOKEN}"},
            params={"limit": limit}
        )
        resp.raise_for_status()
        return resp.json().get("data", {}).get("items", [])
    
async def main():
    """
    Main workflow:
    1. Load previously exported run IDs
    2. Fetch recent runs from Apify
    3. Filter for completed runs we haven't exported yet
    4. Export each new completed run
    5. Save updated state
    """
    print(f"[{datetime.now(timezone.utc).isoformat()}] Starting scheduled export check...")

    # Load state
    exported_runs = load_exported_runs()
    print(f"✓ Loaded {len(exported_runs)} previously exported runs")

    # Fetch recent runs
    try:
        runs = await fetch_recent_runs(limit=50)
        print(f"✓ Fetched {len(runs)} recent runs from Apify")
    except Exception as e:
        print(f"✗ Error fetching runs: {e}")
        return

    # Filter for new completed runs
    new_completed_runs = [
        r for r in runs
        if r.get("status") == "SUCCEEDED"
        and r.get("id") not in exported_runs
        and r.get("defaultDatasetId")
    ]

    if not new_completed_runs:
        print("✓ No new completed runs to export")
        return

    print(f"Found {len(new_completed_runs)} new completed runs to export")

    # Create database connection pool
    db_pool = None
    if SUPABASE_DB_URL:
        try:
            db_pool = await asyncpg.create_pool(
                SUPABASE_DB_URL,
                min_size=1,
                max_size=5,
                command_timeout=60
            )
            print("✓ Database connection pool created")
        except Exception as e:
            print(f"⚠ Could not connect to database: {e}")
            print("  Continuing without database storage...")
    
    # Process each run
    newly_exported = []
    for run in new_completed_runs:
        run_id = run["id"]
        dataset_id = run["defaultDatasetId"]

        print(f"\n--- Processing run {run_id} ---")

        try:
            result = await fetch_and_export_run(
                run_id=run_id,
                dataset_id=dataset_id,
                output_folder="exports",
                db_pool=db_pool,
                store_in_db=True,
                filter_fast_food=True
            )

            if result["success"]:
                newly_exported.append(run_id)
                print(f"✓ Successfully exported run {run_id}")
            else:
                print(f"✗ Failed to export run {run_id}: {result.get('error')}")
        except Exception as e:
            print(f"✗ Error processing run {run_id}: {e}")
        
    # close database pool
    if db_pool:
        await db_pool.close()
        print("\n✓ Database connection pool closed")
    
    # Update state file
    if newly_exported:
        exported_runs.update(newly_exported)
        save_exported_runs(exported_runs)
        print(f"\n✓ Updated state file with {len(newly_exported)} new exports")

    print(f"\n[{datetime.now(timezone.utc).isoformat()}] Export check complete")
    print(f"Summary: {len(newly_exported)} runs exported, {len(exported_runs)} total in history")

if __name__ == "__main__":
    asyncio.run(main())