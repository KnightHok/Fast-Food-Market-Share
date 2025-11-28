import os
from typing import Any, Dict, Optional, List
from datetime import datetime
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, Body
from pydantic import BaseModel
import httpx
import asyncpg
import asyncio
from dotenv import load_dotenv

from processor import fetch_and_export_run

load_dotenv(".env")

# -----------------------
# Config (env variables)
# -----------------------
APIFY_TOKEN = os.getenv("APIFY_TOKEN", "apify_api_xxx")
APIFY_ACTOR_ID = os.getenv("APIFY_ACTOR_ID", "your-actor-id")
APIFY_WEBHOOK_SECRET = os.getenv("APIFY_WEBHOOK_SECRET", "supersecret")
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL", "")

if APIFY_TOKEN == "apify_api_xxx":
    print("WARNING: APIFY_TOKEN is still the default placeholder.")


# -----------------------
# App + in-memory storage
# -----------------------

RUNS: Dict[str, Dict[str, Optional[str]]] = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage database connection lifecycle"""
    # Startup: Create database connection pool
    if SUPABASE_DB_URL:
        try:
            app.state.db_pool = await asyncpg.create_pool(
                SUPABASE_DB_URL,
                min_size=1,
                max_size=10,
                command_timeout=60
            )
            print("✓ Database connection pool created")
        except Exception as e:
            print(f"✗ Failed to connect to database: {e}")
            app.state.db_pool = None
    else:
        print("⚠ No SUPABASE_DB_URL found, running without database")
        app.state.db_pool = None

    yield  # Application runs here

    # Shutdown: Close database connection pool
    if hasattr(app.state, 'db_pool') and app.state.db_pool:
        await app.state.db_pool.close()
        print("✓ Database connection pool closed")

app = FastAPI(title="Apify Yelp Runner", lifespan=lifespan)


# -----------------------
# Models
# -----------------------

class StartRunInput(BaseModel):
    # whatever you want to send as the actor input
    input: Dict[str, Any] = {}
    # Optional override for webhook URL (otherwise we build from Host header)
    webhook_url: Optional[str] = None

class RunStatus(BaseModel):
    run_id: str
    status: str
    dataset_id: Optional[str] = None

class BatchRunInput(BaseModel):
    input: Dict[str, Any] = {}
    batch_size: int = 10
    delay_between_batches: int = 2 # seconds


# -----------------------
# Routes
# -----------------------

@app.get("/healthz")
async def healthz():
    return {"status": "ok"}

@app.post("/runs/start", response_model=RunStatus)
async def start_run(payload: StartRunInput, request: Request):
    """
    Starts an Apify actor run. Optionally attaches a webhook if URL can be determined.
    If no webhook is attached, you can poll /runs/{run_id} for status updates.
    """
    print("damn")
    webhook_url = None

    if payload.webhook_url:
        webhook_url = payload.webhook_url
    else:
        # Try to build from the incoming request
        scheme = request.headers.get("X-Forwarded-Proto", request.url.scheme)
        host = request.headers.get("X-Forwarded-Host", request.headers.get("host"))

        # Skip webhooks for localhost or if host is missing
        if host and "localhost" not in host and "127.0.0.1" not in host:
            webhook_url = f"{scheme}://{host}/apify/webhook"

    # Build the request payload
    run_payload = payload.input.copy() if isinstance(payload.input, dict) else {}

    # Only add webhook if we have a valid URL
    if webhook_url:
        webhook_def = {
            "eventTypes": [
                "ACTOR.RUN.SUCCEEDED",
                "ACTOR.RUN.FAILED",
                "ACTOR.RUN.TIMED_OUT",
                "ACTOR.RUN.ABORTED",
            ],
            "requestUrl": webhook_url,
            "headers": [
                {"name": "X-Webhook-Secret", "value": APIFY_WEBHOOK_SECRET}
            ],
        }
        run_payload["webhooks"] = [webhook_def]

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"https://api.apify.com/v2/acts/{APIFY_ACTOR_ID}/runs",
                headers = {
                    "Authorization": f"Bearer {APIFY_TOKEN}",
                    "Content-Type": "application/json"
                },
                json=run_payload,
            )
            resp.raise_for_status()
            data = resp.json()["data"]
            print(resp.json())
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Apify API error: {str(e)}")
    except KeyError:
        raise HTTPException(status_code=502, detail=f"Unexpected response from Apify API")

    
    run_id = data["id"]
    status = data["status"]
    dataset_id = data.get("defaultDatasetId")

    # Save initial state in memory
    RUNS[run_id] = {
        "status": status,
        "datasetId": dataset_id,
    }

    return RunStatus(run_id=run_id, status=status, dataset_id=dataset_id)

@app.post("/runs/batch-start")
async def batch_start_runs(payload: BatchRunInput, request: Request):
    """
    Start multiple Apify runs in batches for a list of cities.
    Returns list of started run IDs.
    """
    # Extract the full input
    actor_input = payload.input.copy()

    # Get the search strings array (list of cities)
    location_strings = actor_input.get("locations", [])

    if not location_strings:
        raise HTTPException(status_code=400, detail="locations in required in input")
    
    batch_size = payload.batch_size

    # split cities into batches
    batches = [location_strings[i:i + batch_size] for i in range(0, len(location_strings), batch_size)]

    started_runs = []

    for i, batch in enumerate(batches):
        print(f"Starting batch {i+1}/{len(batches)} with {len(batch)} cities...")

        # Build webhook url
        webhook_url = None
        scheme = request.headers.get("X-Forwarded-Proto", request.url.scheme)
        host = request.headers.get("X-Forwarded-Host", request.headers.get("host"))

        if host and "localhost" not in host and "127.0.0.1" not in host:
            webhook_url = f"{scheme}://{host}/apify/webhook"
        
        # Build run payload for this batch
        run_payload = actor_input.copy()
        run_payload["locations"] = batch

        # Add webhook if available
        if webhook_url:
            webhook_def = {
                "eventTypes": [
                    "ACTOR.RUN.SUCCEEDED",
                    "ACTOR.RUN.FAILED",
                    "ACTOR.RUN.TIMED_OUT",
                    "ACTOR.RUN.ABORTED",
                ],
                "requestUrl": webhook_url,
                "headers": [
                    {"name": "X-Webhook-Secret", "value": APIFY_WEBHOOK_SECRET}
                ],
            }
            run_payload["webhooks"] = [webhook_def]
        # start the run
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(
                    f"https://api.apify.com/v2/acts/{APIFY_ACTOR_ID}/runs",
                    headers={
                        "Authorization": f"Bearer {APIFY_TOKEN}",
                        "Content-Type": "application/json"
                    },
                    json=run_payload,
                )
                resp.raise_for_status()
                data = resp.json()["data"]

                run_id = data["id"]
                status = data["status"]
                dataset_id = data.get("defaultDatasetId")

                # save in memory
                RUNS[run_id] = {
                    "status": status,
                    "datasetId": dataset_id,
                    "batch_number": i + 1,
                    "search_count": len(batch)
                }
                
                started_runs.append({
                    "batch": i + 1,
                    "search_count": len(batch),
                    "run_id": run_id,
                    "status": status,
                    "dataset_id": dataset_id
                })

                print(f"✅ Batch {i+1} started: {run_id}")
        except httpx.HTTPError as e:
            error_msg = f"Apify API error: {str(e)}"
            print(f"✗ Batch {i+1} failed: {error_msg}")
            started_runs.append({
                "batch": i + 1,
                "search_count": len(batch),
                "error": error_msg,
                "failed": True
            })
        except KeyError:
            error_msg = "Unexpected response from Apify API"
            print(f"✗ Batch {i+1} failed: {error_msg}")
            started_runs.append({
                "batch": i + 1,
                "search_count": len(batch),
                "error": error_msg,
                "failed": True
            })
        
        # delay between batches (except after last batch)
        if i < len(batches) - 1:
            await asyncio.sleep(payload.delay_between_batches)

    return {
        "message": "Batch runs started successfully",
        "total_locations": len(location_strings),
        "total_batches": len(batches),
        "batch_size": batch_size,
        "runs": started_runs
    }

@app.get("/runs/{run_id}", response_model=RunStatus)
async def get_run_status(run_id: str, refresh: bool = False):
    """
    Returns the status of a run.
    - If refresh=false (default): returns cached status from memory
    - If refresh=true: fetches latest status from Apify API
    """
    if refresh or run_id not in RUNS:
        # Fetch fresh data from Apify
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.get(
                    f"https://api.apify.com/v2/actor-runs/{run_id}",
                    headers={"Authorization": f"Bearer {APIFY_TOKEN}"}
                )
                resp.raise_for_status()
                data = resp.json()["data"]

            status = data["status"]
            dataset_id = data.get("defaultDatasetId")

            # Update cache
            RUNS[run_id] = {
                "status": status,
                "datasetId": dataset_id,
            }
        except httpx.HTTPError as e:
            raise HTTPException(status_code=502, detail=f"Apify API error: {str(e)}")
        except KeyError:
            raise HTTPException(status_code=502, detail=f"Unexpected response from Apify API")

    info = RUNS.get(run_id)
    if not info:
        raise HTTPException(status_code=404, detail="Unknown run_id")

    return RunStatus(
        run_id=run_id,
        status=info.get("status", "UNKNOWN"),
        dataset_id=info.get("datasetId")
    )

@app.post("/apify/webhook")
async def apify_webhook(request: Request):
    """
    Endpoint that Apify calls on run events
    """
    secret = request.headers.get("X-Webhook-Secret")
    if secret != APIFY_WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="Invalid webhook secret")
    
    body = await request.json()

    # Apify default webhook body has an `eventData` object
    event = body.get("eventData", {})
    run_id = event.get("id")
    status = event.get("status")
    dataset_id = event.get("defaultDatasetId")

    if not run_id or not status:
        raise HTTPException(status_code=400, detail="Malformed webhook payload")
    
    # Update in-memory store
    if run_id not in RUNS:
        RUNS[run_id] = {}
    RUNS[run_id]["status"] = status
    if dataset_id:
        RUNS[run_id]["datasetId"] = dataset_id
    
    print(f"Webhook received for run {run_id}: {status} (dataset={dataset_id})")

    return {"ok": True}

@app.post("/runs/{run_id}/export")
async def export_run(run_id: str, request: Request):
    """
    Manually trigger export for a specific run.
    Fetches dataset, stores in DB, and exports to CSV.
    """

    # Check if we have the run in memory
    run_info = RUNS.get(run_id)

    # If not in memory or no dataset_id, fetch from Apify API
    if not run_info or not run_info.get("datasetId"):
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.get(
                    f"https://api.apify.com/v2/actor-runs/{run_id}",
                    headers={"Authorization": f"Bearer {APIFY_TOKEN}"}
                )
                resp.raise_for_status()
                data = resp.json()["data"]

                run_info = {
                    "status": data["status"],
                    "datasetId": data.get("defaultDatasetId")
                }
                RUNS[run_id] = run_info
        except httpx.HTTPError as e:
            raise HTTPException(status_code=502, detail=f"Apify API error: {str(e)}")
        except KeyError:
            raise HTTPException(status_code=404, detail="Run not found")
        
    # Check if run succeeded
    if run_info["status"] != "SUCCEEDED":
        raise HTTPException(
            status_code=400,
            detail=f"Run status is {run_info['status']}, must be SUCCEEDED to export"
        )
    
    dataset_id = run_info.get("datasetId")
    if not dataset_id:
        raise HTTPException(status_code=400, detail="Run has no dataset")
    
    # Import and use processor
    try:
        result = await fetch_and_export_run(
            run_id=run_id,
            dataset_id=dataset_id,
            output_folder="exports",
            db_pool=request.app.state.db_pool,
            store_in_db=True,
            filter_fast_food=True
        )
        if result["success"]:
            return result
        else:
            raise HTTPException(status_code=500, detail=result.get("error"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")