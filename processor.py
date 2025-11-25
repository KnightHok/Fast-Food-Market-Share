"""
Used by both the FastAPI endpoint and scheduled script.
"""
import os
import csv
import json
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
import httpx
import asyncpg
from dotenv import load_dotenv

load_dotenv(".env")

# Config
APIFY_TOKEN = os.getenv("APIFY_TOKEN")
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")

async def fetch_dataset_from_apify(dataset_id: str) -> List[Dict[str, Any]]:
    """
    Fetches dataset items from Apify API

    Args:
        dataset_id: The Apify dataset ID
        
    Returns:
        List of items from the dataset
    """
    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.get(
            f"https://api.apify.com/v2/datasets/{dataset_id}/items",
            headers={
                "Authorization": f"Bearer {APIFY_TOKEN}"
            }
        )
        resp.raise_for_status()
        return resp.json()

async def store_in_database(items: List[Dict[str, Any]], run_id: str, db_pool) -> Dict[str, int]:
    """
    Stores items in Supabase database.
    
    Args:
        items: List of Yelp business items
        run_id: The Apify run ID
        db_pool: asyncpg connection pool
        
    Returns:
        Dict with counts: {"inserted": int, "updated": int, "errors": int}
    """

    if not db_pool:
        raise ValueError("Database pool not provided")
    
    inserted = 0
    updated = 0
    errors = 0

    async with db_pool.acquire() as conn:
        for item in items:
            try:
                # Extract nested address object
                address = item.get("address", {})
                address_line1 = address.get("addressLine1", "")
                address_line2 = address.get("addressLine2", "")
                city = address.get("city", "")
                country = address.get("country", "")
                postal_code = address.get("postalCode", "")
                region_code = address.get("regionCode", "")

                # Extract categories list
                categories = item.get("categories", [])
                category_1 = categories[0] if len(categories) > 0 else ""
                category_2 = categories[1] if len(categories) > 1 else ""
                category_3 = categories[2] if len(categories) > 2 else ""

                # Extract year established from nested object
                about_business = item.get("aboutTheBusiness", {})
                year_established = about_business.get("yearEstablished")

                result = await conn.execute("""
                    INSERT INTO yelp_businesses (
                        run_id, biz_id, name, year_established, aggregated_rating,
                        address_line1, address_line2, city, country, postal_code, region_code,
                        category_1, category_2, category_3, health_score, cuisine, direct_url,
                        created_at
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, NOW()
                    )
                    ON CONFLICT (biz_id)
                    DO UPDATE SET
                        run_id = EXCLUDED.run_id,
                        aggregated_rating = EXCLUDED.aggregated_rating,
                        year_established = EXCLUDED.year_established,
                        address_line1 = EXCLUDED.address_line1,
                        address_line2 = EXCLUDED.address_line2,
                        city = EXCLUDED.city,
                        country = EXCLUDED.country,
                        postal_code = EXCLUDED.postal_code,
                        region_code = EXCLUDED.region_code,
                        category_1 = EXCLUDED.category_1,
                        category_2 = EXCLUDED.category_2,
                        category_3 = EXCLUDED.category_3,
                        health_score = EXCLUDED.health_score,
                        cuisine = EXCLUDED.cuisine,
                        direct_url = EXCLUDED.direct_url,
                        created_at = NOW()
                    RETURNING (xmax = 0) AS inserted
                """,
                run_id,
                item.get("bizId"),
                item.get("name"),
                int(year_established) if year_established else -1,
                item.get("aggregatedRating"),
                address_line1,
                address_line2,
                city,
                country,
                postal_code,
                region_code,
                category_1,
                category_2,
                category_3,
                item.get("healthScore"),
                item.get("cuisine"),
                item.get("directUrl")
                )

                if result == "INSERT 0 1":
                    inserted += 1
                else:
                    updated += 1
            except Exception as e:
                errors += 1
                print(f"Error storing item {item.get('name', 'unknown')}: {e}")
        return {"inserted": inserted, "updated": updated, "errors": errors}


def filter_fast_food_only(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Filters items to only include Fast Food restaurants.
    Checks all 3 category positions (matches your Transform.ipynb logic).

    Args:
        items: Raw Yelp items from Apify

    Returns:
        Filtered list containing only fast food restaurants
    """
    filtered = []

    for item in items:
        categories = item.get("categories", [])

        # Check if "Fast Food" appears in any category position
        is_fast_food = any("Fast Food" in str(cat) for cat in categories if cat)

        if is_fast_food:
            filtered.append(item)

    return filtered


def transform_yelp_data(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Transforms raw Yelp data for CSV export.
    Only includes the specified columns.

    Args:
        items: Raw Yelp items from Apify

    Returns:
        Transformed/cleaned data ready for export
    """
    transformed = []

    for item in items:
        # Extract nested address object
        address = item.get("address", {})

        # Extract categories list
        categories = item.get("categories", [])

        # Extract year established from nested object
        about_business = item.get("aboutTheBusiness", {})

        transformed.append({
            "bizId": item.get("bizId", ""),
            "name": item.get("name", ""),
            "yearEstablished": about_business.get("yearEstablished", ""),
            "aggregatedRating": item.get("aggregatedRating", ""),
            "addressLine1": address.get("addressLine1", ""),
            "addressLine2": address.get("addressLine2", ""),
            "city": address.get("city", ""),
            "country": address.get("country", ""),
            "postalCode": address.get("postalCode", ""),
            "regionCode": address.get("regionCode", ""),
            "category1": categories[0] if len(categories) > 0 else "",
            "category2": categories[1] if len(categories) > 1 else "",
            "category3": categories[2] if len(categories) > 2 else "",
            "healthScore": item.get("healthScore", ""),
            "cuisine": item.get("cuisine", ""),
            "directUrl": item.get("directUrl", ""),
        })

    return transformed
    
def export_to_csv(data: List[Dict[str, Any]], output_path: str) -> None:
    """
    Exports data to CSV file.
    
    Args:
        data: List of dictionaries to export
        output_path: Path to output CSV file
    """
    if not data:
        print("No data to export")
        return
        
    # Ensure directory exists
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    with open(output_path, "w", newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    print(f"✓ Exported {len(data)} rows to {output_path}")

async def fetch_and_export_run(
        run_id: str,
        dataset_id: str,
        output_folder: str = "exports",
        db_pool: Optional[asyncpg.Pool] = None,
        store_in_db: bool = True,
        filter_fast_food: bool = True
) -> Dict[str, Any]:
    """
    Complete workflow: Fetch from Apify → Store in DB → Filter → Transform → Export CSV

    Args:
        run_id: Apify run ID
        dataset_id: Apify dataset ID
        output_folder: Folder to save CSV exports
        db_pool: Database connection pool (optional)
        store_in_db: Whether to store in database
        filter_fast_food: Whether to filter for only fast food restaurants (default: True)

    Returns:
        Dict with operation results
    """
    try:
        # Fetch from Apify
        print(f"Fetching dataset {dataset_id}...")
        items = await fetch_dataset_from_apify(dataset_id=dataset_id)
        print(f"✓ Fetched {len(items)} items")

        if not items:
            return {"success": False, "error": "No items in dataset"}

        # Store ALL data in database (unfiltered - for future use)
        db_stats = None
        if store_in_db and db_pool:
            print(f"Storing in database...")
            db_stats = await store_in_database(items, run_id, db_pool)
            print(f"✓ Database: {db_stats['inserted']} inserted, {db_stats['updated']} updated")

        # Filter for fast food restaurants (for CSV export only)
        items_for_export = items
        if filter_fast_food:
            print("Filtering for fast food restaurants...")
            items_for_export = filter_fast_food_only(items)
            print(f"✓ Filtered to {len(items_for_export)} fast food restaurants (from {len(items)} total)")

        # Transform data
        print("Transforming data...")
        transformed = transform_yelp_data(items_for_export)

        # Export to CSV
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        output_path = os.path.join(output_folder, f"run_{run_id}_{timestamp}.csv")
        export_to_csv(transformed, output_path)

        return {
            "success": True,
            "run_id": run_id,
            "dataset_id": dataset_id,
            "output_path": output_path,
            "total_items": len(items),
            "filtered_items": len(items_for_export) if filter_fast_food else len(items),
            "db_stats": db_stats,
        }
    except Exception as e:
        error_msg = f"Error processing run {run_id}: {str(e)}"
        print(f"✗ {error_msg}")
        return {"success": False, "error": error_msg}

                

