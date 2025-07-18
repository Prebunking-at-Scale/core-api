import asyncio
import os
import sys
from urllib.parse import urljoin

import click
import httpx
from dotenv import load_dotenv

from core.app import pool_factory, postgres_url
from core.videos.claims.repo import ClaimRepository


load_dotenv()


async def fetch_claims(limit: int = 400):
    """Fetch the first 400 claims from the database."""
    pool = pool_factory(postgres_url)
    await pool.open()
    try:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                repo = ClaimRepository(cur)
                claims, _ = await repo.get_all_claims(limit=limit, offset=0)
                return claims
    finally:
        await pool.close()


async def initialize_dashboard(claims):
    """Send claims to the narratives dashboard API."""
    narratives_base_endpoint = os.environ.get("NARRATIVES_BASE_ENDPOINT")
    narratives_api_key = os.environ.get("NARRATIVES_API_KEY")
    app_base_url = os.environ.get("APP_BASE_URL")
    
    if not narratives_base_endpoint or not narratives_api_key or not app_base_url:
        click.echo("Error: NARRATIVES_BASE_ENDPOINT, NARRATIVES_API_KEY, and APP_BASE_URL must be set", err=True)
        sys.exit(1)
    
    # Generate the absolute API endpoint URLs with placeholders
    claim_api_url = urljoin(app_base_url, "/api/videos/{video_id}/claims/{claim_id}")
    narratives_api_url = urljoin(app_base_url, "/api/narratives")
    
    # Prepare the request payload
    payload = {
        "claims": [
            {
                "claim": claim.claim,
                "id": str(claim.id),
                "video_id": str(claim.video_id) if claim.video_id else None
            }
            for claim in claims
        ],
        "narratives_api_url": narratives_api_url,
        "claim_api_url": claim_api_url
    }
    
    # Send POST request to initialize-dashboard endpoint
    url = urljoin(narratives_base_endpoint, "/initialize-dashboard")
    headers = {
        "X-API-TOKEN": narratives_api_key,
        "Content-Type": "application/json"
    }
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(url, json=payload, headers=headers, timeout=60.0)
            response.raise_for_status()
            click.echo(f"Successfully initialized dashboard with {len(claims)} claims")
            return response.json()
        except httpx.HTTPStatusError as e:
            click.echo(f"Error: HTTP {e.response.status_code} - {e.response.text}", err=True)
            sys.exit(1)
        except Exception as e:
            click.echo(f"Error: {e}", err=True)
            sys.exit(1)


@click.command()
@click.argument('num_claims', type=int, default=400)
def start_narratives(num_claims):
    """Extract claims from the database and initialize the narratives dashboard.
    
    NUM_CLAIMS: Number of claims to fetch (default: 400)
    """
    async def main():
        click.echo(f"Fetching {num_claims} claims from database...")
        claims = await fetch_claims(num_claims)
        click.echo(f"Found {len(claims)} claims")
        
        if not claims:
            click.echo("No claims found in database", err=True)
            return
        
        click.echo("Initializing narratives dashboard...")
        result = await initialize_dashboard(claims)
        click.echo("Dashboard initialization complete!")
    
    asyncio.run(main())


if __name__ == "__main__":
    start_narratives()