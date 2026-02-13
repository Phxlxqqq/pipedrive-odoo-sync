"""
Better Proposals API client.
Fetches proposal data (products, prices, taxes) for syncing to Pipedrive.
"""
import requests
from config import BP_API_KEY, BP_BASE


def bp_headers():
    """Return authentication headers for Better Proposals API."""
    return {
        "Bptoken": BP_API_KEY,
        "Content-Type": "application/json"
    }


def bp_get_proposal(proposal_id: str) -> dict:
    """Fetch full proposal details including line items."""
    r = requests.get(
        f"{BP_BASE}/proposal/{proposal_id}",
        headers=bp_headers(),
        timeout=30
    )
    if not r.ok:
        print(f"BP ERROR: {r.status_code} - {r.text}")
    r.raise_for_status()
    result = r.json()
    if result.get("status") == "error":
        raise Exception(f"BP API error: {result}")
    return result.get("data", {})


def bp_get_signed_proposals() -> list:
    """Fetch all signed proposals."""
    r = requests.get(
        f"{BP_BASE}/proposal/signed",
        headers=bp_headers(),
        timeout=30
    )
    r.raise_for_status()
    result = r.json()
    return result.get("data", [])


# ---- TODO: Implement when API token is available ----
# - Parse proposal line items (products, quantities, prices, tax)
# - Map BP proposal to Pipedrive deal
# - Create Pipedrive products from BP line items
