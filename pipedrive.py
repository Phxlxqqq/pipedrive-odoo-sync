"""
Pipedrive API functions.
"""
import requests
from config import PIPEDRIVE_BASE, PIPEDRIVE_TOKEN, GERMANY_USER_IDS


def pd_get(path: str):
    """GET request to Pipedrive API."""
    r = requests.get(
        f"{PIPEDRIVE_BASE}{path}",
        params={"api_token": PIPEDRIVE_TOKEN},
        timeout=30
    )
    r.raise_for_status()
    js = r.json()
    if not js.get("success"):
        raise RuntimeError(js)
    return js["data"]


def pd_post(path: str, data: dict):
    """POST request to Pipedrive API."""
    r = requests.post(
        f"{PIPEDRIVE_BASE}{path}",
        params={"api_token": PIPEDRIVE_TOKEN},
        json=data,
        timeout=30
    )
    r.raise_for_status()
    js = r.json()
    if not js.get("success"):
        raise RuntimeError(js)
    return js["data"]


def pd_put(path: str, data: dict):
    """PUT request to Pipedrive API."""
    r = requests.put(
        f"{PIPEDRIVE_BASE}{path}",
        params={"api_token": PIPEDRIVE_TOKEN},
        json=data,
        timeout=30
    )
    r.raise_for_status()
    js = r.json()
    if not js.get("success"):
        raise RuntimeError(js)
    return js["data"]


def pd_val(field):
    """Extract value from Pipedrive field (handles dict with 'value' key)."""
    if isinstance(field, dict) and "value" in field:
        return field["value"]
    return field


def pd_owner_id(obj: dict):
    """
    Extract owner ID from Pipedrive object.
    Handles both owner_id and user_id fields.
    """
    if not isinstance(obj, dict):
        return None

    owner = obj.get("owner_id")
    if isinstance(owner, dict):
        return owner.get("id")
    if isinstance(owner, int):
        return owner

    owner = obj.get("user_id")
    if isinstance(owner, dict):
        return owner.get("id")
    if isinstance(owner, int):
        return owner

    return None


def owner_allowed(owner_id: int | None) -> bool:
    """Check if owner is in Germany team (or all allowed if no filter)."""
    if not GERMANY_USER_IDS:
        return True
    if owner_id is None:
        return False
    return int(owner_id) in GERMANY_USER_IDS


# ---- Person Operations ----
def pd_create_person(name: str, org_id: int = None, email: str = None,
                     phone: str = None, owner_id: int = None,
                     job_title: str = None) -> dict:
    """Create a new person in Pipedrive."""
    data = {"name": name}

    if org_id:
        data["org_id"] = org_id
    if email:
        data["email"] = [{"value": email, "primary": True, "label": "work"}]
    if phone:
        data["phone"] = [{"value": phone, "primary": True, "label": "mobile"}]
    if owner_id:
        data["owner_id"] = owner_id
    if job_title:
        data["job_title"] = job_title

    return pd_post("/persons", data)


def pd_update_person(person_id: int, email: str = None,
                     phone: str = None, job_title: str = None) -> dict | None:
    """Update an existing person in Pipedrive."""
    data = {}

    if email:
        data["email"] = [{"value": email, "primary": True, "label": "work"}]
    if phone:
        data["phone"] = [{"value": phone, "primary": True, "label": "mobile"}]
    if job_title:
        data["job_title"] = job_title

    if data:
        return pd_put(f"/persons/{person_id}", data)
    return None


# ---- Deal Operations ----
def pd_link_person_to_deal(deal_id: int, person_id: int) -> dict:
    """Link a person to a deal in Pipedrive."""
    return pd_put(f"/deals/{deal_id}", {"person_id": person_id})


# ---- Organization Operations ----
def pd_update_org(org_id: int, website: str = None) -> dict | None:
    """Update an organization in Pipedrive (e.g., set website)."""
    data = {}
    if website:
        if not website.startswith("http"):
            website = f"https://{website}"
        data["website"] = website

    if data:
        return pd_put(f"/organizations/{org_id}", data)
    return None


# ---- Notes ----
def pd_add_note_to_deal(deal_id: int, content: str) -> dict:
    """Add a note to a deal in Pipedrive."""
    return pd_post("/notes", {
        "deal_id": deal_id,
        "content": content
    })
