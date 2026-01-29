"""
Odoo JSON-RPC functions and upsert operations.
"""
import requests

from config import (
    ODOO_URL, ODOO_DB, ODOO_USER, ODOO_KEY,
    PIPELINE_MAP, STAGE_MAP, STATUS_STAGE_MAP, OWNER_MAP,
    GERMANY_USER_IDS, PD_LANG_FIELD_KEY
)
from db import mapping_get, mapping_set
from helpers import map_lang_to_odoo, normalize_probability
from pipedrive import pd_get, pd_val, pd_owner_id, owner_allowed


# ---- Odoo JSON-RPC Core ----
def odoo_rpc(payload: dict):
    """Execute JSON-RPC call to Odoo."""
    r = requests.post(f"{ODOO_URL}/jsonrpc", json=payload, timeout=30)
    r.raise_for_status()
    data = r.json()
    if data.get("error"):
        raise RuntimeError(data["error"])
    return data["result"]


def odoo_login() -> int:
    """Login to Odoo and return UID."""
    uid = odoo_rpc({
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "service": "common",
            "method": "login",
            "args": [ODOO_DB, ODOO_USER, ODOO_KEY]
        },
        "id": 1
    })
    if not isinstance(uid, int) or uid <= 0:
        raise RuntimeError("Odoo login failed (uid invalid). Check DB/USER/API_KEY.")
    return uid


def odoo_execute(uid: int, model: str, method: str, args=None, kwargs=None):
    """Execute Odoo model method."""
    args = args or []
    kwargs = kwargs or {}
    return odoo_rpc({
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "service": "object",
            "method": "execute_kw",
            "args": [ODOO_DB, uid, ODOO_KEY, model, method, args, kwargs],
        },
        "id": 1
    })


def odoo_search(uid: int, model: str, domain, limit=1):
    """Search Odoo model."""
    return odoo_execute(uid, model, "search", args=[domain], kwargs={"limit": limit})


def odoo_search_read(uid: int, model: str, domain, fields=None, limit=1):
    """Search and read Odoo model."""
    fields = fields or ["id"]
    return odoo_execute(
        uid, model, "search_read",
        args=[domain],
        kwargs={"fields": fields, "limit": limit}
    )


def odoo_create(uid: int, model: str, vals: dict):
    """Create Odoo record."""
    return odoo_execute(uid, model, "create", args=[vals])


def odoo_write(uid: int, model: str, record_id: int, vals: dict):
    """Update Odoo record."""
    return odoo_execute(uid, model, "write", args=[[record_id], vals])


# ---- Deal Matching ----
def find_existing_deal_in_odoo(uid: int, title: str, partner_id: int | None, team_id: int | None):
    """
    Try to find an existing Odoo crm.lead that matches this Pipedrive deal.
    """
    base_filters = [("type", "=", "opportunity"), ("active", "in", [True, False])]

    # 1) name + partner (best)
    if partner_id:
        domain = base_filters + [("name", "=", title), ("partner_id", "=", partner_id)]
        if team_id:
            domain.append(("team_id", "=", team_id))

        rows = odoo_search_read(uid, "crm.lead", domain, fields=["id"], limit=1)
        if rows:
            return rows[0]["id"]

    # 2) name only, but only if it's unique
    domain = base_filters + [("name", "=", title)]
    if team_id:
        domain.append(("team_id", "=", team_id))

    rows = odoo_search_read(uid, "crm.lead", domain, fields=["id"], limit=2)
    if len(rows) == 1:
        return rows[0]["id"]

    return None


# ---- Archive/Delete ----
def archive_deal_in_odoo(uid: int, pipedrive_deal_id: int) -> bool:
    """Archive a deal in Odoo when deleted in Pipedrive."""
    mapped = mapping_get("deal", pipedrive_deal_id)
    if not mapped:
        print(f"DELETE: No mapping found for deal {pipedrive_deal_id}, nothing to archive")
        return False

    odoo_write(uid, "crm.lead", mapped, {"active": False})
    print(f"DELETE: Archived Odoo crm.lead {mapped} for Pipedrive deal {pipedrive_deal_id}")
    return True


# ---- Upsert Operations ----
def upsert_org(uid: int, org_id: int) -> int:
    """Upsert organization from Pipedrive to Odoo."""
    org = pd_get(f"/organizations/{org_id}")

    org_owner_id = pd_owner_id(org)
    if not owner_allowed(org_owner_id):
        print(f"SKIP org {org_id}: owner {org_owner_id} not in Germany team")
        return -1

    name = org.get("name") or f"Org {org_id}"
    mapped = mapping_get("org", org_id)

    vals = {"name": name, "is_company": True}

    website = org.get("website")
    if website:
        vals["website"] = website

    pd_lang = org.get(PD_LANG_FIELD_KEY)
    odoo_lang = map_lang_to_odoo(pd_lang)
    if odoo_lang:
        vals["lang"] = odoo_lang

    if mapped:
        odoo_write(uid, "res.partner", mapped, vals)
        return mapped

    found = odoo_search(uid, "res.partner", [("name", "=", name), ("is_company", "=", True)], limit=1)
    if found:
        odoo_id = found[0]
        odoo_write(uid, "res.partner", odoo_id, vals)
    else:
        odoo_id = odoo_create(uid, "res.partner", vals)

    mapping_set("org", org_id, odoo_id)
    return odoo_id


def upsert_person(uid: int, person_id: int) -> int:
    """Upsert person from Pipedrive to Odoo."""
    p = pd_get(f"/persons/{person_id}")

    person_owner_id = pd_owner_id(p)
    if not owner_allowed(person_owner_id):
        print(f"SKIP person {person_id}: owner {person_owner_id} not in Germany team")
        return -1

    name = p.get("name") or f"Person {person_id}"

    org_id = pd_val(p.get("org_id"))
    parent_id = upsert_org(uid, int(org_id)) if org_id else None
    if parent_id == -1:
        parent_id = None

    email = None
    if isinstance(p.get("email"), list) and p["email"]:
        email = p["email"][0].get("value")

    phone = None
    if isinstance(p.get("phone"), list) and p["phone"]:
        phone = p["phone"][0].get("value")

    mapped = mapping_get("person", person_id)

    vals = {"name": name, "parent_id": parent_id, "email": email, "phone": phone}

    job_title = p.get("job_title")
    if job_title:
        vals["function"] = job_title

    pd_lang = p.get(PD_LANG_FIELD_KEY)
    odoo_lang = map_lang_to_odoo(pd_lang)
    if odoo_lang:
        vals["lang"] = odoo_lang

    if mapped:
        odoo_write(uid, "res.partner", mapped, vals)
        return mapped

    if email:
        found = odoo_search(uid, "res.partner", [("email", "=", email)], limit=1)
        if found:
            odoo_id = found[0]
            odoo_write(uid, "res.partner", odoo_id, vals)
            mapping_set("person", person_id, odoo_id)
            return odoo_id

    odoo_id = odoo_create(uid, "res.partner", vals)
    mapping_set("person", person_id, odoo_id)
    return odoo_id


def upsert_deal(uid: int, deal_id: int) -> int:
    """Upsert deal from Pipedrive to Odoo."""
    d = pd_get(f"/deals/{deal_id}")
    title = d.get("title") or f"Deal {deal_id}"

    if d.get("status") == "deleted":
        print(f"SKIP deal {deal_id}: status=deleted")
        return -1

    pd_pipeline_id = d.get("pipeline_id")
    if pd_pipeline_id is None or int(pd_pipeline_id) not in PIPELINE_MAP:
        print(f"SKIP deal {deal_id}: pipeline_id={pd_pipeline_id} not allowed")
        return -1
    odoo_team_id = PIPELINE_MAP.get(int(pd_pipeline_id))

    pd_owner = d.get("user_id")
    owner_id = pd_owner.get("id") if isinstance(pd_owner, dict) else pd_owner

    if GERMANY_USER_IDS and owner_id not in GERMANY_USER_IDS:
        print(f"SKIP deal {deal_id}: owner {owner_id} not in Germany team")
        return -1

    odoo_user_id = OWNER_MAP.get(int(owner_id)) if owner_id is not None else None

    person_id = pd_val(d.get("person_id"))
    org_id = pd_val(d.get("org_id"))

    partner_id = None
    if person_id:
        partner_id = upsert_person(uid, int(person_id))
        if partner_id == -1:
            partner_id = None
    elif org_id:
        partner_id = upsert_org(uid, int(org_id))
        if partner_id == -1:
            partner_id = None

    value = float(d.get("value") or 0.0)

    status = d.get("status")
    odoo_stage_id = None
    if status in STATUS_STAGE_MAP:
        odoo_stage_id = STATUS_STAGE_MAP[status]
    else:
        pd_stage_id = d.get("stage_id")
        if pd_stage_id is not None:
            odoo_stage_id = STAGE_MAP.get(int(pd_stage_id))

    mapped = mapping_get("deal", deal_id)

    vals = {
        "name": title,
        "type": "opportunity",
        "partner_id": partner_id,
        "expected_revenue": value,
        "team_id": odoo_team_id,
    }

    prob = normalize_probability(d.get("probability"))
    if prob is not None:
        vals["probability"] = prob

    expected_close = d.get("expected_close_date")
    if expected_close:
        vals["date_deadline"] = expected_close

    if odoo_user_id:
        vals["user_id"] = odoo_user_id

    if odoo_stage_id:
        vals["stage_id"] = odoo_stage_id

    if mapped:
        odoo_write(uid, "crm.lead", mapped, vals)
        return mapped

    existing = find_existing_deal_in_odoo(uid, title=title, partner_id=partner_id, team_id=odoo_team_id)
    if existing:
        odoo_write(uid, "crm.lead", existing, vals)
        mapping_set("deal", deal_id, existing)
        print(f"LINK deal {deal_id}: found existing Odoo crm.lead {existing}, updated + mapped")
        return existing

    odoo_id = odoo_create(uid, "crm.lead", vals)
    mapping_set("deal", deal_id, odoo_id)
    return odoo_id
