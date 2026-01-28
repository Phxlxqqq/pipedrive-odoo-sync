import os
import sqlite3
import requests
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from dotenv import load_dotenv

load_dotenv()

PIPEDRIVE_TOKEN = os.getenv("PIPEDRIVE_TOKEN")
ODOO_URL = os.getenv("ODOO_URL")
ODOO_DB = os.getenv("ODOO_DB")
ODOO_USER = os.getenv("ODOO_USER")
ODOO_KEY = os.getenv("ODOO_API_KEY")
WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN")

PIPEDRIVE_BASE = "https://api.pipedrive.com/v1"
DB_PATH = os.getenv("SYNC_DB", "sync.db")

# Only sync deals whose owner is in Germany team
GERMANY_USER_IDS = set(
    int(x.strip()) for x in os.getenv("GERMANY_USER_IDS", "").split(",") if x.strip()
)

# ---- Surfe API Configuration ----
SURFE_API_KEY = os.getenv("SURFE_API_KEY")
SURFE_WEBHOOK_URL = os.getenv("SURFE_WEBHOOK_URL")
SURFE_WEBHOOK_TOKEN = os.getenv("SURFE_WEBHOOK_TOKEN")
SURFE_BASE = "https://api.surfe.com/v2"

# Stage IDs for Surfe triggers
DOWNLOAD_STAGE_ID = int(os.getenv("SURFE_DOWNLOAD_STAGE_ID", "37"))
LEADFEEDER_STAGE_ID = int(os.getenv("SURFE_LEADFEEDER_STAGE_ID", "68"))

# ICP Job Titles for Leadfeeder search (priority order)
ICP_JOB_TITLES = [t.strip() for t in os.getenv(
    "ICP_JOB_TITLES",
    "CISO,Chief Information Security Officer,Compliance Manager,IT Manager,IT Director,CTO,Chief Technology Officer,CEO,Chief Executive Officer,Founder,Geschäftsführer"
).split(",")]

# ---- Pipeline Allowlist + Odoo Team Mapping ----
# Only deals in these Pipedrive pipelines will be synced to Odoo.
# Value maps pipedrive pipeline_id -> odoo crm.team id (team_id).
# Note: Pipeline 6 is NOT synced to Odoo - it's only for Surfe triggers
PIPELINE_MAP = {
    4: 1,   # Original pipeline -> Odoo Team 1
    # Pipeline 6 is intentionally NOT included - Surfe-only, no Odoo sync
}

# Pipelines where Germany team filter is NOT applied (all owners allowed)
SURFE_ONLY_PIPELINES = {6}

# ---- Stage mapping ----
# pipedrive stage_id -> odoo crm.stage id (stage_id)
STAGE_MAP = {
    65: 3,
    25: 3,
    26: 5,
    27: 5,
    28: 7,
}

# Deal status mapping (status = open/won/lost/deleted)
# won/lost are NOT stage_ids - they are deal status
STATUS_STAGE_MAP = {
    "won": 7,
    "lost": 9,
}

# ---- Owner mapping ----
# pipedrive user_id -> odoo res.users id (user_id)
OWNER_MAP = {
    24183342: 39,  # Philipp
    23265106: 24,  # Hinnerk
    23570355: 7,   # Thomas
}

# ---- Pipedrive custom field keys (hardcoded; no .env change needed) ----
# Your language custom field key from Pipedrive
PD_LANG_FIELD_KEY = "0a6493b05167a35971de14baa3b6e2b0175c11a7"

app = FastAPI()


# ---------------- Helpers: Pipedrive -> Odoo ----------------
def map_lang_to_odoo(pd_lang_value):
    """
    Map Pipedrive language/custom value to Odoo lang codes.
    Adjust mapping if your Pipedrive stores other values.
    """
    if pd_lang_value is None:
        return None

    v = str(pd_lang_value).strip()
    if not v:
        return None

    vl = v.lower()

    # Common human values
    if vl in ("de", "deutsch", "german", "de_de", "de-de"):
        return "de_DE"
    if vl in ("en", "englisch", "english", "en_us", "en-us", "en_gb", "en-gb"):
        # Pick one (must exist in your Odoo languages)
        return "en_US"

    # If Pipedrive already stores Odoo lang codes, pass through (e.g. de_DE)
    if len(v) == 5 and v[2] == "_" and v[:2].isalpha() and v[3:].isalpha():
        return v

    return None


def normalize_probability(pd_prob):
    """
    Odoo expects 0..100 for probability.
    Pipedrive is usually 0..100 but some setups use 0..1.
    """
    if pd_prob is None:
        return None
    try:
        prob = float(pd_prob)
    except Exception:
        return None

    if 0.0 <= prob <= 1.0:
        prob *= 100.0
    # clamp (just in case)
    if prob < 0:
        prob = 0.0
    if prob > 100:
        prob = 100.0
    return prob


# ---------------- DB ----------------
def get_con():
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS mapping (
            object_type TEXT NOT NULL,
            external_id TEXT NOT NULL,
            odoo_id INTEGER NOT NULL,
            PRIMARY KEY(object_type, external_id)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS events (
            event_key TEXT PRIMARY KEY,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS surfe_enrichments (
            enrichment_id TEXT PRIMARY KEY,
            pipedrive_deal_id INTEGER NOT NULL,
            pipedrive_person_id INTEGER,
            enrichment_type TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            pending_person_data TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    return con


def mapping_get(obj_type: str, external_id: str):
    con = get_con()
    row = con.execute(
        "SELECT odoo_id FROM mapping WHERE object_type=? AND external_id=?",
        (obj_type, str(external_id))
    ).fetchone()
    con.close()
    return row[0] if row else None


def mapping_set(obj_type: str, external_id: str, odoo_id: int):
    con = get_con()
    con.execute(
        "INSERT OR REPLACE INTO mapping(object_type, external_id, odoo_id) VALUES(?,?,?)",
        (obj_type, str(external_id), int(odoo_id))
    )
    con.commit()
    con.close()


def event_seen(event_key: str) -> bool:
    con = get_con()
    try:
        con.execute("INSERT INTO events(event_key) VALUES(?)", (event_key,))
        con.commit()
        return False
    except sqlite3.IntegrityError:
        return True
    finally:
        con.close()


# ---------------- Surfe Enrichment Tracking ----------------
def save_enrichment(enrichment_id: str, deal_id: int, person_id: int = None, enrichment_type: str = "download",
                    pending_person_data: dict = None):
    """Save enrichment request for later matching when webhook callback arrives.

    For leadfeeder type, pending_person_data contains the person info to create when enrichment completes:
    - name, org_id, owner_id, job_title, linkedin_url
    """
    con = get_con()

    # Store pending person data as JSON if provided
    import json
    pending_json = json.dumps(pending_person_data) if pending_person_data else None

    con.execute("""
        INSERT OR REPLACE INTO surfe_enrichments
        (enrichment_id, pipedrive_deal_id, pipedrive_person_id, enrichment_type, pending_person_data)
        VALUES (?, ?, ?, ?, ?)
    """, (enrichment_id, deal_id, person_id, enrichment_type, pending_json))
    con.commit()
    con.close()


def get_enrichment(enrichment_id: str) -> dict | None:
    """Get enrichment data by Surfe enrichment ID."""
    import json
    con = get_con()
    row = con.execute("""
        SELECT enrichment_id, pipedrive_deal_id, pipedrive_person_id,
               enrichment_type, status, pending_person_data
        FROM surfe_enrichments WHERE enrichment_id = ?
    """, (enrichment_id,)).fetchone()
    con.close()

    if row:
        pending_data = None
        if row[5]:
            try:
                pending_data = json.loads(row[5])
            except:
                pass
        return {
            "enrichment_id": row[0],
            "deal_id": row[1],
            "person_id": row[2],
            "type": row[3],
            "status": row[4],
            "pending_person_data": pending_data
        }
    return None


def complete_enrichment(enrichment_id: str):
    """Mark enrichment as completed."""
    con = get_con()
    con.execute("""
        UPDATE surfe_enrichments
        SET status = 'completed'
        WHERE enrichment_id = ?
    """, (enrichment_id,))
    con.commit()
    con.close()


# ---------------- SURFE API ----------------
def surfe_headers():
    """Return authentication headers for Surfe API."""
    return {
        "Authorization": f"Bearer {SURFE_API_KEY}",
        "Content-Type": "application/json"
    }


def surfe_enrich_person(first_name: str = None, last_name: str = None,
                        company_domain: str = None, company_name: str = None,
                        email: str = None, linkedin_url: str = None) -> dict:
    """
    Start async Surfe person enrichment.
    Returns dict with enrichmentID for tracking.
    """
    person = {}
    if first_name:
        person["firstName"] = first_name
    if last_name:
        person["lastName"] = last_name
    if company_domain:
        person["companyDomain"] = company_domain
    if company_name:
        person["companyName"] = company_name
    if email:
        person["email"] = email
    if linkedin_url:
        person["linkedinUrl"] = linkedin_url

    payload = {
        "include": {
            "email": True,
            "mobile": True,
            "jobHistory": False,
            "linkedInUrl": True
        },
        "people": [person],
        "notificationOptions": {
            "webhookUrl": SURFE_WEBHOOK_URL
        }
    }

    print(f"SURFE ENRICH REQUEST: {payload}")

    r = requests.post(
        f"{SURFE_BASE}/people/enrich",
        headers=surfe_headers(),
        json=payload,
        timeout=30
    )
    if not r.ok:
        print(f"SURFE ENRICH ERROR: {r.status_code} - {r.text}")
    r.raise_for_status()
    return r.json()


def surfe_search_people(domain: str = None, company_name: str = None, job_titles: list = None, limit: int = 10) -> list:
    """
    Synchronously search for people at a company by domain or company name and job titles.
    Returns list of people found.
    
    Args:
        domain: Company domain (e.g. "example.com"). Either domain or company_name must be provided.
        company_name: Company name (e.g. "Acme Corp"). Used when domain is not available.
        job_titles: List of job titles to filter by
        limit: Maximum number of results
    """
    if not domain and not company_name:
        raise ValueError("Either domain or company_name must be provided")
    
    payload = {
        "companies": {},
        "limit": limit,
        "peoplePerCompany": limit
    }
    
    # Use domain if available, otherwise use company name
    if domain:
        payload["companies"]["domains"] = [domain]
    elif company_name:
        payload["companies"]["names"] = [company_name]
    
    # Add job titles filter if provided
    if job_titles:
        payload["people"] = {
            "jobTitles": job_titles
        }

    r = requests.post(
        f"{SURFE_BASE}/people/search",
        headers=surfe_headers(),
        json=payload,
        timeout=30
    )
    if not r.ok:
        print(f"SURFE SEARCH ERROR: {r.status_code} - {r.text}")
    r.raise_for_status()
    result = r.json()
    return result.get("people", [])


# ---------------- ODOO JSON-RPC ----------------
def odoo_rpc(payload: dict):
    r = requests.post(f"{ODOO_URL}/jsonrpc", json=payload, timeout=30)
    r.raise_for_status()
    data = r.json()
    if data.get("error"):
        raise RuntimeError(data["error"])
    return data["result"]


def odoo_login() -> int:
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
    return odoo_execute(uid, model, "search", args=[domain], kwargs={"limit": limit})


def odoo_search_read(uid: int, model: str, domain, fields=None, limit=1):
    fields = fields or ["id"]
    return odoo_execute(
        uid, model, "search_read",
        args=[domain],
        kwargs={"fields": fields, "limit": limit}
    )


def find_existing_deal_in_odoo(uid: int, title: str, partner_id: int | None, team_id: int | None):
    """
    Try to find an existing Odoo crm.lead that matches this Pipedrive deal,
    even if we don't have a mapping yet.

    Matching strategy:
      1) exact name + partner (+ team if available)
      2) exact name (+ team if available) only if unique (limit=2 check)
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

    # 2) name only, but only if it's unique (avoid overwriting wrong deal)
    domain = base_filters + [("name", "=", title)]
    if team_id:
        domain.append(("team_id", "=", team_id))

    rows = odoo_search_read(uid, "crm.lead", domain, fields=["id"], limit=2)
    if len(rows) == 1:
        return rows[0]["id"]

    return None


def odoo_create(uid: int, model: str, vals: dict):
    return odoo_execute(uid, model, "create", args=[vals])


def odoo_write(uid: int, model: str, record_id: int, vals: dict):
    return odoo_execute(uid, model, "write", args=[[record_id], vals])


# ---------------- PIPEDRIVE ----------------
def pd_get(path: str):
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


def pd_val(field):
    if isinstance(field, dict) and "value" in field:
        return field["value"]
    return field

def pd_owner_id(obj: dict):
    """
    Pipedrive owner is usually in obj["owner_id"] for persons/orgs,
    and in obj["user_id"] for deals (already handled separately in your code).
    Returns int or None.
    """
    if not isinstance(obj, dict):
        return None

    owner = obj.get("owner_id")
    if isinstance(owner, dict):
        return owner.get("id")
    if isinstance(owner, int):
        return owner

    # fallback (some entities may have user_id style)
    owner = obj.get("user_id")
    if isinstance(owner, dict):
        return owner.get("id")
    if isinstance(owner, int):
        return owner

    return None


def owner_allowed(owner_id: int | None) -> bool:
    """
    If GERMANY_USER_IDS is empty -> allow all.
    Otherwise allow only if owner_id in set.
    """
    if not GERMANY_USER_IDS:
        return True
    if owner_id is None:
        return False
    return int(owner_id) in GERMANY_USER_IDS


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


def pd_link_person_to_deal(deal_id: int, person_id: int) -> dict:
    """Link a person to a deal in Pipedrive."""
    return pd_put(f"/deals/{deal_id}", {"person_id": person_id})


def pd_add_note_to_deal(deal_id: int, content: str) -> dict:
    """Add a note to a deal in Pipedrive."""
    return pd_post("/notes", {
        "deal_id": deal_id,
        "content": content
    })


# ---------------- Delete handling ----------------
def archive_deal_in_odoo(uid: int, pipedrive_deal_id: int) -> bool:
    mapped = mapping_get("deal", pipedrive_deal_id)
    if not mapped:
        print(f"DELETE: No mapping found for deal {pipedrive_deal_id}, nothing to archive")
        return False

    # Archive in Odoo (crm.lead has field 'active')
    odoo_write(uid, "crm.lead", mapped, {"active": False})
    print(f"DELETE: Archived Odoo crm.lead {mapped} for Pipedrive deal {pipedrive_deal_id}")
    return True


# ---------------- UPSERTS ----------------
def upsert_org(uid: int, org_id: int) -> int:
    org = pd_get(f"/organizations/{org_id}")
        # ---- Owner / Germany filter (same logic as deals) ----
    org_owner_id = pd_owner_id(org)
    if not owner_allowed(org_owner_id):
        print(f"SKIP org {org_id}: owner {org_owner_id} not in Germany team")
        return -1

    name = org.get("name") or f"Org {org_id}"

    mapped = mapping_get("org", org_id)

    # Keep your working fields, only add website + lang if present
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
    p = pd_get(f"/persons/{person_id}")
        # ---- Owner / Germany filter (same logic as deals) ----
    person_owner_id = pd_owner_id(p)
    if not owner_allowed(person_owner_id):
        print(f"SKIP person {person_id}: owner {person_owner_id} not in Germany team")
        return -1

    name = p.get("name") or f"Person {person_id}"

    org_id = pd_val(p.get("org_id"))
    parent_id = upsert_org(uid, int(org_id)) if org_id else None
    # upsert_org returns -1 if org owner not in Germany team - treat as None for Odoo
    if parent_id == -1:
        parent_id = None

    email = None
    if isinstance(p.get("email"), list) and p["email"]:
        email = p["email"][0].get("value")

    phone = None
    if isinstance(p.get("phone"), list) and p["phone"]:
        phone = p["phone"][0].get("value")

    mapped = mapping_get("person", person_id)

    # Keep working fields, add function + lang only
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
    d = pd_get(f"/deals/{deal_id}")
    title = d.get("title") or f"Deal {deal_id}"

    # Skip deleted status (extra safety)
    if d.get("status") == "deleted":
        print(f"SKIP deal {deal_id}: status=deleted")
        return -1

    # ---- Pipeline allowlist + team mapping ----
    pd_pipeline_id = d.get("pipeline_id")
    if pd_pipeline_id is None or int(pd_pipeline_id) not in PIPELINE_MAP:
        print(f"SKIP deal {deal_id}: pipeline_id={pd_pipeline_id} not allowed")
        return -1
    odoo_team_id = PIPELINE_MAP.get(int(pd_pipeline_id))

    # ---- Owner / Germany filter ----
    pd_owner = d.get("user_id")
    owner_id = pd_owner.get("id") if isinstance(pd_owner, dict) else pd_owner

    if GERMANY_USER_IDS and owner_id not in GERMANY_USER_IDS:
        print(f"SKIP deal {deal_id}: owner {owner_id} not in Germany team")
        return -1

    odoo_user_id = OWNER_MAP.get(int(owner_id)) if owner_id is not None else None

    # ---- Partner (person/org) ----
    person_id = pd_val(d.get("person_id"))
    org_id = pd_val(d.get("org_id"))

    partner_id = None
    if person_id:
        partner_id = upsert_person(uid, int(person_id))
        # upsert_person returns -1 if person owner not in Germany team - treat as None for Odoo
        if partner_id == -1:
            partner_id = None
    elif org_id:
        partner_id = upsert_org(uid, int(org_id))
        # upsert_org returns -1 if org owner not in Germany team - treat as None for Odoo
        if partner_id == -1:
            partner_id = None

    # ---- Value ----
    value = float(d.get("value") or 0.0)

    # ---- Stage mapping (status wins over stage_id) ----
    status = d.get("status")  # open, won, lost, deleted
    odoo_stage_id = None
    if status in STATUS_STAGE_MAP:
        odoo_stage_id = STATUS_STAGE_MAP[status]
    else:
        pd_stage_id = d.get("stage_id")
        if pd_stage_id is not None:
            odoo_stage_id = STAGE_MAP.get(int(pd_stage_id))

    mapped = mapping_get("deal", deal_id)

    # Keep existing working fields; add probability + expected close date mapping
    vals = {
        "name": title,
        "type": "opportunity",
        "partner_id": partner_id,
        "expected_revenue": value,
        "team_id": odoo_team_id,
    }

    # probability
    prob = normalize_probability(d.get("probability"))
    if prob is not None:
        vals["probability"] = prob

    # expected close date -> Odoo close deadline
    expected_close = d.get("expected_close_date")
    if expected_close:
        # Odoo expects YYYY-MM-DD
        vals["date_deadline"] = expected_close

    if odoo_user_id:
        vals["user_id"] = odoo_user_id

    if odoo_stage_id:
        vals["stage_id"] = odoo_stage_id

    # 1) If we already mapped this Pipedrive deal -> update it
    if mapped:
        odoo_write(uid, "crm.lead", mapped, vals)
        return mapped

    # 2) No mapping yet -> try to find an existing deal in Odoo (avoid duplicates)
    existing = find_existing_deal_in_odoo(uid, title=title, partner_id=partner_id, team_id=odoo_team_id)
    if existing:
        odoo_write(uid, "crm.lead", existing, vals)
        mapping_set("deal", deal_id, existing)
        print(f"LINK deal {deal_id}: found existing Odoo crm.lead {existing}, updated + mapped")
        return existing

    # 3) Still nothing -> create new
    odoo_id = odoo_create(uid, "crm.lead", vals)
    mapping_set("deal", deal_id, odoo_id)
    return odoo_id


# ---------------- SURFE STAGE HANDLERS ----------------
def extract_domain_from_website(website: str) -> str | None:
    """Extract clean domain from website URL."""
    if not website:
        return None
    domain = website.strip()
    domain = domain.replace("https://", "").replace("http://", "")
    domain = domain.split("/")[0]
    domain = domain.replace("www.", "")
    return domain if domain else None


def extract_domain_from_email(email: str) -> str | None:
    """Extract domain from email address."""
    if not email or "@" not in email:
        return None
    return email.split("@")[1].lower()


def company_name_matches(person: dict, target_company: str) -> bool:
    """
    Check if person's current company matches the target company.
    Uses fuzzy matching to handle slight variations in company names.
    """
    if not target_company:
        return True  # No filter if no target specified

    target_lower = target_company.lower().strip()
    # Remove common suffixes for comparison
    target_clean = target_lower.replace(" ag", "").replace(" gmbh", "").replace(" inc", "").replace(" ltd", "").replace(" co.", "").strip()

    # Check person's current company
    person_company = (person.get("company") or person.get("companyName") or "").lower().strip()
    person_company_clean = person_company.replace(" ag", "").replace(" gmbh", "").replace(" inc", "").replace(" ltd", "").replace(" co.", "").strip()

    # Exact or substring match
    if target_clean in person_company_clean or person_company_clean in target_clean:
        return True

    # Check if the main word matches (e.g., "KUMAVISION" in "KUMAVISION AG")
    target_words = set(target_clean.split())
    person_words = set(person_company_clean.split())
    if target_words & person_words:  # If there's any common word
        return True

    return False


def select_best_icp_person(people: list, target_company: str = None) -> dict | None:
    """
    Select the best person based on ICP priority.
    Priority: CISO > Compliance Manager > IT Manager > CTO > CEO > Founder
    Filters out people who don't work at the target company.
    """
    # First, filter to only people who work at the target company
    if target_company:
        filtered_people = [p for p in people if company_name_matches(p, target_company)]
        print(f"ICP FILTER: {len(filtered_people)}/{len(people)} people match company '{target_company}'")
        if not filtered_people:
            print(f"ICP FILTER: No people found matching company, showing all candidates:")
            for p in people[:5]:  # Show first 5 for debugging
                print(f"  - {p.get('firstName')} {p.get('lastName')} at {p.get('company') or p.get('companyName')}")
            return None
        people = filtered_people

    priority_keywords = [
        ("ciso", "information security"),
        ("compliance",),
        ("it manager", "it director", "it leiter"),
        ("cto", "chief technology"),
        ("ceo", "chief executive", "geschäftsführer"),
        ("founder", "gründer", "owner", "inhaber"),
    ]

    for keywords in priority_keywords:
        for person in people:
            job_title = (person.get("jobTitle") or "").lower()
            seniorities = [s.lower() for s in person.get("seniorities", [])]

            for keyword in keywords:
                if keyword in job_title or keyword in " ".join(seniorities):
                    return person

    # Fallback: First person with C-Level/VP seniority
    for person in people:
        seniorities = [s.lower() for s in person.get("seniorities", [])]
        if any(s in ["c-level", "vp", "director"] for s in seniorities):
            return person

    # Last fallback: First person
    return people[0] if people else None


def handle_download_stage(deal: dict):
    """
    Scenario 1: Deal created in Download stage (37)
    - Person has email but missing phone number
    - Start Surfe enrichment to get phone, name, company
    - Note: No Odoo sync - Pipeline 6 is Surfe-only
    """
    deal_id = deal.get("id")
    person_id = pd_val(deal.get("person_id"))

    if not person_id:
        print(f"DOWNLOAD: Deal {deal_id} has no person, skip")
        return

    person = pd_get(f"/persons/{person_id}")

    # Extract email
    email = None
    if isinstance(person.get("email"), list) and person["email"]:
        email = person["email"][0].get("value")

    if not email:
        print(f"DOWNLOAD: Person {person_id} has no email, skip")
        return

    # Check if phone is missing
    phone = None
    if isinstance(person.get("phone"), list) and person["phone"]:
        phone = person["phone"][0].get("value")

    if phone and len(phone) > 5:
        print(f"DOWNLOAD: Person {person_id} already has phone, skip")
        return

    # Split name
    name = person.get("name", "")
    name_parts = name.split(" ", 1)
    first_name = name_parts[0] if name_parts else ""
    last_name = name_parts[1] if len(name_parts) > 1 else ""

    # Get company domain from org
    org_id = pd_val(person.get("org_id"))
    company_domain = None
    company_name = None
    if org_id:
        org = pd_get(f"/organizations/{org_id}")
        website = org.get("address") or org.get("cc_email")
        if not website:
            website = org.get("website")
        company_domain = extract_domain_from_website(website)
        company_name = org.get("name")

    # Fallback: extract domain from email if no company domain available
    if not company_domain and email:
        company_domain = extract_domain_from_email(email)
        print(f"DOWNLOAD: Using email domain as company domain: {company_domain}")

    # Surfe requires at least company_domain or company_name for enrichment
    if not company_domain and not company_name:
        print(f"DOWNLOAD: No company info available for person {person_id}, skip enrichment")
        return

    # Start Surfe enrichment
    try:
        result = surfe_enrich_person(
            first_name=first_name,
            last_name=last_name,
            email=email,
            company_domain=company_domain,
            company_name=company_name
        )
        enrichment_id = result.get("enrichmentID")
        if enrichment_id:
            save_enrichment(enrichment_id, deal_id, person_id, "download")
            print(f"DOWNLOAD: Surfe enrichment started: {enrichment_id} for person {person_id}")
        else:
            print(f"DOWNLOAD: No enrichment ID returned from Surfe")
    except Exception as e:
        print(f"DOWNLOAD: Surfe enrichment failed: {e}")


def handle_leadfeeder_stage(deal: dict):
    """
    Scenario 2: Deal created in Leadfeeder stage (68)
    - Organization known (from Leadfeeder), no person yet
    - Search for ICP person via Surfe and add to deal
    - Works with domain OR company name (Leadfeeder doesn't provide domains)
    - Note: No Odoo sync - Pipeline 6 is Surfe-only
    - Person is only created AFTER enrichment completes (to ensure email is present)
    """
    deal_id = deal.get("id")
    org_id = pd_val(deal.get("org_id"))

    if not org_id:
        print(f"LEADFEEDER: Deal {deal_id} has no organization, skip")
        pd_add_note_to_deal(deal_id, "⚠️ Surfe: Kein Unternehmen im Deal vorhanden - keine Kontaktsuche möglich.")
        return

    # If deal already has person, skip
    person_id = pd_val(deal.get("person_id"))
    if person_id:
        print(f"LEADFEEDER: Deal {deal_id} already has person, skip")
        return

    org = pd_get(f"/organizations/{org_id}")
    org_name = org.get("name")

    if not org_name:
        print(f"LEADFEEDER: Org {org_id} has no name, skip")
        pd_add_note_to_deal(deal_id, "⚠️ Surfe: Unternehmen hat keinen Namen - keine Kontaktsuche möglich.")
        return

    # Try to extract domain from website field if available
    website = org.get("website")
    domain = extract_domain_from_website(website)

    # Determine search method
    if domain:
        search_by = f"domain: {domain}"
    else:
        search_by = f"company name: {org_name}"

    print(f"LEADFEEDER: Searching for ICP person by {search_by}")

    # Get deal owner for new person
    pd_owner = deal.get("user_id")
    owner_id = pd_owner.get("id") if isinstance(pd_owner, dict) else pd_owner

    # Surfe search for ICP persons (use domain if available, otherwise company name)
    try:
        people = surfe_search_people(
            domain=domain,
            company_name=org_name if not domain else None,
            job_titles=ICP_JOB_TITLES,
            limit=10
        )

        if not people:
            print(f"LEADFEEDER: No people found for {search_by}")
            pd_add_note_to_deal(deal_id, f"⚠️ Surfe: Keine Kontakte bei '{org_name}' gefunden (Suchkriterien: {', '.join(ICP_JOB_TITLES[:5])}...).")
            return

        # Select best person by ICP priority (filter by company name to avoid wrong matches)
        best_person = select_best_icp_person(people, target_company=org_name)

        if not best_person:
            print(f"LEADFEEDER: No matching ICP person found at {org_name}")
            pd_add_note_to_deal(deal_id, f"⚠️ Surfe: Kontakte gefunden, aber keiner arbeitet bei '{org_name}'. Bitte manuell prüfen.")
            return

        full_name = f"{best_person.get('firstName', '')} {best_person.get('lastName', '')}".strip()
        job_title = best_person.get("jobTitle")
        linkedin_url = best_person.get("linkedInUrl")

        print(f"LEADFEEDER: Found best ICP person: {full_name} ({job_title}) for {search_by}")

        # DON'T create person yet - wait for enrichment to get email first
        # Store pending person data for creation when enrichment completes
        pending_person_data = {
            "name": full_name,
            "org_id": org_id,
            "org_name": org_name,
            "owner_id": owner_id,
            "job_title": job_title,
            "linkedin_url": linkedin_url,
            "domain": domain
        }

        # Start Surfe enrichment for email/phone
        if linkedin_url or domain or org_name:
            try:
                result = surfe_enrich_person(
                    first_name=best_person.get("firstName"),
                    last_name=best_person.get("lastName"),
                    linkedin_url=linkedin_url,
                    company_domain=domain,
                    company_name=org_name if not domain else None
                )
                enrichment_id = result.get("enrichmentID")
                if enrichment_id:
                    # Save with pending person data - person will be created when enrichment completes
                    save_enrichment(enrichment_id, deal_id, None, "leadfeeder", pending_person_data)
                    print(f"LEADFEEDER: Surfe enrichment started: {enrichment_id} - waiting for email before creating person")
                else:
                    print(f"LEADFEEDER: No enrichment ID returned from Surfe")
                    pd_add_note_to_deal(deal_id, f"⚠️ Surfe: Kontakt gefunden ({full_name}, {job_title}), aber Anreicherung fehlgeschlagen.")
            except Exception as e:
                print(f"LEADFEEDER: Surfe enrichment failed: {e}")
                pd_add_note_to_deal(deal_id, f"⚠️ Surfe: Kontakt gefunden ({full_name}, {job_title}), aber Anreicherung fehlgeschlagen: {e}")
        else:
            print(f"LEADFEEDER: No identifiers for enrichment (no LinkedIn, domain, or company name)")
            pd_add_note_to_deal(deal_id, f"⚠️ Surfe: Kontakt gefunden ({full_name}), aber keine Daten für E-Mail-Anreicherung vorhanden.")

    except Exception as e:
        print(f"LEADFEEDER: Error: {e}")
        pd_add_note_to_deal(deal_id, f"⚠️ Surfe: Fehler bei der Kontaktsuche: {e}")


# ---------------- WEBHOOK ----------------
@app.post("/webhooks/pipedrive")
async def pipedrive_webhook(req: Request):
    if req.query_params.get("token") != WEBHOOK_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")

    payload = await req.json()
    meta = payload.get("meta", {}) or {}

    # Pipedrive webhook v2 info is mainly in meta
    event = payload.get("event")          # v1 style, might be None
    entity = meta.get("entity")           # "deal", "person", "organization"
    action = meta.get("action")           # "create", "update", "delete", ...
    obj_id = meta.get("entity_id") or meta.get("id")  # entity_id is the record id (deal/person/org)

    # Build event name if not present
    if not event and entity and action:
        event = f"{entity}.{action}"

    print("WEBHOOK EVENT:", event)
    print("WEBHOOK ENTITY:", entity, "ACTION:", action, "OBJ_ID:", obj_id)

    # Idempotency key: event + object id + timestamp
    event_key = f"{event}:{obj_id}:{meta.get('v') or meta.get('timestamp') or ''}"
    if event_seen(event_key):
        return {"ok": True, "deduped": True}

    uid = odoo_login()

    # DELETE handling (archive Odoo deal)
    if entity == "deal" and action == "delete":
        archive_deal_in_odoo(uid, int(obj_id))
        return {"ok": True, "archived": True}

    # Normal upserts
    if entity == "organization" or (event and event.startswith("organization.")):
        upsert_org(uid, int(obj_id))
    elif entity == "person" or (event and event.startswith("person.")):
        upsert_person(uid, int(obj_id))
    elif entity == "deal" or (event and event.startswith("deal.")):
        deal_id = int(obj_id)
        skip_odoo_sync = False

        # Surfe stage triggers on deal creation only
        # Pipedrive may send "create", "added", or similar actions for new deals
        if action in ("create", "added"):
            try:
                deal = pd_get(f"/deals/{deal_id}")
                stage_id = deal.get("stage_id")
                pipeline_id = deal.get("pipeline_id")

                print(f"SURFE CHECK: Deal {deal_id} in stage {stage_id}, pipeline {pipeline_id}")

                # Check if this is a Surfe-only pipeline (no Odoo sync, no Germany filter)
                if pipeline_id and int(pipeline_id) in SURFE_ONLY_PIPELINES:
                    skip_odoo_sync = True
                    print(f"SURFE ONLY: Pipeline {pipeline_id} - skipping Odoo sync")

                if stage_id == DOWNLOAD_STAGE_ID:
                    print(f"SURFE TRIGGER: Download stage {DOWNLOAD_STAGE_ID} detected for deal {deal_id}")
                    handle_download_stage(deal)
                elif stage_id == LEADFEEDER_STAGE_ID:
                    print(f"SURFE TRIGGER: Leadfeeder stage {LEADFEEDER_STAGE_ID} detected for deal {deal_id}")
                    handle_leadfeeder_stage(deal)
                else:
                    print(f"SURFE CHECK: Stage {stage_id} is not a Surfe trigger stage (37 or 68)")
            except Exception as e:
                print(f"SURFE TRIGGER: Error handling stage trigger: {e}")
        else:
            # For non-create actions, check pipeline to decide on Odoo sync
            try:
                deal = pd_get(f"/deals/{deal_id}")
                pipeline_id = deal.get("pipeline_id")
                if pipeline_id and int(pipeline_id) in SURFE_ONLY_PIPELINES:
                    skip_odoo_sync = True
                    print(f"SURFE ONLY: Pipeline {pipeline_id} - skipping Odoo sync for action {action}")
            except Exception as e:
                print(f"Pipeline check failed: {e}")

        # Only sync to Odoo if not a Surfe-only pipeline
        if not skip_odoo_sync:
            upsert_deal(uid, deal_id)
        else:
            print(f"SKIP ODOO: Deal {deal_id} is in Surfe-only pipeline")
    else:
        return {"ok": True, "ignored": True}

    return {"ok": True}


@app.post("/webhooks/surfe")
async def surfe_webhook(req: Request):
    """
    Receive Surfe enrichment results via webhook callback.
    Updates Pipedrive person with enriched data (phone, email, job title).
    """
    # Token validation
    if req.query_params.get("token") != SURFE_WEBHOOK_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")

    payload = await req.json()
    event_type = payload.get("eventType")

    print(f"SURFE WEBHOOK: Received event {event_type}")

    if event_type != "person.enrichment.completed":
        return {"ok": True, "ignored": True}

    data = payload.get("data", {})
    enrichment_id = data.get("enrichmentID")

    # Handle bulk enrichment response - results are in 'people' array
    people_results = data.get("people", [])
    if not people_results:
        print(f"SURFE: No people in enrichment result {enrichment_id}")
        return {"ok": True, "no_results": True}

    person_data = people_results[0] if people_results else {}

    # Load enrichment tracking
    enrichment = get_enrichment(enrichment_id)
    if not enrichment:
        print(f"SURFE: Unknown enrichment {enrichment_id}")
        return {"ok": True, "unknown": True}

    deal_id = enrichment["deal_id"]
    person_id = enrichment["person_id"]
    enrichment_type = enrichment["type"]
    pending_person_data = enrichment.get("pending_person_data")

    print(f"SURFE: Processing enrichment {enrichment_id} for deal {deal_id}, person {person_id}, type {enrichment_type}")

    # Extract email
    emails = person_data.get("emails", [])
    email = None
    for e in emails:
        if e.get("validationStatus") == "VALID":
            email = e.get("email")
            break
    if not email and emails:
        email = emails[0].get("email")

    # Extract phone (highest confidence)
    mobiles = person_data.get("mobilePhones", [])
    phone = None
    if mobiles:
        mobiles_sorted = sorted(mobiles, key=lambda x: x.get("confidence", 0), reverse=True)
        phone = mobiles_sorted[0].get("phone")

    job_title = person_data.get("jobTitle")

    print(f"SURFE: Enriched data - email: {email}, phone: {phone}, job_title: {job_title}")

    # Handle leadfeeder type: Create person NOW if email found
    if enrichment_type == "leadfeeder" and pending_person_data:
        if not email:
            # No email found - add note to deal and skip person creation
            print(f"SURFE: No email found for leadfeeder enrichment, skipping person creation")
            pd_add_note_to_deal(
                deal_id,
                f"⚠️ Surfe: Kontakt gefunden ({pending_person_data.get('name')}, {pending_person_data.get('job_title')}), "
                f"aber keine E-Mail-Adresse ermittelt. Bitte manuell recherchieren."
            )
            complete_enrichment(enrichment_id)
            return {"ok": True, "no_email": True}

        # Email found! Create person in Pipedrive with full data
        try:
            new_person = pd_create_person(
                name=pending_person_data.get("name"),
                org_id=pending_person_data.get("org_id"),
                owner_id=pending_person_data.get("owner_id"),
                email=email,
                phone=phone,
                job_title=job_title or pending_person_data.get("job_title")
            )
            person_id = new_person.get("id")

            # Link person to deal
            pd_link_person_to_deal(deal_id, person_id)

            print(f"SURFE: Created person {person_id} ({pending_person_data.get('name')}) with email {email} and linked to deal {deal_id}")

            # Add success note
            pd_add_note_to_deal(
                deal_id,
                f"✅ Surfe: Kontakt automatisch hinzugefügt:\n"
                f"• Name: {pending_person_data.get('name')}\n"
                f"• Position: {job_title or pending_person_data.get('job_title')}\n"
                f"• E-Mail: {email}\n"
                f"• Telefon: {phone or 'nicht gefunden'}"
            )

        except Exception as e:
            print(f"SURFE: Failed to create person: {e}")
            pd_add_note_to_deal(deal_id, f"⚠️ Surfe: Fehler beim Erstellen des Kontakts: {e}")

    # Handle download type: Update existing person
    elif person_id:
        try:
            # For download type, we already have email - only update phone/job_title
            update_email = email if enrichment_type != "download" else None
            pd_update_person(
                person_id=person_id,
                email=update_email,
                phone=phone,
                job_title=job_title
            )
            print(f"SURFE: Updated Pipedrive person {person_id}")
        except Exception as e:
            print(f"SURFE: Pipedrive update failed: {e}")

    # Mark enrichment as completed
    complete_enrichment(enrichment_id)

    return {"ok": True}


# ---------------- HEALTH ----------------
@app.get("/")
def root():
    return {"status": "ok", "service": "pipedrive-odoo-sync"}


@app.get("/webhooks/surfe")
def surfe_webhook_test():
    """Test endpoint to verify webhook URL is reachable."""
    return {"status": "ok", "message": "Surfe webhook endpoint is reachable"}


@app.get("/health/odoo")
def health_odoo():
    try:
        uid = odoo_login()
        return {"ok": True, "uid": uid}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})