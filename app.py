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
# Note: Surfe triggers work independently of this map (based on stage_id only)
PIPELINE_MAP = {
    4: 1,   # Original pipeline -> Odoo Team 1
    6: 1,   # Surfe/Leadfeeder pipeline -> Odoo Team 1 (adjust team_id if needed)
}

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
def save_enrichment(enrichment_id: str, deal_id: int, person_id: int = None, enrichment_type: str = "download"):
    """Save enrichment request for later matching when webhook callback arrives."""
    con = get_con()
    con.execute("""
        INSERT OR REPLACE INTO surfe_enrichments
        (enrichment_id, pipedrive_deal_id, pipedrive_person_id, enrichment_type)
        VALUES (?, ?, ?, ?)
    """, (enrichment_id, deal_id, person_id, enrichment_type))
    con.commit()
    con.close()


def get_enrichment(enrichment_id: str) -> dict | None:
    """Get enrichment data by Surfe enrichment ID."""
    con = get_con()
    row = con.execute("""
        SELECT enrichment_id, pipedrive_deal_id, pipedrive_person_id,
               enrichment_type, status
        FROM surfe_enrichments WHERE enrichment_id = ?
    """, (enrichment_id,)).fetchone()
    con.close()

    if row:
        return {
            "enrichment_id": row[0],
            "deal_id": row[1],
            "person_id": row[2],
            "type": row[3],
            "status": row[4]
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

    r = requests.post(
        f"{SURFE_BASE}/people/enrich",
        headers=surfe_headers(),
        json=payload,
        timeout=30
    )
    r.raise_for_status()
    return r.json()


def surfe_search_people(domain: str, job_titles: list, limit: int = 10) -> list:
    """
    Synchronously search for people at a company by domain and job titles.
    Returns list of people found.
    """
    payload = {
        "companies": {
            "domains": [domain]
        },
        "people": {
            "jobTitles": job_titles
        },
        "limit": limit,
        "peoplePerCompany": limit
    }

    r = requests.post(
        f"{SURFE_BASE}/people/search",
        headers=surfe_headers(),
        json=payload,
        timeout=30
    )
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
    elif org_id:
        partner_id = upsert_org(uid, int(org_id))

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


def select_best_icp_person(people: list) -> dict | None:
    """
    Select the best person based on ICP priority.
    Priority: CISO > Compliance Manager > IT Manager > CTO > CEO > Founder
    """
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


def handle_download_stage(deal: dict, uid: int):
    """
    Scenario 1: Deal created in Download stage (37)
    - Person has email but missing phone number
    - Start Surfe enrichment to get phone, name, company
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


def handle_leadfeeder_stage(deal: dict, uid: int):
    """
    Scenario 2: Deal created in Leadfeeder stage (68)
    - Organization known (from Leadfeeder), no person yet
    - Search for ICP person via Surfe and add to deal
    """
    deal_id = deal.get("id")
    org_id = pd_val(deal.get("org_id"))

    if not org_id:
        print(f"LEADFEEDER: Deal {deal_id} has no organization, skip")
        return

    # If deal already has person, skip
    person_id = pd_val(deal.get("person_id"))
    if person_id:
        print(f"LEADFEEDER: Deal {deal_id} already has person, skip")
        return

    org = pd_get(f"/organizations/{org_id}")

    # Extract domain from website field
    website = org.get("website")
    domain = extract_domain_from_website(website)

    if not domain:
        print(f"LEADFEEDER: Org {org_id} has no website/domain, skip")
        return

    # Get deal owner for new person
    pd_owner = deal.get("user_id")
    owner_id = pd_owner.get("id") if isinstance(pd_owner, dict) else pd_owner

    # Surfe search for ICP persons
    try:
        people = surfe_search_people(
            domain=domain,
            job_titles=ICP_JOB_TITLES,
            limit=10
        )

        if not people:
            print(f"LEADFEEDER: No people found at {domain}")
            return

        # Select best person by ICP priority
        best_person = select_best_icp_person(people)

        if not best_person:
            print(f"LEADFEEDER: No matching ICP person found")
            return

        full_name = f"{best_person.get('firstName', '')} {best_person.get('lastName', '')}".strip()
        job_title = best_person.get("jobTitle")

        print(f"LEADFEEDER: Found best ICP person: {full_name} ({job_title}) at {domain}")

        # Create person in Pipedrive
        new_person = pd_create_person(
            name=full_name,
            org_id=org_id,
            owner_id=owner_id,
            job_title=job_title
        )

        new_person_id = new_person.get("id")

        # Link person to deal
        pd_link_person_to_deal(deal_id, new_person_id)

        print(f"LEADFEEDER: Created person {new_person_id} ({full_name}) and linked to deal {deal_id}")

        # Start Surfe enrichment for email/phone
        linkedin_url = best_person.get("linkedInUrl")
        if linkedin_url or domain:
            try:
                result = surfe_enrich_person(
                    first_name=best_person.get("firstName"),
                    last_name=best_person.get("lastName"),
                    linkedin_url=linkedin_url,
                    company_domain=domain
                )
                enrichment_id = result.get("enrichmentID")
                if enrichment_id:
                    save_enrichment(enrichment_id, deal_id, new_person_id, "leadfeeder")
                    print(f"LEADFEEDER: Surfe enrichment started: {enrichment_id}")
            except Exception as e:
                print(f"LEADFEEDER: Surfe enrichment failed: {e}")

        # Sync to Odoo
        try:
            upsert_person(uid, new_person_id)
        except Exception as e:
            print(f"LEADFEEDER: Odoo sync failed: {e}")

    except Exception as e:
        print(f"LEADFEEDER: Error: {e}")


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

        # Surfe stage triggers on deal creation only
        # Pipedrive may send "create", "added", or similar actions for new deals
        if action in ("create", "added"):
            try:
                deal = pd_get(f"/deals/{deal_id}")
                stage_id = deal.get("stage_id")
                pipeline_id = deal.get("pipeline_id")

                print(f"SURFE CHECK: Deal {deal_id} in stage {stage_id}, pipeline {pipeline_id}")

                if stage_id == DOWNLOAD_STAGE_ID:
                    print(f"SURFE TRIGGER: Download stage {DOWNLOAD_STAGE_ID} detected for deal {deal_id}")
                    handle_download_stage(deal, uid)
                elif stage_id == LEADFEEDER_STAGE_ID:
                    print(f"SURFE TRIGGER: Leadfeeder stage {LEADFEEDER_STAGE_ID} detected for deal {deal_id}")
                    handle_leadfeeder_stage(deal, uid)
                else:
                    print(f"SURFE CHECK: Stage {stage_id} is not a Surfe trigger stage (37 or 68)")
            except Exception as e:
                print(f"SURFE TRIGGER: Error handling stage trigger: {e}")

        upsert_deal(uid, deal_id)
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

    # Update Pipedrive person
    if person_id:
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

    # Sync to Odoo
    try:
        uid = odoo_login()
        if person_id:
            upsert_person(uid, person_id)
            print(f"SURFE: Synced person {person_id} to Odoo")
    except Exception as e:
        print(f"SURFE: Odoo sync failed: {e}")

    # Mark enrichment as completed
    complete_enrichment(enrichment_id)

    return {"ok": True}


# ---------------- HEALTH ----------------
@app.get("/health/odoo")
def health_odoo():
    try:
        uid = odoo_login()
        return {"ok": True, "uid": uid}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})
