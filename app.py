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

# ---- Pipeline Allowlist + Odoo Team Mapping ----
# Only deals in these Pipedrive pipelines will be synced.
# Value maps pipedrive pipeline_id -> odoo crm.team id (team_id).
PIPELINE_MAP = {
    4: 1,
}

# ---- Stage mapping ----
# pipedrive stage_id -> odoo crm.stage id (stage_id)
STAGE_MAP = {
    65: 3,
    25: 3,
    26: 5,
    27: 5,
    28: 8,
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

app = FastAPI()


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
    name = org.get("name") or f"Org {org_id}"

    mapped = mapping_get("org", org_id)
    vals = {"name": name, "is_company": True}

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
    vals = {"name": name, "parent_id": parent_id, "email": email, "phone": phone}

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

    vals = {
        "name": title,
        "type": "opportunity",
        "partner_id": partner_id,
        "expected_revenue": value,
        "team_id": odoo_team_id,
    }

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
        upsert_deal(uid, int(obj_id))
    else:
        return {"ok": True, "ignored": True}

    return {"ok": True}


# ---------------- HEALTH ----------------
@app.get("/health/odoo")
def health_odoo():
    try:
        uid = odoo_login()
        return {"ok": True, "uid": uid}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})
    
@app.get("/debug/odoo/fields/{model}")
def debug_odoo_fields(model: str):
    uid = odoo_login()
    # listet ALLE Felder mit Typ, String-Label, Help etc.
    fields = odoo_execute(uid, model, "fields_get", args=[], kwargs={"attributes": ["string", "type", "help", "relation", "required"]})
    return fields

