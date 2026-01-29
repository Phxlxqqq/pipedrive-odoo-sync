"""
Configuration and constants for Pipedrive-Odoo-Surfe sync.
All environment variables and mappings are centralized here.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ---- Core API Credentials ----
PIPEDRIVE_TOKEN = os.getenv("PIPEDRIVE_TOKEN")
PIPEDRIVE_BASE = "https://api.pipedrive.com/v1"

ODOO_URL = os.getenv("ODOO_URL")
ODOO_DB = os.getenv("ODOO_DB")
ODOO_USER = os.getenv("ODOO_USER")
ODOO_KEY = os.getenv("ODOO_API_KEY")

WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN")

# ---- Database ----
DB_PATH = os.getenv("SYNC_DB", "sync.db")

# ---- Germany Team Filter ----
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

# ICP Job Titles for person search (priority order)
ICP_JOB_TITLES = [t.strip() for t in os.getenv(
    "ICP_JOB_TITLES",
    "CISO,Chief Information Security Officer,Compliance Manager,IT Manager,IT Director,CTO,Chief Technology Officer,CEO,Chief Executive Officer,Founder,Geschäftsführer"
).split(",")]

# ---- Region-based TLDs for domain guessing ----
# Priority order: most likely TLDs first for each region
REGION_TLDS = {
    "DACH": [".de", ".at", ".ch", ".com", ".eu"],
    "UK": [".co.uk", ".uk", ".com", ".eu"],
    "BENELUX": [".nl", ".be", ".lu", ".com", ".eu"],
    "SV": [".se", ".com", ".eu"],  # Sweden
    "DEFAULT": [".com", ".de", ".eu", ".io"],  # Fallback
}

# ---- Brave Search API (for company domain lookup) ----
BRAVE_API_KEY = os.getenv("BRAVE_API_KEY")

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

# ---- Pipedrive custom field keys ----
PD_LANG_FIELD_KEY = "0a6493b05167a35971de14baa3b6e2b0175c11a7"
