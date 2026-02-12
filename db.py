"""
Database functions for mapping and event tracking.
Uses SQLite for persistence.
"""
import json
import sqlite3
from config import DB_PATH


def get_con():
    """Get database connection and ensure tables exist."""
    con = sqlite3.connect(DB_PATH, timeout=10)
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
    con.execute("""
        CREATE TABLE IF NOT EXISTS surfe_processed_deals (
            deal_id INTEGER NOT NULL,
            action_type TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(deal_id, action_type)
        )
    """)
    return con


# ---- Mapping Functions ----
def mapping_get(obj_type: str, external_id: str):
    """Get Odoo ID for a Pipedrive object."""
    con = get_con()
    row = con.execute(
        "SELECT odoo_id FROM mapping WHERE object_type=? AND external_id=?",
        (obj_type, str(external_id))
    ).fetchone()
    con.close()
    return row[0] if row else None


def mapping_set(obj_type: str, external_id: str, odoo_id: int):
    """Save mapping between Pipedrive and Odoo objects."""
    con = get_con()
    con.execute(
        "INSERT OR REPLACE INTO mapping(object_type, external_id, odoo_id) VALUES(?,?,?)",
        (obj_type, str(external_id), int(odoo_id))
    )
    con.commit()
    con.close()


# ---- Event Deduplication ----
def event_seen(event_key: str) -> bool:
    """Check if event was already processed. Returns True if duplicate."""
    con = get_con()
    try:
        con.execute("INSERT INTO events(event_key) VALUES(?)", (event_key,))
        con.commit()
        return False
    except sqlite3.IntegrityError:
        return True
    finally:
        con.close()


# ---- Surfe Enrichment Tracking ----
def save_enrichment(enrichment_id: str, deal_id: int, person_id: int = None,
                    enrichment_type: str = "download", pending_person_data: dict = None):
    """Save enrichment request for later matching when webhook callback arrives.

    For leadfeeder type, pending_person_data contains the person info to create when enrichment completes:
    - name, org_id, owner_id, job_title, linkedin_url
    """
    con = get_con()
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


# ---- Surfe Deal Processing (Atomic Deduplication) ----
def claim_surfe_deal(deal_id: int, action_type: str) -> bool:
    """Atomically claim a deal for Surfe processing.

    Tries to INSERT a row. If it succeeds, this is the first webhook for this deal
    and returns True (proceed). If it fails (duplicate or locked), returns False (skip).

    This is atomic - no race condition possible between check and mark.
    """
    con = get_con()
    try:
        con.execute(
            "INSERT INTO surfe_processed_deals(deal_id, action_type) VALUES(?, ?)",
            (deal_id, action_type)
        )
        con.commit()
        return True  # Successfully claimed - proceed with processing
    except (sqlite3.IntegrityError, sqlite3.OperationalError):
        return False  # Already claimed or DB locked - skip either way
    finally:
        con.close()


def clear_surfe_processed_deals():
    """Clear all processed deal records (called on startup)."""
    con = get_con()
    con.execute("DELETE FROM surfe_processed_deals")
    con.commit()
    deleted = con.total_changes
    con.close()
    return deleted
