"""
Pipedrive-Odoo-Surfe Sync Application
FastAPI app with webhook handlers.
"""
import json
import sqlite3
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse

from config import (
    WEBHOOK_TOKEN, SURFE_WEBHOOK_TOKEN, DB_PATH,
    DOWNLOAD_STAGE_ID, LEADFEEDER_STAGE_ID, SURFE_ONLY_PIPELINES
)
from db import event_seen, get_enrichment, complete_enrichment, clear_surfe_processed_deals
from odoo import odoo_login, upsert_org, upsert_person, upsert_deal, archive_deal_in_odoo
from pipedrive import (
    pd_get, pd_val, pd_create_person, pd_update_person,
    pd_link_person_to_deal, pd_add_note_to_deal
)
from surfe import handle_download_stage, handle_leadfeeder_stage


# ---- App Lifecycle ----
@asynccontextmanager
async def lifespan(_app):
    """Lifespan handler: runs on startup and shutdown."""
    # Startup: Clear the events deduplication table
    try:
        con = sqlite3.connect(DB_PATH)
        con.execute("DELETE FROM events")
        con.commit()
        deleted = con.total_changes
        con.close()
        print(f"STARTUP: Cleared {deleted} old events from deduplication table")
    except Exception as e:
        print(f"STARTUP: Could not clear events table: {e}")

    # Startup: Clear processed deals table (allows re-processing after restart)
    try:
        deleted = clear_surfe_processed_deals()
        print(f"STARTUP: Cleared {deleted} old surfe processed deals")
    except Exception as e:
        print(f"STARTUP: Could not clear surfe processed deals: {e}")

    yield

    print("SHUTDOWN: App stopping")


app = FastAPI(lifespan=lifespan)


# ---- Pipedrive Webhook ----
@app.post("/webhooks/pipedrive")
async def pipedrive_webhook(req: Request):
    if req.query_params.get("token") != WEBHOOK_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")

    payload = await req.json()
    meta = payload.get("meta", {}) or {}

    event = payload.get("event")
    entity = meta.get("entity")
    action = meta.get("action")
    obj_id = meta.get("entity_id") or meta.get("id")

    if not event and entity and action:
        event = f"{entity}.{action}"

    print("WEBHOOK EVENT:", event)
    print("WEBHOOK ENTITY:", entity, "ACTION:", action, "OBJ_ID:", obj_id)

    # Idempotency
    event_key = f"{event}:{obj_id}:{meta.get('v') or meta.get('timestamp') or ''}"
    if event_seen(event_key):
        print(f"WEBHOOK SKIP: Duplicate event {event_key} - already processed")
        return {"ok": True, "deduped": True}

    uid = odoo_login()

    # DELETE handling
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

        if action in ("create", "added"):
            try:
                deal = pd_get(f"/deals/{deal_id}")
                stage_id = deal.get("stage_id")
                pipeline_id = deal.get("pipeline_id")

                print(f"SURFE CHECK: Deal {deal_id} in stage {stage_id}, pipeline {pipeline_id}")

                if pipeline_id and int(pipeline_id) in SURFE_ONLY_PIPELINES:
                    skip_odoo_sync = True
                    print(f"SURFE ONLY: Pipeline {pipeline_id} - skipping Odoo sync")

                if stage_id == DOWNLOAD_STAGE_ID:
                    print(f"SURFE TRIGGER: Download stage {DOWNLOAD_STAGE_ID} detected for deal {deal_id}")
                    handle_download_stage(deal)
                elif stage_id == LEADFEEDER_STAGE_ID:
                    print(f"SURFE TRIGGER: Stage {LEADFEEDER_STAGE_ID} (Person Search) detected for deal {deal_id}")
                    handle_leadfeeder_stage(deal)
                else:
                    print(f"SURFE CHECK: Stage {stage_id} is not a Surfe trigger stage (37 or 68)")
            except Exception as e:
                print(f"SURFE TRIGGER: Error handling stage trigger: {e}")
        else:
            try:
                deal = pd_get(f"/deals/{deal_id}")
                pipeline_id = deal.get("pipeline_id")
                stage_id = deal.get("stage_id")

                if pipeline_id and int(pipeline_id) in SURFE_ONLY_PIPELINES:
                    skip_odoo_sync = True
                    print(f"SURFE ONLY: Pipeline {pipeline_id} - skipping Odoo sync for action {action}")

                if action in ("update", "change"):
                    person_id = pd_val(deal.get("person_id"))

                    if stage_id == LEADFEEDER_STAGE_ID:
                        if not person_id:
                            print(f"SURFE UPDATE: Stage 68 deal {deal_id} has no person, running person search")
                            handle_leadfeeder_stage(deal)
                        else:
                            print(f"SURFE UPDATE: Stage 68 deal {deal_id} already has person {person_id}, skip")

                    elif stage_id == DOWNLOAD_STAGE_ID:
                        if person_id:
                            print(f"SURFE UPDATE: Stage 37 deal {deal_id} has person {person_id}, running enrichment")
                            handle_download_stage(deal)
                        else:
                            print(f"SURFE UPDATE: Stage 37 deal {deal_id} has no person, skip (Download requires person)")

            except Exception as e:
                print(f"Pipeline check failed: {e}")

        if not skip_odoo_sync:
            upsert_deal(uid, deal_id)
        else:
            print(f"SKIP ODOO: Deal {deal_id} is in Surfe-only pipeline")
    else:
        return {"ok": True, "ignored": True}

    return {"ok": True}


# ---- Surfe Webhook ----
@app.post("/webhooks/surfe")
async def surfe_webhook(req: Request):
    """Receive Surfe enrichment results via webhook callback."""
    if req.query_params.get("token") != SURFE_WEBHOOK_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")

    payload = await req.json()
    event_type = payload.get("eventType")

    print(f"SURFE WEBHOOK: Received event {event_type}")
    print(f"SURFE WEBHOOK FULL PAYLOAD: {json.dumps(payload, indent=2, default=str)}")

    if event_type != "person.enrichment.completed":
        return {"ok": True, "ignored": True}

    data = payload.get("data", {})
    enrichment_id = data.get("enrichmentID")

    person_data = data.get("person")
    if not person_data:
        people_results = data.get("people", [])
        if people_results:
            person_data = people_results[0]

    if not person_data:
        print(f"SURFE: No person data in enrichment result {enrichment_id}")
        print(f"SURFE: Available data keys: {list(data.keys())}")
        return {"ok": True, "no_results": True}

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
        mobiles_sorted = sorted(mobiles, key=lambda x: x.get("confidenceScore", 0), reverse=True)
        phone = mobiles_sorted[0].get("mobilePhone")

    job_title = person_data.get("jobTitle")

    print(f"SURFE: Enriched data - email: {email}, phone: {phone}, job_title: {job_title}")

    # Handle leadfeeder type: Create person NOW if email found
    if enrichment_type == "leadfeeder" and pending_person_data:
        if not email:
            print(f"SURFE: No email found for leadfeeder enrichment, skipping person creation")
            pd_add_note_to_deal(
                deal_id,
                f"⚠️ Surfe: Contact found ({pending_person_data.get('name')}, {pending_person_data.get('job_title')}), "
                f"but no email address found. Please research manually."
            )
            complete_enrichment(enrichment_id)
            return {"ok": True, "no_email": True}

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

            pd_link_person_to_deal(deal_id, person_id)

            print(f"SURFE: Created person {person_id} ({pending_person_data.get('name')}) with email {email} and linked to deal {deal_id}")

            pd_add_note_to_deal(
                deal_id,
                f"✅ Surfe: Contact automatically added:\n"
                f"• Name: {pending_person_data.get('name')}\n"
                f"• Position: {job_title or pending_person_data.get('job_title')}\n"
                f"• Email: {email}\n"
                f"• Phone: {phone or 'not found'}"
            )

        except Exception as e:
            print(f"SURFE: Failed to create person: {e}")
            pd_add_note_to_deal(deal_id, f"⚠️ Surfe: Error creating contact: {e}")

    # Handle download type: Update existing person
    elif person_id:
        try:
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

    complete_enrichment(enrichment_id)
    return {"ok": True}


# ---- Health Endpoints ----
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
