"""
Surfe API functions and stage handlers.
"""
import requests

from config import (
    SURFE_API_KEY, SURFE_BASE, SURFE_WEBHOOK_URL, ICP_JOB_TITLES
)
from db import save_enrichment
from helpers import (
    extract_domain_from_website, extract_domain_from_email,
    extract_region_from_title, guess_company_domain, search_company_domain,
    select_best_icp_person
)
from pipedrive import (
    pd_get, pd_val, pd_add_note_to_deal, pd_update_org
)


# ---- Surfe API ----
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


def surfe_search_people(domain: str = None, company_name: str = None,
                        job_titles: list = None, limit: int = 10) -> list:
    """
    Synchronously search for people at a company by domain or company name.
    Returns list of people found.
    """
    if not domain and not company_name:
        raise ValueError("Either domain or company_name must be provided")

    companies_filter = {}
    if domain:
        companies_filter["domains"] = [domain]
    elif company_name:
        companies_filter["names"] = [company_name]

    payload = {
        "companies": companies_filter,
        "limit": limit,
        "peoplePerCompany": limit
    }

    if job_titles:
        payload["people"] = {
            "jobTitles": job_titles
        }

    print(f"SURFE SEARCH REQUEST: {payload}")

    r = requests.post(
        f"{SURFE_BASE}/people/search",
        headers=surfe_headers(),
        json=payload,
        timeout=30
    )
    if not r.ok:
        print(f"SURFE SEARCH ERROR: {r.status_code} - {r.text}")
        print(f"SURFE SEARCH RESPONSE: {r.text}")
    r.raise_for_status()
    result = r.json()

    print(f"SURFE SEARCH RESPONSE: {result}")

    return result.get("people", [])


# ---- Stage Handlers ----
def handle_download_stage(deal: dict):
    """
    Scenario 1: Deal in Download stage (37)
    - Person has email but missing phone number
    - Start Surfe enrichment to get phone
    """
    deal_id = deal.get("id")
    person_id = pd_val(deal.get("person_id"))

    if not person_id:
        print(f"DOWNLOAD: Deal {deal_id} has no person, skip")
        return

    person = pd_get(f"/persons/{person_id}")

    email = None
    if isinstance(person.get("email"), list) and person["email"]:
        email = person["email"][0].get("value")

    if not email:
        print(f"DOWNLOAD: Person {person_id} has no email, skip")
        return

    phone = None
    if isinstance(person.get("phone"), list) and person["phone"]:
        phone = person["phone"][0].get("value")

    if phone and len(phone) > 5:
        print(f"DOWNLOAD: Person {person_id} already has phone, skip")
        return

    name = person.get("name", "")
    name_parts = name.split(" ", 1)
    first_name = name_parts[0] if name_parts else ""
    last_name = name_parts[1] if len(name_parts) > 1 else ""

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

    if not company_domain and email:
        company_domain = extract_domain_from_email(email)
        print(f"DOWNLOAD: Using email domain as company domain: {company_domain}")

    if not company_domain and not company_name:
        print(f"DOWNLOAD: No company info available for person {person_id}, skip enrichment")
        return

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
    Scenario 2: Deal in Stage 68 (Person Search)
    - Organization known, no person yet
    - Search for ICP person via Surfe API and add to deal
    """
    deal_id = deal.get("id")
    deal_title = deal.get("title", "")
    org_id = pd_val(deal.get("org_id"))

    if not org_id:
        print(f"SURFE SEARCH: Deal {deal_id} has no organization, skip")
        pd_add_note_to_deal(deal_id, "⚠️ Surfe: No organization in deal - contact search not possible.")
        return

    person_id = pd_val(deal.get("person_id"))
    if person_id:
        print(f"SURFE SEARCH: Deal {deal_id} already has person, skip")
        return

    org = pd_get(f"/organizations/{org_id}")
    org_name = org.get("name")

    if not org_name:
        print(f"SURFE SEARCH: Org {org_id} has no name, skip")
        pd_add_note_to_deal(deal_id, "⚠️ Surfe: Organization has no name - contact search not possible.")
        return

    region = extract_region_from_title(deal_title)
    print(f"SURFE SEARCH: Detected region '{region}' from deal title '{deal_title}'")

    website = org.get("website")
    domain = extract_domain_from_website(website)
    domain_was_discovered = False

    if not domain:
        print(f"SURFE SEARCH: No website found, trying to guess domain from company name '{org_name}' (region: {region})")
        domain = guess_company_domain(org_name, region=region)
        if domain:
            print(f"SURFE SEARCH: Guessed domain: {domain}")
            domain_was_discovered = True

    if not domain:
        print(f"SURFE SEARCH: Domain guessing failed, trying Brave Search API")
        domain = search_company_domain(org_name)
        if domain:
            print(f"SURFE SEARCH: Found domain via Brave Search: {domain}")
            domain_was_discovered = True

    if domain_was_discovered and domain:
        try:
            pd_update_org(org_id, website=domain)
            print(f"SURFE SEARCH: Saved discovered domain '{domain}' to organization {org_id}")
        except Exception as e:
            print(f"SURFE SEARCH: Failed to save domain to organization: {e}")

    if domain:
        search_by = f"domain: {domain}"
    else:
        search_by = f"company name: {org_name}"

    print(f"SURFE SEARCH: Searching for ICP person by {search_by}")

    pd_owner = deal.get("user_id")
    owner_id = pd_owner.get("id") if isinstance(pd_owner, dict) else pd_owner

    try:
        people = surfe_search_people(
            domain=domain,
            company_name=org_name if not domain else None,
            job_titles=ICP_JOB_TITLES,
            limit=10
        )

        if not people:
            print(f"SURFE SEARCH: No ICP persons found, trying without job title filter...")
            people = surfe_search_people(
                domain=domain,
                company_name=org_name if not domain else None,
                job_titles=None,
                limit=10
            )

        if not people:
            print(f"SURFE SEARCH: No people found at all for {search_by}")
            pd_add_note_to_deal(deal_id, f"⚠️ Surfe: No contacts found at '{org_name}' (searched by domain and company name).")
            return
        else:
            print(f"SURFE SEARCH: Found {len(people)} people")

        best_person = select_best_icp_person(people, target_company=org_name)

        if not best_person:
            print(f"SURFE SEARCH: No matching ICP person found at {org_name}")
            pd_add_note_to_deal(deal_id, f"⚠️ Surfe: Contacts found, but none work at '{org_name}'. Please check manually.")
            return

        full_name = f"{best_person.get('firstName', '')} {best_person.get('lastName', '')}".strip()
        job_title = best_person.get("jobTitle")
        linkedin_url = best_person.get("linkedInUrl")

        print(f"SURFE SEARCH: Found best ICP person: {full_name} ({job_title}) for {search_by}")

        pending_person_data = {
            "name": full_name,
            "org_id": org_id,
            "org_name": org_name,
            "owner_id": owner_id,
            "job_title": job_title,
            "linkedin_url": linkedin_url,
            "domain": domain
        }

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
                    save_enrichment(enrichment_id, deal_id, None, "leadfeeder", pending_person_data)
                    print(f"SURFE SEARCH: Surfe enrichment started: {enrichment_id} - waiting for email before creating person")
                else:
                    print(f"SURFE SEARCH: No enrichment ID returned from Surfe")
                    pd_add_note_to_deal(deal_id, f"⚠️ Surfe: Contact found ({full_name}, {job_title}), but enrichment failed.")
            except Exception as e:
                print(f"SURFE SEARCH: Surfe enrichment failed: {e}")
                pd_add_note_to_deal(deal_id, f"⚠️ Surfe: Contact found ({full_name}, {job_title}), but enrichment failed: {e}")
        else:
            print(f"SURFE SEARCH: No identifiers for enrichment (no LinkedIn, domain, or company name)")
            pd_add_note_to_deal(deal_id, f"⚠️ Surfe: Contact found ({full_name}), but no data available for email enrichment.")

    except Exception as e:
        print(f"SURFE SEARCH: Error: {e}")
        pd_add_note_to_deal(deal_id, f"⚠️ Surfe: Error during contact search: {e}")
