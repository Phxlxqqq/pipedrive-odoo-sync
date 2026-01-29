"""
Helper functions for data transformation, domain guessing, and ICP selection.
"""
import re
import socket
from urllib.parse import urlparse
import requests

from config import REGION_TLDS, BRAVE_API_KEY


# ---- Language Mapping ----
def map_lang_to_odoo(pd_lang_value):
    """Map Pipedrive language/custom value to Odoo lang codes."""
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
        return "en_US"

    # If Pipedrive already stores Odoo lang codes, pass through
    if len(v) == 5 and v[2] == "_" and v[:2].isalpha() and v[3:].isalpha():
        return v

    return None


def normalize_probability(pd_prob):
    """Normalize probability to 0-100 range for Odoo."""
    if pd_prob is None:
        return None
    try:
        prob = float(pd_prob)
    except Exception:
        return None

    if 0.0 <= prob <= 1.0:
        prob *= 100.0
    if prob < 0:
        prob = 0.0
    if prob > 100:
        prob = 100.0
    return prob


# ---- Domain Extraction ----
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


# ---- Region Detection ----
def extract_region_from_title(deal_title: str) -> str:
    """
    Extract region indicator from deal title.
    Returns: "UK", "BENELUX", "SV", "DACH", or "DEFAULT"
    """
    if not deal_title:
        return "DEFAULT"

    title_upper = deal_title.upper()

    if "UK" in title_upper or "UNITED KINGDOM" in title_upper or "BRITAIN" in title_upper:
        return "UK"
    if "BENELUX" in title_upper or "BELGIUM" in title_upper or "NETHERLANDS" in title_upper or "LUXEMBOURG" in title_upper:
        return "BENELUX"
    if "SV" in title_upper or "SWEDEN" in title_upper or "SWEDISH" in title_upper:
        return "SV"
    if "DACH" in title_upper or "GERMANY" in title_upper or "AUSTRIA" in title_upper or "SWITZERLAND" in title_upper:
        return "DACH"

    # Default to DACH if no region indicator found
    return "DACH"


# ---- Domain Guessing ----
def guess_company_domain(company_name: str, region: str = "DEFAULT") -> str | None:
    """
    Try to guess a company's domain from its name by testing common TLD patterns.
    Uses region-specific TLDs for better accuracy.
    """
    if not company_name:
        return None

    # Clean company name
    clean = company_name.lower().strip()

    # Remove common legal suffixes
    suffixes = [" gmbh", " ag", " kg", " ohg", " gbr", " se", " co. kg", " & co.",
                " inc", " inc.", " ltd", " ltd.", " llc", " corp", " corporation",
                " ug", " e.v.", " e.k.",
                " bv", " nv", " vof",  # Dutch/Belgian
                " ab", " hb",  # Swedish
                " plc", " limited"]  # UK
    for suffix in suffixes:
        clean = clean.replace(suffix, "")

    # Replace special characters
    clean = clean.replace(" + ", "-").replace(" & ", "-").replace("&", "-")
    clean = clean.replace(" - ", "-").replace("–", "-").replace("—", "-")

    # Replace spaces and other chars with hyphens
    clean = re.sub(r'[^a-z0-9-]', '-', clean)
    clean = re.sub(r'-+', '-', clean)
    clean = clean.strip('-')

    if not clean:
        return None

    # Also try without hyphens
    clean_no_hyphen = clean.replace("-", "")

    # Get region-specific TLDs
    tlds = REGION_TLDS.get(region, REGION_TLDS["DEFAULT"])
    print(f"DOMAIN GUESS: Using TLDs for region '{region}': {tlds}")

    variations = [clean]
    if clean != clean_no_hyphen:
        variations.append(clean_no_hyphen)

    def domain_exists(domain: str) -> bool:
        try:
            socket.gethostbyname(domain)
            return True
        except socket.gaierror:
            return False

    for variation in variations:
        for tld in tlds:
            domain = f"{variation}{tld}"
            print(f"DOMAIN GUESS: Trying {domain}...")
            if domain_exists(domain):
                print(f"DOMAIN GUESS: Found working domain: {domain}")
                return domain

    print(f"DOMAIN GUESS: No working domain found for '{company_name}'")
    return None


def search_company_domain(company_name: str) -> str | None:
    """Search for a company's domain using Brave Search API."""
    if not BRAVE_API_KEY:
        print("BRAVE SEARCH: No API key configured, skipping")
        return None

    if not company_name:
        return None

    query = f"{company_name} official website"
    print(f"BRAVE SEARCH: Searching for '{query}'")

    try:
        r = requests.get(
            "https://api.search.brave.com/res/v1/web/search",
            headers={"X-Subscription-Token": BRAVE_API_KEY},
            params={"q": query, "count": 5},
            timeout=10
        )
        if not r.ok:
            print(f"BRAVE SEARCH ERROR: {r.status_code} - {r.text}")
            return None

        results = r.json()
        web_results = results.get("web", {}).get("results", [])

        if not web_results:
            print(f"BRAVE SEARCH: No results for '{company_name}'")
            return None

        # Clean company name for matching
        company_clean = company_name.lower().strip()
        for suffix in [" gmbh", " ag", " kg", " inc", " ltd", " llc"]:
            company_clean = company_clean.replace(suffix, "")
        company_words = set(company_clean.split())

        # Look for a result that matches the company name
        for result in web_results:
            url = result.get("url", "")
            title = (result.get("title") or "").lower()

            try:
                parsed = urlparse(url)
                domain = parsed.netloc.replace("www.", "")
            except:
                continue

            if not domain:
                continue

            # Skip generic/social domains
            skip_domains = ["linkedin.com", "facebook.com", "twitter.com", "xing.com",
                           "wikipedia.org", "bloomberg.com", "crunchbase.com", "dnb.com",
                           "google.com", "yelp.com", "glassdoor.com", "indeed.com"]
            if any(skip in domain for skip in skip_domains):
                continue

            # Check if title or domain contains company name words
            domain_words = set(domain.replace(".", " ").replace("-", " ").split())
            title_words = set(title.split())

            if company_words & domain_words or company_words & title_words:
                print(f"BRAVE SEARCH: Found matching domain: {domain}")
                return domain

        # Fallback: return first non-social result
        for result in web_results:
            url = result.get("url", "")
            try:
                parsed = urlparse(url)
                domain = parsed.netloc.replace("www.", "")
            except:
                continue

            skip_domains = ["linkedin.com", "facebook.com", "twitter.com", "xing.com",
                           "wikipedia.org", "bloomberg.com", "crunchbase.com", "dnb.com",
                           "google.com", "yelp.com", "glassdoor.com", "indeed.com"]
            if not any(skip in domain for skip in skip_domains):
                print(f"BRAVE SEARCH: Using first result domain: {domain}")
                return domain

        print(f"BRAVE SEARCH: No suitable domain found for '{company_name}'")
        return None

    except Exception as e:
        print(f"BRAVE SEARCH ERROR: {e}")
        return None


# ---- ICP Matching ----
def company_name_matches(person: dict, target_company: str) -> bool:
    """Check if person's current company matches the target company."""
    if not target_company:
        return True

    target_lower = target_company.lower().strip()
    target_clean = target_lower.replace(" ag", "").replace(" gmbh", "").replace(" inc", "").replace(" ltd", "").replace(" co.", "").strip()

    person_company = (person.get("company") or person.get("companyName") or "").lower().strip()
    person_company_clean = person_company.replace(" ag", "").replace(" gmbh", "").replace(" inc", "").replace(" ltd", "").replace(" co.", "").strip()

    # Exact or substring match
    if target_clean in person_company_clean or person_company_clean in target_clean:
        return True

    # Check if the main word matches
    target_words = set(target_clean.split())
    person_words = set(person_company_clean.split())
    if target_words & person_words:
        return True

    return False


def select_best_icp_person(people: list, target_company: str = None) -> dict | None:
    """
    Select the best person based on ICP priority.
    Priority: CISO > Compliance Manager > IT Manager > CTO > CEO > Founder
    """
    # First, filter to only people who work at the target company
    if target_company:
        filtered_people = [p for p in people if company_name_matches(p, target_company)]
        print(f"ICP FILTER: {len(filtered_people)}/{len(people)} people match company '{target_company}'")
        if not filtered_people:
            print(f"ICP FILTER: No people found matching company, showing all candidates:")
            for p in people[:5]:
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
