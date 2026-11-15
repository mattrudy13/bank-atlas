"""Settings for the Bank Atlas pipeline.

Everything that is a decision (which metro, which FDIC fields, how strict a
check is) lives here so the other modules only hold logic.
"""

from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SITE_DATA_DIR = PROJECT_ROOT / "site-data"

# --- FDIC BankFind API -------------------------------------------------------

FDIC_API = "https://api.fdic.gov/banks"
PAGE_SIZE = 10_000  # the API's maximum per request
HTTP_TIMEOUT_SECONDS = 60
HTTP_RETRIES = 3

# Only the fields we use, so pages stay small. Bronze keeps them as raw text;
# widening bronze later (v0.2 bank lookup) is just adding names here.
FIELDS = {
    "institutions": [
        "CERT", "NAME", "CITY", "STALP", "STNAME", "NAMEHCR", "BKCLASS",
        "ACTIVE", "ESTYMD", "ENDEFYMD", "WEBADDR", "FED_RSSD", "ASSET",
        "DEP", "REPDTE", "OFFDOM",
    ],
    "locations": [
        "UNINUM", "CERT", "NAME", "OFFNAME", "OFFNUM", "ADDRESS", "CITY",
        "STALP", "ZIP", "COUNTY", "STCNTY", "CBSA_NO", "LATITUDE",
        "LONGITUDE", "SERVTYPE", "SERVTYPE_DESC", "MAINOFF", "ESTYMD",
        "ACQDATE",
    ],
    "history": [
        "UNINUM", "CERT", "INSTNAME", "CHANGECODE", "CHANGECODE_DESC",
        "EFFDATE", "PROCDATE", "TRANSNUM", "ORG_ROLE_CDE", "OFF_NAME",
        "OFF_NUM", "OFF_PADDR", "OFF_PCITY", "OFF_PSTALP", "OFF_PSTNUM",
        "OFF_CNTYNUM", "OFF_CNTYNAME", "OFF_LATITUDE", "OFF_LONGITUDE",
        "OFF_SERVTYPE", "ACQ_UNINUM", "OUT_UNINUM",
    ],
    "sod": [
        "YEAR", "CERT", "NAMEFULL", "NAMEHCR", "UNINUMBR", "BRNUM", "NAMEBR",
        "ADDRESBR", "CITYBR", "STALPBR", "STCNTYBR", "CNTYNAMB", "BRSERTYP",
        "DEPSUMBR", "SIMS_LATITUDE", "SIMS_LONGITUDE",
    ],
}

# Sorting keeps offset paging from skipping or repeating rows. The API can't
# sort on ID, so history and SOD rely on the index being static during a run;
# fetch.py still fails if the distinct IDs don't add up to FDIC's total.
SORT_BY = {"institutions": "CERT", "locations": "UNINUM"}

# --- The map's area ------------------------------------------------------------

METRO_CBSA = "47260"
METRO_NAME = "Virginia Beach-Chesapeake-Norfolk, VA-NC"

# Today's 19 counties and independent cities in CBSA 47260 (pulled from FDIC
# locations on 2026-11-15), with the names the site shows. The run also adds
# any county FDIC lists under the CBSA today. Pinning matters for small
# counties: if Camden NC's only branch closed, deriving the list from branches
# alone would drop Camden from every past year too.
METRO_COUNTIES = {
    "51810": "Virginia Beach",
    "51550": "Chesapeake",
    "51710": "Norfolk",
    "51700": "Newport News",
    "51650": "Hampton",
    "51740": "Portsmouth",
    "51800": "Suffolk",
    "51830": "Williamsburg",
    "51735": "Poquoson",
    "51620": "Franklin",
    "51095": "James City County",
    "51199": "York County",
    "51073": "Gloucester County",
    "51093": "Isle of Wight County",
    "51115": "Mathews County",
    "51175": "Southampton County",
    "37053": "Currituck County, NC",
    "37029": "Camden County, NC",
    "37073": "Gates County, NC",
}

FIRST_YEAR = 1994  # first Summary of Deposits year; gold starts here

# FDIC's history only records branch openings (711), closings (721) and moves
# (520) from 1999-08-02 on, nationwide; mergers and sales go back to 1970.
# The events gold table starts at 2000, the first full year, so every count
# in it is complete (Rudy, 2026-11-15). Silver keeps every year.
OPEN_CLOSE_TRACKED_FROM = "1999-08-02"
EVENTS_FIRST_YEAR = 2000

# Banks the site shows like any other but never names in automatic callouts
# ("biggest gainer", "most closures"...). 35095 = TowneBank.
NO_CALLOUT_CERTS = [35095]

# FDIC history CHANGECODEs we use. A branch that changes hands in a merger
# (713) or a sale (722) is neither a closing nor an opening; counting it as both
# would inflate both lines every merger year. 712 (Branch Purchased) is the
# buyer's side of the same sale as 722, so it's left out to avoid double counts.
EVENT_TYPES = {
    711: "opened",
    721: "closed",
    713: "changed_hands",
    722: "changed_hands",
    520: "moved",
}

# --- Checks --------------------------------------------------------------------

# Each table's row count may move this much from the last successful run
# before the run is stopped for a human look. Weekly branch changes are well
# under 1%; the yearly SOD release adds about 2%.
ROW_COUNT_TOLERANCE_PCT = 10

# Loose box around the metro: catches branches geocoded to 0,0 or another state
# without failing on a branch at the edge of Currituck or Southampton.
METRO_BOUNDS = {"lat_min": 35.8, "lat_max": 38.2, "lon_min": -77.6, "lon_max": -75.3}

# --- Lake location -----------------------------------------------------------


def load_dotenv(path: Path = PROJECT_ROOT / ".env") -> None:
    """Read KEY=VALUE lines from .env into the environment.

    Small on purpose (no python-dotenv dependency). Values already set in the
    environment win, so GitHub Actions secrets aren't overridden by a file.
    """
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.strip().strip('"').strip("'")
        if value:
            os.environ.setdefault(key.strip(), value)


def lake_uri() -> str:
    """Where the Delta tables live: LAKE_URI, else ./lake in the project."""
    load_dotenv()
    uri = os.environ.get("LAKE_URI", "").strip()
    if not uri:
        return str(PROJECT_ROOT / "lake")
    if "://" not in uri:
        return str((PROJECT_ROOT / uri).resolve())
    return uri.rstrip("/")


def storage_options() -> dict[str, str] | None:
    """deltalake storage options for Cloudflare R2, or None for a local lake.

    R2 speaks the S3 API. It supports conditional writes (If-Match on ETag),
    which lets delta-rs commit safely without a DynamoDB lock table. We still
    assume one writer at a time (the Actions concurrency group).
    """
    uri = lake_uri()
    if not uri.startswith("s3://"):
        return None
    missing = [
        name
        for name in ("R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY")
        if not os.environ.get(name)
    ]
    if missing:
        raise RuntimeError(
            f"LAKE_URI is {uri} but these are not set (see .env.example): {', '.join(missing)}"
        )
    return {
        "AWS_ACCESS_KEY_ID": os.environ["R2_ACCESS_KEY_ID"],
        "AWS_SECRET_ACCESS_KEY": os.environ["R2_SECRET_ACCESS_KEY"],
        "AWS_ENDPOINT_URL": f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        "AWS_REGION": "auto",
        "aws_conditional_put": "etag",
    }
