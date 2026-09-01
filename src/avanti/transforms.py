"""Pure transformation logic.

Nothing in this module touches Spark, the network, or the filesystem. That is
deliberate: these are the functions where the subtle bugs live (a phone number
that normalises differently in two places silently degrades the identity match
rate), so they need to be unit-testable in milliseconds without a cluster.

Anything that needs Spark belongs in src/jobs/.
"""

from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone

# Statuses we accept from Nookal, mapped to our canonical vocabulary.
# Anything not listed here lands in 'unknown' and shows up in the quality gate
# rather than being silently coerced into something plausible.
STATUS_MAP = {
    "completed": "completed",
    "complete": "completed",
    "attended": "completed",
    "arrived": "arrived",
    "booked": "booked",
    "confirmed": "booked",
    "cancelled": "cancelled",
    "canceled": "cancelled",
    "dna": "dna",
    "no show": "dna",
    "no_show": "dna",
    "did not attend": "dna",
    "rescheduled": "rescheduled",
}


def normalise_email(raw: str | None) -> str | None:
    """Lowercase and trim. Returns None for anything that is not plausibly an email."""
    if not raw:
        return None
    cleaned = raw.strip().lower()
    if "@" not in cleaned or cleaned.startswith("@") or cleaned.endswith("@"):
        return None
    return cleaned


def normalise_phone(raw: str | None) -> str | None:
    """Reduce an Australian phone number to 10 digits, or None if implausible.

    Handles the formats that actually turn up in practice-management data:
    spaces, brackets, hyphens, +61 and 0061 international prefixes.
    """
    if not raw:
        return None

    digits = re.sub(r"[^0-9]", "", raw)

    if digits.startswith("0061"):
        digits = "0" + digits[4:]
    elif digits.startswith("61") and len(digits) == 11:
        digits = "0" + digits[2:]

    if len(digits) == 9 and not digits.startswith("0"):
        digits = "0" + digits  # dropped leading zero, common in spreadsheets

    return digits if len(digits) == 10 and digits.startswith("0") else None


def name_key(first: str | None, last: str | None) -> str | None:
    """Uppercase alphabetic-only join of the name parts, for blocking and matching."""
    parts = [p for p in (first, last) if p]
    if not parts:
        return None
    key = re.sub(r"[^A-Za-z]", "", "".join(parts)).upper()
    return key or None


def canonical_status(raw: str | None) -> str:
    """Map a source status to our vocabulary. Unrecognised values become 'unknown'."""
    if not raw:
        return "unknown"
    return STATUS_MAP.get(raw.strip().lower(), "unknown")


def surrogate_key(source: str, natural_id: str | int) -> str:
    """Deterministic SHA-256 surrogate key.

    Deterministic rather than sequential so that a full rebuild produces
    identical keys, dev matches prod, and the computation parallelises safely.
    Never use a monotonically increasing id for a persisted key.
    """
    return hashlib.sha256(f"{source}||{natural_id}".encode()).hexdigest()[:32]


def match_confidence(a: dict, b: dict) -> float | None:
    """Score two normalised party records. None means no rule fired.

    Deterministic tiers only, highest confidence first. Fuzzy scoring belongs
    downstream of this, applied to whatever these rules leave unmatched.

    The 0.90 auto-link threshold matters clinically: a false merge exposes one
    patient's record to another. Precision beats recall here, always.
    """
    email_match = a.get("email") and a["email"] == b.get("email")
    phone_match = a.get("phone") and a["phone"] == b.get("phone")
    name_match = a.get("name_key") and a["name_key"] == b.get("name_key")
    dob_match = a.get("dob") and a["dob"] == b.get("dob")
    postcode_match = a.get("postcode") and a["postcode"] == b.get("postcode")

    if email_match and dob_match:
        return 0.99
    if email_match:
        return 0.95
    if phone_match and name_match:
        return 0.93
    if name_match and dob_match and postcode_match:
        return 0.88
    return None


def landing_path(catalog: str, schema: str, source: str, entity: str, run_date: str) -> str:
    """Where an extractor writes raw files. One convention, used everywhere."""
    return (
        f"/Volumes/{catalog}/{schema}/raw/{source}/{entity}/dt={run_date}"
    )


def utc_now_iso() -> str:
    """Timestamps are stored in UTC everywhere; local dates are derived via dim_date."""
    return datetime.now(timezone.utc).isoformat()
