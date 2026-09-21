"""Unit tests for pure transformation logic.

These run in milliseconds with no cluster and no data. That is the point: the
bugs they catch (a phone format that normalises to None, a status silently
mapped to the wrong bucket) are the ones that fail quietly in production and
show up months later as a metric nobody trusts.

Run with:  pytest tests/unit -v
"""

import pytest

from lakehouse.transforms import (
    canonical_status,
    match_confidence,
    name_key,
    normalise_email,
    normalise_phone,
    surrogate_key,
)


class TestNormalisePhone:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("0412 345 678", "0412345678"),
            ("0412345678", "0412345678"),
            ("(04) 1234 5678", "0412345678"),
            ("04-1234-5678", "0412345678"),
            ("+61 412 345 678", "0412345678"),
            ("61412345678", "0412345678"),
            ("0061412345678", "0412345678"),
            ("412345678", "0412345678"),  # leading zero lost in a spreadsheet
        ],
    )
    def test_australian_formats_converge(self, raw, expected):
        assert normalise_phone(raw) == expected

    @pytest.mark.parametrize("raw", [None, "", "   ", "12345", "not a phone", "0"])
    def test_implausible_input_returns_none(self, raw):
        assert normalise_phone(raw) is None

    def test_all_formats_of_one_number_match(self):
        """The property that actually matters: same human, same key."""
        variants = ["0412 345 678", "+61412345678", "(04) 1234 5678", "0412345678"]
        normalised = {normalise_phone(v) for v in variants}
        assert len(normalised) == 1


class TestNormaliseEmail:
    def test_lowercases_and_trims(self):
        assert normalise_email("  Sarah.Mitchell@Gmail.COM ") == "sarah.mitchell@gmail.com"

    @pytest.mark.parametrize("raw", [None, "", "notanemail", "@nope.com", "nope@"])
    def test_invalid_returns_none(self, raw):
        assert normalise_email(raw) is None


class TestNameKey:
    def test_strips_punctuation_and_case(self):
        assert name_key("Sarah-Jane", "O'Mitchell") == "SARAHJANEOMITCHELL"

    def test_handles_missing_parts(self):
        assert name_key("Sarah", None) == "SARAH"
        assert name_key(None, None) is None
        assert name_key("", "") is None


class TestCanonicalStatus:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("Completed", "completed"),
            ("attended", "completed"),
            ("DNA", "dna"),
            ("no show", "dna"),
            ("Did Not Attend", "dna"),
            ("canceled", "cancelled"),
            ("cancelled", "cancelled"),
        ],
    )
    def test_maps_known_variants(self, raw, expected):
        assert canonical_status(raw) == expected

    def test_unknown_is_explicit_not_guessed(self):
        """Unrecognised values must surface in the quality gate, not be coerced."""
        assert canonical_status("wibble") == "unknown"
        assert canonical_status(None) == "unknown"


class TestSurrogateKey:
    def test_deterministic_across_runs(self):
        assert surrogate_key("nookal", 4471) == surrogate_key("nookal", 4471)

    def test_source_scoped(self):
        """Same natural id in two systems must not collide."""
        assert surrogate_key("nookal", 4471) != surrogate_key("hapana", 4471)

    def test_accepts_int_or_str(self):
        assert surrogate_key("nookal", 4471) == surrogate_key("nookal", "4471")


class TestMatchConfidence:
    def test_email_plus_dob_is_highest(self):
        a = {"email": "s@x.com", "dob": "1984-03-12"}
        b = {"email": "s@x.com", "dob": "1984-03-12"}
        assert match_confidence(a, b) == 0.99

    def test_phone_and_name_below_auto_link_is_still_above_threshold(self):
        a = {"phone": "0412345678", "name_key": "SARAHMITCHELL"}
        b = {"phone": "0412345678", "name_key": "SARAHMITCHELL"}
        assert match_confidence(a, b) == 0.93

    def test_no_rule_fires_returns_none(self):
        a = {"email": "a@x.com", "phone": "0412345678", "name_key": "AB"}
        b = {"email": "b@x.com", "phone": "0499999999", "name_key": "CD"}
        assert match_confidence(a, b) is None

    def test_null_fields_never_match(self):
        """Two records missing the same field are not thereby the same person."""
        a = {"email": None, "phone": None, "name_key": None}
        b = {"email": None, "phone": None, "name_key": None}
        assert match_confidence(a, b) is None

    def test_weak_match_stays_below_auto_link_threshold(self):
        """Clinical safety: a false merge exposes one patient's record to another."""
        a = {"name_key": "JAMESCHEN", "dob": "1990-01-01", "postcode": "2000"}
        b = {"name_key": "JAMESCHEN", "dob": "1990-01-01", "postcode": "2000"}
        assert match_confidence(a, b) < 0.90
