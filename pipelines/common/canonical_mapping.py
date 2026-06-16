"""PRONABEC canonical mappings loader and helper utilities."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
import yaml

from pipelines.common.text_normalization import normalize_text_for_matching


@dataclass(frozen=True)
class CanonicalMatch:
    original_value: str | None
    normalized_key: str | None
    canonical_value: str | None
    domain: str
    match_method: str | None
    confidence: str | None
    review_required: bool
    matched: bool


def normalize_canonical_key(value: str | None) -> str | None:
    """Normalize textual key for canonical matching (uppercase, ASCII, no punctuation)."""
    return normalize_text_for_matching(value)


def build_alias_index(catalog: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Build a lookup index mapping domain and normalized keys to target mappings."""
    index: dict[str, dict[str, Any]] = {}
    for domain, mappings in catalog.get("domains", {}).items():
        domain_index: dict[str, Any] = {}
        for mapping in mappings:
            for alias in mapping.get("aliases", []):
                norm_alias = normalize_canonical_key(alias)
                if norm_alias:
                    domain_index[norm_alias] = mapping
            norm_canon = normalize_canonical_key(mapping.get("canonical_value"))
            if norm_canon:
                domain_index[norm_canon] = mapping
            norm_key = mapping.get("normalized_key")
            if norm_key:
                norm_key_clean = normalize_canonical_key(norm_key)
                if norm_key_clean:
                    domain_index[norm_key_clean] = mapping
        index[domain] = domain_index
    return index


def load_canonical_mappings(path: str | Path) -> dict[str, Any]:
    """Load and validate the PRONABEC canonical mappings configuration catalog."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Catalog file not found: {path}")

    with open(p, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if not isinstance(data, dict):
        raise ValueError("Invalid YAML catalog structure, must be a dictionary")

    if "version" not in data:
        raise ValueError("Catalog is missing required field: 'version'")

    if "domains" not in data:
        raise ValueError("Catalog is missing required field: 'domains'")

    domains = data["domains"]
    if not isinstance(domains, dict):
        raise ValueError("'domains' must be a dictionary")

    seen_aliases: dict[tuple[str, str], str] = {}

    for domain, mappings in domains.items():
        if not isinstance(mappings, list):
            raise ValueError(f"Domain '{domain}' mappings must be a list")

        for mapping in mappings:
            if not isinstance(mapping, dict):
                raise ValueError(f"Mapping in domain '{domain}' must be a dictionary")

            # Validate required fields
            required_fields = [
                "canonical_value",
                "aliases",
                "normalized_key",
                "match_method",
                "confidence",
                "review_required",
            ]
            for field in required_fields:
                if field not in mapping:
                    raise ValueError(f"Mapping in domain '{domain}' is missing required field: '{field}'")

            canonical_value = mapping["canonical_value"]
            if not isinstance(canonical_value, str) or not canonical_value.strip():
                raise ValueError(f"canonical_value in domain '{domain}' must be a non-empty string")

            aliases = mapping["aliases"]
            if not isinstance(aliases, list) or len(aliases) == 0:
                raise ValueError(f"aliases in domain '{domain}' must be a non-empty list")

            for alias in aliases:
                if not isinstance(alias, str) or not alias.strip():
                    raise ValueError(f"Alias in domain '{domain}' must be a non-empty string")

            normalized_key = mapping["normalized_key"]
            if not isinstance(normalized_key, str) or not normalized_key.strip():
                raise ValueError(f"normalized_key in domain '{domain}' must be a non-empty string")

            match_method = mapping["match_method"]
            allowed_methods = {"manual_alias", "safe_equivalent", "profile_candidate"}
            if match_method not in allowed_methods:
                raise ValueError(f"Invalid match_method '{match_method}' in domain '{domain}'")

            confidence = mapping["confidence"]
            allowed_confidences = {"high", "medium", "low"}
            if confidence not in allowed_confidences:
                raise ValueError(f"Invalid confidence '{confidence}' in domain '{domain}'")

            review_required = mapping["review_required"]
            if not isinstance(review_required, bool):
                raise ValueError(f"review_required in domain '{domain}' must be a boolean")

            # Duplicate / Ambiguity validation
            for alias in aliases:
                norm_alias = normalize_canonical_key(alias)
                if norm_alias:
                    key = (domain, norm_alias)
                    if key in seen_aliases and seen_aliases[key] != canonical_value:
                        raise ValueError(
                            f"Ambiguous alias '{alias}' in domain '{domain}' maps to both "
                            f"'{seen_aliases[key]}' and '{canonical_value}'"
                        )
                    seen_aliases[key] = canonical_value

            norm_canon = normalize_canonical_key(canonical_value)
            if norm_canon:
                key = (domain, norm_canon)
                if key in seen_aliases and seen_aliases[key] != canonical_value:
                    raise ValueError(
                        f"Ambiguous canonical normalized key for '{canonical_value}' in domain '{domain}' "
                        f"conflicts with '{seen_aliases[key]}'"
                    )
                seen_aliases[key] = canonical_value

            norm_key = normalize_canonical_key(normalized_key)
            if norm_key:
                key = (domain, norm_key)
                if key in seen_aliases and seen_aliases[key] != canonical_value:
                    raise ValueError(
                        f"Ambiguous normalized_key '{normalized_key}' in domain '{domain}' "
                        f"conflicts with '{seen_aliases[key]}'"
                    )
                seen_aliases[key] = canonical_value

    data["_alias_index"] = build_alias_index(data)
    return data


def lookup_canonical_value(
    catalog: dict[str, Any], domain: str, value: str | None
) -> CanonicalMatch:
    """Look up canonical match for value in the specified domain."""
    if not value or not value.strip():
        return CanonicalMatch(
            original_value=value,
            normalized_key=None,
            canonical_value=None,
            domain=domain,
            match_method=None,
            confidence=None,
            review_required=False,
            matched=False,
        )

    normalized = normalize_canonical_key(value)
    alias_index = catalog.get("_alias_index", {})
    if domain not in alias_index:
        return CanonicalMatch(
            original_value=value,
            normalized_key=normalized,
            canonical_value=None,
            domain=domain,
            match_method=None,
            confidence=None,
            review_required=False,
            matched=False,
        )

    domain_index = alias_index.get(domain, {})
    if normalized in domain_index:
        mapping = domain_index[normalized]
        return CanonicalMatch(
            original_value=value,
            normalized_key=normalized,
            canonical_value=mapping["canonical_value"],
            domain=domain,
            match_method=mapping["match_method"],
            confidence=mapping["confidence"],
            review_required=mapping["review_required"],
            matched=True,
        )

    return CanonicalMatch(
        original_value=value,
        normalized_key=normalized,
        canonical_value=None,
        domain=domain,
        match_method=None,
        confidence=None,
        review_required=False,
        matched=False,
    )
