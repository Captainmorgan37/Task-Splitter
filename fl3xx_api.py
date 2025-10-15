"""Utilities for interacting with the FL3XX external flight API."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from typing import Any, Dict, Iterable, List, MutableMapping, Optional, Tuple

import requests


DEFAULT_FL3XX_BASE_URL = "https://app.fl3xx.us/api/external/flight/flights"


@dataclass(frozen=True)
class Fl3xxApiConfig:
    """Configuration for issuing requests to the FL3XX API."""

    base_url: str = DEFAULT_FL3XX_BASE_URL
    api_token: Optional[str] = None
    auth_header: Optional[str] = None
    auth_header_name: str = "Authorization"
    api_token_scheme: Optional[str] = None
    extra_headers: Dict[str, str] = field(default_factory=dict)
    verify_ssl: bool = True
    timeout: int = 30
    extra_params: Dict[str, str] = field(default_factory=dict)

    def build_headers(self) -> Dict[str, str]:
        headers = {"Accept": "application/json"}
        header_name = self.auth_header_name or "Authorization"
        if self.auth_header:
            headers[header_name] = self.auth_header
        elif self.api_token:
            token = str(self.api_token)
            scheme = self.api_token_scheme
            if scheme is None:
                scheme = "Bearer" if header_name.lower() == "authorization" else ""
            else:
                scheme = scheme.strip()
            headers[header_name] = f"{scheme} {token}".strip() if scheme else token
        headers.update(self.extra_headers)
        return headers


def compute_fetch_dates(now: Optional[datetime] = None) -> Tuple[date, date]:
    """Return the inclusive date range that should be requested from the API."""

    current = now or datetime.now(timezone.utc)
    start = current.date()
    end = start + timedelta(days=2)
    return start, end


def _normalise_payload(data: Any) -> List[Dict[str, Any]]:
    if isinstance(data, list):
        return data
    if isinstance(data, MutableMapping):
        if "items" in data and isinstance(data["items"], Iterable):
            items = list(data["items"])
            if all(isinstance(item, MutableMapping) for item in items):
                return items  # type: ignore[return-value]
        raise ValueError("Unsupported FL3XX API payload structure: mapping without 'items' list")
    raise ValueError("Unsupported FL3XX API payload structure")


def compute_flights_digest(flights: Iterable[Any]) -> str:
    """Return a stable SHA256 digest for the provided flight payload."""

    digest_input = json.dumps(list(flights), sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(digest_input).hexdigest()


def fetch_flights(
    config: Fl3xxApiConfig,
    *,
    from_date: Optional[date] = None,
    to_date: Optional[date] = None,
    session: Optional[requests.Session] = None,
    now: Optional[datetime] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Retrieve flights from the FL3XX API and return them with metadata."""

    reference_time = now or datetime.now(timezone.utc)
    if from_date is None or to_date is None:
        default_from, default_to = compute_fetch_dates(reference_time)
        if from_date is None:
            from_date = default_from
        if to_date is None:
            to_date = default_to

    params: Dict[str, str] = {
        "from": from_date.isoformat(),
        "to": to_date.isoformat(),
        "timeZone": "UTC",
        "value": "ALL",
    }
    params.update(config.extra_params)

    headers = config.build_headers()

    http = session or requests.Session()
    response = http.get(
        config.base_url,
        params=params,
        headers=headers,
        timeout=config.timeout,
        verify=config.verify_ssl,
    )
    response.raise_for_status()
    payload = response.json()
    flights = _normalise_payload(payload)

    digest = compute_flights_digest(flights)
    fetched_at = reference_time.isoformat().replace("+00:00", "Z")

    metadata = {
        "from_date": from_date.isoformat(),
        "to_date": to_date.isoformat(),
        "time_zone": params["timeZone"],
        "value": params["value"],
        "fetched_at": fetched_at,
        "hash": digest,
        "request_url": config.base_url,
        "request_params": params,
    }
    return flights, metadata


def _build_flight_endpoint(base_url: str, flight_id: Any) -> str:
    base = base_url.rstrip("/")
    if base.lower().endswith("/flights"):
        base = base[: -len("/flights")]
    return f"{base}/{flight_id}/crew"


def _normalise_crew_payload(payload: Any) -> List[Dict[str, Any]]:
    """Return a list of crew member dictionaries from various payload layouts."""

    if payload is None:
        return []

    def _coerce_members(obj: Any) -> Optional[List[Dict[str, Any]]]:
        if obj is None:
            return []
        if isinstance(obj, MutableMapping):
            return [value for value in obj.values() if isinstance(value, MutableMapping)]
        if isinstance(obj, Iterable) and not isinstance(obj, (str, bytes, bytearray)):
            return [item for item in obj if isinstance(item, MutableMapping)]
        return None

    if isinstance(payload, MutableMapping):
        for key in ("crewMembers", "items", "crew", "data", "results", "crews"):
            if key in payload:
                members = _coerce_members(payload[key])
                if members is not None:
                    return members

        if any(
            key in payload
            for key in ("role", "firstName", "lastName", "logName", "email", "trigram", "personnelNumber")
        ):
            return [payload]

        if not payload:
            return []

        raise ValueError("Unsupported FL3XX crew payload structure")

    members = _coerce_members(payload)
    if members is not None:
        return members

    raise ValueError("Unsupported FL3XX crew payload structure")


def fetch_flight_crew(
    config: Fl3xxApiConfig,
    flight_id: Any,
    *,
    session: Optional[requests.Session] = None,
) -> List[Dict[str, Any]]:
    """Return the crew payload for a specific flight."""

    http = session or requests.Session()
    close_session = session is None
    try:
        response = http.get(
            _build_flight_endpoint(config.base_url, flight_id),
            headers=config.build_headers(),
            timeout=config.timeout,
            verify=config.verify_ssl,
        )
        response.raise_for_status()
        payload = response.json()
        return _normalise_crew_payload(payload)
    finally:
        if close_session:
            try:
                http.close()
            except AttributeError:
                pass


def _select_crew_member(crew: Iterable[Dict[str, Any]], role: str) -> Optional[Dict[str, Any]]:
    for member in crew:
        if not isinstance(member, MutableMapping):
            continue
        member_role = str(member.get("role") or "").upper()
        if member_role == role.upper():
            return member
    return None


def _format_crew_name(member: Optional[Dict[str, Any]]) -> str:
    if not member:
        return ""
    parts = []
    for key in ("firstName", "middleName", "lastName"):
        value = member.get(key)
        if isinstance(value, str):
            value = value.strip()
        if value:
            parts.append(str(value))
    if parts:
        return " ".join(parts)
    for fallback_key in ("logName", "email", "trigram", "personnelNumber"):
        fallback = member.get(fallback_key)
        if isinstance(fallback, str):
            fallback = fallback.strip()
        if fallback:
            return str(fallback)
    return ""


def enrich_flights_with_crew(
    config: Fl3xxApiConfig,
    flights: Iterable[Dict[str, Any]],
    *,
    force: bool = False,
    session: Optional[requests.Session] = None,
) -> Dict[str, Any]:
    """Populate crew information (PIC/SIC names) onto the provided flights."""

    summary = {"fetched": 0, "errors": [], "updated": False}
    mutable_flights = [flight for flight in flights if isinstance(flight, MutableMapping)]
    if not mutable_flights:
        return summary

    http = session or requests.Session()
    close_session = session is None
    try:
        for flight in mutable_flights:
            flight_id = flight.get("flightId") or flight.get("id")
            if not flight_id:
                continue
            if not force and flight.get("picName") and flight.get("sicName"):
                continue
            try:
                crew_payload = fetch_flight_crew(config, flight_id, session=http)
            except Exception as exc:  # pragma: no cover - defensive path
                summary["errors"].append({"flight_id": flight_id, "error": str(exc)})
                continue

            summary["fetched"] += 1
            flight["crewMembers"] = crew_payload
            pic_member = _select_crew_member(crew_payload, "CMD")
            sic_member = _select_crew_member(crew_payload, "FO")
            pic_name = _format_crew_name(pic_member)
            sic_name = _format_crew_name(sic_member)
            if pic_name:
                if flight.get("picName") != pic_name:
                    summary["updated"] = True
                flight["picName"] = pic_name
            if sic_name:
                if flight.get("sicName") != sic_name:
                    summary["updated"] = True
                flight["sicName"] = sic_name
    finally:
        if close_session:
            try:
                http.close()
            except AttributeError:
                pass

    return summary


__all__ = [
    "Fl3xxApiConfig",
    "DEFAULT_FL3XX_BASE_URL",
    "compute_fetch_dates",
    "compute_flights_digest",
    "fetch_flights",
    "fetch_flight_crew",
    "enrich_flights_with_crew",
]
