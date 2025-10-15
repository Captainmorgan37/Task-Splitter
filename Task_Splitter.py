import json
from dataclasses import dataclass
from datetime import datetime, timedelta, date, time
from typing import List, Dict, Any

import pandas as pd
import pytz
from zoneinfo import ZoneInfo
import streamlit as st
import requests

# ----------------------------
# App Config
# ----------------------------
st.set_page_config(page_title="Night-Shift Tail Splitter", layout="wide")
st.title("🛫 Night-Shift Tail Splitter")

st.caption(
    "Assign next-day tails to on-duty shifts as evenly as possible, while keeping all legs of a tail together."
)

LOCAL_TZ = ZoneInfo("America/Edmonton")

# ----------------------------
# Types
# ----------------------------
@dataclass
class TailPackage:
    tail: str
    legs: int
    first_local_dt: datetime  # first dep local datetime for the day
    sample_legs: List[Dict[str, Any]]  # optional preview rows for UI (subset)


# ----------------------------
# Helpers
# ----------------------------
def _safe_parse_dt(dt_str: str) -> datetime:
    """Parse ISO-like datetime. If timezone-naive, assume UTC."""
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return dt.replace(tzinfo=pytz.UTC)
        return dt
    except Exception:
        # Last resort: try pandas
        dt = pd.to_datetime(dt_str, utc=True).to_pydatetime()
        return dt


def _to_local(dt: datetime, tz_name: str | None) -> datetime:
    if tz_name:
        try:
            return dt.astimezone(ZoneInfo(tz_name))
        except Exception:
            pass
    # Fallback: leave in original tz; if naive, assume UTC then convert to LOCAL_TZ so ordering is at least consistent
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=pytz.UTC)
    return dt.astimezone(LOCAL_TZ)


def _tomorrow_local() -> date:
    now_local = datetime.now(LOCAL_TZ)
    return (now_local + timedelta(days=1)).date()


# ----------------------------
# Data Fetch (stub or real)
# ----------------------------
@st.cache_data(show_spinner=False)
def fetch_next_day_legs(
    target_date: date,
    *,
    use_stub: bool,
    api_url: str | None = None,
    api_token: str | None = None,
    auth_header_name: str = "Authorization",
) -> pd.DataFrame:
    """
    Return a DataFrame of legs for target_date with columns at least:
      tail (str), leg_id (str/int), dep_time (ISO str), dep_tz (IANA tz name)
    You can extend with more columns if your API provides (dep_apt, arr_apt, etc.).
    """
    if use_stub:
        # ---------- STUB DATA (edit as desired) ----------
        # 6 tails, uneven leg counts, mixed timezones
        raw = [
            {"tail": "C-GASL", "leg_id": "L1", "dep_time": f"{target_date}T06:15:00-04:00", "dep_tz": "America/New_York"},
            {"tail": "C-GASL", "leg_id": "L2", "dep_time": f"{target_date}T09:40:00-04:00", "dep_tz": "America/New_York"},

            {"tail": "C-FLYR", "leg_id": "L3", "dep_time": f"{target_date}T05:55:00-07:00", "dep_tz": "America/Los_Angeles"},

            {"tail": "C-JETA", "leg_id": "L4", "dep_time": f"{target_date}T07:20:00-06:00", "dep_tz": "America/Denver"},
            {"tail": "C-JETA", "leg_id": "L5", "dep_time": f"{target_date}T12:10:00-06:00", "dep_tz": "America/Denver"},
            {"tail": "C-JETA", "leg_id": "L6", "dep_time": f"{target_date}T18:25:00-06:00", "dep_tz": "America/Denver"},

            {"tail": "C-LEGC", "leg_id": "L7", "dep_time": f"{target_date}T14:45:00+01:00", "dep_tz": "Europe/London"},
            {"tail": "C-LEGC", "leg_id": "L8", "dep_time": f"{target_date}T19:30:00+01:00", "dep_tz": "Europe/London"},

            {"tail": "C-CJ25", "leg_id": "L9", "dep_time": f"{target_date}T06:05:00-05:00", "dep_tz": "America/Chicago"},

            {"tail": "C-HAWK", "leg_id": "L10", "dep_time": f"{target_date}T08:00:00-06:00", "dep_tz": "America/Denver"},
            {"tail": "C-HAWK", "leg_id": "L11", "dep_time": f"{target_date}T16:40:00-06:00", "dep_tz": "America/Denver"},
        ]
        return pd.DataFrame(raw)

    # ---------- REAL FETCH ----------
    if not api_url:
        st.error("API URL required when real data mode is enabled.")
        return pd.DataFrame()
    if not api_token:
        st.error("API token required when real data mode is enabled.")
        return pd.DataFrame()

    start_local = datetime.combine(target_date, time.min, tzinfo=LOCAL_TZ)
    end_local = start_local + timedelta(days=1)
    start_utc = start_local.astimezone(pytz.UTC)
    end_utc = end_local.astimezone(pytz.UTC)

    params = {
        "start": start_utc.isoformat().replace("+00:00", "Z"),
        "end": end_utc.isoformat().replace("+00:00", "Z"),
        "date": target_date.isoformat(),
    }

    headers = {auth_header_name: api_token}

    try:
        response = requests.get(api_url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
    except requests.RequestException as exc:
        st.error(f"Error fetching data from API: {exc}")
        return pd.DataFrame()

    try:
        payload = response.json()
    except ValueError:
        st.error("API response was not valid JSON.")
        return pd.DataFrame()

    rows = _normalize_fl3xx_payload(payload)
    if not rows:
        st.warning("API returned no recognizable legs for the selected date.")
        return pd.DataFrame()

    return pd.DataFrame(rows)


def _extract_first(obj: Dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in obj and obj[key] not in (None, ""):
            return obj[key]
    return None


def _normalize_fl3xx_payload(payload: Any) -> List[Dict[str, Any]]:
    """Best-effort normalization of FL3XX flights/legs payload to rows with required fields."""

    def _iterable_items(data: Any) -> List[Dict[str, Any]]:
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for key in ("data", "items", "flights", "legs"):
                nested = data.get(key)
                if isinstance(nested, list):
                    return nested
        return []

    items = _iterable_items(payload)
    if not items and isinstance(payload, dict):
        items = [payload]
    elif not items and isinstance(payload, list):
        items = payload

    normalized: List[Dict[str, Any]] = []
    for flight in items:
        legs = []
        if isinstance(flight, dict):
            legs_data = flight.get("legs")
            if isinstance(legs_data, list) and legs_data:
                legs = legs_data
            else:
                legs = [flight]
        elif isinstance(flight, list):
            legs = flight
        else:
            continue

        flight_tail = {}
        if isinstance(flight, dict):
            flight_tail = flight

        for leg in legs:
            if not isinstance(leg, dict):
                continue
            tail = _extract_first(
                leg,
                "tail",
                "tailNumber",
                "tail_number",
                "aircraft",
                "aircraftRegistration",
            )
            if not tail and isinstance(flight_tail, dict):
                tail = _extract_first(
                    flight_tail,
                    "tail",
                    "tailNumber",
                    "tail_number",
                    "aircraft",
                    "aircraftRegistration",
                )

            leg_id = _extract_first(
                leg,
                "id",
                "legId",
                "leg_id",
                "uuid",
                "externalId",
                "external_id",
            )

            dep_time = _extract_first(
                leg,
                "departureTimeUtc",
                "departure_time_utc",
                "departureTime",
                "departure_time",
                "offBlockTimeUtc",
                "scheduledTimeUtc",
                "scheduled_departure_utc",
            )

            dep_tz = _extract_first(
                leg,
                "departureTimezone",
                "departureTimeZone",
                "departure_timezone",
                "departure_tz",
            )
            if not dep_tz:
                dep = leg.get("departure") if isinstance(leg.get("departure"), dict) else {}
                if isinstance(dep, dict):
                    dep_tz = _extract_first(dep, "timezone", "timeZone")

            if not tail or not dep_time:
                continue

            normalized.append(
                {
                    "tail": str(tail),
                    "leg_id": str(leg_id) if leg_id is not None else str(len(normalized) + 1),
                    "dep_time": dep_time,
                    "dep_tz": dep_tz,
                }
            )

    return normalized


def build_tail_packages(df: pd.DataFrame, target_date: date) -> List[TailPackage]:
    if df.empty:
        return []
    # Ensure required columns
    required = {"tail", "leg_id", "dep_time"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in data: {missing}")

    # Derive local first departure per tail for the day
    def first_local_for_tail(g: pd.DataFrame) -> datetime:
        # Filter legs that depart on target_date in their *local* timezone
        times_local: List[datetime] = []
        for _, row in g.iterrows():
            dt = _safe_parse_dt(str(row["dep_time"]))
            tz_name = str(row.get("dep_tz", "")) or None
            dt_local = _to_local(dt, tz_name)
            if dt_local.date() == target_date:
                times_local.append(dt_local)
        if not times_local:
            # If none match exactly by local date, fall back to min local
            for _, row in g.iterrows():
                dt = _safe_parse_dt(str(row["dep_time"]))
                tz_name = str(row.get("dep_tz", "")) or None
                times_local.append(_to_local(dt, tz_name))
        return min(times_local)

    packages: List[TailPackage] = []
    for tail, g in df.groupby("tail", sort=False):
        # Limit to target_date legs (by local date)
        legs_rows = []
        for _, row in g.iterrows():
            dt = _safe_parse_dt(str(row["dep_time"]))
            tz_name = str(row.get("dep_tz", "")) or None
            dt_local = _to_local(dt, tz_name)
            if dt_local.date() == target_date:
                legs_rows.append(row.to_dict())
        # If none strictly on target_date by local, treat all as same-day package
        if not legs_rows:
            legs_rows = [row.to_dict() for _, row in g.iterrows()]
        first_dt = first_local_for_tail(pd.DataFrame(legs_rows))
        packages.append(
            TailPackage(
                tail=str(tail),
                legs=len(legs_rows),
                first_local_dt=first_dt,
                sample_legs=legs_rows[:3],
            )
        )
    return packages


def assign_round_robin_by_first(packages: List[TailPackage], labels: List[str]) -> Dict[str, List[TailPackage]]:
    packages_sorted = sorted(packages, key=lambda p: p.first_local_dt)
    buckets: Dict[str, List[TailPackage]] = {lab: [] for lab in labels}
    for i, pkg in enumerate(packages_sorted):
        label = labels[i % len(labels)]
        buckets[label].append(pkg)
    return buckets


def assign_balanced_by_legs(packages: List[TailPackage], labels: List[str]) -> Dict[str, List[TailPackage]]:
    # Greedy bin-pack: biggest packages first → assign to bucket with lowest total legs
    buckets: Dict[str, List[TailPackage]] = {lab: [] for lab in labels}
    totals = {lab: 0 for lab in labels}
    for pkg in sorted(packages, key=lambda p: p.legs, reverse=True):
        # choose label with smallest total, then smallest count, then order
        label = sorted(labels, key=lambda lab: (totals[lab], len(buckets[lab]), labels.index(lab)))[0]
        buckets[label].append(pkg)
        totals[label] += pkg.legs
    return buckets


def _offset_hours(dt: datetime) -> float:
    offset = dt.utcoffset()
    if offset is None:
        return 0.0
    return offset.total_seconds() / 3600


def assign_preference_weighted(packages: List[TailPackage], labels: List[str]) -> Dict[str, List[TailPackage]]:
    if not packages or not labels:
        return {lab: [] for lab in labels}

    offsets = [_offset_hours(pkg.first_local_dt) for pkg in packages]
    min_off, max_off = min(offsets), max(offsets)
    if len(labels) == 1:
        targets = [min_off]
    elif max_off == min_off:
        targets = [min_off for _ in labels]
    else:
        targets = [min_off + (max_off - min_off) * (idx / (len(labels) - 1)) for idx in range(len(labels))]

    buckets: Dict[str, List[TailPackage]] = {lab: [] for lab in labels}
    totals = {lab: 0 for lab in labels}

    for pkg in sorted(packages, key=lambda p: p.first_local_dt):
        pkg_offset = _offset_hours(pkg.first_local_dt)

        def score(lab: str) -> tuple[float, int, int]:
            target = targets[labels.index(lab)]
            tz_penalty = abs(pkg_offset - target)
            return (
                round(tz_penalty, 4),
                totals[lab] + pkg.legs,
                len(buckets[lab]),
            )

        label = min(labels, key=score)
        buckets[label].append(pkg)
        totals[label] += pkg.legs

    return buckets


def buckets_to_df(buckets: Dict[str, List[TailPackage]]) -> pd.DataFrame:
    rows = []
    for label, pkgs in buckets.items():
        for pkg in sorted(pkgs, key=lambda p: (p.first_local_dt, p.tail)):
            rows.append({
                "Shift": label,
                "Tail": pkg.tail,
                "Legs": pkg.legs,
                "First Local Dep": pkg.first_local_dt.strftime("%Y-%m-%d %H:%M %Z"),
            })
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(["Shift", "First Local Dep", "Tail"]).reset_index(drop=True)
    return df


def summarize(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    agg = df.groupby("Shift").agg(Tails=("Tail", "count"), Legs=("Legs", "sum")).reset_index()
    # Add spread metrics
    total_legs = agg["Legs"].sum()
    total_shifts = agg.shape[0]
    target = total_legs / total_shifts if total_shifts else 0
    agg["Δ Legs vs Even"] = (agg["Legs"] - target).round(1)
    return agg


# ----------------------------
# Sidebar: Inputs
# ----------------------------
st.sidebar.header("Inputs")

fl3xx_cfg: Dict[str, Any] = {}
try:
    if "fl3xx_api" in st.secrets:
        cfg = st.secrets["fl3xx_api"]
        if isinstance(cfg, dict):
            fl3xx_cfg = dict(cfg)
except Exception:
    # Accessing secrets outside Streamlit Cloud may raise; ignore gracefully.
    fl3xx_cfg = {}

default_api_url = str(fl3xx_cfg.get("base_url", "")) if fl3xx_cfg else ""
default_api_token = str(fl3xx_cfg.get("api_token", "")) if fl3xx_cfg else ""
default_auth_header = str(fl3xx_cfg.get("auth_header_name", "X-Auth-Token")) if fl3xx_cfg else "X-Auth-Token"

use_stub = st.sidebar.toggle(
    "Use stub data",
    value=not bool(default_api_url and default_api_token),
    help="Uncheck to use your real FL3XX API.",
)
api_url = st.sidebar.text_input("API URL", value=default_api_url)
api_token = st.sidebar.text_input("API Token", value=default_api_token, type="password")
auth_header_name = st.sidebar.text_input("Auth header name", value=default_auth_header)

assign_mode = st.sidebar.radio(
    "Assignment mode",
    [
        "Round-robin by first local departure",
        "Balanced by legs (bin-pack)",
        "Preference-weighted east→west",
    ],
    help=(
        "Preference weighting leans earlier shifts toward eastern departures and later shifts toward western ones "
        "while still keeping leg counts even."
    ),
)

num_people = st.sidebar.number_input("Number of on-duty people", min_value=1, max_value=12, value=4, step=1)

default_labels = ["Early", "Next 1", "Next 2", "Late"]
labels = []
for i in range(num_people):
    lbl = st.sidebar.text_input(f"Label for person {i+1}", value=default_labels[i] if i < len(default_labels) else f"Shift {i+1}")
    labels.append(lbl or f"Shift {i+1}")

# Date selection (default = tomorrow local)
selected_date = st.sidebar.date_input("Target date", value=_tomorrow_local())


# ----------------------------
# Main Action
# ----------------------------
col1, col2 = st.columns([1, 2])
with col1:
    if st.button("🔄 Fetch & Assign", use_container_width=True):
        st.session_state["_run"] = True

# Show current settings
with col2:
    st.write(
        "**Settings:**",
        {
            "date": str(selected_date),
            "mode": assign_mode,
            "labels": labels,
        }
    )

# ----------------------------
# Processing & Output
# ----------------------------
if st.session_state.get("_run"):
    legs_df = fetch_next_day_legs(
        selected_date,
        use_stub=use_stub,
        api_url=api_url or None,
        api_token=api_token or None,
        auth_header_name=auth_header_name or "Authorization",
    )

    if legs_df.empty:
        st.warning("No legs returned for the selected date.")
        st.stop()

    with st.expander("Raw legs (preview)", expanded=False):
        st.dataframe(legs_df, use_container_width=True)

    packages = build_tail_packages(legs_df, selected_date)

    if not packages:
        st.info("No tail packages found for the selected date.")
        st.stop()

    st.subheader("Assignments")

    if assign_mode.startswith("Round-robin"):
        buckets = assign_round_robin_by_first(packages, labels)
    elif assign_mode.startswith("Balanced"):
        buckets = assign_balanced_by_legs(packages, labels)
    else:
        buckets = assign_preference_weighted(packages, labels)

    # Display per-shift tables
    tabs = st.tabs(labels)
    for i, lab in enumerate(labels):
        with tabs[i]:
            pkgs = buckets.get(lab, [])
            df = buckets_to_df({lab: pkgs})
            if df.empty:
                st.write("No tails assigned.")
            else:
                st.dataframe(df, use_container_width=True, hide_index=True)
                st.metric("Total legs", int(df["Legs"].sum()))
                st.metric("Tails", int(df.shape[0]))

    # Combined view
    combined_df = buckets_to_df(buckets)
    st.markdown("---")
    st.subheader("Combined view")
    st.dataframe(combined_df, use_container_width=True, hide_index=True)

    # Summary
    st.subheader("Summary")
    summary_df = summarize(combined_df)
    st.dataframe(summary_df, use_container_width=True, hide_index=True)

    # Downloads
    st.download_button(
        label="⬇️ Download assignments (CSV)",
        data=combined_df.to_csv(index=False).encode("utf-8"),
        file_name=f"tail_assignments_{selected_date}.csv",
        mime="text/csv",
        use_container_width=True,
    )

    # JSON mapping for integrations
    mapping = {lab: [p.tail for p in buckets.get(lab, [])] for lab in labels}
    st.code(json.dumps({
        "date": str(selected_date),
        "mode": assign_mode,
        "mapping": mapping,
    }, indent=2))

    st.success("Done. Adjust labels or mode and re-run as needed.")

# ----------------------------
# Notes / How-To
# ----------------------------
st.markdown(
    """
---
### How to wire your real API
1. Store your FL3XX credentials inside `.streamlit/secrets.toml` under `[fl3xx_api]` to auto-populate the sidebar inputs.
2. If your payload structure differs, tweak `_normalize_fl3xx_payload` so each row exposes `tail`, `leg_id`, `dep_time`, and optionally `dep_tz`.
3. If your API only has departure airport (e.g., `dep_apt`), add a lookup to map airport → IANA tz and set `dep_tz` before calling `build_tail_packages`.
4. The *round-robin* mode sorts packages by the first local departure time per tail and distributes in sequence.
5. The *balanced* mode packs by legs to minimize spread.

> You can easily add manual overrides later: a multiselect per shift for \"locked\" tails and re-run the solver for the rest.
"""
)
