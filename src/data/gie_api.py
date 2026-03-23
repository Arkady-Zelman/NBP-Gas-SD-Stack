"""Client for GIE (Gas Infrastructure Europe) APIs — AGSI+ and ALSI.

AGSI+ (https://agsi.gie.eu/) — storage inventory / injection / withdrawal
ALSI  (https://alsi.gie.eu/) — LNG terminal send-out / inventory

Endpoints:
  AGSI+: GET https://agsi.gie.eu/api?country=GB&from=YYYY-MM-DD&to=YYYY-MM-DD&size=N
  ALSI:  GET https://alsi.gie.eu/api?country=GB&from=YYYY-MM-DD&to=YYYY-MM-DD&size=N

Auth: Officially requires a free API key (register at https://www.gie.eu/),
      passed as header ``x-key``.  In practice, the endpoints currently
      respond without a key, but rate-limiting may apply.  Registering is
      recommended for reliability.

Response structure (AGSI+ country-level):
  {
    "data": [
      {
        "name": "United Kingdom (Pre-Brexit)",
        "code": "GB",
        "gasDayStart": "2025-03-01",
        "gasInStorage": "...",    // TWh
        "injection": "...",       // GWh/d
        "withdrawal": "...",      // GWh/d
        "workingGasVolume": "...",
        "full": "...",            // % full
        ...
      }
    ]
  }

Note: injection/withdrawal values are in GWh/d from the API.
We convert them to mcm/d using the standard CV factor.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta

import pandas as pd
import requests

from src.units import gwh_to_mcm

logger = logging.getLogger(__name__)

AGSI_BASE = "https://agsi.gie.eu/api"
ALSI_BASE = "https://alsi.gie.eu/api"
TIMEOUT = 45
MAX_DAYS_PER_REQUEST = 90


class GIEClient:
    """Client for GIE AGSI+ and ALSI REST APIs."""

    def __init__(self, api_key: str = ""):
        self.session = requests.Session()
        if api_key:
            self.session.headers["x-key"] = api_key

    def _get_paginated(
        self,
        base_url: str,
        country: str,
        start: date,
        end: date,
    ) -> list[dict]:
        """Fetch data in date-range chunks to avoid timeouts."""
        all_records: list[dict] = []
        chunk_start = start

        while chunk_start <= end:
            chunk_end = min(chunk_start + timedelta(days=MAX_DAYS_PER_REQUEST - 1), end)
            params = {
                "country": country,
                "from": str(chunk_start),
                "to": str(chunk_end),
                "size": 300,
            }
            try:
                resp = self.session.get(base_url, params=params, timeout=TIMEOUT)
                resp.raise_for_status()
                payload = resp.json()
                data = payload.get("data", []) if isinstance(payload, dict) else payload
                if isinstance(data, list):
                    all_records.extend(data)
            except requests.RequestException as exc:
                logger.warning("GIE request failed (%s, %s to %s): %s",
                               base_url, chunk_start, chunk_end, exc)
            chunk_start = chunk_end + timedelta(days=1)

        return all_records

    # ------------------------------------------------------------------
    # AGSI+ — storage
    # ------------------------------------------------------------------

    def get_storage(
        self,
        country: str = "GB",
        start: str | date = "2020-10-01",
        end: str | date | None = None,
    ) -> pd.DataFrame | None:
        """Daily UK storage data: injection (GWh/d), withdrawal (GWh/d),
        inventory (TWh), fill level (%).  Converted to mcm/d."""
        start_dt = date.fromisoformat(str(start))
        end_dt = date.fromisoformat(str(end)) if end else date.today()

        records = self._get_paginated(AGSI_BASE, country, start_dt, end_dt)
        if not records:
            return None

        try:
            df = pd.DataFrame(records)
            df["date"] = pd.to_datetime(df["gasDayStart"])
            for col in ("injection", "withdrawal", "workingGasVolume", "full"):
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            if "injection" in df.columns:
                df["injection_mcm"] = gwh_to_mcm(df["injection"])
            if "withdrawal" in df.columns:
                df["withdrawal_mcm"] = gwh_to_mcm(df["withdrawal"])
            return df
        except Exception as exc:
            logger.warning("Failed to parse AGSI+ data: %s", exc)
            return None

    # ------------------------------------------------------------------
    # ALSI — LNG terminals
    # ------------------------------------------------------------------

    def get_lng_sendout(
        self,
        country: str = "GB",
        start: str | date = "2020-10-01",
        end: str | date | None = None,
    ) -> pd.DataFrame | None:
        """Daily LNG terminal send-out for UK facilities.
        API returns GWh/d; we convert to mcm/d."""
        start_dt = date.fromisoformat(str(start))
        end_dt = date.fromisoformat(str(end)) if end else date.today()

        records = self._get_paginated(ALSI_BASE, country, start_dt, end_dt)
        if not records:
            return None

        try:
            df = pd.DataFrame(records)
            df["date"] = pd.to_datetime(df["gasDayStart"])
            if "sendOut" in df.columns:
                df["sendOut"] = pd.to_numeric(df["sendOut"], errors="coerce")
                df["volume_mcm"] = gwh_to_mcm(df["sendOut"])
            elif "send_out" in df.columns:
                df["send_out"] = pd.to_numeric(df["send_out"], errors="coerce")
                df["volume_mcm"] = gwh_to_mcm(df["send_out"])
            return df
        except Exception as exc:
            logger.warning("Failed to parse ALSI data: %s", exc)
            return None
