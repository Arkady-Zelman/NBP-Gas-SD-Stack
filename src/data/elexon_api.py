"""Client for the Elexon / BMRS Insights API — CCGT generation data.

Endpoint: https://data.elexon.co.uk/bmrs/api/v1
Docs:     https://developer.data.elexon.co.uk/
Auth:     NONE REQUIRED — fully public API.

Uses the FUELHH dataset (half-hourly generation outturn by fuel type)
filtered to fuelType=CCGT, then aggregates to daily average MW and
converts to mcm/d.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta

import pandas as pd
import requests

from src.units import mw_to_mcm

logger = logging.getLogger(__name__)

BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1"
TIMEOUT = 30
MAX_DAYS_PER_REQUEST = 7


class ElexonClient:
    """Fetch CCGT half-hourly generation from Elexon BMRS and convert to mcm/d."""

    def __init__(self, api_key: str = ""):
        self.session = requests.Session()

    def _fetch_chunk(self, start: date, end: date) -> list[dict]:
        url = f"{BASE_URL}/datasets/FUELHH"
        params = {
            "settlementDateFrom": str(start),
            "settlementDateTo": str(end),
            "fuelType": ["CCGT"],
            "format": "json",
        }
        try:
            resp = self.session.get(url, params=params, timeout=TIMEOUT)
            resp.raise_for_status()
            payload = resp.json()
            return payload.get("data", []) if isinstance(payload, dict) else payload
        except requests.RequestException as exc:
            logger.warning("Elexon FUELHH request failed (%s to %s): %s", start, end, exc)
            return []

    def get_ccgt_generation(
        self,
        start: str | date = "2020-10-01",
        end: str | date | None = None,
    ) -> pd.DataFrame | None:
        """Daily average CCGT MW, converted to mcm/d.

        Fetches in weekly chunks to respect API data limits.
        Response fields: dataset, publishTime, startTime, settlementDate,
                         settlementPeriod, fuelType, generation (MW).
        """
        start_dt = date.fromisoformat(str(start))
        end_dt = date.fromisoformat(str(end)) if end else date.today()

        all_records: list[dict] = []
        chunk_start = start_dt
        while chunk_start <= end_dt:
            chunk_end = min(chunk_start + timedelta(days=MAX_DAYS_PER_REQUEST - 1), end_dt)
            records = self._fetch_chunk(chunk_start, chunk_end)
            all_records.extend(records)
            chunk_start = chunk_end + timedelta(days=1)

        if not all_records:
            logger.warning("Elexon returned no CCGT data for %s to %s", start, end)
            return None

        df = pd.DataFrame(all_records)
        df["date"] = pd.to_datetime(df["settlementDate"])
        df["generation"] = pd.to_numeric(df["generation"], errors="coerce")

        daily = df.groupby("date", as_index=False)["generation"].mean()
        daily = daily.rename(columns={"generation": "avg_mw"})
        daily["volume_mcm"] = mw_to_mcm(daily["avg_mw"])
        return daily[["date", "volume_mcm"]]
