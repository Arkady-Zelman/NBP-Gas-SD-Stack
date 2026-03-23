"""Client for the National Gas Transmission Data Portal.

Endpoint: https://data.nationalgas.com/api/find-gas-data-download
Auth:     NONE REQUIRED — fully public, open data policy.

Uses the CSV download API with Publication Object IDs (PUBOBs) to fetch
daily gas data items.  ID catalogue downloaded from:
https://www.nationalgas.com/sites/default/files/documents/API%20Data%20Item%20List%20v2.3.7.xlsx

Units returned by the API are in GWh/d for energy items, mcm/d for volume
items.  Entry Volume items (e.g. Langeled, South Hook) are in mcm/d.
Physical Flow items are in GWh/d.  Demand items are in mcm/d.
"""

from __future__ import annotations

import io
import logging
from datetime import date, timedelta

import pandas as pd
import requests

from src.units import kwh_to_mcm

logger = logging.getLogger(__name__)

BASE_URL = "https://data.nationalgas.com/api/find-gas-data-download"
TIMEOUT = 45
MAX_DAYS_PER_REQUEST = 365

# =================================================================
# Publication Object ID registry
# =================================================================

PUBOB_IDS = {
    # --- Interconnector physical flows (daily, GWh/d) ---
    "IUK": "PUBOB2038",                     # NTS Physical Flows, Bacton, Interconnector
    "BBL": "PUBOBJ1307",                     # NTS Physical Flows, BactonBBL, Interconnector
    "Moffat": "PUBOB2039",                   # NTS Physical Flows, Moffat, Interconnector

    # --- Entry volumes (daily, mcm/d — D+2 publication) ---
    "Langeled": "PUBOB452",                  # System Entry Volume, Easington - Langeled, D+2
    "Bacton_IUK_entry": "PUBOB386",          # System Entry Volume, Bacton Interconnector, D+2
    "South_Hook": "PUBOB3480",               # System Entry Volume, South Hook, D+2
    "Dragon": "PUBOB3564",                   # System Entry Volume, Milford Haven - Dragon, D+2
    "Grain_NTS1": "PUBOB371",                # System Entry Volume, Grain NTS 1, D+2
    "Grain_NTS2": "PUBOB3473",               # System Entry Volume, Grain NTS 2, D+2

    # --- UKCS entry points (daily, mcm/d) ---
    "Easington_Dimlington": "PUBOB407",      # Easington - Dimlington (UKCS)
    "Easington_WestSole": "PUBOB401",        # Easington - West Sole (UKCS)
    "Bacton_Perenco": "PUBOB377",            # Bacton - Perenco (UKCS)
    "Bacton_Shell": "PUBOB383",              # Bacton - Shell (UKCS)
    "Bacton_Tullow": "PUBOB380",             # Bacton - Tullow (UKCS)
    "Barrow": "PUBOB1826",                   # Barrow (UKCS)
    "StFergus_Mobil": "PUBOB428",            # St Fergus - Mobil (UKCS)
    "StFergus_Shell": "PUBOB431",            # St Fergus - Shell (UKCS)
    "StFergus_NSMP": "PUBOB434",             # St Fergus - NSMP (UKCS)
    "Teesside_CATS": "PUBOB437",             # Teesside - CATS (UKCS)
    "Teesside_PX": "PUBOB440",              # Teesside - PX (UKCS)

    # --- NTS demand (daily, mcm/d) ---
    "NTS_Demand_Actual": "PUBOB637",         # Demand Actual, NTS, D+1
    "NTS_Demand_Forecast": "PUBOB28",        # Demand Forecast, NTS, hourly update

    # --- Residential (NDM) demand by LDZ (daily, mcm/d, D+1) ---
    "NDM_EA": "PUBOB3755", "NDM_EM": "PUBOB3756", "NDM_NE": "PUBOB3757",
    "NDM_NO": "PUBOB3758", "NDM_NT": "PUBOB3759", "NDM_NW": "PUBOB3760",
    "NDM_SC": "PUBOB3761", "NDM_SE": "PUBOB3762", "NDM_SO": "PUBOB3763",
    "NDM_SW": "PUBOB3764", "NDM_WN": "PUBOB3765", "NDM_WS": "PUBOB3766",
    "NDM_WM": "PUBOB3767",

    # --- Industrial (DM) demand by LDZ (daily, mcm/d, D+1) ---
    "DM_EA": "PUBOB3742", "DM_EM": "PUBOB3743", "DM_NE": "PUBOB3744",
    "DM_NO": "PUBOB3745", "DM_NT": "PUBOB3746", "DM_NW": "PUBOB3747",
    "DM_SC": "PUBOB3748", "DM_SE": "PUBOB3749", "DM_SO": "PUBOB3750",
    "DM_SW": "PUBOB3751", "DM_WN": "PUBOB3752", "DM_WS": "PUBOB3753",
    "DM_WM": "PUBOB3754",

    # --- Storage inflows (injection, daily, kWh) ---
    "INF_HumblyGrove": "PUBOBJ2401", "INF_Hornsea": "PUBOBJ2402",
    "INF_Rough": "PUBOBJ2404", "INF_HatfieldMoor": "PUBOBJ2405",
    "INF_HolehouseFarm": "PUBOBJ2406", "INF_Aldbrough": "PUBOBJ2407",
    "INF_Holford": "PUBOBJ2408", "INF_HillTop": "PUBOBJ2409",
    "INF_Stublach": "PUBOBJ2410",

    # --- Storage outflows (withdrawal, daily, kWh) ---
    "OUT_HumblyGrove": "PUBOBJ2413", "OUT_Hornsea": "PUBOBJ2414",
    "OUT_Rough": "PUBOBJ2416", "OUT_HatfieldMoor": "PUBOBJ2417",
    "OUT_HolehouseFarm": "PUBOBJ2418", "OUT_Aldbrough": "PUBOBJ2419",
    "OUT_Holford": "PUBOBJ2420", "OUT_HillTop": "PUBOBJ2421",
    "OUT_Stublach": "PUBOBJ2422",
}

# Grouped IDs for batch fetches
SUPPLY_ENTRY_IDS = [
    "Langeled", "South_Hook", "Dragon", "Grain_NTS1", "Grain_NTS2",
    "Bacton_IUK_entry",
    "Easington_Dimlington", "Easington_WestSole",
    "Bacton_Perenco", "Bacton_Shell", "Bacton_Tullow",
    "Barrow", "StFergus_Mobil", "StFergus_Shell", "StFergus_NSMP",
    "Teesside_CATS", "Teesside_PX",
]

UKCS_ENTRY_IDS = [
    "Easington_Dimlington", "Easington_WestSole",
    "Bacton_Perenco", "Bacton_Shell", "Bacton_Tullow",
    "Barrow", "StFergus_Mobil", "StFergus_Shell", "StFergus_NSMP",
    "Teesside_CATS", "Teesside_PX",
]

LNG_ENTRY_IDS = ["South_Hook", "Dragon", "Grain_NTS1", "Grain_NTS2"]

NDM_LDZ_IDS = [k for k in PUBOB_IDS if k.startswith("NDM_")]

DM_LDZ_IDS = [k for k in PUBOB_IDS if k.startswith("DM_")]

STORAGE_INFLOW_IDS = [k for k in PUBOB_IDS if k.startswith("INF_")]

STORAGE_OUTFLOW_IDS = [k for k in PUBOB_IDS if k.startswith("OUT_")]


class NationalGasClient:
    """Client for National Gas CSV download API.  No authentication required."""

    def __init__(self, api_key: str = ""):
        self.session = requests.Session()

    def _fetch_csv(
        self,
        pubob_ids: list[str],
        start: date,
        end: date,
    ) -> pd.DataFrame | None:
        """Fetch data as CSV for a list of PUBOB IDs over a date range."""
        ids_str = ",".join(pubob_ids)
        params = {
            "applicableFor": "Y",
            "dateFrom": f"{start}T00:00:00",
            "dateTo": f"{end}T23:59:59",
            "dateType": "GASDAY",
            "latestFlag": "Y",
            "ids": ids_str,
            "type": "CSV",
        }
        try:
            resp = self.session.get(BASE_URL, params=params, timeout=TIMEOUT)
            resp.raise_for_status()
            if "text/html" in resp.headers.get("content-type", ""):
                return None
            df = pd.read_csv(io.StringIO(resp.text))
            if df.empty:
                return None
            df.columns = df.columns.str.strip()
            return df
        except Exception as exc:
            logger.warning("National Gas CSV fetch failed: %s", exc)
            return None

    def _fetch_chunked(
        self,
        pubob_ids: list[str],
        start: date,
        end: date,
    ) -> pd.DataFrame | None:
        """Fetch in yearly chunks to stay within the 3600-item API limit."""
        all_frames: list[pd.DataFrame] = []
        chunk_start = start

        while chunk_start <= end:
            chunk_end = min(chunk_start + timedelta(days=MAX_DAYS_PER_REQUEST - 1), end)
            df = self._fetch_csv(pubob_ids, chunk_start, chunk_end)
            if df is not None and not df.empty:
                all_frames.append(df)
            chunk_start = chunk_end + timedelta(days=1)

        if not all_frames:
            return None
        return pd.concat(all_frames, ignore_index=True)

    def _to_daily(self, df: pd.DataFrame) -> pd.DataFrame:
        """Parse the standard CSV response into a clean daily DataFrame."""
        df = df.copy()
        df["date"] = pd.to_datetime(df["Applicable For"], dayfirst=True)
        df["Value"] = pd.to_numeric(df["Value"], errors="coerce")
        return df

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------

    def get_physical_flows(
        self,
        point: str,
        start: str | date = "2020-10-01",
        end: str | date | None = None,
    ) -> pd.DataFrame | None:
        """Fetch daily physical flows for IUK, BBL, or Moffat (GWh/d)."""
        pubob_key = point.replace(" ", "_")
        pubob_id = PUBOB_IDS.get(pubob_key) or PUBOB_IDS.get(point)
        if pubob_id is None:
            logger.warning("No PUBOB ID registered for point: %s", point)
            return None

        start_dt = date.fromisoformat(str(start))
        end_dt = date.fromisoformat(str(end)) if end else date.today()

        df = self._fetch_chunked([pubob_id], start_dt, end_dt)
        if df is None:
            return None

        df = self._to_daily(df)
        out = df[["date", "Value"]].rename(columns={"Value": "volume_mcm"})
        out = out.dropna(subset=["volume_mcm"])
        return out.sort_values("date").reset_index(drop=True)

    def get_entry_volumes(
        self,
        entry_point: str,
        start: str | date = "2020-10-01",
        end: str | date | None = None,
    ) -> pd.DataFrame | None:
        """Fetch daily system entry volumes (mcm/d) for a named entry point."""
        return self.get_physical_flows(entry_point, start, end)

    def get_all_supply_entries(
        self,
        start: str | date = "2020-10-01",
        end: str | date | None = None,
    ) -> pd.DataFrame | None:
        """Fetch all supply entry point volumes in one batch call."""
        pubob_ids = [PUBOB_IDS[k] for k in SUPPLY_ENTRY_IDS if k in PUBOB_IDS]
        start_dt = date.fromisoformat(str(start))
        end_dt = date.fromisoformat(str(end)) if end else date.today()

        df = self._fetch_chunked(pubob_ids, start_dt, end_dt)
        if df is None:
            return None

        df = self._to_daily(df)
        df = df.rename(columns={"Data Item": "source", "Value": "volume_mcm"})
        return df[["date", "source", "volume_mcm"]].dropna(subset=["volume_mcm"])

    def get_demand(
        self,
        demand_type: str = "NTS",
        start: str | date = "2020-10-01",
        end: str | date | None = None,
    ) -> pd.DataFrame | None:
        """Fetch daily demand (mcm/d).

        demand_type: "NTS" (total actual), "FORECAST", "LDZ"/"NDM"
        (residential, sum of all 13 LDZ zones), "DM" (industrial, sum of
        all 13 LDZ zones).
        """
        dtype = demand_type.upper()
        if dtype == "NTS":
            ids = [PUBOB_IDS["NTS_Demand_Actual"]]
        elif dtype == "FORECAST":
            ids = [PUBOB_IDS["NTS_Demand_Forecast"]]
        elif dtype in ("LDZ", "NDM"):
            ids = [PUBOB_IDS[k] for k in NDM_LDZ_IDS]
        elif dtype in ("DM", "LDM", "INDUSTRIAL"):
            ids = [PUBOB_IDS[k] for k in DM_LDZ_IDS]
        else:
            logger.warning("Unknown demand type: %s", demand_type)
            return None

        start_dt = date.fromisoformat(str(start))
        end_dt = date.fromisoformat(str(end)) if end else date.today()

        df = self._fetch_chunked(ids, start_dt, end_dt)
        if df is None:
            return None

        df = self._to_daily(df)

        if dtype in ("LDZ", "NDM", "DM", "LDM", "INDUSTRIAL"):
            out = df.groupby("date", as_index=False)["Value"].sum()
            out = out.rename(columns={"Value": "volume_mcm"})
            out["volume_mcm"] = kwh_to_mcm(out["volume_mcm"])
        else:
            out = df[["date", "Value"]].rename(columns={"Value": "volume_mcm"})

        out = out.dropna(subset=["volume_mcm"])
        return out.sort_values("date").reset_index(drop=True)

    def get_ukcs_production(
        self,
        start: str | date = "2020-10-01",
        end: str | date | None = None,
    ) -> pd.DataFrame | None:
        """Fetch aggregated UKCS terminal entry volumes (mcm/d)."""
        ids = [PUBOB_IDS[k] for k in UKCS_ENTRY_IDS if k in PUBOB_IDS]
        start_dt = date.fromisoformat(str(start))
        end_dt = date.fromisoformat(str(end)) if end else date.today()

        df = self._fetch_chunked(ids, start_dt, end_dt)
        if df is None:
            return None

        df = self._to_daily(df)
        out = df.groupby("date", as_index=False)["Value"].sum()
        out = out.rename(columns={"Value": "volume_mcm"})
        return out.dropna(subset=["volume_mcm"]).sort_values("date").reset_index(drop=True)

    def get_lng_entry_volumes(
        self,
        start: str | date = "2020-10-01",
        end: str | date | None = None,
    ) -> pd.DataFrame | None:
        """Fetch total LNG terminal entry volumes (mcm/d) — South Hook + Dragon + Grain."""
        ids = [PUBOB_IDS[k] for k in LNG_ENTRY_IDS if k in PUBOB_IDS]
        start_dt = date.fromisoformat(str(start))
        end_dt = date.fromisoformat(str(end)) if end else date.today()

        df = self._fetch_chunked(ids, start_dt, end_dt)
        if df is None:
            return None

        df = self._to_daily(df)
        out = df.groupby("date", as_index=False)["Value"].sum()
        out = out.rename(columns={"Value": "volume_mcm"})
        return out.dropna(subset=["volume_mcm"]).sort_values("date").reset_index(drop=True)

    def _fetch_storage_aggregate(
        self,
        id_list: list[str],
        start: str | date,
        end: str | date | None,
    ) -> pd.DataFrame | None:
        ids = [PUBOB_IDS[k] for k in id_list if k in PUBOB_IDS]
        start_dt = date.fromisoformat(str(start))
        end_dt = date.fromisoformat(str(end)) if end else date.today()

        df = self._fetch_chunked(ids, start_dt, end_dt)
        if df is None:
            return None

        df = self._to_daily(df)
        out = df.groupby("date", as_index=False)["Value"].sum()
        out = out.rename(columns={"Value": "volume_mcm"})
        out["volume_mcm"] = kwh_to_mcm(out["volume_mcm"])
        return out.dropna(subset=["volume_mcm"]).sort_values("date").reset_index(drop=True)

    def get_storage_withdrawal(
        self,
        start: str | date = "2020-10-01",
        end: str | date | None = None,
    ) -> pd.DataFrame | None:
        """Sum of all UK storage outflows (mcm/d), converted from kWh."""
        return self._fetch_storage_aggregate(STORAGE_OUTFLOW_IDS, start, end)

    def get_storage_injection(
        self,
        start: str | date = "2020-10-01",
        end: str | date | None = None,
    ) -> pd.DataFrame | None:
        """Sum of all UK storage inflows (mcm/d), converted from kWh."""
        return self._fetch_storage_aggregate(STORAGE_INFLOW_IDS, start, end)
