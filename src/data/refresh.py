"""Standalone data refresh script.

Fetches the latest data from all configured APIs, saves to the local
parquet cache in data/raw/, and reports what succeeded and what failed.

Usage:
    python -m src.data.refresh              # one-shot refresh
    python -m src.data.refresh --loop 24    # repeat every 24 hours
    python -m src.data.refresh --loop 1     # repeat every 1 hour

Scheduling (Windows Task Scheduler):
    Action:   python -m src.data.refresh
    Start in: your project folder
    Trigger:  every 24 hours
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

# Ensure project root on path
_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.data import cache
from src.config import get
from src.data.gie_api import GIEClient
from src.data.elexon_api import ElexonClient
from src.data.national_gas import NationalGasClient
from src.units import gwh_to_mcm

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("refresh")


def _refresh_elexon() -> dict[str, str]:
    """Fetch CCGT generation from Elexon BMRS (no auth)."""
    results = {}
    try:
        client = ElexonClient()
        df = client.get_ccgt_generation()
        if df is not None and not df.empty:
            df["source"] = "Power Gen"
            df["data_quality"] = "api"
            cache.save("CCGT Power Gen", df)
            results["CCGT Power Gen"] = f"OK ({len(df)} rows)"
        else:
            results["CCGT Power Gen"] = "EMPTY"
    except Exception as exc:
        results["CCGT Power Gen"] = f"FAIL: {exc}"
    return results


def _refresh_gie() -> dict[str, str]:
    """Fetch storage + LNG from GIE AGSI+/ALSI."""
    results = {}
    api_key = get("api_keys.gie", "")
    client = GIEClient(api_key=api_key)

    # AGSI+ — storage
    try:
        df = client.get_storage(country="GB")
        if df is not None and not df.empty:
            # Withdrawal
            if "withdrawal_mcm" in df.columns:
                wd = df[["date", "withdrawal_mcm"]].copy()
                wd = wd.rename(columns={"withdrawal_mcm": "volume_mcm"})
                wd["volume_mcm"] = wd["volume_mcm"].fillna(0).clip(lower=0)
                wd["source"] = "Storage Withdrawal"
                wd["data_quality"] = "api"
                cache.save("Storage Withdrawal", wd)
                results["Storage Withdrawal"] = f"OK ({len(wd)} rows)"

            # Injection
            if "injection_mcm" in df.columns:
                inj = df[["date", "injection_mcm"]].copy()
                inj = inj.rename(columns={"injection_mcm": "volume_mcm"})
                inj["volume_mcm"] = inj["volume_mcm"].fillna(0).clip(lower=0)
                inj["source"] = "Storage Injection"
                inj["data_quality"] = "api"
                cache.save("Storage Injection", inj)
                results["Storage Injection"] = f"OK ({len(inj)} rows)"
        else:
            results["Storage (AGSI+)"] = "EMPTY"
    except Exception as exc:
        results["Storage (AGSI+)"] = f"FAIL: {exc}"

    # ALSI — LNG
    try:
        df = client.get_lng_sendout(country="GB")
        if df is not None and not df.empty and "volume_mcm" in df.columns:
            lng = df[["date", "volume_mcm"]].copy()
            lng["volume_mcm"] = lng["volume_mcm"].fillna(0).clip(lower=0)
            lng["source"] = "LNG"
            lng["data_quality"] = "api"
            cache.save("LNG Terminals", lng)
            results["LNG Terminals"] = f"OK ({len(lng)} rows)"
        else:
            results["LNG Terminals"] = "EMPTY"
    except Exception as exc:
        results["LNG Terminals"] = f"FAIL: {exc}"

    return results


def _refresh_national_gas() -> dict[str, str]:
    """Fetch all National Gas data via CSV download API (no auth required)."""
    results = {}
    client = NationalGasClient()

    # Interconnector physical flows
    for point, component in [
        ("IUK", "IUK Import"),
        ("BBL", "BBL Pipeline"),
        ("Langeled", "Norwegian Pipelines"),
        ("Moffat", "Moffat Export"),
    ]:
        try:
            df = client.get_physical_flows(point)
            if df is not None and not df.empty:
                df["data_quality"] = "api"
                cache.save(component, df)
                results[component] = f"OK ({len(df)} rows)"
            else:
                results[component] = "EMPTY"
        except Exception as exc:
            results[component] = f"FAIL: {exc}"

    # UKCS entry volumes (aggregated from terminal entry points)
    try:
        df = client.get_ukcs_production()
        if df is not None and not df.empty:
            df["data_quality"] = "api"
            cache.save("UKCS Production", df)
            results["UKCS Production"] = f"OK ({len(df)} rows)"
        else:
            results["UKCS Production"] = "EMPTY"
    except Exception as exc:
        results["UKCS Production"] = f"FAIL: {exc}"

    # LNG entry volumes
    try:
        df = client.get_lng_entry_volumes()
        if df is not None and not df.empty:
            df["data_quality"] = "api"
            cache.save("LNG Terminals", df)
            results["LNG Terminals (NatGas)"] = f"OK ({len(df)} rows)"
        else:
            results["LNG Terminals (NatGas)"] = "EMPTY"
    except Exception as exc:
        results["LNG Terminals (NatGas)"] = f"FAIL: {exc}"

    # Storage
    for fetch_fn, component in [
        (client.get_storage_withdrawal, "Storage Withdrawal"),
        (client.get_storage_injection, "Storage Injection"),
    ]:
        try:
            df = fetch_fn()
            if df is not None and not df.empty:
                df["data_quality"] = "api"
                cache.save(component, df)
                results[component] = f"OK ({len(df)} rows)"
            else:
                results[component] = "EMPTY"
        except Exception as exc:
            results[component] = f"FAIL: {exc}"

    # IUK Export (same endpoint as IUK Import, cached separately)
    try:
        df = client.get_physical_flows("IUK")
        if df is not None and not df.empty:
            df["data_quality"] = "api"
            cache.save("IUK Export", df)
            results["IUK Export"] = f"OK ({len(df)} rows)"
        else:
            results["IUK Export"] = "EMPTY"
    except Exception as exc:
        results["IUK Export"] = f"FAIL: {exc}"

    # Demand
    for dtype, component in [
        ("LDZ", "Residential/Commercial"),
        ("DM", "Industrial"),
        ("NTS", "NTS Demand Total"),
    ]:
        try:
            df = client.get_demand(dtype)
            if df is not None and not df.empty:
                df["data_quality"] = "api"
                cache.save(component, df)
                results[component] = f"OK ({len(df)} rows)"
            else:
                results[component] = "EMPTY"
        except Exception as exc:
            results[component] = f"FAIL: {exc}"

    return results


def refresh_all() -> dict[str, str]:
    """Run all data refreshes and return a status dict."""
    logger.info("=" * 60)
    logger.info("DATA REFRESH STARTED at %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("=" * 60)

    results: dict[str, str] = {}
    results.update(_refresh_elexon())
    results.update(_refresh_gie())
    results.update(_refresh_national_gas())

    logger.info("-" * 60)
    logger.info("REFRESH SUMMARY:")
    for component, status in results.items():
        icon = "+" if status.startswith("OK") else ("-" if "MANUAL" in status or "NOT CONFIGURED" in status else "!")
        logger.info("  [%s] %s: %s", icon, component, status)
    logger.info("=" * 60)

    return results


def main():
    parser = argparse.ArgumentParser(description="Refresh NBP gas data from APIs")
    parser.add_argument(
        "--loop", type=float, default=None,
        help="If set, repeat every N hours (e.g. --loop 1 for hourly)",
    )
    args = parser.parse_args()

    if args.loop is None:
        refresh_all()
    else:
        interval_secs = args.loop * 3600
        logger.info("Running refresh loop every %.1f hours (%.0f seconds)", args.loop, interval_secs)
        while True:
            refresh_all()
            logger.info("Next refresh in %.1f hours. Press Ctrl+C to stop.", args.loop)
            time.sleep(interval_secs)


if __name__ == "__main__":
    main()
