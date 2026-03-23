"""Unit conversion utilities for gas volumes.

Base unit throughout the model is mcm/d (million cubic meters per day).
"""

from __future__ import annotations

import pandas as pd

from src.config import get

MCM_TO_GWH: float = get("project.mcm_to_gwh", 11.16)
MCM_TO_THERMS: float = get("project.mcm_to_therms", 375_694.0)


def mcm_to_gwh(mcm: float | pd.Series) -> float | pd.Series:
    return mcm * MCM_TO_GWH


def gwh_to_mcm(gwh: float | pd.Series) -> float | pd.Series:
    return gwh / MCM_TO_GWH


def mcm_to_therms(mcm: float | pd.Series) -> float | pd.Series:
    return mcm * MCM_TO_THERMS


def therms_to_mcm(therms: float | pd.Series) -> float | pd.Series:
    return therms / MCM_TO_THERMS


def mw_to_mcm(mw: float | pd.Series, efficiency: float = 0.49) -> float | pd.Series:
    """Convert electrical MW to mcm/d of gas input.

    Uses CCGT thermal efficiency (default 49 %) and standard CV.
    MW * 24h = MWh/d → * 3600 MJ/MWh → / efficiency → / CV_MJ_per_m3 = m³/d → / 1e6 = mcm/d
    """
    cv_mj_m3 = get("project.calorific_value", 39.5)
    gas_m3 = (mw * 24 * 3600) / (efficiency * cv_mj_m3)
    return gas_m3 / 1e6


def kwh_to_mcm(kwh: float | pd.Series) -> float | pd.Series:
    """Convert kWh to mcm.  1 mcm ≈ MCM_TO_GWH * 1e6 kWh."""
    return kwh / (MCM_TO_GWH * 1e6)


UNIT_CONVERTERS = {
    ("mcm", "gwh"): mcm_to_gwh,
    ("gwh", "mcm"): gwh_to_mcm,
    ("mcm", "therms"): mcm_to_therms,
    ("therms", "mcm"): therms_to_mcm,
}


def convert(value: float | pd.Series, from_unit: str, to_unit: str) -> float | pd.Series:
    """Generic converter dispatching to the correct function."""
    if from_unit == to_unit:
        return value
    key = (from_unit.lower(), to_unit.lower())
    fn = UNIT_CONVERTERS.get(key)
    if fn is None:
        raise ValueError(f"No converter registered for {from_unit} -> {to_unit}")
    return fn(value)
