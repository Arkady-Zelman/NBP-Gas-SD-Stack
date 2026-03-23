"""Roll up daily data to monthly / seasonal / annual granularity.

Gas Year runs Oct–Sep.  Seasons (quarters) follow the convention:
  Q1 = Oct–Dec, Q2 = Jan–Mar, Q3 = Apr–Jun, Q4 = Jul–Sep
"""

from __future__ import annotations

from typing import Literal

import numpy as np
import pandas as pd

Granularity = Literal["daily", "monthly", "seasonal", "annual"]


def _gas_year(dt: pd.Timestamp) -> int:
    """Return the gas-year label (start calendar year) for a date."""
    return dt.year if dt.month >= 10 else dt.year - 1


def _gas_quarter(dt: pd.Timestamp) -> str:
    m = dt.month
    if m in (10, 11, 12):
        return "Q1"
    if m in (1, 2, 3):
        return "Q2"
    if m in (4, 5, 6):
        return "Q3"
    return "Q4"


class TimeAggregator:
    """Aggregate a daily-resolution DataFrame to coarser horizons."""

    @staticmethod
    def aggregate(
        df: pd.DataFrame,
        granularity: Granularity = "monthly",
        value_col: str = "volume_mcm",
    ) -> pd.DataFrame:
        if granularity == "daily":
            out = df.copy()
            out["granularity"] = "daily"
            return out

        df = df.copy()
        df["date"] = pd.to_datetime(df["date"])
        df["gas_year"] = df["date"].apply(_gas_year)

        if granularity == "monthly":
            df["period"] = df["date"].dt.to_period("M").astype(str)
            group_cols = ["gas_year", "period", "source"]

        elif granularity == "seasonal":
            df["gas_quarter"] = df["date"].apply(_gas_quarter)
            df["period"] = df["gas_year"].astype(str) + " " + df["gas_quarter"]
            group_cols = ["gas_year", "period", "source"]

        elif granularity == "annual":
            df["period"] = df["gas_year"].astype(str) + "/" + (df["gas_year"] + 1).astype(str)
            group_cols = ["gas_year", "period", "source"]

        else:
            raise ValueError(f"Unknown granularity: {granularity}")

        agg = (
            df.groupby(group_cols, as_index=False)
            .agg(
                avg_daily_mcm=(value_col, "mean"),
                total_mcm=(value_col, "sum"),
                days=("date", "count"),
                date_start=("date", "min"),
                date_end=("date", "max"),
            )
        )
        agg["granularity"] = granularity
        return agg

    @staticmethod
    def multi_horizon(
        df: pd.DataFrame,
        value_col: str = "volume_mcm",
    ) -> dict[Granularity, pd.DataFrame]:
        """Return all four horizons in a dict keyed by granularity."""
        return {
            g: TimeAggregator.aggregate(df, g, value_col)
            for g in ("daily", "monthly", "seasonal", "annual")
        }
