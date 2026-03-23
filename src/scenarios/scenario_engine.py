"""Scenario engine — apply what-if adjustments to the base S&D stack.

Pre-built templates:
  - Cold snap: demand shock via residential uplift + storage draw
  - LNG diversion: reduce LNG send-out by a percentage
  - Norwegian outage: zero a pipeline for N days
  - Interconnector reversal: flip IUK from import to export
  - Custom: user-defined multipliers on any component
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from typing import Callable

import numpy as np
import pandas as pd

from src.balance.balance_engine import BalanceEngine


@dataclass
class ScenarioAdjustment:
    """A single adjustment to apply to a component's volume series."""
    source_name: str
    side: str                # "supply" or "demand"
    description: str
    transform: Callable[[pd.Series], pd.Series]


@dataclass
class Scenario:
    """Named collection of adjustments."""
    name: str
    adjustments: list[ScenarioAdjustment] = field(default_factory=list)


class ScenarioEngine:
    """Apply scenarios to a BalanceEngine and compare results."""

    def __init__(self, base_engine: BalanceEngine | None = None):
        self.base = base_engine or BalanceEngine()

    def apply(
        self,
        scenario: Scenario,
        start: str | None = None,
        end: str | None = None,
    ) -> pd.DataFrame:
        """Run a scenario and return the adjusted daily balance."""
        breakdown = self.base.component_breakdown(start, end).copy()

        for adj in scenario.adjustments:
            mask = (breakdown["source"].str.lower() == adj.source_name.lower()) & (
                breakdown["side"] == adj.side
            )
            if mask.any():
                breakdown.loc[mask, "volume_mcm"] = adj.transform(
                    breakdown.loc[mask, "volume_mcm"]
                )

        supply = (
            breakdown[breakdown["side"] == "supply"]
            .groupby("date", as_index=False)["volume_mcm"]
            .sum()
            .rename(columns={"volume_mcm": "total_supply"})
        )
        demand = (
            breakdown[breakdown["side"] == "demand"]
            .groupby("date", as_index=False)["volume_mcm"]
            .sum()
            .rename(columns={"volume_mcm": "total_demand"})
        )

        df = pd.merge(supply, demand, on="date", how="outer").sort_values("date")
        df["total_supply"] = df["total_supply"].fillna(0)
        df["total_demand"] = df["total_demand"].fillna(0)
        df["balance_mcm"] = df["total_supply"] - df["total_demand"]
        df["scenario"] = scenario.name
        return df.reset_index(drop=True)

    def compare(
        self,
        scenarios: list[Scenario],
        start: str | None = None,
        end: str | None = None,
    ) -> pd.DataFrame:
        """Run multiple scenarios and return a combined DataFrame for comparison."""
        base_bal = self.base.daily_balance(start, end)
        base_bal["scenario"] = "Base Case"
        frames = [base_bal]
        for sc in scenarios:
            frames.append(self.apply(sc, start, end))
        return pd.concat(frames, ignore_index=True)

    # ==================================================================
    # Pre-built scenario templates
    # ==================================================================

    @staticmethod
    def cold_snap(
        demand_uplift_pct: float = 30.0,
        storage_draw_multiplier: float = 2.0,
        start_date: str = "2025-01-15",
        end_date: str = "2025-02-15",
    ) -> Scenario:
        """Prolonged cold spell: residential demand surges, storage draws harder."""
        start_ts, end_ts = pd.Timestamp(start_date), pd.Timestamp(end_date)

        def boost_residential(vol: pd.Series) -> pd.Series:
            return vol * (1 + demand_uplift_pct / 100)

        def boost_withdrawal(vol: pd.Series) -> pd.Series:
            return vol * storage_draw_multiplier

        return Scenario(
            name=f"Cold Snap ({start_date} to {end_date})",
            adjustments=[
                ScenarioAdjustment(
                    "Residential", "demand",
                    f"+{demand_uplift_pct}% residential demand",
                    boost_residential,
                ),
                ScenarioAdjustment(
                    "Storage Withdrawal", "supply",
                    f"{storage_draw_multiplier}x storage withdrawal",
                    boost_withdrawal,
                ),
            ],
        )

    @staticmethod
    def lng_diversion(reduction_pct: float = 50.0) -> Scenario:
        """Global LNG tightness diverts cargoes away from UK."""
        def reduce(vol: pd.Series) -> pd.Series:
            return vol * (1 - reduction_pct / 100)

        return Scenario(
            name=f"LNG Diversion (-{reduction_pct}%)",
            adjustments=[
                ScenarioAdjustment(
                    "LNG", "supply",
                    f"-{reduction_pct}% LNG send-out",
                    reduce,
                ),
            ],
        )

    @staticmethod
    def norwegian_outage(
        pipeline: str = "Norway",
        duration_days: int = 14,
        start_date: str = "2025-11-01",
    ) -> Scenario:
        """Unplanned Norwegian pipeline outage — zero flow for N days."""
        def zero_out(vol: pd.Series) -> pd.Series:
            return vol * 0

        return Scenario(
            name=f"Norwegian Outage ({duration_days}d from {start_date})",
            adjustments=[
                ScenarioAdjustment(
                    pipeline, "supply",
                    f"Zero flow for {duration_days} days",
                    zero_out,
                ),
            ],
        )

    @staticmethod
    def interconnector_reversal(export_volume_mcm: float = 20.0) -> Scenario:
        """IUK flips from import to full export — UK supplying continent."""
        def zero_import(vol: pd.Series) -> pd.Series:
            return vol * 0

        def set_export(vol: pd.Series) -> pd.Series:
            return pd.Series(np.full(len(vol), export_volume_mcm), index=vol.index)

        return Scenario(
            name=f"IUK Reversal (export {export_volume_mcm} mcm/d)",
            adjustments=[
                ScenarioAdjustment(
                    "IUK Import", "supply",
                    "Zero IUK imports",
                    zero_import,
                ),
                ScenarioAdjustment(
                    "IUK Export", "demand",
                    f"IUK export set to {export_volume_mcm} mcm/d",
                    set_export,
                ),
            ],
        )

    @staticmethod
    def custom(
        name: str,
        adjustments: list[dict],
    ) -> Scenario:
        """Build a custom scenario from a list of adjustment dicts.

        Each dict: {"source": str, "side": str, "multiplier": float}
        """
        adj_list = []
        for a in adjustments:
            mult = a["multiplier"]
            adj_list.append(
                ScenarioAdjustment(
                    source_name=a["source"],
                    side=a["side"],
                    description=f"×{mult}",
                    transform=lambda vol, m=mult: vol * m,
                )
            )
        return Scenario(name=name, adjustments=adj_list)
