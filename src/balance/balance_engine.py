"""S&D balance engine — supply minus demand, linepack change, surplus/deficit."""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from src.supply.supply_stack import SupplyStack
from src.demand.demand_stack import DemandStack


@dataclass
class BalanceEngine:
    """Compute the daily NBP supply–demand balance."""

    supply: SupplyStack = field(default_factory=SupplyStack)
    demand: DemandStack = field(default_factory=DemandStack)

    def daily_balance(
        self,
        start: str | None = None,
        end: str | None = None,
    ) -> pd.DataFrame:
        """Return a DataFrame with daily supply, demand, balance, and status.

        Columns: date, total_supply, total_demand, balance_mcm, status
        ``balance_mcm`` = supply - demand (positive = surplus / linepack build).
        """
        supply_total = self.supply.get_total(start, end).rename(
            columns={"volume_mcm": "total_supply"}
        )[["date", "total_supply"]]
        demand_total = self.demand.get_total(start, end).rename(
            columns={"volume_mcm": "total_demand"}
        )[["date", "total_demand"]]

        df = pd.merge(supply_total, demand_total, on="date", how="outer").sort_values("date")
        df["total_supply"] = df["total_supply"].fillna(0)
        df["total_demand"] = df["total_demand"].fillna(0)
        df["balance_mcm"] = df["total_supply"] - df["total_demand"]
        df["status"] = df["balance_mcm"].apply(
            lambda b: "surplus" if b > 2 else ("deficit" if b < -2 else "balanced")
        )
        return df.reset_index(drop=True)

    def component_breakdown(
        self,
        start: str | None = None,
        end: str | None = None,
    ) -> pd.DataFrame:
        """Return all supply and demand components labelled with side."""
        supply_all = self.supply.get_all(start, end)
        supply_all["side"] = "supply"
        demand_all = self.demand.get_all(start, end)
        demand_all["side"] = "demand"
        return pd.concat([supply_all, demand_all], ignore_index=True)

    def summary_stats(
        self,
        start: str | None = None,
        end: str | None = None,
    ) -> dict:
        """Aggregate stats for the period."""
        bal = self.daily_balance(start, end)
        return {
            "avg_supply_mcm": bal["total_supply"].mean(),
            "avg_demand_mcm": bal["total_demand"].mean(),
            "avg_balance_mcm": bal["balance_mcm"].mean(),
            "days_surplus": (bal["status"] == "surplus").sum(),
            "days_deficit": (bal["status"] == "deficit").sum(),
            "days_balanced": (bal["status"] == "balanced").sum(),
            "max_surplus_mcm": bal["balance_mcm"].max(),
            "max_deficit_mcm": bal["balance_mcm"].min(),
        }
