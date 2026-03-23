"""Supply stack aggregator — combines all supply components."""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from src.base import StackComponent
from src.supply.ukcs import UKCSProduction
from src.supply.norway import NorwayPipelines
from src.supply.interconnectors import IUKImport, BBLPipeline
from src.supply.lng import LNGTerminals
from src.supply.storage_withdrawal import StorageWithdrawal


@dataclass
class SupplyStack:
    """Aggregate of all NBP supply-side components."""

    components: list[StackComponent] = field(default_factory=list, init=False)

    def __post_init__(self) -> None:
        self.components = [
            UKCSProduction(),
            NorwayPipelines(),
            IUKImport(),
            BBLPipeline(),
            LNGTerminals(),
            StorageWithdrawal(),
        ]

    def get_all(
        self,
        start: str | None = None,
        end: str | None = None,
    ) -> pd.DataFrame:
        """Return a single DataFrame with all supply components stacked."""
        frames = [c.get_data(start, end) for c in self.components]
        return pd.concat(frames, ignore_index=True)

    def get_total(
        self,
        start: str | None = None,
        end: str | None = None,
    ) -> pd.DataFrame:
        """Return daily total supply (sum across components)."""
        df = self.get_all(start, end)
        total = df.groupby("date", as_index=False)["volume_mcm"].sum()
        total["source"] = "Total Supply"
        return total

    def get_component(self, name: str) -> StackComponent | None:
        for c in self.components:
            if c.name.lower() == name.lower():
                return c
        return None

    def summary(self, start: str | None = None, end: str | None = None) -> pd.DataFrame:
        """Average daily volume per component over the period."""
        df = self.get_all(start, end)
        return (
            df.groupby("source", as_index=False)
            .agg(
                avg_mcm=("volume_mcm", "mean"),
                min_mcm=("volume_mcm", "min"),
                max_mcm=("volume_mcm", "max"),
                data_quality=("data_quality", "first"),
            )
            .sort_values("avg_mcm", ascending=False)
        )
