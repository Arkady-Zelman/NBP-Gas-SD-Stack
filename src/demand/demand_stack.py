"""Demand stack aggregator — combines all demand components."""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from src.base import StackComponent
from src.demand.residential import ResidentialDemand
from src.demand.industrial import IndustrialDemand
from src.demand.power_gen import PowerGenDemand
from src.demand.exports import IUKExport, MoffatExport
from src.demand.storage_injection import StorageInjection


@dataclass
class DemandStack:
    """Aggregate of all NBP demand-side components."""

    components: list[StackComponent] = field(default_factory=list, init=False)

    def __post_init__(self) -> None:
        self.components = [
            ResidentialDemand(),
            IndustrialDemand(),
            PowerGenDemand(),
            IUKExport(),
            MoffatExport(),
            StorageInjection(),
        ]

    def get_all(
        self,
        start: str | None = None,
        end: str | None = None,
    ) -> pd.DataFrame:
        """Return a single DataFrame with all demand components stacked."""
        frames = [c.get_data(start, end) for c in self.components]
        return pd.concat(frames, ignore_index=True)

    def get_total(
        self,
        start: str | None = None,
        end: str | None = None,
    ) -> pd.DataFrame:
        """Return daily total demand (sum across components)."""
        df = self.get_all(start, end)
        total = df.groupby("date", as_index=False)["volume_mcm"].sum()
        total["source"] = "Total Demand"
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
