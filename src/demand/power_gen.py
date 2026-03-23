"""CCGT gas-for-power demand component.

Data source: Elexon / BMRS half-hourly CCGT generation, aggregated to
daily averages and converted from MW to mcm/d.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from src.base import StackComponent
from src.data.dummy_data import DummyDataGenerator
from src.data.loaders import DataLoader
from src.data.elexon_api import ElexonClient
from src.config import get


@dataclass
class PowerGenDemand(StackComponent):
    name: str = "CCGT Power Gen"
    source_label: str = "Power Gen"
    default_quality: str = "dummy"

    _loader: DataLoader = field(default_factory=DataLoader, repr=False, init=False)

    def _api_fetch(self) -> pd.DataFrame | None:
        client = ElexonClient(api_key=get("api_keys.elexon", ""))
        return client.get_ccgt_generation()

    def _fetch_data(self) -> pd.DataFrame:
        return self._loader.load(
            component_name=self.name,
            api_fn=self._api_fetch,
            manual_csv="power_gen_demand.csv",
            dummy_fn=DummyDataGenerator.power_gen,
        )
