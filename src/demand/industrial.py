"""Industrial (DM + LDM) gas demand component.

Data source: partially National Gas API (DM/LDM categories), partially
manual download.  Falls back to dummy data.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from src.base import StackComponent
from src.data.dummy_data import DummyDataGenerator
from src.data.loaders import DataLoader
from src.data.national_gas import NationalGasClient
from src.config import get


@dataclass
class IndustrialDemand(StackComponent):
    name: str = "Industrial"
    source_label: str = "Industrial"
    default_quality: str = "dummy"

    _loader: DataLoader = field(default_factory=DataLoader, repr=False, init=False)

    def _api_fetch(self) -> pd.DataFrame | None:
        client = NationalGasClient(api_key=get("api_keys.national_gas", ""))
        dm = client.get_demand(demand_type="DM")
        ldm = client.get_demand(demand_type="LDM")
        frames = [f for f in (dm, ldm) if f is not None and not f.empty]
        if not frames:
            return None
        combined = pd.concat(frames).groupby("date", as_index=False)["volume_mcm"].sum()
        return combined

    def _fetch_data(self) -> pd.DataFrame:
        return self._loader.load(
            component_name=self.name,
            api_fn=self._api_fetch,
            manual_csv="industrial_demand.csv",
            dummy_fn=DummyDataGenerator.industrial,
        )
