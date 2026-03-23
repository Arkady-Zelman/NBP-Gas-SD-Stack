"""Norwegian pipeline imports (Langeled + Vesterled).

Data source: Gassco Transparency portal
(https://www.gassco.no/en/our-activities/transparency/).

Falls back to dummy data if no API/manual data available.
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
class NorwayPipelines(StackComponent):
    name: str = "Norwegian Pipelines"
    source_label: str = "Norway"
    default_quality: str = "dummy"

    _loader: DataLoader = field(default_factory=DataLoader, repr=False, init=False)

    def _api_fetch(self) -> pd.DataFrame | None:
        client = NationalGasClient(api_key=get("api_keys.national_gas", ""))
        df = client.get_physical_flows("Langeled")
        if df is None:
            return None
        df_v = client.get_physical_flows("Vesterled")
        if df_v is not None and not df_v.empty:
            combined = pd.concat([df, df_v]).groupby("date", as_index=False)["volume_mcm"].sum()
            return combined
        return df

    def _fetch_data(self) -> pd.DataFrame:
        return self._loader.load(
            component_name=self.name,
            api_fn=self._api_fetch,
            manual_csv="norway_pipelines.csv",
            dummy_fn=DummyDataGenerator.norway_pipelines,
        )
