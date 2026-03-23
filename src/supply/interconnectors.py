"""IUK and BBL interconnector import components.

IUK (Interconnector UK): bidirectional Bacton–Zeebrugge, capacity ~25.5 mcm/d.
BBL (Balgzand-Bacton Line): uni-directional NL→UK, capacity ~15 mcm/d.

Data source: National Gas Data Portal (https://data.nationalgas.com/).
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
class IUKImport(StackComponent):
    name: str = "IUK Import"
    source_label: str = "IUK Import"
    default_quality: str = "dummy"

    _loader: DataLoader = field(default_factory=DataLoader, repr=False, init=False)

    def _api_fetch(self) -> pd.DataFrame | None:
        client = NationalGasClient(api_key=get("api_keys.national_gas", ""))
        return client.get_physical_flows("IUK")

    def _fetch_data(self) -> pd.DataFrame:
        return self._loader.load(
            component_name=self.name,
            api_fn=self._api_fetch,
            manual_csv="iuk_import.csv",
            dummy_fn=DummyDataGenerator.iuk_import,
        )


@dataclass
class BBLPipeline(StackComponent):
    name: str = "BBL Pipeline"
    source_label: str = "BBL"
    default_quality: str = "dummy"

    _loader: DataLoader = field(default_factory=DataLoader, repr=False, init=False)

    def _api_fetch(self) -> pd.DataFrame | None:
        client = NationalGasClient(api_key=get("api_keys.national_gas", ""))
        return client.get_physical_flows("BBL")

    def _fetch_data(self) -> pd.DataFrame:
        return self._loader.load(
            component_name=self.name,
            api_fn=self._api_fetch,
            manual_csv="bbl_pipeline.csv",
            dummy_fn=DummyDataGenerator.bbl_pipeline,
        )
