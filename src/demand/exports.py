"""Export demand components — IUK exports and Moffat (Ireland) exports.

IUK is bidirectional; when net flow is Bacton→Zeebrugge it counts as
demand.  Moffat is always an export (demand-side).

Data source: National Gas Data Portal.
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
class IUKExport(StackComponent):
    name: str = "IUK Export"
    source_label: str = "IUK Export"
    default_quality: str = "dummy"

    _loader: DataLoader = field(default_factory=DataLoader, repr=False, init=False)

    def _api_fetch(self) -> pd.DataFrame | None:
        client = NationalGasClient(api_key=get("api_keys.national_gas", ""))
        return client.get_physical_flows("IUK")

    def _fetch_data(self) -> pd.DataFrame:
        return self._loader.load(
            component_name=self.name,
            api_fn=self._api_fetch,
            manual_csv="iuk_export.csv",
            dummy_fn=DummyDataGenerator.iuk_export,
        )


@dataclass
class MoffatExport(StackComponent):
    name: str = "Moffat Export"
    source_label: str = "Moffat Export"
    default_quality: str = "dummy"

    _loader: DataLoader = field(default_factory=DataLoader, repr=False, init=False)

    def _api_fetch(self) -> pd.DataFrame | None:
        client = NationalGasClient(api_key=get("api_keys.national_gas", ""))
        return client.get_physical_flows("Moffat")

    def _fetch_data(self) -> pd.DataFrame:
        return self._loader.load(
            component_name=self.name,
            api_fn=self._api_fetch,
            manual_csv="moffat_export.csv",
            dummy_fn=DummyDataGenerator.moffat_export,
        )
