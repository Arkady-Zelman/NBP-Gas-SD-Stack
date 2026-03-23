"""UKCS (UK Continental Shelf) gas production component.

Primary data source: National Gas entry volumes from UKCS terminals
(Easington, Bacton UKCS operators, St Fergus, Teesside, Barrow).
These are daily NTS entry volumes in mcm/d — no API key required.

Fallback: manual CSV from NSTA (nstauthority.co.uk/data-centre/).
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from src.base import StackComponent
from src.data.dummy_data import DummyDataGenerator
from src.data.loaders import DataLoader
from src.data.national_gas import NationalGasClient


@dataclass
class UKCSProduction(StackComponent):
    name: str = "UKCS Production"
    source_label: str = "UKCS"
    default_quality: str = "api"

    _loader: DataLoader = field(default_factory=DataLoader, repr=False, init=False)

    def _api_fetch(self) -> pd.DataFrame | None:
        client = NationalGasClient()
        return client.get_ukcs_production()

    def _fetch_data(self) -> pd.DataFrame:
        return self._loader.load(
            component_name=self.name,
            api_fn=self._api_fetch,
            manual_csv="ukcs_production.csv",
            dummy_fn=DummyDataGenerator.ukcs_production,
        )
