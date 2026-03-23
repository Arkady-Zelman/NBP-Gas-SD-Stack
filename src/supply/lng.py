"""LNG terminal send-out component (South Hook, Dragon, Isle of Grain).

Primary data source: National Gas entry volumes (mcm/d) — no API key.
Fallback: GIE ALSI API (https://alsi.gie.eu/) — free but sometimes slow.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from src.base import StackComponent
from src.data.dummy_data import DummyDataGenerator
from src.data.loaders import DataLoader
from src.data.national_gas import NationalGasClient
from src.data.gie_api import GIEClient
from src.config import get


@dataclass
class LNGTerminals(StackComponent):
    name: str = "LNG Terminals"
    source_label: str = "LNG"
    default_quality: str = "api"

    _loader: DataLoader = field(default_factory=DataLoader, repr=False, init=False)

    def _api_fetch(self) -> pd.DataFrame | None:
        # Try National Gas first (faster, no auth)
        ng = NationalGasClient()
        df = ng.get_lng_entry_volumes()
        if df is not None and not df.empty:
            return df

        # Fallback to GIE ALSI
        client = GIEClient(api_key=get("api_keys.gie", ""))
        df = client.get_lng_sendout(country="GB")
        if df is None or df.empty:
            return None
        if "volume_mcm" not in df.columns:
            return None
        return df[["date", "volume_mcm"]]

    def _fetch_data(self) -> pd.DataFrame:
        return self._loader.load(
            component_name=self.name,
            api_fn=self._api_fetch,
            manual_csv="lng_terminals.csv",
            dummy_fn=DummyDataGenerator.lng_terminals,
        )
