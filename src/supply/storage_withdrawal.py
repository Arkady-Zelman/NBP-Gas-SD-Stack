"""UK gas storage withdrawal component.

Primary data source: National Gas storage outflows (9 facilities, daily kWh→mcm).
Fallback: GIE AGSI+ API (https://agsi.gie.eu/).
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
class StorageWithdrawal(StackComponent):
    name: str = "Storage Withdrawal"
    source_label: str = "Storage Withdrawal"
    default_quality: str = "api"

    _loader: DataLoader = field(default_factory=DataLoader, repr=False, init=False)

    def _api_fetch(self) -> pd.DataFrame | None:
        ng = NationalGasClient()
        df = ng.get_storage_withdrawal()
        if df is not None and not df.empty:
            return df

        client = GIEClient(api_key=get("api_keys.gie", ""))
        df = client.get_storage(country="GB")
        if df is None or df.empty:
            return None
        if "withdrawal_mcm" in df.columns:
            out = df[["date", "withdrawal_mcm"]].copy()
            out = out.rename(columns={"withdrawal_mcm": "volume_mcm"})
        elif "withdrawal" in df.columns:
            out = df[["date", "withdrawal"]].copy()
            out = out.rename(columns={"withdrawal": "volume_mcm"})
        else:
            return None
        out["volume_mcm"] = out["volume_mcm"].fillna(0).clip(lower=0)
        return out

    def _fetch_data(self) -> pd.DataFrame:
        return self._loader.load(
            component_name=self.name,
            api_fn=self._api_fetch,
            manual_csv="storage_withdrawal.csv",
            dummy_fn=DummyDataGenerator.storage_withdrawal,
        )
