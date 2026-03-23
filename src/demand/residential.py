"""Residential / commercial (LDZ) gas demand component.

Data source: National Gas LDZ actual daily demand API.
For forecasting: CWV (Composite Weather Variable) regression model.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from src.base import StackComponent
from src.data.dummy_data import DummyDataGenerator
from src.data.loaders import DataLoader
from src.data.national_gas import NationalGasClient
from src.config import get


@dataclass
class ResidentialDemand(StackComponent):
    name: str = "Residential/Commercial"
    source_label: str = "Residential"
    default_quality: str = "dummy"

    _loader: DataLoader = field(default_factory=DataLoader, repr=False, init=False)

    def _api_fetch(self) -> pd.DataFrame | None:
        client = NationalGasClient(api_key=get("api_keys.national_gas", ""))
        return client.get_demand(demand_type="LDZ")

    def _fetch_data(self) -> pd.DataFrame:
        return self._loader.load(
            component_name=self.name,
            api_fn=self._api_fetch,
            manual_csv="residential_demand.csv",
            dummy_fn=DummyDataGenerator.residential,
        )

    # ------------------------------------------------------------------
    # CWV-based forecast helper
    # ------------------------------------------------------------------

    @staticmethod
    def forecast_from_cwv(
        cwv_series: pd.Series,
        dates: pd.DatetimeIndex,
        alpha: float = -6.5,
        beta: float = 200.0,
    ) -> pd.DataFrame:
        """Simple linear CWV regression: demand = alpha * CWV + beta.

        Coefficients are illustrative defaults calibrated roughly to
        National Gas seasonal normal demand curves.  Replace with fitted
        values from historical CWV vs LDZ regression.

        Parameters
        ----------
        cwv_series : temperature-equivalent composite weather variable (°C).
        alpha      : slope  (mcm/d per °C, negative → higher demand in cold).
        beta       : intercept (mcm/d at CWV=0).
        """
        demand = alpha * cwv_series + beta
        demand = np.maximum(demand, 20)
        return pd.DataFrame({
            "date": dates,
            "volume_mcm": demand,
            "source": "Residential",
            "data_quality": "forecast",
        })
