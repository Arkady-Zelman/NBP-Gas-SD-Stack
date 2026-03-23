"""Base classes for all stack components."""

from __future__ import annotations

import abc
from dataclasses import dataclass, field
from typing import Literal

import pandas as pd

DataQuality = Literal["api", "manual", "dummy", "forecast"]


@dataclass
class StackComponent(abc.ABC):
    """Abstract base for every supply or demand component.

    Subclasses must implement ``_fetch_data`` which returns a DataFrame
    with at least columns ``date`` (datetime) and ``volume_mcm`` (float).
    The base class adds ``source`` and ``data_quality`` columns automatically.
    """

    name: str
    source_label: str = ""
    default_quality: DataQuality = "dummy"
    _data: pd.DataFrame = field(default_factory=pd.DataFrame, repr=False, init=False)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_data(
        self,
        start: str | pd.Timestamp | None = None,
        end: str | pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        """Return component data, optionally filtered to a date window."""
        if self._data.empty:
            self._data = self._build()
        df = self._data.copy()
        if start is not None:
            df = df[df["date"] >= pd.Timestamp(start)]
        if end is not None:
            df = df[df["date"] <= pd.Timestamp(end)]
        return df.reset_index(drop=True)

    def refresh(self) -> None:
        """Force re-fetch of underlying data."""
        self._data = self._build()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _build(self) -> pd.DataFrame:
        df = self._fetch_data()
        required = {"date", "volume_mcm"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"{self.name}: _fetch_data must return columns {missing}")
        df["date"] = pd.to_datetime(df["date"])
        df["source"] = self.source_label or self.name
        if "data_quality" not in df.columns:
            df["data_quality"] = self.default_quality
        return df.sort_values("date").reset_index(drop=True)

    @abc.abstractmethod
    def _fetch_data(self) -> pd.DataFrame:
        """Return raw DataFrame with ``date`` and ``volume_mcm``."""
        ...
