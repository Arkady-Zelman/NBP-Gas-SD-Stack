"""Unified data loading interface.

Waterfall: cache → API → manual CSV → dummy data.
When an API succeeds, the result is cached to disk automatically.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Callable

import pandas as pd

from src.data import cache
from src.data.dummy_data import DummyDataGenerator
from src.data.manual_input import ManualInputReader

logger = logging.getLogger(__name__)


class DataLoader:
    """Orchestrate the data-source waterfall for any component."""

    def __init__(self, manual_folder: str | Path | None = None):
        self.manual = ManualInputReader(manual_folder)

    def load(
        self,
        component_name: str,
        api_fn: Callable[[], pd.DataFrame | None] | None = None,
        manual_csv: str | None = None,
        dummy_fn: Callable[[], pd.DataFrame] | None = None,
        cache_max_age_hours: float | None = None,
    ) -> pd.DataFrame:
        """Try cache → API → manual CSV → dummy."""

        # 0. Try cache
        cached = cache.load(component_name, max_age_hours=cache_max_age_hours)
        if cached is not None and not cached.empty:
            return cached

        # 1. Try API
        if api_fn is not None:
            try:
                df = api_fn()
                if df is not None and not df.empty:
                    df["data_quality"] = "api"
                    cache.save(component_name, df)
                    logger.info("%s: loaded from API (%d rows)", component_name, len(df))
                    return df
            except Exception as exc:
                logger.warning("%s: API failed — %s", component_name, exc)

        # 2. Try manual CSV
        if manual_csv is not None:
            try:
                df = self.manual.read(manual_csv, source_label=component_name)
                if not df.empty:
                    logger.info("%s: loaded from manual CSV '%s'", component_name, manual_csv)
                    return df
                logger.info("%s: manual CSV '%s' is empty, skipping", component_name, manual_csv)
            except FileNotFoundError:
                logger.info("%s: manual CSV '%s' not found", component_name, manual_csv)

        # 3. Fall back to dummy
        if dummy_fn is not None:
            df = dummy_fn()
            logger.info("%s: using DUMMY data", component_name)
            return df

        raise RuntimeError(f"{component_name}: no data source available")
