"""Reader for manual CSV input files in data/manual/.

Expected CSV format (minimum columns):
    date,volume_mcm
    2024-01-01,95.2
    2024-01-02,94.8
    ...

Additional columns are preserved.  The ``data_quality`` column is set to
"manual" if not already present.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.config import get


class ManualInputReader:
    """Load a component's data from a CSV in the manual data folder."""

    def __init__(self, folder: str | Path | None = None):
        self.folder = Path(folder) if folder else Path(get("data_paths.manual", "data/manual"))

    def read(self, filename: str, source_label: str = "") -> pd.DataFrame:
        path = self.folder / filename
        if not path.exists():
            raise FileNotFoundError(
                f"Manual input file not found: {path}\n"
                f"Create a CSV with at least 'date' and 'volume_mcm' columns."
            )
        df = pd.read_csv(path, parse_dates=["date"])
        if "volume_mcm" not in df.columns:
            raise ValueError(f"CSV {path} must contain a 'volume_mcm' column.")
        if "source" not in df.columns and source_label:
            df["source"] = source_label
        if "data_quality" not in df.columns:
            df["data_quality"] = "manual"
        return df

    def list_files(self) -> list[str]:
        if not self.folder.exists():
            return []
        return [f.name for f in self.folder.glob("*.csv")]
