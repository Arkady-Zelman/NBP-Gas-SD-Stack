"""Basic tests for the supply stack."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from src.supply.supply_stack import SupplyStack


def test_supply_stack_loads():
    stack = SupplyStack()
    df = stack.get_all()
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert {"date", "volume_mcm", "source", "data_quality"}.issubset(df.columns)


def test_supply_total():
    stack = SupplyStack()
    total = stack.get_total()
    assert "volume_mcm" in total.columns
    assert (total["volume_mcm"] >= 0).all()


def test_supply_summary():
    stack = SupplyStack()
    summary = stack.summary()
    assert len(summary) == 6


def test_supply_components_have_data_quality():
    stack = SupplyStack()
    df = stack.get_all()
    valid = {"api", "manual", "dummy", "forecast"}
    assert df["data_quality"].isin(valid).all()


if __name__ == "__main__":
    test_supply_stack_loads()
    test_supply_total()
    test_supply_summary()
    test_supply_components_have_data_quality()
    print("All supply tests passed.")
