"""Basic tests for the demand stack."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from src.demand.demand_stack import DemandStack


def test_demand_stack_loads():
    stack = DemandStack()
    df = stack.get_all()
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert {"date", "volume_mcm", "source", "data_quality"}.issubset(df.columns)


def test_demand_total():
    stack = DemandStack()
    total = stack.get_total()
    assert "volume_mcm" in total.columns
    assert (total["volume_mcm"] >= 0).all()


def test_demand_summary():
    stack = DemandStack()
    summary = stack.summary()
    assert len(summary) == 6


def test_balance_engine():
    from src.balance.balance_engine import BalanceEngine
    engine = BalanceEngine()
    bal = engine.daily_balance()
    assert {"date", "total_supply", "total_demand", "balance_mcm", "status"}.issubset(bal.columns)
    assert len(bal) > 0


def test_scenario_engine():
    from src.scenarios.scenario_engine import ScenarioEngine
    sc_engine = ScenarioEngine()
    cold = ScenarioEngine.cold_snap()
    result = sc_engine.apply(cold)
    assert "scenario" in result.columns
    assert len(result) > 0


if __name__ == "__main__":
    test_demand_stack_loads()
    test_demand_total()
    test_demand_summary()
    test_balance_engine()
    test_scenario_engine()
    print("All demand/balance/scenario tests passed.")
