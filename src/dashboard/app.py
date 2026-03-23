"""Streamlit dashboard for NBP Gas S&D Stack Model.

Run with:  python -m streamlit run src/dashboard/app.py --server.headless true
"""

from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.balance.balance_engine import BalanceEngine
from src.supply.supply_stack import SupplyStack
from src.demand.demand_stack import DemandStack
from src.aggregation.time_aggregator import TimeAggregator
from src.scenarios.scenario_engine import ScenarioEngine
from src.data.refresh import refresh_all
from src.data import cache
from src.units import convert

# =====================================================================
# Page config
# =====================================================================

st.set_page_config(
    page_title="NBP Gas S&D Stack",
    page_icon="🔥",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =====================================================================
# Smart auto-refresh: only if cache is stale (>24 hours) or missing
# =====================================================================

CACHE_MAX_AGE_HOURS = 24.0

def _cache_is_stale() -> bool:
    """Check if the main cache file is missing or older than the threshold."""
    age = cache.age_hours("UKCS Production")
    if age is None:
        return True
    return age > CACHE_MAX_AGE_HOURS


def _do_refresh():
    """Run the full API refresh and store results in session state."""
    st.session_state["refresh_results"] = refresh_all()
    st.session_state["last_refresh_age"] = 0.0
    st.cache_data.clear()


if "startup_checked" not in st.session_state:
    st.session_state["startup_checked"] = True
    if _cache_is_stale():
        with st.spinner("Cache is stale — refreshing from APIs (runs once per day)..."):
            _do_refresh()

# =====================================================================
# Cached data loaders
# =====================================================================

@st.cache_data(ttl=3600)
def _load_balance(start_str: str | None, end_str: str | None):
    engine = BalanceEngine()
    return engine, engine.daily_balance(start_str, end_str), engine.component_breakdown(start_str, end_str)


@st.cache_data(ttl=3600)
def _load_supply(start_str: str | None, end_str: str | None):
    stack = SupplyStack()
    return stack.get_all(start_str, end_str), stack.summary(start_str, end_str)


@st.cache_data(ttl=3600)
def _load_demand(start_str: str | None, end_str: str | None):
    stack = DemandStack()
    return stack.get_all(start_str, end_str), stack.summary(start_str, end_str)


# =====================================================================
# Sidebar
# =====================================================================

st.sidebar.title("NBP Gas S&D Stack")
page = st.sidebar.radio(
    "Navigate",
    ["Overview", "Supply Drill-down", "Demand Drill-down", "Scenarios", "Data Quality"],
)

st.sidebar.markdown("---")

st.sidebar.subheader("Date Range")
default_start = date.today() - timedelta(days=365)
default_end = date.today()
date_start = st.sidebar.date_input("From", value=default_start, key="d_start")
date_end = st.sidebar.date_input("To", value=default_end, key="d_end")
start_str = str(date_start)
end_str = str(date_end)

st.sidebar.markdown("---")
unit = st.sidebar.selectbox("Display unit", ["mcm/d", "GWh/d", "therms/d"], index=0)
granularity = st.sidebar.selectbox(
    "Time horizon", ["daily", "monthly", "seasonal", "annual"], index=0
)

st.sidebar.markdown("---")
if st.sidebar.button("🔄 Refresh Data Now", type="primary"):
    with st.spinner("Fetching latest data from APIs..."):
        cache.clear()
        _do_refresh()
    st.rerun()

cache_age = cache.age_hours("UKCS Production")
if cache_age is not None:
    if cache_age < 1:
        st.sidebar.caption(f"Data refreshed {cache_age * 60:.0f} min ago. Auto-refreshes daily.")
    elif cache_age < 24:
        st.sidebar.caption(f"Data is {cache_age:.1f}h old. Auto-refreshes daily.")
    else:
        st.sidebar.caption(f"Data is {cache_age:.0f}h old — will refresh on next load.")
else:
    st.sidebar.caption("No cached data. Click Refresh to fetch.")


def _convert_col(df: pd.DataFrame, col: str = "volume_mcm") -> pd.DataFrame:
    df = df.copy()
    if unit == "mcm/d":
        return df
    target = "gwh" if unit == "GWh/d" else "therms"
    df[col] = convert(df[col], "mcm", target)
    return df


def _value_label() -> str:
    return unit


# =====================================================================
# PAGE 1 — Overview
# =====================================================================

def page_overview():
    st.title("NBP Gas Supply & Demand — Overview")
    st.caption(f"Showing {date_start} to {date_end} — all data sourced from live APIs")

    _, balance_df, breakdown_df = _load_balance(start_str, end_str)

    if balance_df.empty:
        st.warning("No data in the selected date range.")
        return

    # KPI row
    avg_supply = balance_df["total_supply"].mean()
    avg_demand = balance_df["total_demand"].mean()
    avg_balance = balance_df["balance_mcm"].mean()
    days_deficit = int((balance_df["balance_mcm"] < -2).sum())

    cols = st.columns(4)
    for col_widget, (label, val) in zip(cols, [
        ("Avg Supply", avg_supply),
        ("Avg Demand", avg_demand),
        ("Avg Balance", avg_balance),
        ("Days in Deficit", days_deficit),
    ]):
        if label == "Days in Deficit":
            col_widget.metric(label, f"{val}")
        else:
            display = convert(val, "mcm", "gwh") if unit == "GWh/d" else val
            col_widget.metric(label, f"{display:,.1f} {_value_label()}")

    # Aggregated view
    supply_all = breakdown_df[breakdown_df["side"] == "supply"]
    demand_all = breakdown_df[breakdown_df["side"] == "demand"]

    if granularity != "daily":
        supply_all = TimeAggregator.aggregate(supply_all, granularity)
        demand_all = TimeAggregator.aggregate(demand_all, granularity)

    st.subheader("Supply vs Demand")

    if granularity == "daily":
        supply_all = _convert_col(supply_all)
        demand_all = _convert_col(demand_all)

        supply_pivot = supply_all.pivot_table(
            index="date", columns="source", values="volume_mcm", aggfunc="sum"
        ).fillna(0)
        demand_pivot = demand_all.pivot_table(
            index="date", columns="source", values="volume_mcm", aggfunc="sum"
        ).fillna(0)

        fig = go.Figure()
        supply_colors = px.colors.qualitative.Set2
        demand_colors = px.colors.qualitative.Pastel1
        for i, col_name in enumerate(supply_pivot.columns):
            fig.add_trace(go.Scatter(
                x=supply_pivot.index, y=supply_pivot[col_name],
                name=f"S: {col_name}", stackgroup="supply", line=dict(width=0),
                fillcolor=supply_colors[i % len(supply_colors)],
            ))
        for i, col_name in enumerate(demand_pivot.columns):
            fig.add_trace(go.Scatter(
                x=demand_pivot.index, y=-demand_pivot[col_name],
                name=f"D: {col_name}", stackgroup="demand", line=dict(width=0),
                fillcolor=demand_colors[i % len(demand_colors)],
            ))
        fig.update_layout(
            yaxis_title=_value_label(), height=550,
            legend=dict(orientation="h", y=-0.2),
            hovermode="x unified",
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        agg_col = "avg_daily_mcm"
        supply_all = _convert_col(supply_all, agg_col)
        demand_all = _convert_col(demand_all, agg_col)

        combined = pd.concat([
            supply_all.assign(side="Supply"),
            demand_all.assign(side="Demand"),
        ])
        fig = px.bar(
            combined, x="period", y=agg_col, color="source",
            facet_row="side", height=600,
            labels={agg_col: _value_label()},
        )
        st.plotly_chart(fig, use_container_width=True)

    # Balance bar chart
    st.subheader("Daily Balance (surplus / deficit)")
    bal = balance_df.copy()
    if unit != "mcm/d":
        target = "gwh" if unit == "GWh/d" else "therms"
        bal["balance_mcm"] = convert(bal["balance_mcm"], "mcm", target)

    colors = bal["balance_mcm"].apply(lambda x: "#2ecc71" if x >= 0 else "#e74c3c")
    fig_bal = go.Figure(go.Bar(x=bal["date"], y=bal["balance_mcm"], marker_color=colors))
    fig_bal.update_layout(yaxis_title=_value_label(), height=350, hovermode="x unified")
    st.plotly_chart(fig_bal, use_container_width=True)


# =====================================================================
# PAGE 2 — Supply Drill-down
# =====================================================================

def page_supply():
    st.title("Supply Stack — Drill-down")
    st.caption(f"Live data: {date_start} to {date_end}")

    all_supply, summary = _load_supply(start_str, end_str)
    all_supply = _convert_col(all_supply)

    st.subheader("Component Averages")
    display_summary = summary.copy()
    display_summary["data_quality"] = display_summary["data_quality"].map(
        {"api": "🟢 Live API", "manual": "🟡 Manual CSV", "dummy": "🔴 Dummy", "forecast": "🔵 Forecast"}
    ).fillna(display_summary["data_quality"])
    st.dataframe(
        display_summary.style.format({"avg_mcm": "{:.1f}", "min_mcm": "{:.1f}", "max_mcm": "{:.1f}"}),
        use_container_width=True,
        hide_index=True,
    )

    st.subheader("Component Timeseries")
    selected = st.multiselect(
        "Select components",
        all_supply["source"].unique().tolist(),
        default=all_supply["source"].unique().tolist(),
        key="supply_sel",
    )
    filtered = all_supply[all_supply["source"].isin(selected)]

    if granularity == "daily":
        fig = px.line(
            filtered, x="date", y="volume_mcm", color="source",
            labels={"volume_mcm": _value_label()}, height=500,
        )
    else:
        agg = TimeAggregator.aggregate(filtered, granularity)
        agg = _convert_col(agg, "avg_daily_mcm")
        fig = px.bar(
            agg, x="period", y="avg_daily_mcm", color="source",
            labels={"avg_daily_mcm": _value_label()}, height=500,
        )

    fig.update_layout(hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Supply Merit Order (average daily contribution)")
    avg = summary.sort_values("avg_mcm")
    fig_merit = px.bar(
        avg, x="avg_mcm", y="source", orientation="h",
        labels={"avg_mcm": _value_label(), "source": ""},
        color="source", height=350,
    )
    fig_merit.update_layout(showlegend=False)
    st.plotly_chart(fig_merit, use_container_width=True)


# =====================================================================
# PAGE 3 — Demand Drill-down
# =====================================================================

def page_demand():
    st.title("Demand Stack — Drill-down")
    st.caption(f"Live data: {date_start} to {date_end}")

    all_demand, summary = _load_demand(start_str, end_str)
    all_demand = _convert_col(all_demand)

    st.subheader("Component Averages")
    display_summary = summary.copy()
    display_summary["data_quality"] = display_summary["data_quality"].map(
        {"api": "🟢 Live API", "manual": "🟡 Manual CSV", "dummy": "🔴 Dummy", "forecast": "🔵 Forecast"}
    ).fillna(display_summary["data_quality"])
    st.dataframe(
        display_summary.style.format({"avg_mcm": "{:.1f}", "min_mcm": "{:.1f}", "max_mcm": "{:.1f}"}),
        use_container_width=True,
        hide_index=True,
    )

    st.subheader("Component Timeseries")
    selected = st.multiselect(
        "Select components",
        all_demand["source"].unique().tolist(),
        default=all_demand["source"].unique().tolist(),
        key="demand_sel",
    )
    filtered = all_demand[all_demand["source"].isin(selected)]

    if granularity == "daily":
        fig = px.line(
            filtered, x="date", y="volume_mcm", color="source",
            labels={"volume_mcm": _value_label()}, height=500,
        )
    else:
        agg = TimeAggregator.aggregate(filtered, granularity)
        agg = _convert_col(agg, "avg_daily_mcm")
        fig = px.bar(
            agg, x="period", y="avg_daily_mcm", color="source",
            labels={"avg_daily_mcm": _value_label()}, height=500,
        )

    fig.update_layout(hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Demand Share (period average)")
    fig_pie = px.pie(summary, values="avg_mcm", names="source", height=400)
    st.plotly_chart(fig_pie, use_container_width=True)


# =====================================================================
# PAGE 4 — Scenarios
# =====================================================================

def page_scenarios():
    st.title("Scenario Analysis")
    st.caption("Applies adjustments to real baseline data")

    engine = BalanceEngine()
    sc_engine = ScenarioEngine(engine)

    st.markdown("Select pre-built scenarios to compare against the Base Case.")

    col1, col2 = st.columns(2)

    with col1:
        run_cold = st.checkbox("Cold Snap", value=True)
        cold_uplift = st.slider("Residential demand uplift %", 10, 80, 30, key="cold") if run_cold else 30

    with col2:
        run_lng = st.checkbox("LNG Diversion", value=False)
        lng_cut = st.slider("LNG reduction %", 10, 100, 50, key="lng") if run_lng else 50

    col3, col4 = st.columns(2)

    with col3:
        run_norway = st.checkbox("Norwegian Outage", value=False)
        norway_days = st.slider("Outage days", 7, 60, 14, key="nor") if run_norway else 14

    with col4:
        run_iuk = st.checkbox("IUK Reversal", value=False)
        iuk_vol = st.slider("Export volume mcm/d", 5, 25, 20, key="iuk") if run_iuk else 20

    scenarios = []
    if run_cold:
        scenarios.append(ScenarioEngine.cold_snap(demand_uplift_pct=cold_uplift))
    if run_lng:
        scenarios.append(ScenarioEngine.lng_diversion(reduction_pct=lng_cut))
    if run_norway:
        scenarios.append(ScenarioEngine.norwegian_outage(duration_days=norway_days))
    if run_iuk:
        scenarios.append(ScenarioEngine.interconnector_reversal(export_volume_mcm=iuk_vol))

    if not scenarios:
        st.info("Select at least one scenario above.")
        return

    comparison = sc_engine.compare(scenarios)

    if unit != "mcm/d":
        target = "gwh" if unit == "GWh/d" else "therms"
        for c in ("total_supply", "total_demand", "balance_mcm"):
            if c in comparison.columns:
                comparison[c] = convert(comparison[c], "mcm", target)

    st.subheader("Balance Comparison")
    fig = px.line(
        comparison, x="date", y="balance_mcm", color="scenario",
        labels={"balance_mcm": f"Balance ({_value_label()})"}, height=500,
    )
    fig.add_hline(y=0, line_dash="dash", line_color="grey")
    fig.update_layout(hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Supply Comparison")
    fig_s = px.line(
        comparison, x="date", y="total_supply", color="scenario",
        labels={"total_supply": _value_label()}, height=400,
    )
    fig_s.update_layout(hovermode="x unified")
    st.plotly_chart(fig_s, use_container_width=True)

    st.subheader("Demand Comparison")
    fig_d = px.line(
        comparison, x="date", y="total_demand", color="scenario",
        labels={"total_demand": _value_label()}, height=400,
    )
    fig_d.update_layout(hovermode="x unified")
    st.plotly_chart(fig_d, use_container_width=True)


# =====================================================================
# PAGE 5 — Data Quality
# =====================================================================

def page_data_quality():
    st.title("Data Quality & Sources")
    st.markdown(
        "Every component below is sourced from **live public APIs** — "
        "no API keys, no manual downloads. "
        "Data auto-refreshes hourly; click **Refresh Data Now** in the sidebar for an immediate update."
    )

    _, _, breakdown = _load_balance(start_str, end_str)

    if breakdown.empty:
        st.warning("No data in the selected date range.")
        return

    quality = (
        breakdown.groupby(["side", "source"], as_index=False)
        .agg(
            data_quality=("data_quality", "first"),
            rows=("date", "count"),
            date_min=("date", "min"),
            date_max=("date", "max"),
        )
        .sort_values(["side", "source"])
    )

    quality_map = {"api": "🟢 Live API", "manual": "🟡 Manual CSV", "dummy": "🔴 Dummy", "forecast": "🔵 Forecast"}
    quality["status"] = quality["data_quality"].map(quality_map).fillna("⚪ Unknown")
    quality["date_min"] = quality["date_min"].dt.strftime("%Y-%m-%d")
    quality["date_max"] = quality["date_max"].dt.strftime("%Y-%m-%d")

    st.dataframe(
        quality[["status", "side", "source", "rows", "date_min", "date_max"]].rename(columns={
            "status": "Source",
            "side": "Stack",
            "source": "Component",
            "rows": "Data Points",
            "date_min": "Earliest",
            "date_max": "Latest",
        }),
        use_container_width=True,
        hide_index=True,
    )

    # API Sources
    st.markdown("---")
    st.subheader("API Sources")
    st.markdown("""
| API | Data | Auth | Endpoint |
|---|---|---|---|
| **National Gas** | Flows, demand, entry volumes, storage | None | `data.nationalgas.com/api/find-gas-data-download` |
| **Elexon BMRS** | CCGT generation (half-hourly) | None | `data.elexon.co.uk/bmrs/api/v1/datasets/FUELHH` |
| **GIE AGSI+** | Storage (fallback) | None* | `agsi.gie.eu/api` |

*GIE works without a key but may be slow; National Gas storage is now the primary source.
""")

    # Last refresh results
    st.markdown("---")
    st.subheader("Last Refresh Results")
    refresh_results = st.session_state.get("refresh_results", {})
    if refresh_results:
        ok_count = sum(1 for s in refresh_results.values() if s.startswith("OK"))
        total_count = len(refresh_results)
        st.markdown(f"**{ok_count}/{total_count}** components refreshed successfully.")
        for component, status in refresh_results.items():
            if status.startswith("OK"):
                st.success(f"**{component}**: {status}")
            elif "EMPTY" in status:
                st.warning(f"**{component}**: {status}")
            else:
                st.error(f"**{component}**: {status}")
    else:
        st.info("No refresh has been run yet. Click 'Refresh Data Now' in the sidebar.")

    # Cache status
    st.markdown("---")
    st.subheader("Cache Status")
    all_components = [
        "UKCS Production", "Norwegian Pipelines", "IUK Import", "BBL Pipeline",
        "LNG Terminals", "Storage Withdrawal", "Residential/Commercial",
        "Industrial", "CCGT Power Gen", "IUK Export", "Moffat Export",
        "Storage Injection", "NTS Demand Total",
    ]
    cache_rows = []
    for comp in all_components:
        age = cache.age_hours(comp)
        cache_rows.append({
            "Component": comp,
            "Cached": "✅" if age is not None else "❌",
            "Age (hours)": f"{age:.1f}" if age is not None else "—",
        })
    st.dataframe(pd.DataFrame(cache_rows), use_container_width=True, hide_index=True)


# =====================================================================
# Router
# =====================================================================

PAGES = {
    "Overview": page_overview,
    "Supply Drill-down": page_supply,
    "Demand Drill-down": page_demand,
    "Scenarios": page_scenarios,
    "Data Quality": page_data_quality,
}

PAGES[page]()
