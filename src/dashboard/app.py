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
import streamlit.components.v1 as components

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
# Global CSS: fade-in transitions, chart borders, polish
# =====================================================================

st.markdown("""
<style>
/* ── Page entrance: slide up from below with blur dissolve ── */
@keyframes pageEnter {
    0%   { opacity: 0; transform: translateY(32px); filter: blur(3px); }
    100% { opacity: 1; transform: translateY(0);    filter: blur(0);   }
}
section.main .block-container {
    animation: pageEnter 0.5s cubic-bezier(0.22, 1, 0.36, 1) both;
}

/* ── Borders around chart containers ── */
div[data-testid="stPlotlyChart"] {
    border: 1px solid rgba(255, 255, 255, 0.08);
    border-radius: 10px;
    padding: 8px;
    background: rgba(255, 255, 255, 0.02);
    margin-bottom: 1rem;
}

/* ── Style metric cards ── */
div[data-testid="stMetric"] {
    border: 1px solid rgba(255, 255, 255, 0.08);
    border-radius: 10px;
    padding: 12px 16px;
    background: rgba(255, 255, 255, 0.03);
}

/* ── Style dataframes ── */
div[data-testid="stDataFrame"] {
    border: 1px solid rgba(255, 255, 255, 0.08);
    border-radius: 10px;
    overflow: hidden;
}

/* ── Sidebar radio — tighter spacing ── */
div[data-testid="stSidebar"] .stRadio > div { gap: 0.25rem; }

/* ── Smooth sidebar width ── */
section[data-testid="stSidebar"] {
    transition: width 0.3s ease;
}

/* ── Hide Streamlit chrome ── */
#MainMenu { visibility: hidden; }
footer { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

# =====================================================================
# Smart auto-refresh: only if cache is stale (>24 hours) or missing
# =====================================================================

CACHE_MAX_AGE_HOURS = 24.0


def _cache_is_stale() -> bool:
    age = cache.age_hours("UKCS Production")
    if age is None:
        return True
    return age > CACHE_MAX_AGE_HOURS


def _do_refresh():
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
# Plotly dark theme + crosshair defaults
# =====================================================================

PLOTLY_LAYOUT = dict(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Inter, sans-serif", size=13, color="#e0e0e0"),
    margin=dict(l=50, r=20, t=40, b=40),
    xaxis=dict(
        showspikes=True,
        spikemode="across",
        spikethickness=1,
        spikecolor="rgba(255,255,255,0.2)",
        spikedash="solid",
        showgrid=True,
        gridcolor="rgba(255,255,255,0.05)",
    ),
    yaxis=dict(
        showspikes=True,
        spikemode="across",
        spikethickness=1,
        spikecolor="rgba(255,255,255,0.2)",
        spikedash="solid",
        showgrid=True,
        gridcolor="rgba(255,255,255,0.05)",
    ),
    hoverlabel=dict(
        bgcolor="rgba(20,20,30,0.9)",
        font_size=13,
        font_color="#e0e0e0",
        bordercolor="rgba(255,255,255,0.1)",
    ),
    hovermode="closest",
    legend=dict(
        bgcolor="rgba(0,0,0,0)",
        font=dict(size=12),
    ),
)

SUPPLY_COLORS = ["#2ecc71", "#27ae60", "#1abc9c", "#16a085", "#3498db", "#2980b9"]
DEMAND_COLORS = ["#e74c3c", "#c0392b", "#e67e22", "#d35400", "#9b59b6", "#8e44ad"]


def _apply_layout(fig: go.Figure, **overrides) -> go.Figure:
    merged = {**PLOTLY_LAYOUT, **overrides}
    fig.update_layout(**merged)
    return fig


# =====================================================================
# Sidebar
# =====================================================================

st.sidebar.title("NBP Gas S&D Stack")
page = st.sidebar.radio(
    "Navigate",
    ["Overview", "Supply Drill-down", "Demand Drill-down", "Scenarios", "Storage Map", "Data Quality"],
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
if st.sidebar.button("Refresh Data Now", type="primary"):
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


def _vlabel() -> str:
    return unit


def _fmt_summary(summary: pd.DataFrame) -> pd.DataFrame:
    """Rename raw summary columns to human-readable labels."""
    out = summary.copy()
    out["data_quality"] = out["data_quality"].map(
        {"api": "Live API", "manual": "Manual CSV", "dummy": "Dummy", "forecast": "Forecast"}
    ).fillna(out["data_quality"])
    label = _vlabel()
    return out.rename(columns={
        "source": "Component",
        "avg_mcm": f"Average ({label})",
        "min_mcm": f"Min ({label})",
        "max_mcm": f"Max ({label})",
        "data_quality": "Source",
    })


# =====================================================================
# PAGE 1 — Overview
# =====================================================================

def page_overview():
    st.title("NBP Gas Supply & Demand — Overview")
    st.caption(f"{date_start.strftime('%d %b %Y')} to {date_end.strftime('%d %b %Y')}  ·  Live API data")

    _, balance_df, breakdown_df = _load_balance(start_str, end_str)

    if balance_df.empty:
        st.warning("No data in the selected date range.")
        return

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
            col_widget.metric(label, f"{display:,.1f} {_vlabel()}")

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
        for i, col_name in enumerate(supply_pivot.columns):
            fig.add_trace(go.Scatter(
                x=supply_pivot.index, y=supply_pivot[col_name],
                name=col_name, stackgroup="supply", line=dict(width=0),
                fillcolor=SUPPLY_COLORS[i % len(SUPPLY_COLORS)],
                hovertemplate=f"<b>{col_name}</b><br>%{{y:,.1f}} {_vlabel()}<extra></extra>",
            ))
        for i, col_name in enumerate(demand_pivot.columns):
            fig.add_trace(go.Scatter(
                x=demand_pivot.index, y=-demand_pivot[col_name],
                name=col_name, stackgroup="demand", line=dict(width=0),
                fillcolor=DEMAND_COLORS[i % len(DEMAND_COLORS)],
                hovertemplate=f"<b>{col_name}</b><br>%{{y:,.1f}} {_vlabel()}<extra></extra>",
            ))
        _apply_layout(fig,
            yaxis_title=_vlabel(), height=550,
            hovermode="x unified",
            legend=dict(orientation="h", y=-0.15, bgcolor="rgba(0,0,0,0)"),
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
            labels={agg_col: _vlabel(), "source": "Component", "period": "Period"},
        )
        _apply_layout(fig, hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Daily Balance (surplus / deficit)")
    bal = balance_df.copy()
    if unit != "mcm/d":
        target = "gwh" if unit == "GWh/d" else "therms"
        bal["balance_mcm"] = convert(bal["balance_mcm"], "mcm", target)

    colors = bal["balance_mcm"].apply(lambda x: "#2ecc71" if x >= 0 else "#e74c3c")
    fig_bal = go.Figure(go.Bar(
        x=bal["date"], y=bal["balance_mcm"],
        marker_color=colors,
        hovertemplate="<b>%{x|%d %b %Y}</b><br>Balance: %{y:,.1f} " + _vlabel() + "<extra></extra>",
    ))
    _apply_layout(fig_bal, yaxis_title=_vlabel(), height=350, hovermode="x unified")
    st.plotly_chart(fig_bal, use_container_width=True)


# =====================================================================
# PAGE 2 — Supply Drill-down
# =====================================================================

def page_supply():
    st.title("Supply Stack — Drill-down")
    st.caption(f"{date_start.strftime('%d %b %Y')} to {date_end.strftime('%d %b %Y')}  ·  Live API data")

    all_supply, summary = _load_supply(start_str, end_str)
    all_supply = _convert_col(all_supply)

    st.subheader("Component Averages")
    display = _fmt_summary(summary)
    vl = _vlabel()
    st.dataframe(
        display.style.format({
            f"Average ({vl})": "{:.1f}",
            f"Min ({vl})": "{:.1f}",
            f"Max ({vl})": "{:.1f}",
        }),
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
        fig = go.Figure()
        for i, src in enumerate(filtered["source"].unique()):
            src_df = filtered[filtered["source"] == src]
            fig.add_trace(go.Scatter(
                x=src_df["date"], y=src_df["volume_mcm"],
                name=src, mode="lines",
                line=dict(color=SUPPLY_COLORS[i % len(SUPPLY_COLORS)], width=1.5),
                hovertemplate=f"<b>{src}</b><br>%{{x|%d %b %Y}}<br>%{{y:,.1f}} {_vlabel()}<extra></extra>",
            ))
        _apply_layout(fig, yaxis_title=_vlabel(), height=500)
    else:
        agg = TimeAggregator.aggregate(filtered, granularity)
        agg = _convert_col(agg, "avg_daily_mcm")
        fig = px.bar(
            agg, x="period", y="avg_daily_mcm", color="source",
            labels={"avg_daily_mcm": _vlabel(), "source": "Component", "period": "Period"},
            height=500, color_discrete_sequence=SUPPLY_COLORS,
        )
        _apply_layout(fig)

    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Supply Merit Order")
    avg = summary.sort_values("avg_mcm")
    fig_merit = go.Figure(go.Bar(
        x=avg["avg_mcm"], y=avg["source"],
        orientation="h",
        marker_color=SUPPLY_COLORS[:len(avg)],
        hovertemplate="<b>%{y}</b><br>Average: %{x:,.1f} " + _vlabel() + "<extra></extra>",
    ))
    _apply_layout(fig_merit,
        xaxis_title=f"Average ({_vlabel()})",
        yaxis_title="",
        height=350,
        showlegend=False,
    )
    st.plotly_chart(fig_merit, use_container_width=True)


# =====================================================================
# PAGE 3 — Demand Drill-down
# =====================================================================

def page_demand():
    st.title("Demand Stack — Drill-down")
    st.caption(f"{date_start.strftime('%d %b %Y')} to {date_end.strftime('%d %b %Y')}  ·  Live API data")

    all_demand, summary = _load_demand(start_str, end_str)
    all_demand = _convert_col(all_demand)

    st.subheader("Component Averages")
    display = _fmt_summary(summary)
    vl = _vlabel()
    st.dataframe(
        display.style.format({
            f"Average ({vl})": "{:.1f}",
            f"Min ({vl})": "{:.1f}",
            f"Max ({vl})": "{:.1f}",
        }),
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
        fig = go.Figure()
        for i, src in enumerate(filtered["source"].unique()):
            src_df = filtered[filtered["source"] == src]
            fig.add_trace(go.Scatter(
                x=src_df["date"], y=src_df["volume_mcm"],
                name=src, mode="lines",
                line=dict(color=DEMAND_COLORS[i % len(DEMAND_COLORS)], width=1.5),
                hovertemplate=f"<b>{src}</b><br>%{{x|%d %b %Y}}<br>%{{y:,.1f}} {_vlabel()}<extra></extra>",
            ))
        _apply_layout(fig, yaxis_title=_vlabel(), height=500)
    else:
        agg = TimeAggregator.aggregate(filtered, granularity)
        agg = _convert_col(agg, "avg_daily_mcm")
        fig = px.bar(
            agg, x="period", y="avg_daily_mcm", color="source",
            labels={"avg_daily_mcm": _vlabel(), "source": "Component", "period": "Period"},
            height=500, color_discrete_sequence=DEMAND_COLORS,
        )
        _apply_layout(fig)

    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Demand Share")
    fig_pie = go.Figure(go.Pie(
        labels=summary["source"],
        values=summary["avg_mcm"],
        hole=0.45,
        marker=dict(colors=DEMAND_COLORS[:len(summary)]),
        textinfo="label+percent",
        textfont=dict(size=13, color="#e0e0e0"),
        hovertemplate="<b>%{label}</b><br>%{value:,.1f} " + _vlabel() + "<br>%{percent}<extra></extra>",
    ))
    _apply_layout(fig_pie, height=420, showlegend=False)
    st.plotly_chart(fig_pie, use_container_width=True)


# =====================================================================
# PAGE 4 — Scenarios
# =====================================================================

def page_scenarios():
    st.title("Scenario Analysis")
    st.caption("Applies adjustments to real baseline data")

    engine = BalanceEngine()
    sc_engine = ScenarioEngine(engine)

    st.markdown("Select pre-built scenarios to compare against the **Base Case**.")

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

    scenario_colors = ["#3498db", "#e74c3c", "#2ecc71", "#e67e22", "#9b59b6"]

    st.subheader("Balance Comparison")
    fig = go.Figure()
    for i, sc_name in enumerate(comparison["scenario"].unique()):
        sc_df = comparison[comparison["scenario"] == sc_name]
        fig.add_trace(go.Scatter(
            x=sc_df["date"], y=sc_df["balance_mcm"],
            name=sc_name, mode="lines",
            line=dict(color=scenario_colors[i % len(scenario_colors)], width=2),
            hovertemplate=f"<b>{sc_name}</b><br>%{{x|%d %b %Y}}<br>Balance: %{{y:,.1f}} {_vlabel()}<extra></extra>",
        ))
    fig.add_hline(y=0, line_dash="dash", line_color="rgba(255,255,255,0.3)")
    _apply_layout(fig, yaxis_title=f"Balance ({_vlabel()})", height=500)
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Supply Comparison")
    fig_s = go.Figure()
    for i, sc_name in enumerate(comparison["scenario"].unique()):
        sc_df = comparison[comparison["scenario"] == sc_name]
        fig_s.add_trace(go.Scatter(
            x=sc_df["date"], y=sc_df["total_supply"],
            name=sc_name, mode="lines",
            line=dict(color=scenario_colors[i % len(scenario_colors)], width=2),
            hovertemplate=f"<b>{sc_name}</b><br>%{{x|%d %b %Y}}<br>Supply: %{{y:,.1f}} {_vlabel()}<extra></extra>",
        ))
    _apply_layout(fig_s, yaxis_title=_vlabel(), height=400)
    st.plotly_chart(fig_s, use_container_width=True)

    st.subheader("Demand Comparison")
    fig_d = go.Figure()
    for i, sc_name in enumerate(comparison["scenario"].unique()):
        sc_df = comparison[comparison["scenario"] == sc_name]
        fig_d.add_trace(go.Scatter(
            x=sc_df["date"], y=sc_df["total_demand"],
            name=sc_name, mode="lines",
            line=dict(color=scenario_colors[i % len(scenario_colors)], width=2),
            hovertemplate=f"<b>{sc_name}</b><br>%{{x|%d %b %Y}}<br>Demand: %{{y:,.1f}} {_vlabel()}<extra></extra>",
        ))
    _apply_layout(fig_d, yaxis_title=_vlabel(), height=400)
    st.plotly_chart(fig_d, use_container_width=True)


# =====================================================================
# PAGE 5 — Data Quality
# =====================================================================

def page_data_quality():
    st.title("Data Quality & Sources")
    st.caption(
        "All components sourced from live public APIs — no keys, no manual downloads. "
        "Data auto-refreshes daily."
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

    quality_map = {"api": "Live API", "manual": "Manual CSV", "dummy": "Dummy", "forecast": "Forecast"}
    quality["status"] = quality["data_quality"].map(quality_map).fillna("Unknown")
    quality["date_min"] = quality["date_min"].dt.strftime("%d %b %Y")
    quality["date_max"] = quality["date_max"].dt.strftime("%d %b %Y")

    st.subheader("Component Status")
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

    st.markdown("---")
    st.subheader("API Endpoints")
    api_data = pd.DataFrame([
        {"API": "National Gas", "Data": "Flows, demand, entry volumes, storage", "Auth": "None",
         "Endpoint": "data.nationalgas.com/api/find-gas-data-download"},
        {"API": "Elexon BMRS", "Data": "CCGT generation (half-hourly)", "Auth": "None",
         "Endpoint": "data.elexon.co.uk/bmrs/api/v1/datasets/FUELHH"},
        {"API": "GIE AGSI+", "Data": "Storage (fallback)", "Auth": "None",
         "Endpoint": "agsi.gie.eu/api"},
    ])
    st.dataframe(api_data, use_container_width=True, hide_index=True)

    st.markdown("---")
    st.subheader("Last Refresh")
    refresh_results = st.session_state.get("refresh_results", {})
    if refresh_results:
        ok_count = sum(1 for s in refresh_results.values() if s.startswith("OK"))
        total_count = len(refresh_results)
        st.caption(f"{ok_count} of {total_count} components refreshed successfully.")
        for component, status in refresh_results.items():
            if status.startswith("OK"):
                st.success(f"**{component}** — {status}")
            elif "EMPTY" in status:
                st.warning(f"**{component}** — {status}")
            else:
                st.error(f"**{component}** — {status}")
    else:
        st.info("No refresh has been run yet. Click **Refresh Data Now** in the sidebar.")

    st.markdown("---")
    st.subheader("Cache Status")
    all_components = [
        "UKCS Production", "Norwegian Pipelines", "IUK Import", "BBL Pipeline",
        "LNG Terminals", "Storage Withdrawal", "Residential/Commercial",
        "Industrial", "CCGT Power Gen", "IUK Export", "Moffat Export",
        "Storage Injection", "Storage By Site", "NTS Demand Total",
    ]
    cache_rows = []
    for comp in all_components:
        age = cache.age_hours(comp)
        cache_rows.append({
            "Component": comp,
            "Cached": "Yes" if age is not None else "No",
            "Age (hours)": f"{age:.1f}" if age is not None else "-",
        })
    st.dataframe(pd.DataFrame(cache_rows), use_container_width=True, hide_index=True)


# =====================================================================
# PAGE 6 — Storage Map
# =====================================================================

STORAGE_SITES = {
    "Rough":          {"lat": 53.82, "lon":  0.43, "type": "Depleted field (offshore)", "capacity_mcm": 32.6},
    "Aldbrough":      {"lat": 53.84, "lon": -0.23, "type": "Salt cavern",               "capacity_mcm": 3.3},
    "Hornsea":        {"lat": 53.96, "lon": -0.17, "type": "Salt cavern",               "capacity_mcm": 3.3},
    "Hatfield Moor":  {"lat": 53.52, "lon": -1.05, "type": "Depleted field",            "capacity_mcm": 1.2},
    "Holford":        {"lat": 53.21, "lon": -2.54, "type": "Salt cavern",               "capacity_mcm": 1.6},
    "Hill Top":       {"lat": 53.23, "lon": -2.56, "type": "Salt cavern",               "capacity_mcm": 0.6},
    "Stublach":       {"lat": 53.20, "lon": -2.52, "type": "Salt cavern",               "capacity_mcm": 4.0},
    "Holehouse Farm": {"lat": 53.22, "lon": -2.48, "type": "Salt cavern",               "capacity_mcm": 0.5},
    "Humbly Grove":   {"lat": 51.18, "lon": -1.05, "type": "Depleted oil field",        "capacity_mcm": 2.8},
}


@st.cache_data(ttl=3600)
def _load_storage_by_site(start_str: str | None, end_str: str | None):
    from src.data.national_gas import NationalGasClient
    cached = cache.load("Storage By Site")
    if cached is not None and not cached.empty:
        df = cached.copy()
        df["date"] = pd.to_datetime(df["date"])
        if start_str:
            df = df[df["date"] >= start_str]
        if end_str:
            df = df[df["date"] <= end_str]
        return df
    client = NationalGasClient()
    return client.get_storage_by_site(start=start_str or "2020-10-01", end=end_str)


def page_storage_map():
    st.title("UK Gas Storage — Live Map")
    st.caption("Per-site injection and withdrawal flows from National Gas (kWh → mcm/d)")

    site_df = _load_storage_by_site(start_str, end_str)

    if site_df is None or site_df.empty:
        st.warning(
            "No per-site storage data in cache. "
            "Click **Refresh Data Now** in the sidebar to fetch it."
        )
        return

    site_df["date"] = pd.to_datetime(site_df["date"])

    latest_date = site_df["date"].max()
    latest = site_df[site_df["date"] == latest_date].copy()

    map_rows = []
    for _, row in latest.iterrows():
        name = row["site"]
        meta = STORAGE_SITES.get(name, {})
        if not meta:
            continue
        map_rows.append({
            "site": name,
            "lat": meta["lat"],
            "lon": meta["lon"],
            "type": meta["type"],
            "capacity_mcm": meta["capacity_mcm"],
            "injection_mcm": row.get("injection_mcm", 0),
            "withdrawal_mcm": row.get("withdrawal_mcm", 0),
            "net_mcm": row.get("net_mcm", 0),
        })

    if not map_rows:
        st.warning("Could not match site data to known locations.")
        return

    map_df = pd.DataFrame(map_rows)
    map_df["abs_net"] = map_df["net_mcm"].abs()
    map_df["status"] = map_df["net_mcm"].apply(
        lambda x: "Injecting" if x > 0.001 else ("Withdrawing" if x < -0.001 else "Idle")
    )
    color_map = {"Injecting": "#2ecc71", "Withdrawing": "#e74c3c", "Idle": "#7f8c8d"}
    map_df["color"] = map_df["status"].map(color_map)
    map_df["bubble_size"] = (map_df["abs_net"] * 8).clip(lower=6, upper=45)

    st.subheader(f"Storage Activity — {latest_date.strftime('%d %b %Y')}")

    col_inj, col_wdr, col_net = st.columns(3)
    col_inj.metric("Total Injection", f"{map_df['injection_mcm'].sum():,.2f} mcm/d")
    col_wdr.metric("Total Withdrawal", f"{map_df['withdrawal_mcm'].sum():,.2f} mcm/d")
    net_total = map_df["net_mcm"].sum()
    col_net.metric(
        "Net Position",
        f"{net_total:+,.2f} mcm/d",
        delta=f"{'Filling' if net_total > 0 else 'Drawing'}",
        delta_color="normal" if net_total > 0 else "inverse",
    )

    fig = go.Figure()
    for status_val in ["Injecting", "Withdrawing", "Idle"]:
        subset = map_df[map_df["status"] == status_val]
        if subset.empty:
            continue
        fig.add_trace(go.Scattermapbox(
            lat=subset["lat"],
            lon=subset["lon"],
            mode="markers+text",
            marker=dict(
                size=subset["bubble_size"],
                color=color_map[status_val],
                opacity=0.85,
            ),
            text=subset["site"],
            textposition="top center",
            textfont=dict(size=11, color="#e0e0e0"),
            name=status_val,
            customdata=np.stack([
                subset["site"],
                subset["type"],
                subset["capacity_mcm"],
                subset["injection_mcm"],
                subset["withdrawal_mcm"],
                subset["net_mcm"],
            ], axis=-1),
            hovertemplate=(
                "<b>%{customdata[0]}</b><br>"
                "Type: %{customdata[1]}<br>"
                "Capacity: %{customdata[2]:.1f} mcm<br>"
                "Injection: %{customdata[3]:.2f} mcm/d<br>"
                "Withdrawal: %{customdata[4]:.2f} mcm/d<br>"
                "Net: %{customdata[5]:+.2f} mcm/d"
                "<extra></extra>"
            ),
        ))

    fig.update_layout(
        mapbox=dict(
            style="carto-darkmatter",
            center=dict(lat=53.0, lon=-1.5),
            zoom=5.3,
        ),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=0, r=0, t=0, b=0),
        height=600,
        legend=dict(
            bgcolor="rgba(20,20,30,0.8)",
            font=dict(color="#e0e0e0", size=12),
            x=0.01, y=0.99,
        ),
        font=dict(family="Inter, sans-serif", color="#e0e0e0"),
    )
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Site Detail Table")
    display = map_df[["site", "type", "capacity_mcm", "injection_mcm", "withdrawal_mcm", "net_mcm", "status"]].copy()
    display = display.rename(columns={
        "site": "Site",
        "type": "Type",
        "capacity_mcm": "Capacity (mcm)",
        "injection_mcm": "Injection (mcm/d)",
        "withdrawal_mcm": "Withdrawal (mcm/d)",
        "net_mcm": "Net (mcm/d)",
        "status": "Status",
    })
    st.dataframe(
        display.style.format({
            "Capacity (mcm)": "{:.1f}",
            "Injection (mcm/d)": "{:.3f}",
            "Withdrawal (mcm/d)": "{:.3f}",
            "Net (mcm/d)": "{:+.3f}",
        }),
        use_container_width=True,
        hide_index=True,
    )

    st.subheader("Historical Flows by Site")
    sites_avail = sorted(site_df["site"].unique().tolist())
    selected_sites = st.multiselect(
        "Select sites", sites_avail, default=sites_avail, key="storage_sites"
    )
    hist = site_df[site_df["site"].isin(selected_sites)]
    if not hist.empty:
        fig_hist = go.Figure()
        site_colors = ["#2ecc71", "#3498db", "#e67e22", "#e74c3c", "#9b59b6",
                       "#1abc9c", "#f1c40f", "#2980b9", "#c0392b"]
        for i, site_name in enumerate(selected_sites):
            s = hist[hist["site"] == site_name]
            fig_hist.add_trace(go.Scatter(
                x=s["date"], y=s["net_mcm"],
                name=site_name, mode="lines",
                line=dict(color=site_colors[i % len(site_colors)], width=1.5),
                hovertemplate=(
                    f"<b>{site_name}</b><br>"
                    "%{x|%d %b %Y}<br>"
                    "Net: %{y:+.3f} mcm/d<extra></extra>"
                ),
            ))
        fig_hist.add_hline(y=0, line_dash="dash", line_color="rgba(255,255,255,0.2)")
        _apply_layout(fig_hist, yaxis_title="Net flow (mcm/d)", height=450)
        st.plotly_chart(fig_hist, use_container_width=True)


# =====================================================================
# Router
# =====================================================================

PAGES = {
    "Overview": page_overview,
    "Supply Drill-down": page_supply,
    "Demand Drill-down": page_demand,
    "Scenarios": page_scenarios,
    "Storage Map": page_storage_map,
    "Data Quality": page_data_quality,
}

PAGES[page]()

# =====================================================================
# Page-exit animation: JS hooks navigation clicks to float content up
# =====================================================================

components.html("""
<script>
(function() {
    var pd = window.parent.document;

    function bind() {
        var labels = pd.querySelectorAll(
            '[data-testid="stSidebar"] [role="radiogroup"] label'
        );
        for (var i = 0; i < labels.length; i++) {
            if (labels[i].dataset._tx) continue;
            labels[i].dataset._tx = '1';
            labels[i].addEventListener('mousedown', function() {
                var mc = pd.querySelector('section.main .block-container');
                if (!mc) return;
                mc.style.transition =
                    'opacity 0.22s cubic-bezier(0.22,1,0.36,1), ' +
                    'transform 0.22s cubic-bezier(0.22,1,0.36,1), ' +
                    'filter 0.22s cubic-bezier(0.22,1,0.36,1)';
                mc.style.opacity = '0';
                mc.style.transform = 'translateY(-28px)';
                mc.style.filter = 'blur(3px)';
            });
        }
    }

    bind();
    new MutationObserver(bind).observe(pd.body, {childList: true, subtree: true});
})();
</script>
""", height=0, scrolling=False)
