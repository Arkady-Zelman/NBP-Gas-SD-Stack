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


@st.cache_data(ttl=3600)
def _load_prices():
    cached = cache.load("NBP Prices")
    if cached is not None and not cached.empty:
        cached["date"] = pd.to_datetime(cached["date"])
        return cached
    try:
        from src.data.national_gas import NationalGasClient, PUBOB_IDS
        client = NationalGasClient()
        ids = [PUBOB_IDS[k] for k in
               ("SAP_Daily","SMP_Buy_Daily","SMP_Sell_Daily","SAP_7d_Avg","SAP_30d_Avg",
                "Linepack_Open","Linepack_Close") if k in PUBOB_IDS]
        raw = client._fetch_chunked(ids, date.today() - timedelta(days=365), date.today())
        if raw is None or raw.empty:
            return pd.DataFrame()
        raw = client._to_daily(raw)
        raw.columns = raw.columns.str.strip()
        pivot = raw.pivot_table(index="date", columns="Data Item", values="Value", aggfunc="first").reset_index()
        col_map = {}
        for col in pivot.columns:
            cl = str(col).lower()
            if "sap" in cl and "actual day" in cl: col_map[col] = "sap"
            elif "smp buy" in cl and "actual day" in cl: col_map[col] = "smp_buy"
            elif "smp sell" in cl and "actual day" in cl: col_map[col] = "smp_sell"
            elif "sap" in cl and "7 day" in cl: col_map[col] = "sap_7d"
            elif "sap" in cl and "30 day" in cl: col_map[col] = "sap_30d"
            elif "opening linepack" in cl: col_map[col] = "linepack_open"
            elif "predicted closing" in cl or "pclp" in cl: col_map[col] = "linepack_close"
        pivot = pivot.rename(columns=col_map)
        keep = ["date"] + [c for c in col_map.values() if c in pivot.columns]
        return pivot[keep].sort_values("date").reset_index(drop=True)
    except Exception:
        return pd.DataFrame()


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
    ["Trading Dashboard", "Overview", "Supply Drill-down", "Demand Drill-down",
     "Scenarios", "Storage Map", "Technical Indicators", "Storage Forecast",
     "Trading Charts", "Price Forecast", "Data Quality"],
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
        "Storage Injection", "Storage By Site", "NTS Demand Total", "NBP Prices",
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
    cached = cache.load("Storage By Site")
    if cached is not None and not cached.empty:
        df = cached.copy()
        df["date"] = pd.to_datetime(df["date"])
        if start_str:
            df = df[df["date"] >= start_str]
        if end_str:
            df = df[df["date"] <= end_str]
        return df
    try:
        from src.data.national_gas import (
            NationalGasClient, PUBOB_IDS,
            STORAGE_INFLOW_IDS, STORAGE_OUTFLOW_IDS,
        )
        from src.units import kwh_to_mcm
        client = NationalGasClient()
        all_ids = STORAGE_INFLOW_IDS + STORAGE_OUTFLOW_IDS
        pubob_ids = [PUBOB_IDS[k] for k in all_ids if k in PUBOB_IDS]
        start_dt = date.fromisoformat(start_str) if start_str else date.today() - timedelta(days=90)
        end_dt = date.fromisoformat(end_str) if end_str else date.today()
        raw = client._fetch_chunked(pubob_ids, start_dt, end_dt)
        if raw is None or raw.empty:
            return None
        raw = client._to_daily(raw)
        raw.columns = raw.columns.str.strip()
        parts = raw["Data Item"].str.split(",", n=2, expand=True)
        raw["direction"] = parts[0].str.strip().str.lower()
        raw["site"] = parts[1].str.strip()
        inj = (
            raw[raw["direction"] == "inflow"]
            .groupby(["date", "site"], as_index=False)["Value"].sum()
            .rename(columns={"Value": "injection_mcm"})
        )
        inj["injection_mcm"] = kwh_to_mcm(inj["injection_mcm"])
        wdr = (
            raw[raw["direction"] == "outflow"]
            .groupby(["date", "site"], as_index=False)["Value"].sum()
            .rename(columns={"Value": "withdrawal_mcm"})
        )
        wdr["withdrawal_mcm"] = kwh_to_mcm(wdr["withdrawal_mcm"])
        merged = pd.merge(inj, wdr, on=["date", "site"], how="outer").fillna(0)
        merged["net_mcm"] = merged["injection_mcm"] - merged["withdrawal_mcm"]
        return merged.sort_values(["date", "site"]).reset_index(drop=True)
    except Exception:
        return None


def page_storage_map():
    st.title("UK Gas Storage — Live Map")
    st.caption("Per-site injection and withdrawal flows from National Gas (kWh → mcm/d)")

    site_df = _load_storage_by_site(start_str, end_str)

    if site_df is None or site_df.empty:
        st.info(
            "Per-site storage data is not cached yet.  \n"
            "Click **Refresh Data Now** in the sidebar — the next refresh "
            "will fetch per-site flows from National Gas."
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
# PAGE 7 — Trading Dashboard (front page)
# =====================================================================

def page_trading_dashboard():
    st.title("Trading Dashboard")
    st.caption("Key metrics at a glance — updated daily from live APIs")

    _, balance_df, breakdown_df = _load_balance(start_str, end_str)
    prices = _load_prices()

    if balance_df.empty:
        st.warning("No data in the selected date range.")
        return

    latest_bal = balance_df.sort_values("date").iloc[-1]
    today_str = pd.to_datetime(latest_bal["date"]).strftime("%d %b %Y")

    # Price row
    if not prices.empty and "sap" in prices.columns:
        p = prices.dropna(subset=["sap"]).sort_values("date")
        if not p.empty:
            latest_p = p.iloc[-1]
            prev_p = p.iloc[-2] if len(p) > 1 else latest_p
            delta_p = latest_p["sap"] - prev_p["sap"]
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("SAP (p/therm)", f"{latest_p['sap']:.2f}", f"{delta_p:+.2f}")
            smp_buy = latest_p.get("smp_buy", np.nan)
            smp_sell = latest_p.get("smp_sell", np.nan)
            c2.metric("SMP Buy", f"{smp_buy:.2f}" if pd.notna(smp_buy) else "-")
            c3.metric("SMP Sell", f"{smp_sell:.2f}" if pd.notna(smp_sell) else "-")
            spread = (smp_buy - smp_sell) if pd.notna(smp_buy) and pd.notna(smp_sell) else np.nan
            c4.metric("Buy-Sell Spread", f"{spread:.2f}" if pd.notna(spread) else "-")
            st.markdown("---")

    # Balance KPIs
    st.subheader(f"System Balance — {today_str}")
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Supply", f"{latest_bal['total_supply']:,.1f} mcm/d")
    k2.metric("Demand", f"{latest_bal['total_demand']:,.1f} mcm/d")
    bal_val = latest_bal["balance_mcm"]
    k3.metric("Balance", f"{bal_val:+,.1f} mcm/d",
              delta="Surplus" if bal_val > 0 else "Deficit",
              delta_color="normal" if bal_val > 0 else "inverse")

    # Linepack
    if not prices.empty and "linepack_open" in prices.columns:
        lp = prices.dropna(subset=["linepack_open"]).sort_values("date")
        if not lp.empty:
            lp_val = lp.iloc[-1]["linepack_open"]
            k4.metric("Linepack (open)", f"{lp_val:,.0f} mcm")

    st.markdown("---")

    # Monthly imports / exports
    bal_month = balance_df.copy()
    bal_month["date"] = pd.to_datetime(bal_month["date"])
    this_month = bal_month[bal_month["date"].dt.to_period("M") == pd.Period(date.today(), "M")]

    supply_month = breakdown_df[
        (breakdown_df["side"] == "supply") &
        (pd.to_datetime(breakdown_df["date"]).dt.to_period("M") == pd.Period(date.today(), "M"))
    ]
    demand_month = breakdown_df[
        (breakdown_df["side"] == "demand") &
        (pd.to_datetime(breakdown_df["date"]).dt.to_period("M") == pd.Period(date.today(), "M"))
    ]

    col_s, col_d = st.columns(2)

    with col_s:
        st.subheader("Imports (MTD avg)")
        if not supply_month.empty:
            s_avg = (supply_month.groupby("source")["volume_mcm"].mean()
                     .sort_values(ascending=False))
            for src, val in s_avg.items():
                st.markdown(f"**{src}** — {val:,.1f} mcm/d")
            st.caption(f"Total supply: {s_avg.sum():,.1f} mcm/d")
        else:
            st.info("No import data for this month yet.")

    with col_d:
        st.subheader("Exports & Demand (MTD avg)")
        if not demand_month.empty:
            d_avg = (demand_month.groupby("source")["volume_mcm"].mean()
                     .sort_values(ascending=False))
            for src, val in d_avg.items():
                st.markdown(f"**{src}** — {val:,.1f} mcm/d")
            st.caption(f"Total demand: {d_avg.sum():,.1f} mcm/d")
        else:
            st.info("No demand data for this month yet.")

    st.markdown("---")

    # Mini 30-day balance chart
    st.subheader("Balance — Last 30 Days")
    recent = bal_month.tail(30)
    if not recent.empty:
        colors = recent["balance_mcm"].apply(lambda x: "#2ecc71" if x >= 0 else "#e74c3c")
        fig = go.Figure(go.Bar(
            x=recent["date"], y=recent["balance_mcm"],
            marker_color=colors,
            hovertemplate="<b>%{x|%d %b}</b><br>%{y:+,.1f} mcm/d<extra></extra>",
        ))
        _apply_layout(fig, yaxis_title="mcm/d", height=280, hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True)

    # Mini SAP price chart
    if not prices.empty and "sap" in prices.columns:
        st.subheader("SAP Price — Last 90 Days")
        p90 = prices.dropna(subset=["sap"]).tail(90)
        if not p90.empty:
            fig_p = go.Figure()
            fig_p.add_trace(go.Scatter(
                x=p90["date"], y=p90["sap"], name="SAP",
                line=dict(color="#e67e22", width=2),
                hovertemplate="<b>%{x|%d %b %Y}</b><br>SAP: %{y:.2f} p/therm<extra></extra>",
            ))
            if "sap_7d" in p90.columns:
                fig_p.add_trace(go.Scatter(
                    x=p90["date"], y=p90["sap_7d"], name="7d MA",
                    line=dict(color="#3498db", width=1, dash="dot"),
                ))
            if "sap_30d" in p90.columns:
                fig_p.add_trace(go.Scatter(
                    x=p90["date"], y=p90["sap_30d"], name="30d MA",
                    line=dict(color="#9b59b6", width=1, dash="dash"),
                ))
            _apply_layout(fig_p, yaxis_title="p/therm", height=280)
            st.plotly_chart(fig_p, use_container_width=True)


# =====================================================================
# PAGE 8 — Technical Indicators
# =====================================================================

def _compute_rsi(series: pd.Series, window: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(window).mean()
    loss = (-delta.clip(upper=0)).rolling(window).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def _compute_macd(series: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
    ema_fast = series.ewm(span=fast, adjust=False).mean()
    ema_slow = series.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram


def _compute_stochastic(series: pd.Series, k_period: int = 14, d_period: int = 3):
    low_min = series.rolling(k_period).min()
    high_max = series.rolling(k_period).max()
    k = 100 * (series - low_min) / (high_max - low_min).replace(0, np.nan)
    d = k.rolling(d_period).mean()
    return k, d


def page_technical_indicators():
    st.title("Technical Indicators")
    st.caption("Stochastic, RSI, MACD, and Bollinger Bands on SAP price and system balance")

    prices = _load_prices()
    _, balance_df, _ = _load_balance(start_str, end_str)

    data_col = st.sidebar.selectbox(
        "Apply indicators to",
        ["SAP Price (p/therm)", "System Balance (mcm/d)"],
        key="ta_series",
    )

    if data_col == "SAP Price (p/therm)":
        if prices.empty or "sap" not in prices.columns:
            st.info("No SAP price data cached. Click **Refresh Data Now**.")
            return
        df = prices.dropna(subset=["sap"]).sort_values("date").copy()
        series = df["sap"]
        series_name = "SAP (p/therm)"
        y_label = "p/therm"
    else:
        if balance_df.empty:
            st.warning("No balance data.")
            return
        df = balance_df.sort_values("date").copy()
        series = df["balance_mcm"]
        series_name = "Balance (mcm/d)"
        y_label = "mcm/d"

    rsi_window = st.sidebar.slider("RSI window", 5, 30, 14, key="rsi_w")
    bb_window = st.sidebar.slider("Bollinger window", 10, 50, 20, key="bb_w")
    bb_std = st.sidebar.slider("Bollinger std", 1.0, 3.0, 2.0, 0.5, key="bb_s")

    dates = df["date"]

    # Price/Series + Bollinger Bands
    st.subheader(f"{series_name} with Bollinger Bands")
    sma = series.rolling(bb_window).mean()
    upper = sma + bb_std * series.rolling(bb_window).std()
    lower = sma - bb_std * series.rolling(bb_window).std()

    fig_bb = go.Figure()
    fig_bb.add_trace(go.Scatter(x=dates, y=upper, name="Upper", line=dict(width=0),
                                showlegend=False))
    fig_bb.add_trace(go.Scatter(x=dates, y=lower, name="Lower", line=dict(width=0),
                                fill="tonexty", fillcolor="rgba(52,152,219,0.1)",
                                showlegend=False))
    fig_bb.add_trace(go.Scatter(x=dates, y=sma, name=f"SMA({bb_window})",
                                line=dict(color="#3498db", dash="dot", width=1)))
    fig_bb.add_trace(go.Scatter(x=dates, y=series, name=series_name,
                                line=dict(color="#e67e22", width=2),
                                hovertemplate="%{x|%d %b %Y}<br>%{y:.2f}<extra></extra>"))
    _apply_layout(fig_bb, yaxis_title=y_label, height=400)
    st.plotly_chart(fig_bb, use_container_width=True)

    # RSI
    st.subheader(f"RSI ({rsi_window})")
    rsi = _compute_rsi(series, rsi_window)
    fig_rsi = go.Figure()
    fig_rsi.add_trace(go.Scatter(x=dates, y=rsi, name="RSI",
                                 line=dict(color="#e67e22", width=1.5),
                                 hovertemplate="%{x|%d %b %Y}<br>RSI: %{y:.1f}<extra></extra>"))
    fig_rsi.add_hline(y=70, line_dash="dash", line_color="#e74c3c", annotation_text="Overbought")
    fig_rsi.add_hline(y=30, line_dash="dash", line_color="#2ecc71", annotation_text="Oversold")
    _apply_layout(fig_rsi, yaxis_title="RSI", height=250)
    st.plotly_chart(fig_rsi, use_container_width=True)

    # MACD
    st.subheader("MACD (12, 26, 9)")
    macd_line, signal_line, hist = _compute_macd(series)
    fig_macd = go.Figure()
    fig_macd.add_trace(go.Bar(x=dates, y=hist, name="Histogram",
                              marker_color=hist.apply(lambda x: "#2ecc71" if x >= 0 else "#e74c3c")))
    fig_macd.add_trace(go.Scatter(x=dates, y=macd_line, name="MACD",
                                  line=dict(color="#3498db", width=1.5)))
    fig_macd.add_trace(go.Scatter(x=dates, y=signal_line, name="Signal",
                                  line=dict(color="#e67e22", width=1.5, dash="dot")))
    _apply_layout(fig_macd, yaxis_title="MACD", height=300)
    st.plotly_chart(fig_macd, use_container_width=True)

    # Stochastic Oscillator
    st.subheader("Stochastic Oscillator (14, 3)")
    k, d = _compute_stochastic(series)
    fig_sto = go.Figure()
    fig_sto.add_trace(go.Scatter(x=dates, y=k, name="%K",
                                 line=dict(color="#3498db", width=1.5)))
    fig_sto.add_trace(go.Scatter(x=dates, y=d, name="%D",
                                 line=dict(color="#e67e22", width=1.5, dash="dot")))
    fig_sto.add_hline(y=80, line_dash="dash", line_color="#e74c3c", annotation_text="Overbought")
    fig_sto.add_hline(y=20, line_dash="dash", line_color="#2ecc71", annotation_text="Oversold")
    _apply_layout(fig_sto, yaxis_title="Stochastic", height=250)
    st.plotly_chart(fig_sto, use_container_width=True)


# =====================================================================
# PAGE 9 — Storage Forecast
# =====================================================================

def page_storage_forecast():
    st.title("Storage Level Forecast")
    st.caption("Stochastic simulation of UK aggregate storage based on historical injection/withdrawal patterns")

    site_df = _load_storage_by_site(start_str, end_str)
    if site_df is None or site_df.empty:
        st.info("No per-site storage data. Click **Refresh Data Now** to fetch.")
        return

    site_df["date"] = pd.to_datetime(site_df["date"])

    # Aggregate across all sites
    agg = site_df.groupby("date", as_index=False).agg(
        injection=("injection_mcm", "sum"),
        withdrawal=("withdrawal_mcm", "sum"),
        net=("net_mcm", "sum"),
    ).sort_values("date")

    # Build cumulative storage proxy (relative)
    agg["cumulative"] = agg["net"].cumsum()

    st.subheader("Historical Net Storage Flow (cumulative)")
    fig_cum = go.Figure()
    fig_cum.add_trace(go.Scatter(
        x=agg["date"], y=agg["cumulative"], name="Cumulative Net",
        fill="tozeroy", fillcolor="rgba(46,204,113,0.15)",
        line=dict(color="#2ecc71", width=2),
        hovertemplate="%{x|%d %b %Y}<br>Cumulative: %{y:,.1f} mcm<extra></extra>",
    ))
    _apply_layout(fig_cum, yaxis_title="Cumulative net (mcm)", height=350)
    st.plotly_chart(fig_cum, use_container_width=True)

    # Forecast parameters
    st.markdown("---")
    st.subheader("Monte Carlo Forecast")
    col1, col2, col3 = st.columns(3)
    horizon = col1.slider("Forecast horizon (days)", 14, 180, 60, key="fc_horizon")
    n_sims = col2.slider("Simulations", 100, 2000, 500, step=100, key="fc_sims")
    lookback = col3.slider("Lookback window (days)", 30, 365, 90, key="fc_lookback")

    recent = agg.tail(lookback)
    net_mean = recent["net"].mean()
    net_std = recent["net"].std()
    last_cum = agg["cumulative"].iloc[-1]
    last_date = agg["date"].iloc[-1]

    np.random.seed(42)
    forecast_dates = pd.date_range(last_date + timedelta(days=1), periods=horizon, freq="D")
    simulations = np.zeros((n_sims, horizon))
    for i in range(n_sims):
        daily_shocks = np.random.normal(net_mean, net_std, horizon)
        simulations[i] = last_cum + np.cumsum(daily_shocks)

    p5 = np.percentile(simulations, 5, axis=0)
    p25 = np.percentile(simulations, 25, axis=0)
    p50 = np.percentile(simulations, 50, axis=0)
    p75 = np.percentile(simulations, 75, axis=0)
    p95 = np.percentile(simulations, 95, axis=0)

    fig_fc = go.Figure()
    # Historical
    fig_fc.add_trace(go.Scatter(
        x=agg["date"], y=agg["cumulative"], name="Historical",
        line=dict(color="#e67e22", width=2),
    ))
    # Confidence bands
    fig_fc.add_trace(go.Scatter(x=forecast_dates, y=p95, name="95th", line=dict(width=0), showlegend=False))
    fig_fc.add_trace(go.Scatter(x=forecast_dates, y=p5, name="5th", line=dict(width=0),
                                fill="tonexty", fillcolor="rgba(52,152,219,0.1)", showlegend=False))
    fig_fc.add_trace(go.Scatter(x=forecast_dates, y=p75, name="75th", line=dict(width=0), showlegend=False))
    fig_fc.add_trace(go.Scatter(x=forecast_dates, y=p25, name="25th", line=dict(width=0),
                                fill="tonexty", fillcolor="rgba(52,152,219,0.2)", showlegend=False))
    fig_fc.add_trace(go.Scatter(
        x=forecast_dates, y=p50, name="Median forecast",
        line=dict(color="#3498db", width=2, dash="dash"),
        hovertemplate="%{x|%d %b %Y}<br>Median: %{y:,.1f} mcm<extra></extra>",
    ))
    _apply_layout(fig_fc, yaxis_title="Cumulative net (mcm)", height=450)
    st.plotly_chart(fig_fc, use_container_width=True)

    st.caption(
        f"Based on {lookback}-day lookback: mean daily net = {net_mean:+.2f} mcm/d, "
        f"std = {net_std:.2f} mcm/d. {n_sims} Monte Carlo paths."
    )


# =====================================================================
# PAGE 10 — Top 5 Trading Charts
# =====================================================================

def page_trading_charts():
    st.title("Top 5 Trading Charts")
    st.caption("Five high-signal charts for NBP gas trading")

    _, balance_df, breakdown_df = _load_balance(start_str, end_str)
    prices = _load_prices()
    all_supply, _ = _load_supply(start_str, end_str)
    all_demand, _ = _load_demand(start_str, end_str)

    if balance_df.empty:
        st.warning("No data in range.")
        return

    balance_df = balance_df.copy()
    balance_df["date"] = pd.to_datetime(balance_df["date"])

    # CHART 1: SAP vs Balance scatter — reveals price sensitivity
    st.subheader("1. Price vs Balance (SAP sensitivity)")
    if not prices.empty and "sap" in prices.columns:
        merged = pd.merge(
            balance_df[["date", "balance_mcm"]],
            prices[["date", "sap"]].dropna(),
            on="date", how="inner",
        )
        if not merged.empty:
            fig1 = go.Figure(go.Scatter(
                x=merged["balance_mcm"], y=merged["sap"],
                mode="markers",
                marker=dict(size=5, color=merged["sap"], colorscale="YlOrRd",
                            showscale=True, colorbar=dict(title="p/therm")),
                hovertemplate="Balance: %{x:,.1f} mcm/d<br>SAP: %{y:.2f} p/therm<extra></extra>",
            ))
            z = np.polyfit(merged["balance_mcm"], merged["sap"], 1)
            x_fit = np.linspace(merged["balance_mcm"].min(), merged["balance_mcm"].max(), 100)
            fig1.add_trace(go.Scatter(x=x_fit, y=np.polyval(z, x_fit), name="Trend",
                                      line=dict(color="white", dash="dash", width=1)))
            _apply_layout(fig1, xaxis_title="Balance (mcm/d)", yaxis_title="SAP (p/therm)", height=400,
                          showlegend=False)
            st.plotly_chart(fig1, use_container_width=True)
    else:
        st.info("SAP price data not available. Refresh to load.")

    # CHART 2: Supply stack composition over time
    st.subheader("2. Supply Stack Composition")
    supply_piv = all_supply.pivot_table(
        index="date", columns="source", values="volume_mcm", aggfunc="sum"
    ).fillna(0)
    fig2 = go.Figure()
    for i, col_name in enumerate(supply_piv.columns):
        fig2.add_trace(go.Scatter(
            x=supply_piv.index, y=supply_piv[col_name],
            name=col_name, stackgroup="s", line=dict(width=0),
            fillcolor=SUPPLY_COLORS[i % len(SUPPLY_COLORS)],
        ))
    _apply_layout(fig2, yaxis_title="mcm/d", height=350,
                  hovermode="x unified",
                  legend=dict(orientation="h", y=-0.15, bgcolor="rgba(0,0,0,0)"))
    st.plotly_chart(fig2, use_container_width=True)

    # CHART 3: Demand seasonality heatmap
    st.subheader("3. Demand Seasonality Heatmap")
    dem_total = balance_df[["date", "total_demand"]].copy()
    dem_total["month"] = dem_total["date"].dt.month
    dem_total["year"] = dem_total["date"].dt.year
    heat_piv = dem_total.pivot_table(index="year", columns="month", values="total_demand", aggfunc="mean")
    month_labels = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    fig3 = go.Figure(go.Heatmap(
        z=heat_piv.values,
        x=[month_labels[m-1] for m in heat_piv.columns],
        y=heat_piv.index.astype(str),
        colorscale="YlOrRd",
        hovertemplate="Year: %{y}<br>Month: %{x}<br>Avg demand: %{z:,.1f} mcm/d<extra></extra>",
    ))
    _apply_layout(fig3, height=300)
    st.plotly_chart(fig3, use_container_width=True)

    # CHART 4: IUK net flow (import vs export indicator)
    st.subheader("4. IUK Net Flow (positive = import)")
    iuk_imp = all_supply[all_supply["source"].str.contains("IUK", case=False)]
    iuk_exp = all_demand[all_demand["source"].str.contains("IUK", case=False)]
    if not iuk_imp.empty and not iuk_exp.empty:
        iuk_i = iuk_imp.groupby("date")["volume_mcm"].sum().reset_index().rename(columns={"volume_mcm": "import"})
        iuk_e = iuk_exp.groupby("date")["volume_mcm"].sum().reset_index().rename(columns={"volume_mcm": "export"})
        iuk = pd.merge(iuk_i, iuk_e, on="date", how="outer").fillna(0)
        iuk["net"] = iuk["import"] - iuk["export"]
        colors_iuk = iuk["net"].apply(lambda x: "#2ecc71" if x >= 0 else "#e74c3c")
        fig4 = go.Figure(go.Bar(
            x=iuk["date"], y=iuk["net"], marker_color=colors_iuk,
            hovertemplate="%{x|%d %b %Y}<br>Net: %{y:+,.1f} mcm/d<extra></extra>",
        ))
        _apply_layout(fig4, yaxis_title="mcm/d", height=300, hovermode="x unified")
        st.plotly_chart(fig4, use_container_width=True)

    # CHART 5: Rolling 7d supply vs demand with crossover signals
    st.subheader("5. Supply vs Demand — 7-day Moving Average")
    bal = balance_df.copy()
    bal["supply_7d"] = bal["total_supply"].rolling(7).mean()
    bal["demand_7d"] = bal["total_demand"].rolling(7).mean()
    fig5 = go.Figure()
    fig5.add_trace(go.Scatter(x=bal["date"], y=bal["supply_7d"], name="Supply 7d MA",
                              line=dict(color="#2ecc71", width=2)))
    fig5.add_trace(go.Scatter(x=bal["date"], y=bal["demand_7d"], name="Demand 7d MA",
                              line=dict(color="#e74c3c", width=2)))
    fig5.add_trace(go.Scatter(x=bal["date"], y=bal["total_supply"], name="Supply (daily)",
                              line=dict(color="#2ecc71", width=0.5), opacity=0.3))
    fig5.add_trace(go.Scatter(x=bal["date"], y=bal["total_demand"], name="Demand (daily)",
                              line=dict(color="#e74c3c", width=0.5), opacity=0.3))
    _apply_layout(fig5, yaxis_title="mcm/d", height=400)
    st.plotly_chart(fig5, use_container_width=True)


# =====================================================================
# PAGE 11 — Price Forecast
# =====================================================================

def page_price_forecast():
    st.title("NBP Price Forecast")
    st.caption("Machine-learning model using supply, demand, storage, and seasonality as features")

    prices = _load_prices()
    _, balance_df, _ = _load_balance(start_str, end_str)

    if prices.empty or "sap" not in prices.columns:
        st.info("No SAP price data. Click **Refresh Data Now** to fetch.")
        return
    if balance_df.empty:
        st.warning("No balance data in range.")
        return

    balance_df = balance_df.copy()
    balance_df["date"] = pd.to_datetime(balance_df["date"])
    prices["date"] = pd.to_datetime(prices["date"])

    df = pd.merge(balance_df, prices[["date", "sap"]], on="date", how="inner").dropna(subset=["sap"])
    if len(df) < 30:
        st.warning("Not enough overlapping data for a model (need 30+ days).")
        return

    # Feature engineering
    df["day_of_year"] = df["date"].dt.dayofyear
    df["month"] = df["date"].dt.month
    df["weekday"] = df["date"].dt.weekday
    df["sin_doy"] = np.sin(2 * np.pi * df["day_of_year"] / 365)
    df["cos_doy"] = np.cos(2 * np.pi * df["day_of_year"] / 365)
    df["supply_7d"] = df["total_supply"].rolling(7, min_periods=1).mean()
    df["demand_7d"] = df["total_demand"].rolling(7, min_periods=1).mean()
    df["balance_7d"] = df["balance_mcm"].rolling(7, min_periods=1).mean()
    df["sap_lag1"] = df["sap"].shift(1)
    df["sap_lag7"] = df["sap"].shift(7)
    df = df.dropna()

    features = ["total_supply", "total_demand", "balance_mcm",
                "sin_doy", "cos_doy", "weekday",
                "supply_7d", "demand_7d", "balance_7d",
                "sap_lag1", "sap_lag7"]
    target = "sap"

    from sklearn.ensemble import GradientBoostingRegressor
    from sklearn.model_selection import TimeSeriesSplit
    from sklearn.metrics import mean_absolute_error, r2_score

    X = df[features].values
    y = df[target].values
    dates_arr = df["date"].values

    # Time-series split: train on first 80%, test on last 20%
    split_idx = int(len(X) * 0.8)
    X_train, X_test = X[:split_idx], X[split_idx:]
    y_train, y_test = y[:split_idx], y[split_idx:]
    dates_train, dates_test = dates_arr[:split_idx], dates_arr[split_idx:]

    model = GradientBoostingRegressor(
        n_estimators=200, max_depth=4, learning_rate=0.05,
        subsample=0.8, random_state=42,
    )
    model.fit(X_train, y_train)
    y_pred_train = model.predict(X_train)
    y_pred_test = model.predict(X_test)

    mae = mean_absolute_error(y_test, y_pred_test)
    r2 = r2_score(y_test, y_pred_test)

    # Metrics
    m1, m2, m3 = st.columns(3)
    m1.metric("Test MAE", f"{mae:.3f} p/therm")
    m2.metric("Test R²", f"{r2:.3f}")
    m3.metric("Training samples", f"{len(X_train)}")

    # Actual vs Predicted chart
    st.subheader("Actual vs Predicted SAP")
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=dates_arr, y=y, name="Actual SAP",
                             line=dict(color="#e67e22", width=2)))
    fig.add_trace(go.Scatter(x=dates_train, y=y_pred_train, name="Train pred",
                             line=dict(color="#3498db", width=1, dash="dot"), opacity=0.6))
    fig.add_trace(go.Scatter(x=dates_test, y=y_pred_test, name="Test pred",
                             line=dict(color="#2ecc71", width=2)))
    split_x = str(pd.Timestamp(dates_test[0]).date())
    fig.add_shape(type="line", x0=split_x, x1=split_x, y0=0, y1=1,
                  yref="paper", line=dict(dash="dash", color="rgba(255,255,255,0.3)"))
    _apply_layout(fig, yaxis_title="SAP (p/therm)", height=450)
    st.plotly_chart(fig, use_container_width=True)

    # Feature importance
    st.subheader("Feature Importance")
    imp = pd.DataFrame({
        "Feature": features,
        "Importance": model.feature_importances_,
    }).sort_values("Importance")
    fig_imp = go.Figure(go.Bar(
        x=imp["Importance"], y=imp["Feature"], orientation="h",
        marker_color="#3498db",
        hovertemplate="<b>%{y}</b><br>Importance: %{x:.3f}<extra></extra>",
    ))
    _apply_layout(fig_imp, xaxis_title="Importance", height=350, showlegend=False)
    st.plotly_chart(fig_imp, use_container_width=True)

    # Forward forecast
    st.markdown("---")
    st.subheader("Forward Price Projection")
    fwd_days = st.slider("Forecast horizon (days)", 7, 60, 14, key="price_fwd")

    last_row = df.iloc[-1]
    fwd_dates = pd.date_range(df["date"].iloc[-1] + timedelta(days=1), periods=fwd_days, freq="D")
    fwd_prices = []
    prev_sap = last_row["sap"]
    prev_sap7 = last_row.get("sap_lag7", prev_sap)

    for i, fd in enumerate(fwd_dates):
        feat = np.array([[
            last_row["total_supply"], last_row["total_demand"], last_row["balance_mcm"],
            np.sin(2 * np.pi * fd.dayofyear / 365),
            np.cos(2 * np.pi * fd.dayofyear / 365),
            fd.weekday(),
            last_row["supply_7d"], last_row["demand_7d"], last_row["balance_7d"],
            prev_sap, prev_sap7,
        ]])
        pred = model.predict(feat)[0]
        fwd_prices.append(pred)
        prev_sap7 = prev_sap if i >= 6 else prev_sap7
        prev_sap = pred

    fig_fwd = go.Figure()
    fig_fwd.add_trace(go.Scatter(
        x=df["date"].tail(60), y=df["sap"].tail(60), name="Historical SAP",
        line=dict(color="#e67e22", width=2),
    ))
    fig_fwd.add_trace(go.Scatter(
        x=fwd_dates, y=fwd_prices, name="Forecast",
        line=dict(color="#2ecc71", width=2, dash="dash"),
        hovertemplate="%{x|%d %b %Y}<br>Forecast: %{y:.2f} p/therm<extra></extra>",
    ))
    _apply_layout(fig_fwd, yaxis_title="SAP (p/therm)", height=350)
    st.plotly_chart(fig_fwd, use_container_width=True)

    st.caption(
        f"Gradient Boosting model with {len(features)} features. "
        f"Forward projection holds supply/demand constant at last observed values "
        f"and iterates SAP lag features."
    )


# =====================================================================
# Router
# =====================================================================

PAGES = {
    "Trading Dashboard": page_trading_dashboard,
    "Overview": page_overview,
    "Supply Drill-down": page_supply,
    "Demand Drill-down": page_demand,
    "Scenarios": page_scenarios,
    "Storage Map": page_storage_map,
    "Technical Indicators": page_technical_indicators,
    "Storage Forecast": page_storage_forecast,
    "Trading Charts": page_trading_charts,
    "Price Forecast": page_price_forecast,
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
