# NBP Gas Supply & Demand Stack Model

A modular Python model of UK gas (NBP) supply and demand with an interactive Streamlit dashboard.

## Quick Start

```
streamlit run src/dashboard/app.py
```

The model runs immediately using **dummy data** — no API keys required to get started. Every dummy series documents its assumptions and the real data source it should be replaced with.

## Project Structure

```
config/settings.yaml        — global config (units, API keys, date ranges)
data/manual/                — drop CSV files here for manual-input components
src/
  base.py                   — StackComponent base class
  units.py                  — mcm ↔ GWh ↔ therms conversion
  config.py                 — YAML config loader
  data/                     — data ingestion
  supply/                   — 6 supply components + SupplyStack aggregator
  demand/                   — 6 demand components + DemandStack aggregator
  balance/                  — S&D balance engine
  scenarios/                — scenario engine with pre-built templates
  aggregation/              — daily → monthly → seasonal → annual rollup
  dashboard/app.py          — Streamlit dashboard (5 pages)
```

## Supply Components

| Component | Source |
|---|---|---|
| UKCS Production | NSTA monthly CSV |
| Norwegian Pipelines | Gassco transparency |
| IUK Import | National Gas Data Portal |
| BBL Pipeline | National Gas Data Portal |
| LNG Terminals | GIE ALSI (register at gie.eu) |
| Storage Withdrawal | GIE AGSI+ |

## Demand Components

| Component | Default Source | Replace With |
|---|---|---|
| Residential/Commercial |  National Gas LDZ demand |
| Industrial |  National Gas DM/LDM |
| CCGT Power Gen | dummy → API | Elexon/BMRS |
| IUK Export | National Gas Data Portal |
| Moffat Export | National Gas Data Portal |
| Storage Injection | GIE AGSI+ |

The model tries APIs first, then falls back to manual CSV inputs if run locally, then to labeled dummy data.

## Scenario Analysis

Pre-built scenarios accessible via the dashboard or manually:

```python
from src.scenarios.scenario_engine import ScenarioEngine

engine = ScenarioEngine()
cold = ScenarioEngine.cold_snap(demand_uplift_pct=40)
result = engine.apply(cold)
```

Available templates: **Cold Snap**, **LNG Diversion**, **Norwegian Outage**, **IUK Reversal**, **Custom**.

## Time Horizons

All data is stored at daily granularity and aggregated on demand:
- **Daily** — raw model output
- **Monthly** — average daily rate per calendar month
- **Seasonal** — Gas Year quarters (Q1=Oct–Dec, Q2=Jan–Mar, Q3=Apr–Jun, Q4=Jul–Sep)
- **Annual** — Gas Year (Oct–Sep)

## Units

Base unit is **mcm/d** (million cubic meters per day). The dashboard and API support conversion to GWh/d and therms/d. Calorific value is configurable in `settings.yaml`.
