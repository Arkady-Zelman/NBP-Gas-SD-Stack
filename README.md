# NBP Gas Supply & Demand Stack Model

A modular Python model of UK gas (NBP) supply and demand fundamentals with multi-horizon aggregation, scenario analysis, and an interactive Streamlit dashboard.

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Launch the dashboard
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
  data/                     — data ingestion (API clients, CSV reader, dummy generators)
  supply/                   — 6 supply components + SupplyStack aggregator
  demand/                   — 6 demand components + DemandStack aggregator
  balance/                  — S&D balance engine
  scenarios/                — scenario engine with pre-built templates
  aggregation/              — daily → monthly → seasonal → annual rollup
  dashboard/app.py          — Streamlit dashboard (5 pages)
```

## Supply Components

| Component | Default Source | Replace With |
|---|---|---|
| UKCS Production | dummy | NSTA monthly CSV |
| Norwegian Pipelines | dummy → API | Gassco transparency |
| IUK Import | dummy → API | National Gas Data Portal |
| BBL Pipeline | dummy → API | National Gas Data Portal |
| LNG Terminals | dummy → API | GIE ALSI (register at gie.eu) |
| Storage Withdrawal | dummy → API | GIE AGSI+ |

## Demand Components

| Component | Default Source | Replace With |
|---|---|---|
| Residential/Commercial | dummy → API | National Gas LDZ demand |
| Industrial | dummy | National Gas DM/LDM or manual CSV |
| CCGT Power Gen | dummy → API | Elexon/BMRS |
| IUK Export | dummy → API | National Gas Data Portal |
| Moffat Export | dummy → API | National Gas Data Portal |
| Storage Injection | dummy → API | GIE AGSI+ |

## Connecting Live Data

1. **GIE (AGSI+ / ALSI)**: Register free at https://www.gie.eu/, copy your API key into `config/settings.yaml` under `api_keys.gie`.
2. **National Gas**: The Data Portal at https://data.nationalgas.com/ is publicly accessible. Add any required key to `api_keys.national_gas`.
3. **Elexon/BMRS**: Register at https://www.elexon.co.uk/ and add the key to `api_keys.elexon`.
4. **Manual CSVs**: Place files in `data/manual/` with columns `date,volume_mcm`. See existing templates.

The model tries APIs first, then falls back to manual CSV, then to labeled dummy data.

## Scenario Analysis

Pre-built scenarios accessible via the dashboard or programmatically:

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
