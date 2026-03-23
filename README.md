# NBP Gas Supply & Demand Stack Model

A modular Python model of UK gas (NBP) supply and demand with an interactive Streamlit dashboard, powered entirely by live public API data. Dark-themed UI with crosshair navigation, smooth page transitions, and clean data formatting.

## Quick Start

```bash
pip install -r requirements.txt
python -m streamlit run src/dashboard/app.py --server.headless true
```

Opens at **http://localhost:8501**. On first launch, data is fetched from live APIs (~60 seconds). Subsequent loads are instant from the local parquet cache. Data auto-refreshes once per day; click **Refresh Data Now** in the sidebar for an immediate pull.

## Hosted Dashboard

Deployed on Streamlit Community Cloud for instant access from any device:

```
https://arkady-zelman-nbp-gas-sd-stack.streamlit.app
```

Every push to `master` auto-redeploys.

## Project Structure

```
config/settings.yaml        — global config (units, API keys, date ranges)
data/raw/                   — parquet cache (auto-generated, gitignored)
data/manual/                — drop CSV files here for manual-input overrides
src/
  base.py                   — StackComponent base class
  units.py                  — mcm <> GWh <> therms conversion
  config.py                 — YAML config loader
  data/
    national_gas.py         — National Gas CSV download API client (no auth)
    elexon_api.py           — Elexon BMRS FUELHH client (no auth)
    gie_api.py              — GIE AGSI+/ALSI client (fallback)
    cache.py                — local parquet cache layer
    loaders.py              — data waterfall: cache > API > manual CSV > dummy
    refresh.py              — standalone/scheduled data refresh script
    dummy_data.py           — synthetic fallback generators (documented assumptions)
  supply/                   — 6 supply components + SupplyStack aggregator
  demand/                   — 6 demand components + DemandStack aggregator
  balance/                  — S&D balance engine
  scenarios/                — scenario engine with pre-built templates
  aggregation/              — daily > monthly > seasonal > annual rollup
  dashboard/app.py          — Streamlit dashboard (5 pages)
tests/                      — pytest suite (9 tests)
```

## Data Sources

All components pull live data from public APIs — **no API keys or manual downloads required**.

### Supply Components

| Component | Avg (mcm/d) | API Source | PUBOB IDs |
|---|---|---|---|
| UKCS Production | ~95 | National Gas (11 terminal entries) | PUBOB407, PUBOB401, PUBOB377, etc. |
| Norwegian Pipelines | ~54 | National Gas (Langeled entry volume) | PUBOB452 |
| IUK Import | ~21 | National Gas (Bacton IC physical flows) | PUBOB2038 |
| BBL Pipeline | ~6 | National Gas (BactonBBL physical flows) | PUBOBJ1307 |
| LNG Terminals | ~45 | National Gas (South Hook + Dragon + Grain) | PUBOB3480, PUBOB3564, PUBOB371, PUBOB3473 |
| Storage Withdrawal | ~15 | National Gas (9 facilities outflows) | PUBOBJ2413-PUBOBJ2422 |

### Demand Components

| Component | Avg (mcm/d) | API Source | Notes |
|---|---|---|---|
| Residential/Commercial | ~96 | National Gas NDM (13 LDZ zones) | kWh converted to mcm |
| Industrial | ~19 | National Gas DM (13 LDZ zones) | kWh converted to mcm |
| CCGT Power Gen | ~47 | Elexon BMRS FUELHH | MW converted to mcm via thermal efficiency |
| IUK Export | ~21 | National Gas (Bacton IC flows) | Same endpoint as IUK Import |
| Moffat Export | ~15 | National Gas (Moffat physical flows) | PUBOB2039 |
| Storage Injection | ~12 | National Gas (9 facilities inflows) | PUBOBJ2401-PUBOBJ2410 |

### API Endpoints

| API | Auth | Endpoint |
|---|---|---|
| National Gas | None | `data.nationalgas.com/api/find-gas-data-download` |
| Elexon BMRS | None | `data.elexon.co.uk/bmrs/api/v1/datasets/FUELHH` |
| GIE AGSI+ | None (fallback only) | `agsi.gie.eu/api` |

## Data Refresh

Data is cached locally as parquet files in `data/raw/`. The dashboard auto-refreshes once per day when the cache is older than 24 hours.

**Manual refresh from dashboard:** Click the **Refresh Data Now** button in the sidebar.

**Standalone refresh script:**

```bash
python -m src.data.refresh              # one-shot refresh
python -m src.data.refresh --loop 24    # repeat every 24 hours
```

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
- **Seasonal** — Gas Year quarters (Q1=Oct-Dec, Q2=Jan-Mar, Q3=Apr-Jun, Q4=Jul-Sep)
- **Annual** — Gas Year (Oct-Sep)

## Units

Base unit is **mcm/d** (million cubic meters per day). The dashboard supports conversion to GWh/d and therms/d. Calorific value is configurable in `config/settings.yaml`.
