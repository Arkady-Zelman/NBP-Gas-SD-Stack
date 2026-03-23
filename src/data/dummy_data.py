"""Clearly-labeled dummy / synthetic data generators.

Every generator documents *why* the dummy profile was chosen and what real
data source it approximates, so users know exactly what to replace.

All volumes are in mcm/d (million cubic meters per day).
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.config import get


def _date_range() -> pd.DatetimeIndex:
    start = get("date_range.start", "2020-10-01")
    end = get("date_range.end", "2026-03-23")
    return pd.date_range(start, end, freq="D")


class DummyDataGenerator:
    """Factory for synthetic component series with documented assumptions."""

    # ==================================================================
    # SUPPLY COMPONENTS
    # ==================================================================

    @staticmethod
    def ukcs_production() -> pd.DataFrame:
        """UKCS continental-shelf gas production.

        DUMMY JUSTIFICATION:
        - Baseload ~95 mcm/d with gradual annual decline of ~3 %/yr,
          matching NSTA's reported decline rates (2020–2025).
        - Summer maintenance dips of ~15 % applied Jul–Sep each year.
        - Gaussian noise ±3 mcm/d to represent field-level variability.
        - REPLACE WITH: Monthly NSTA production data downloaded from
          https://www.nstauthority.co.uk/data-centre/
        """
        dates = _date_range()
        years_elapsed = (dates - dates[0]).days / 365.25
        base = 95 * (0.97 ** years_elapsed)
        summer_dip = np.where(dates.month.isin([7, 8, 9]), 0.85, 1.0)
        noise = np.random.default_rng(42).normal(0, 3, len(dates))
        volume = np.maximum(base * summer_dip + noise, 0)
        return pd.DataFrame({
            "date": dates,
            "volume_mcm": volume,
            "source": "UKCS",
            "data_quality": "dummy",
        })

    @staticmethod
    def norway_pipelines() -> pd.DataFrame:
        """Norwegian pipeline imports (Langeled + Vesterled).

        DUMMY JUSTIFICATION:
        - Combined capacity ~100 mcm/d; typical utilisation ~70–90 %.
        - Winter ramp-up (Oct–Mar ≈ 85 mcm/d) vs summer (Apr–Sep ≈ 55 mcm/d)
          reflecting seasonal Norwegian export patterns.
        - Annual planned maintenance in Jul drives a sharper dip.
        - REPLACE WITH: Gassco transparency portal daily flow data
          https://www.gassco.no/en/our-activities/transparency/
        """
        dates = _date_range()
        winter = np.where(dates.month.isin([10, 11, 12, 1, 2, 3]), 85, 55)
        maint = np.where(dates.month == 7, 0.5, 1.0)
        noise = np.random.default_rng(43).normal(0, 5, len(dates))
        volume = np.maximum(winter * maint + noise, 0)
        return pd.DataFrame({
            "date": dates,
            "volume_mcm": volume,
            "source": "Norway",
            "data_quality": "dummy",
        })

    @staticmethod
    def iuk_import() -> pd.DataFrame:
        """IUK (Interconnector UK) imports from Zeebrugge to Bacton.

        DUMMY JUSTIFICATION:
        - Bidirectional pipe, capacity ~25.5 mcm/d each way.
        - Net import typically positive in winter (~15 mcm/d), near zero or
          export in summer; modelled here as import-only supply component.
        - REPLACE WITH: National Gas Data Portal daily physical flows
          https://data.nationalgas.com/
        """
        dates = _date_range()
        seasonal = np.where(dates.month.isin([10, 11, 12, 1, 2, 3]), 15, 3)
        noise = np.random.default_rng(44).normal(0, 3, len(dates))
        volume = np.maximum(seasonal + noise, 0)
        return pd.DataFrame({
            "date": dates,
            "volume_mcm": volume,
            "source": "IUK Import",
            "data_quality": "dummy",
        })

    @staticmethod
    def bbl_pipeline() -> pd.DataFrame:
        """BBL (Balgzand-Bacton Line) imports from Netherlands.

        DUMMY JUSTIFICATION:
        - Capacity ~15 mcm/d, uni-directional into UK.
        - Average utilisation ~60 %, higher in winter (~12 mcm/d) than
          summer (~6 mcm/d), reflecting Dutch export availability.
        - REPLACE WITH: National Gas Data Portal daily flows
        """
        dates = _date_range()
        seasonal = np.where(dates.month.isin([10, 11, 12, 1, 2, 3]), 12, 6)
        noise = np.random.default_rng(45).normal(0, 2, len(dates))
        volume = np.maximum(seasonal + noise, 0)
        return pd.DataFrame({
            "date": dates,
            "volume_mcm": volume,
            "source": "BBL",
            "data_quality": "dummy",
        })

    @staticmethod
    def lng_terminals() -> pd.DataFrame:
        """LNG terminal send-out (South Hook, Dragon, Isle of Grain combined).

        DUMMY JUSTIFICATION:
        - Combined UK regasification capacity ~55 mcm/d.
        - Send-out highly seasonal and volatile — driven by global LNG spot
          prices vs NBP.  Winter average ~35 mcm/d, summer ~15 mcm/d with
          occasional cargoes creating spikes.
        - REPLACE WITH: GIE ALSI API daily send-out data
          https://alsi.gie.eu/
        """
        dates = _date_range()
        rng = np.random.default_rng(46)
        seasonal = np.where(dates.month.isin([10, 11, 12, 1, 2, 3]), 35, 15)
        spikes = rng.choice([0, 10, 20], size=len(dates), p=[0.85, 0.10, 0.05])
        noise = rng.normal(0, 4, len(dates))
        volume = np.maximum(seasonal + spikes + noise, 0)
        return pd.DataFrame({
            "date": dates,
            "volume_mcm": volume,
            "source": "LNG",
            "data_quality": "dummy",
        })

    @staticmethod
    def storage_withdrawal() -> pd.DataFrame:
        """UK gas storage withdrawals.

        DUMMY JUSTIFICATION:
        - Post-Rough closure UK storage capacity is limited (~15 mcm/d max
          withdrawal); Rough partially reopened 2024 adds ~5 mcm/d.
        - Withdrawals concentrated Nov–Mar, zero in injection season.
        - REPLACE WITH: GIE AGSI+ API withdrawal data
          https://agsi.gie.eu/
        """
        dates = _date_range()
        rng = np.random.default_rng(47)
        winter_mask = dates.month.isin([11, 12, 1, 2, 3])
        volume = np.where(winter_mask, rng.uniform(2, 15, len(dates)), 0)
        return pd.DataFrame({
            "date": dates,
            "volume_mcm": volume,
            "source": "Storage Withdrawal",
            "data_quality": "dummy",
        })

    # ==================================================================
    # DEMAND COMPONENTS
    # ==================================================================

    @staticmethod
    def residential() -> pd.DataFrame:
        """Residential / commercial (LDZ) demand.

        DUMMY JUSTIFICATION:
        - Heavily temperature-driven.  Winter peak ~200 mcm/d in a cold
          snap, summer baseload ~30 mcm/d.  Modelled with a sinusoidal
          seasonal shape peaking mid-January.
        - REPLACE WITH: National Gas LDZ actual daily demand API plus
          CWV regression model for forecasting.
        """
        dates = _date_range()
        day_of_year = dates.dayofyear
        seasonal = 115 + 85 * np.cos(2 * np.pi * (day_of_year - 15) / 365)
        noise = np.random.default_rng(50).normal(0, 8, len(dates))
        volume = np.maximum(seasonal + noise, 20)
        return pd.DataFrame({
            "date": dates,
            "volume_mcm": volume,
            "source": "Residential",
            "data_quality": "dummy",
        })

    @staticmethod
    def industrial() -> pd.DataFrame:
        """Industrial (DM + LDM) demand.

        DUMMY JUSTIFICATION:
        - Relatively flat baseload ~30 mcm/d with slight winter uplift
          (~35 mcm/d) due to process heat needs.  Weekday/weekend pattern
          not modelled in dummy.
        - REPLACE WITH: National Gas DM/LDM demand data (partially API,
          partially manual download).
        """
        dates = _date_range()
        seasonal = np.where(dates.month.isin([10, 11, 12, 1, 2, 3]), 35, 30)
        noise = np.random.default_rng(51).normal(0, 2, len(dates))
        volume = np.maximum(seasonal + noise, 15)
        return pd.DataFrame({
            "date": dates,
            "volume_mcm": volume,
            "source": "Industrial",
            "data_quality": "dummy",
        })

    @staticmethod
    def power_gen() -> pd.DataFrame:
        """CCGT gas-for-power demand.

        DUMMY JUSTIFICATION:
        - UK CCGT fleet ~30 GW capacity.  Utilisation depends on spark
          spread, wind output, and interconnector flows.  Average ~25 mcm/d
          with higher demand in winter evenings (~35 mcm/d) and lower in
          summer (~15 mcm/d), with intermittent spikes when wind is low.
        - REPLACE WITH: Elexon/BMRS half-hourly CCGT generation data
          converted via MW-to-mcm using heat rates.
          https://www.bmreports.com/
        """
        dates = _date_range()
        rng = np.random.default_rng(52)
        seasonal = np.where(dates.month.isin([10, 11, 12, 1, 2, 3]), 35, 18)
        wind_dips = rng.choice([0, 10, 15], size=len(dates), p=[0.75, 0.15, 0.10])
        noise = rng.normal(0, 4, len(dates))
        volume = np.maximum(seasonal + wind_dips + noise, 5)
        return pd.DataFrame({
            "date": dates,
            "volume_mcm": volume,
            "source": "Power Gen",
            "data_quality": "dummy",
        })

    @staticmethod
    def iuk_export() -> pd.DataFrame:
        """IUK exports (Bacton to Zeebrugge).

        DUMMY JUSTIFICATION:
        - UK exports gas to continent mainly in summer when NBP < TTF.
          Summer average ~10 mcm/d, winter near zero.
        - REPLACE WITH: National Gas Data Portal net IUK flow (negative =
          export from UK perspective).
        """
        dates = _date_range()
        seasonal = np.where(dates.month.isin([4, 5, 6, 7, 8, 9]), 10, 1)
        noise = np.random.default_rng(53).normal(0, 2, len(dates))
        volume = np.maximum(seasonal + noise, 0)
        return pd.DataFrame({
            "date": dates,
            "volume_mcm": volume,
            "source": "IUK Export",
            "data_quality": "dummy",
        })

    @staticmethod
    def moffat_export() -> pd.DataFrame:
        """Moffat interconnector exports to Ireland.

        DUMMY JUSTIFICATION:
        - Ireland is heavily reliant on UK gas.  Relatively stable
          ~15 mcm/d with mild winter uplift (~18 mcm/d).
        - REPLACE WITH: National Gas Data Portal Moffat offtake data.
        """
        dates = _date_range()
        seasonal = np.where(dates.month.isin([10, 11, 12, 1, 2, 3]), 18, 14)
        noise = np.random.default_rng(54).normal(0, 1.5, len(dates))
        volume = np.maximum(seasonal + noise, 5)
        return pd.DataFrame({
            "date": dates,
            "volume_mcm": volume,
            "source": "Moffat Export",
            "data_quality": "dummy",
        })

    @staticmethod
    def storage_injection() -> pd.DataFrame:
        """UK gas storage injections.

        DUMMY JUSTIFICATION:
        - Injection season Apr–Oct.  Average ~8 mcm/d in peak injection
          months (May–Aug), tapering at shoulders.
        - REPLACE WITH: GIE AGSI+ injection data.
        """
        dates = _date_range()
        rng = np.random.default_rng(55)
        inj_mask = dates.month.isin([4, 5, 6, 7, 8, 9, 10])
        base = np.where(dates.month.isin([5, 6, 7, 8]), 8, 4)
        volume = np.where(inj_mask, rng.uniform(0, base * 2, len(dates)), 0)
        return pd.DataFrame({
            "date": dates,
            "volume_mcm": volume,
            "source": "Storage Injection",
            "data_quality": "dummy",
        })
