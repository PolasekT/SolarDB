# -*- coding: utf-8 -*-

"""
Data pre-processing and assembly.
"""

import datetime
import os
from typing import List, Optional, Tuple, Union

import pandas as pd
import numpy as np

from solardb.config import SolarDBConfig
from solardb.db.data import get_pp_id
from solardb.db.data import PowerPlantIDT
from solardb.db.data import SolarDBData
from solardb.common.util import date_earliest_date_time
from solardb.common.util import freq_ceil


class SolarDBAssembler(object):
    """
    Data pre-processing and assembly.

    :param data: The source of data.
    """

    def __init__(self, data: SolarDBData):
        self._data = data

    def _convert_date_time(self, dt: Union[datetime.date, datetime.datetime]
                           ) -> datetime.datetime:
        """ Convert date and date-time to consistent format. """
        return dt if isinstance(dt, datetime.datetime) else date_earliest_date_time(date=dt)

    def _prepare_frame_index(self, frequency: str,
                             dt_start: Union[datetime.date, datetime.datetime],
                             dt_end: Union[datetime.date, datetime.datetime],
                             ) -> pd.DatetimeIndex:
        """ Prepare date index <dt_start, dt_end) with given properties. """

        return pd.date_range(
            start=dt_start, end=dt_end,
            freq=frequency, closed="left",
            name="dt",
        )

    def prepare_history(self, pp_id: PowerPlantIDT,
                        dt_start: Union[datetime.date, datetime.datetime],
                        history_cnt: int = 0, history_fallback: bool = False,
                        ) -> pd.DataFrame:
        """
        Prepare power history for given starting date. The actual
        data will cover past history_cnt 5-minute samples.

        :param pp_id: Identifier of the target power plant or inverter.
        :param dt_start: Starting date-time of the prediction frame.
        :param history_cnt: Number of historical power values.
        :param history_fallback: Fallback to zero values for missing
            history (False), or use the available data (True)?

        :return: Returns DataFrame with history_cnt power history values
            with 5-minute frequency.
        """

        pp_id = get_pp_id(pp_id=pp_id)
        dt_start = self._convert_date_time(dt=dt_start)

        dt_alt_start = dt_start - SolarDBConfig.P_FREQUENCY_D * history_cnt
        dt_alt_end = dt_start
        dt_ealt_end = dt_start + SolarDBConfig.P_FREQUENCY_D * history_cnt

        frame_index = self._prepare_frame_index(
            frequency=SolarDBConfig.P_FREQUENCY_D,
            dt_start=dt_alt_start, dt_end=dt_alt_end,
        )

        if history_fallback:
            power_df = self._data.get_pp_power(
                pp_id=pp_id,
                dt_start=dt_alt_start, dt_end=dt_ealt_end,
                columns=None, all_indices=True,
            )

            power_df = power_df.iloc[:history_cnt].reset_index().drop(columns="dt").set_index(
                frame_index,
            )

            return power_df.reset_index().set_index([ "pp_id", "inv_id", "dt" ])
        else:
            power_df = self._data.get_pp_power(
                pp_id=pp_id,
                dt_start=dt_alt_start, dt_end=dt_alt_end,
                columns=None, all_indices=True,
            )
            power_df = power_df.iloc[:history_cnt].reset_index().set_index("dt").reindex(
                frame_index,
            )
            zero_cols = power_df.columns.difference([
                "pp_id", "inv_id", "ipolated", "epolated"
            ])
            # inplace does not work for multiple columns.
            power_df.loc[:, zero_cols] = power_df.loc[:, zero_cols].fillna(value=0)
            power_df.loc[:, "ipolated"].fillna(value=False, inplace=True)
            power_df.loc[:, "epolated"].fillna(value=True, inplace=True)

        return power_df.reset_index().assign(
            pp_id=pp_id.pp_id, inv_id=pp_id.inv_id if pp_id.inv_id is not None else -1,
        ).set_index([ "pp_id", "inv_id", "dt" ])

    def prepare_weather(self, pp_id: PowerPlantIDT,
                        dt_start: Union[datetime.date, datetime.datetime],
                        dt_end: Union[datetime.date, datetime.datetime],
                        weather_scheme: str = "realistic", forecast_delta: int = None,
                        ) -> pd.DataFrame:
        """
        Prepare prediction data for <dt_start, dt_end).

        :param pp_id: Identifier of the target power plant or inverter.
        :param dt_start: Starting date-time of the prediction frame.
        :param dt_end: Ending date-time of the prediction frame.
        :param weather_scheme: Weather sampling scheme to use. Must be
            one of { "measured", "forecast", "realistic" }.
        :param forecast_delta: Delta value in hours, used for the forecast
            weather sampling scheme. Must be specified if "forecast" scheme
            is used!

        :return: Returns DataFrame with all weather features for the requested
            interval, interpolated to 5-minute frequency.
        """

        pp_id = get_pp_id(pp_id=pp_id)
        dt_start = self._convert_date_time(dt=dt_start)
        dt_end = self._convert_date_time(dt=dt_end)

        if weather_scheme == "measured":
            weather_df = self._data.get_pp_weather(
                pp_id=pp_id, dt_start=dt_start, dt_end=dt_end,
                columns=None, all_indices=True, src_age=0,
                interpolate_smooth=True,
            )
        elif weather_scheme == "forecast":
            if forecast_delta is None:
                raise RuntimeError(f"Weather sampling \"{weather_scheme}\" used, "
                                   f"but forecast_delta is not set!")
            weather_df = self._data.get_pp_weather(
                pp_id=pp_id, dt_start=dt_start, dt_end=dt_end,
                columns=None, all_indices=True, src_age=forecast_delta,
                interpolate_smooth=True,
            )
        elif weather_scheme == "realistic":
            weather_df = self._data.get_pp_weather(
                pp_id=pp_id, dt_start=dt_start, dt_end=dt_end,
                columns=None, all_indices=True, src_dt_max=dt_start,
                interpolate_smooth=True,
            )
        else:
            raise RuntimeError(f"Unknown weather sampling scheme \"{weather_scheme}\"")

        exogenous_df = self._data.get_pp_exogenous(
            pp_id=pp_id, dt_start=dt_start, dt_end=dt_end,
            columns=None, all_indices=True,
        )

        return weather_df.merge(
            exogenous_df, left_index=True, right_index=True
        ).reset_index().assign(
            inv_id=pp_id.inv_id if pp_id.inv_id is not None else -1
        ).set_index([ "pp_id", "inv_id", "dt" ])

    def prepare_prediction(self, pp_id: PowerPlantIDT,
                           dt_start: Union[datetime.date, datetime.datetime],
                           dt_end: Union[datetime.date, datetime.datetime],
                           weather_scheme: str = "realistic", forecast_delta: int = None,
                           history_cnt: int = 0, history_fallback: bool = False,
                           ) -> ( pd.DataFrame, pd.DataFrame, pd.DataFrame ):
        """
        Prepare all prediction for <dt_start, dt_end) along with empty
        target frame, further used for evaluation.

        :param pp_id: Identifier of the target power plant or inverter.
        :param dt_start: Starting date-time of the prediction frame.
        :param dt_end: Ending date-time of the prediction frame.
        :param weather_scheme: Weather sampling scheme to use. Must be
            one of { "measured", "forecast", "realistic" }.
        :param forecast_delta: Delta value in hours, used for the forecast
            weather sampling scheme. Must be specified if "forecast" scheme
            is used!
        :param history_cnt: Number of historical power values.
        :param history_fallback: Fallback to zero values for missing
            history (False), or use the available data (True)?

        :return: Returns the following dataframes in order:
            1) Power history : May be empty in case history_cnt == 0.
            2) Weather features : Containing weather and exogenous variables.
            3) Prediction frame : Containing correctly set index for the
                prediction and "power" column with unset values.
        """

        history_df = self.prepare_history(
            pp_id=pp_id, dt_start=dt_start, history_cnt=history_cnt,
            history_fallback=history_fallback,
        )
        weather_df = self.prepare_weather(
            pp_id=pp_id, dt_start=dt_start, dt_end=dt_end,
            weather_scheme=weather_scheme,
            forecast_delta=forecast_delta,
        )
        prediction_df = pd.DataFrame(
            data=None, index=weather_df.index,
        ).assign(power=np.nan)

        return history_df, weather_df, prediction_df

