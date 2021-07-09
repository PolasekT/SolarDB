# -*- coding: utf-8 -*-

"""
Data evaluation and visualization.
"""

import datetime
import os
from typing import List, Optional, Tuple, Union

import numpy as np
import pandas as pd
import scipy.stats as scs
import sklearn.metrics as sklm

from solardb.common import update_dict_recursively
from solardb.config import SolarDBConfig
from solardb.db.data import PowerPlantID
from solardb.db.data import PowerPlantIDT
from solardb.db.data import SolarDBData


class SolarDBEvaluator(object):
    """
    Data evaluation and visualization.

    :param data: The source of data.
    """

    def __init__(self, data: SolarDBData):
        self._data = data

    def _evaluate_power(self, pp_id: PowerPlantID, power_df: pd.DataFrame,
                        dt_start: datetime.datetime, dt_end: datetime.datetime,
                        prediction_type: str = "power_ac", fallback: bool = False,
                        ) -> dict:
        """
        Evaluate provided power DataFrame against the ground
        truth in the database.

        :param pp_id: Identifier of the power plant or inverter.
        :param power_df: Input DataFrame to evaluate.
        :param prediction_type: Name of the database column the
            prediction_df contains - "power_ac" or "power_dc".
        :param fallback: Fallback to predicted values.

        :return: Returns dictionary containing the results.
        """

        ground_df = self._data.get_pp_power(
            pp_id=pp_id, dt_start=dt_start, dt_end=dt_end,
            columns=[ prediction_type ], all_indices=True,
        ).rename(columns={ prediction_type: "power" })
        pred_df = power_df.loc[:, [ "power" ]]

        # Fallback to predicted values for power history:
        if ground_df.empty and fallback:
            ground_df = pred_df

        # Hour time integral.
        ground_int = ground_df.sum() * SolarDBConfig.P_FREQUENCY_H
        pred_int = pred_df.sum() * SolarDBConfig.P_FREQUENCY_H

        # Calculate percentage error between them, fixing division by zero.
        error = (1.0 - (pred_int / (ground_int + np.finfo(float).eps))).rename("PError [%]")
        sq_sums = (ground_df ** 2.0).sum()

        # Quantitative metrics:
        difference = pred_df - ground_df
        mse = (difference ** 2.0).mean().rename("MSE [-]")
        rmse = mse ** 0.5
        rrmse = (mse / (sq_sums + np.finfo(float).eps)) ** 0.5
        r2 = sklm.r2_score(ground_df["power"], pred_df["power"])
        corp = scs.pearsonr(pred_df["power"], ground_df["power"])
        cors = scs.spearmanr(pred_df["power"], ground_df["power"])

        return {
            "error": error[0],
            "mse": mse[0],
            "rmse": rmse[0],
            "rrmse": rrmse[0],
            "r2": r2,
            "corp": corp,
            "cors": cors,
        }

    def _evaluate_history(self, pp_id: PowerPlantID, history_df: pd.DataFrame,
                          dt_start: datetime.datetime, dt_end: datetime.datetime,
                          prediction_type: str = "power_ac",
                          ) -> dict:
        """
        Evaluate provided history DataFrame against the ground
        truth in the database.

        :param pp_id: Identifier of the power plant or inverter.
        :param history_df: Input DataFrame to evaluate.
        :param prediction_type: Name of the database column the
            prediction_df contains - "power_ac" or "power_dc".

        :return: Returns dictionary containing the results.
        """

        return self._evaluate_power(
            pp_id=pp_id, power_df=history_df,
            dt_start=history_df.index.get_level_values(level="dt").min(),
            dt_end=history_df.index.get_level_values(level="dt").max(),
            prediction_type=prediction_type, fallback=True,
        )

    def _evaluate_weather(self, pp_id: PowerPlantID, weather_df: pd.DataFrame,
                          dt_start: datetime.datetime, dt_end: datetime.datetime,
                          ) -> dict:
        """
        Evaluate provided weather DataFrame against the ground
        truth in the database.

        :param pp_id: Identifier of the power plant or inverter.
        :param weather_df: Input DataFrame to evaluate.
        :param dt_start: Date-time to start the evaluation at.
        :param dt_end: Date-time to end the evaluation at.

        :return: Returns dictionary containing the results.
        """

        ground_df = self._data.get_pp_weather(
            pp_id=pp_id, dt_start=dt_start, dt_end=dt_end,
            columns=SolarDBConfig.WEATHER_FEATURES,
            all_indices=True, src_age=0, interpolate_smooth=True,
        )
        pred_df = weather_df.loc[:, SolarDBConfig.WEATHER_FEATURES]

        # Hour time integral.
        ground_int = ground_df.sum() * SolarDBConfig.P_FREQUENCY_H
        pred_int = pred_df.sum() * SolarDBConfig.P_FREQUENCY_H

        # Calculate percentage error between them, fixing division by zero.
        error = (1.0 - (pred_int / (ground_int + np.finfo(float).eps))).rename("PError [%]")
        sq_sums = (ground_df ** 2.0).sum()

        # Quantitative metrics:
        difference = pred_df - ground_df
        mse = (difference ** 2.0).mean().rename("MSE [-]")
        rmse = mse ** 0.5
        rrmse = (mse / (sq_sums + np.finfo(float).eps)) ** 0.5

        return {
            "error": error,
            "mse": mse,
            "rmse": rmse,
            "rrmse": rrmse,
        }

    def evaluate_prediction(self,
                            prediction_df: pd.DataFrame,
                            prediction_type: str = "power_ac",
                            history_df: pd.DataFrame = pd.DataFrame(),
                            weather_df: pd.DataFrame = pd.DataFrame(),
                            pp_id: Optional[PowerPlantIDT] = None,
                            ) -> dict:
        """
        Evaluate provided prediction DataFrame and return results.

        :param prediction_df: Input DataFrame to evaluate. Should
            be indexed by ( "pp_id", "inv_id", "dt" ) or ( "dt" ) if
            pp_id is provided.
        :param prediction_type: Name of the database column the
            prediction_df contains - "power_ac" or "power_dc".
        :param history_df: Optional power history DataFrame, used
            for additional statistics. Should be indexed by ( "pp_id",
            "inv_id", "dt" ) or ( "dt" ) if pp_id is provided.
        :param weather_df: Optional weather feature DataFrame, used
            for additional statistics. Should be indexed by ( "pp_id",
            "dt" ) or ( "dt" ) if pp_id is provided. Must use the
            same length and index as the prediction_df.
        :param pp_id: Optional power plant ID used when not available
            in any of the DataFrames.

        :return: Returns a dictionary containing evaluation results.
        """

        has_history = len(history_df) > 0
        has_weather = len(weather_df) > 0

        if "pp_id" not in prediction_df.index.names or \
           "inv_id" not in prediction_df.index.names:
            if pp_id is None and len(prediction_df) > 0:
                raise RuntimeError(f"Prediction frame is missing indices ({prediction_df.index.names}!")
            prediction_df = prediction_df.reset_index().assign(
                pp_id=0 if pp_id is None else pp_id.pp_id,
                inv_id=0 if pp_id is None else (-1 if pp_id.is_whole_power_plant() else pp_id.inv_id)
            ).set_index([ "pp_id", "inv_id", "dt" ])

        if "pp_id" not in history_df.index.names or \
           "inv_id" not in history_df.index.names:
            if pp_id is None and has_history:
                raise RuntimeError(f"History frame is missing indices ({history_df.index.names}!")
            history_df = prediction_df.reset_index().assign(
                pp_id=0 if pp_id is None else pp_id.pp_id,
                inv_id=0 if pp_id is None else (-1 if pp_id.is_whole_power_plant() else pp_id.inv_id)
            ).set_index([ "pp_id", "inv_id", "dt" ])

        if "power" not in prediction_df.columns:
            raise RuntimeError(f"Provided prediction DataFrame does not contain \"power\" column!")

        if prediction_df["power"].count() != len(prediction_df):
            raise RuntimeError(f"Provided prediction DataFrame contains NaN or other invalid values!")

        if has_weather:
            if len(weather_df) != len(prediction_df):
                raise RuntimeError(f"Prediction and Weather DataFrames are not the same length "
                                   f"({len(prediction_df)} vs {len(weather_df)})!")
            weather_df = weather_df.set_index(prediction_df.index)

        if has_history:
            if prediction_type in history_df.columns and "power" not in history_df.columns:
                history_df.rename(columns={ prediction_type: "power" }, inplace=True)
            if "power" not in history_df.columns:
                raise RuntimeError(f"Provided history DataFrame does not contain \"power\" column!")

        pp_ids = prediction_df.index.unique(level="pp_id")
        inv_ids = prediction_df.index.unique(level="inv_id")
        if len(pp_ids) != 1 or len(inv_ids) != 1:
            raise RuntimeError(f"Prediction DataFrame does not contain precisely on power plant "
                               f"({pp_ids}, {inv_ids})!")

        pp_id = PowerPlantID(
            pp_id=pp_ids[0], inv_id=inv_ids[0],
        )
        dt_start = prediction_df.index.get_level_values(level="dt").min()
        dt_end = prediction_df.index.get_level_values(level="dt").max()
        dt_wth_start = weather_df.index.get_level_values(level="dt").min() if has_weather else dt_start
        dt_wth_end = weather_df.index.get_level_values(level="dt").max() if has_weather else dt_end

        if has_weather and (dt_start != dt_wth_start or dt_end != dt_wth_end):
            raise RuntimeError(f"Provided weather DataFrame does not cover the same date-time range "
                               f"as the prediction frame: <{dt_start}, {dt_end}> vs <{dt_wth_start}, {dt_wth_end}>")

        stats = {
            "power": self._evaluate_power(
                pp_id=pp_id, power_df=prediction_df,
                dt_start=dt_start, dt_end=dt_end,
                prediction_type=prediction_type,
            ),
            "weather": self._evaluate_weather(
                pp_id=pp_id, weather_df=weather_df,
                dt_start=dt_start, dt_end=dt_end,
            ) if has_weather else { },
            "history": self._evaluate_history(
                pp_id=pp_id, history_df=history_df,
                dt_start=dt_start, dt_end=dt_end,
            ) if has_history else { },
        }

        return stats




