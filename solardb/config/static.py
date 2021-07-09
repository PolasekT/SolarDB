# -*- coding: utf-8 -*-

"""
SolarDB static configuration.
"""

import pandas as pd
import numpy as np


class SolarDBConfig:
    """ Static SolarDB configuration. """

    POWER_FEATURES = [
        "power_ac", "power_dc",
    ]
    """ Primary power feature columns. """

    WEATHER_FEATURES = [
        "precip_int", "precip_prob",
        "temp", "apparent_temp",
        "dew_point", "humidity",
        "pressure",
        "wind_speed", "wind_bearing",
        "cloud_cover", "visibility",
    ]
    """ Primary weather feature columns. """

    P_FREQUENCY_S = "5min"
    """ String representation of the primary data frequency (power, exogenous). """
    P_FREQUENCY_D = pd.Timedelta(P_FREQUENCY_S)
    """ Delta representation of the primary data frequency (power, exogenous). """
    P_FREQUENCY_H = P_FREQUENCY_D.seconds / ( 60.0 * 60.0 )
    """ Number of hours in the primary data frequency (power, exogenous). """

    S_FREQUENCY_S = "1h"
    """ String representation of the secondary data frequency (weather). """
    S_FREQUENCY_D = pd.Timedelta(S_FREQUENCY_S)
    """ Delta representation of the secondary data frequency (weather). """
    S_FREQUENCY_H = S_FREQUENCY_D.seconds / ( 60.0 * 60.0 )
    """ Number of hours in the secondary data frequency (weather). """
