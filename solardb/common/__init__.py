# -*- coding: utf-8 -*-

"""
SolarDB common utilities and classes.
"""

__all__ = [
    "Cache",
    "deep_copy_dict",
    "update_dict_recursively",

    "Profiler",
    "ProfTimer",

    "date_earliest_date_time",
    "date_latest_date_time",
    "dict_of_lists",
    "freq_ceil",
    "freq_floor",
    "freq_delta",
    "freq_min",
    "numpy_array_to_tuple_numpy",
    "parse_bool_string",
    "parse_datetime",
    "parse_datetime_interval",
    "parse_list",
    "parse_list_string",
    "recurse_dict",
    "recurse_dictionary_endpoint",
    "timedelta_total_seconds",
    "tuple_array_to_numpy",
]

from solardb.common.cache import Cache
from solardb.common.cache import deep_copy_dict
from solardb.common.cache import update_dict_recursively

from solardb.common.profiler import Profiler
from solardb.common.profiler import ProfTimer

from solardb.common.util import date_earliest_date_time
from solardb.common.util import date_latest_date_time
from solardb.common.util import dict_of_lists
from solardb.common.util import freq_ceil
from solardb.common.util import freq_floor
from solardb.common.util import freq_delta
from solardb.common.util import freq_min
from solardb.common.util import numpy_array_to_tuple_numpy
from solardb.common.util import parse_bool_string
from solardb.common.util import parse_datetime
from solardb.common.util import parse_datetime_interval
from solardb.common.util import parse_list
from solardb.common.util import parse_list_string
from solardb.common.util import recurse_dict
from solardb.common.util import recurse_dictionary_endpoint
from solardb.common.util import timedelta_total_seconds
from solardb.common.util import tuple_array_to_numpy
