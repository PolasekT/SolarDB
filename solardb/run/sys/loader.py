# -*- coding: utf-8 -*-

"""
SolarDB loading and importing system.
"""

import os
import pathlib
import re
import sys
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from solardb.config import Config
from solardb.config import Configurable
from solardb.db import *
from solardb.logging import Logger


class DataLoader(Logger, Configurable):
    """
    SolarDB loading and importing system.
    """

    COMMAND_NAME = "Data"
    """ Name of this command, used for configuration. """

    def __init__(self, config: Config):
        super().__init__(config=config)
        self._set_instance()

        self.__l.info("Initializing data loader system...")

        self._data = None

    @classmethod
    def register_options(cls, parser: Config.Parser):
        """ Register configuration options for this class. """

        option_name = cls._add_config_parameter("db_path")
        parser.add_argument("-d", "--db-path",
                            action="store",
                            default="", type=str,
                            metavar=("FILENAME.db"),
                            dest=option_name,
                            help="Path to the SolarDB database file.")

    @property
    def data(self) -> SolarDBData:
        """ Get the currently used data source. """
        return self._data

    def _prepare_db(self, db_path: str) -> SolarDBData:
        """ Prepare SolarDB data source for given path. """

        self.__l.info(f"\tUsing database from {db_path}.")

        pathlib.Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        return SolarDBData(
            url=f"sqlite:///{db_path}",
            perform_logging=False,
        )

    def process(self):
        """ Perform data export operations. """

        self.__l.info("Starting data load operations...")

        if self.c.db_path:
            self._data = self._prepare_db(db_path=self.c.db_path)
        else:
            self.__l.warning("\tNo data specified!")

        self.__l.info("\tLoading operations finished!")


