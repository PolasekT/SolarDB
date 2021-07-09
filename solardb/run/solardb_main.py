# -*- coding: utf-8 -*-

"""
Helper script for testing and data manipulation.
"""

__author__ = "Tomas Polasek"
__copyright__ = "Copyright 2021"
__credits__ = "Tomas Polasek"
__license__ = "MIT"
__version__ = "0.0.1"
__maintainer__ = 'Tomas Polasek'
__email__ = "ipolasek@fit.vutbr.cz"
__status__ = "Development"

# Common:
import logging
import matplotlib.pyplot
import os
import sys
from typing import List, Optional
import traceback

# Utilities:
from solardb.config import Config
from solardb.config import Configurable
from solardb.logging import Logger

# Systems:
from solardb.logging import LoggingConfigurator
from solardb.run.sys import DataLoader
from solardb.run.sys import DataSaver


class SolarDBMain(Logger, Configurable):
    """
    Wrapper for the helper script main function and parameter parsing.
    """

    COMMAND_NAME = "root"
    """ Name of this command, used for configuration. Using the root namespace. """

    def __init__(self):
        pass

    @classmethod
    def register_options(cls, parser: Config.Parser):
        """ Register configuration options for this class. """


    def initialize_libraries(self):
        """
        Perform library initialization.
        """

        self.__l.info("Initializing libraries...")

        if "DISPLAY" in os.environ and os.environ["DISPLAY"] == "False":
            matplotlib.pyplot.switch_backend("agg")

    def print_banner(self):
        print("""
         .M\"""bgd         `7MM                  `7MM\"""Yb. `7MM\"""Yp, 
        ,MI    "Y           MM                    MM    `Yb. MM    Yb 
        `MMb.      ,pW"Wq.  MM   ,6"Yb.  `7Mb,od8 MM     `Mb MM    dP 
          `YMMNq. 6W'   `Wb MM  8)   MM    MM' "' MM      MM MM\"""bg. 
        .     `MM 8M     M8 MM   ,pm9MM    MM     MM     ,MP MM    `Y 
        Mb     dM YA.   ,A9 MM  8M   MM    MM     MM    ,dP' MM    ,9 
        P"Ybmmd"   `Ybmd9'.JMML.`Moo9^Yo..JMML. .JMMmmmdP' .JMMmmmd9  
        
        SolarDB API - Python API for the SolarDB photovoltaic dataset.
        
        Authors:    Tomas Polasek
        E-mails:    ipolasek@fit.vutbr.cz
        Version:    0.0.1
        License:    MIT
        
        For further information about script parameters, please see the 
          --help or -h parameter.
          
        Requires following libraries to run:
          * numpy, matplotlib, pandas, progress, scipy, sqlalchemy
          * mysqlclient or some other kind of MySQL backend
        """)

    def main(self, argv: List[str]) -> int:
        """
        Main function which contains:
            * Parameter processing
            * Calling inner functions according to the parameters
            * Error reporting

        :param argv: Argument vector including the app name.

        :return: Returns success code.
        """

        # Initialize configuration.
        config = Config()

        # Register systems.
        SolarDBMain.register_config(config)
        LoggingConfigurator.register_config(config)
        DataLoader.register_config(config)
        DataSaver.register_config(config)

        config.init_options()

        # Parse arguments passed from the command line.
        argv = argv[1:]
        config.parse_args(argv)

        # Initialize configuration of this application.
        super().__init__(config=config)
        self._set_instance()

        # Enable requested logging.
        logging_config = LoggingConfigurator(config)

        # Display banner if verbose.
        if logging_config.logging_level >= logging.WARNING:
            self.print_banner()

        # Initialize library and configure.
        self.initialize_libraries()

        # Initialize systems.
        try:
            data_loader = DataLoader(config)
            data_saver = DataSaver(config)
        except Exception as e:
            self.__l.error(f"Exception occurred when initializing systems! : " \
                           f"\n{e}\n{traceback.format_exc()}")
            return -1

        # Load data.
        try:
            data_loader.process()
        except Exception as e:
            self.__l.error(f"Exception occurred when loading data! : " \
                           f"\n{e}\n{traceback.format_exc()}")
            return -1

        # Save data.
        try:
            data_saver.process()
        except Exception as e:
            self.__l.error(f"Exception occurred when saving data! : " \
                           f"\n{e}\n{traceback.format_exc()}")
            return -1

        return 0


def main() -> int:
    solardb_main = SolarDBMain()
    return solardb_main.main(sys.argv)


if __name__ == "__main__":
    exit(main())
