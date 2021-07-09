# -*- coding: utf-8 -*-

"""
Logging setup and configuration
"""

import datetime
import logging
import os
import pathlib
import sys
import traceback
from typing import List, Optional

from solardb.config import Config
from solardb.config import Configurable
from solardb.common import Profiler
from solardb.common import parse_bool_string

from solardb.logging.logger import Logger
from solardb.logging.logger import LoggingBar
from solardb.logging.logger import LogMeta


class LoggingConfigurator(Logger, Configurable):
    """
    Configuration of the logging system.

    :param config: Application configuration.
    """

    COMMAND_NAME = "Logging"
    """ Name of this command, used for configuration. """

    def __init__(self, config: Config):
        super().__init__(config=config)

        self._logging_directory = self._generate_logging_directory(
            base_dir=self.c.logging_directory,
            logging_name=self.c.logging_name,
            use_timestamp=self.c.logging_directory_use_timestamp,
            use_model=self.c.logging_directory_use_model
        )
        self.setup_profiling(
            enabled=self.c.prof_enabled,
            display=self.c.prof_display,
            save_results_dir=self.c.prof_save_dir,
        )
        self.setup_logging(
            logging_level=self.c.verbosity,
            save_log_to_file=self.c.save_log_to_file,
            file_prefix=self._logging_directory,
        )
        self.__l.info("Initializing logging configurator...")

    @classmethod
    def register_options(cls, parser: Config.Parser):
        """ Register configuration options for this class. """

        option_name = cls._add_config_parameter("verbosity")
        parser.add_argument("-v", "--verbose",
                            action="store_const",
                            const=logging.INFO,
                            default=logging.INFO,
                            dest=option_name,
                            help="Set to enable informative messages, this is the default state.")

        parser.add_argument("-vv", "--very-verbose",
                            action="store_const",
                            const=logging.DEBUG,
                            dest=option_name,
                            help="Set to enable debug messages.")

        option_name = cls._add_config_parameter("quiet")
        parser.add_argument("-q", "--quiet",
                            action="store_const",
                            const=logging.ERROR,
                            dest=option_name,
                            help="Set to disable all non-error messages.")

        option_name = cls._add_config_parameter("logging_directory")
        parser.add_argument("--logging-directory",
                            action="store",
                            default="", type=str,
                            metavar=("PATH"),
                            dest=option_name,
                            help="Base directory used for model logging.")

        option_name = cls._add_config_parameter("logging_directory_use_timestamp")
        parser.add_argument("--logging-directory-use-timestamp",
                            action="store",
                            default=True, type=parse_bool_string,
                            metavar=("True/False"),
                            dest=option_name,
                            help="Use timestamp for the output logging directory?")

        option_name = cls._add_config_parameter("logging_directory_use_model")
        parser.add_argument("--logging-directory-use-model",
                            action="store",
                            default=True, type=parse_bool_string,
                            metavar=("True/False"),
                            dest=option_name,
                            help="Use model name for the output logging directory?")

        option_name = cls._add_config_parameter("logging_name")
        parser.add_argument("--logging-name",
                            action="store",
                            default="", type=str,
                            metavar=("PATH"),
                            dest=option_name,
                            help="Use given logging name. Specify 'ask' for "
                                 "interactive specification.")

        option_name = cls._add_config_parameter("save_log_to_file")
        parser.add_argument("--save-log-to-file",
                            action="store",
                            default=False, type=parse_bool_string,
                            metavar=("PATH"),
                            dest=option_name,
                            help="Save logs to file in the logging directory?")

        option_name = cls._add_config_parameter("prof_enabled")
        parser.add_argument("--prof-enabled",
                            action="store",
                            default=False, type=parse_bool_string,
                            metavar=("True/False"),
                            dest=option_name,
                            help="Enable collection of profiling information?")

        option_name = cls._add_config_parameter("prof_display")
        parser.add_argument("--prof-display",
                            action="store",
                            default=False, type=parse_bool_string,
                            metavar=("True/False"),
                            dest=option_name,
                            help="Display profiling results at the end of the program?")

        option_name = cls._add_config_parameter("prof_save_dir")
        parser.add_argument("--prof-save-dir",
                            action="store",
                            default=None, type=str,
                            metavar=("DIR"),
                            dest=option_name,
                            help="Directory to save the profiling results to.")

    @staticmethod
    def generate_timestamp_str() -> str:
        """
        Generate timestamp string to identify files.

        :return: Returns string representation of the timestamp.
        """

        now = datetime.datetime.now()

        return "{D}_{M}_{Y}-{h}-{m}-{s}-{ms}".format(
            h=now.hour,
            m=now.minute,
            s=now.second,
            ms=int(now.microsecond / 1000.0),
            D=now.day,
            M=now.month,
            Y=now.month
        )

    def _generate_logging_directory(self, base_dir: str, logging_name: str,
                                    use_timestamp: bool, use_model: bool):
        """ Generate unique logging directory for this run. """

        if not base_dir and not logging_name:
            return ""

        if logging_name == "ask":
            logging_name = input("Enter logging name: ")

        model_names = ("_" + "_".join(self.config.get_requested_models().keys())) if use_model else ""
        timestamp = f"_{LoggingConfigurator.generate_timestamp_str()}" if use_timestamp else ""
        spec_dir = f"{logging_name}{model_names}{timestamp}"

        full_path = pathlib.Path(f"{base_dir}/{spec_dir}").absolute()
        os.makedirs(full_path, exist_ok=True)

        return str(full_path)

    def setup_file_logging_to_file(self, file_path: str) -> logging.FileHandler:
        """ Setup logging of all handlers to given log file. """
        return LogMeta.setup_file_logging_to_file(file_path=file_path)

    def remove_file_logging_to_file(self, file_handler: logging.FileHandler):
        """ Remove logging to given file handler. """
        return LogMeta.remove_file_logging_to_file(file_handler=file_handler)

    def setup_profiling(self, enabled: bool, display: bool, save_results_dir: Optional[str]):
        """
        Setup profiling system.

        :param enabled: Enable collection of profiling information?
        :param display: Display profiling results at the end of the application?
        :param save_results_dir: Base directory to save the results to.
        """

        Profiler.configure(
            collect_data=enabled,
            display_data=display,
            save_results_dir=save_results_dir,
        )

        Profiler.reset_all_data()

    def setup_logging(self, logging_level: int = logging.INFO,
                      save_log_to_file: bool = False,
                      file_prefix: str = ""):
        """
        Setup logging streams and configure them.

        :param logging_level: Which level of messages should
            be displayed?
        :param save_log_to_file: Save logs to file in the
            file_prefix?
        :param file_prefix: Prefix used for saving files.
        """

        logging.basicConfig(level=logging_level)

        # Enable progress bar only when logging INFO or lower.
        LoggingBar.enabled = (logging_level <= logging.INFO)
        LoggingBar.check_tty = False

        # Enable log redirection to file if requested.
        if save_log_to_file:
            LogMeta.setup_file_logging(base_path=file_prefix)

    def generate_runtime_info(self) -> str:
        """
        Generate information about current runtime configuration.

        :return: Returns formatted string containing all information.
        """

        return "Runtime information: \n" \
               f"\tPython runtime: {sys.version}\n" \
               f"\tStart time: {self.config.start_time}\n" \
               f"\tCommand line arguments: {self.config.runtime_arguments}\n" \
               f"\tConfiguration: {self.config.save_config()}"

    def generate_exception_info(self, exception: Optional[Exception]) -> str:
        """
        Generate exception information including a stack trace.

        :param exception: Exception to describe.

        :return: Returns formatted string containing all information.
        """

        return "Exception information: \n" \
               f"\tException type: {type(exception)}\n" \
               f"\tException text: {str(exception)}\n" \
               f"\tTrace: {traceback.format_exc()}"

    @property
    def logging_level(self) -> "logging_level":
        """ Get current logging level, such as logging.WARNING . """
        return self.c.verbosity
