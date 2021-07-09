# -*- coding: utf-8 -*-

"""
SolarDB saving and exporting system.
"""

import datetime
import dateutil.relativedelta
import os
import pathlib
import re
import sys
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import sqlalchemy as sa

from solardb.common import parse_bool_string
from solardb.common import parse_datetime_interval
from solardb.common import parse_list
from solardb.config import Config
from solardb.config import Configurable
from solardb.db import *
from solardb.db.tables import SolarTableMetaData
from solardb.logging import Logger

from solardb.run.sys.loader import DataLoader


class DataSaver(Logger, Configurable):
    """
    SolarDB saving and exporting system.
    """

    COMMAND_NAME = "Export"
    """ Name of this command, used for configuration. """

    def __init__(self, config: Config):
        super().__init__(config=config)
        self._set_instance()

        self._data_loader = self.get_instance(DataLoader)

        self.__l.info("Initializing data saver system...")

    @classmethod
    def register_options(cls, parser: Config.Parser):
        """ Register configuration options for this class. """

        option_name = cls._add_config_parameter("export_csv")
        parser.add_argument("--export-csv",
                            action="store",
                            default=None, type=str,
                            metavar=("PATH"),
                            dest=option_name,
                            help="Export the loaded SolarDB database as CSVs "
                                 "into the provided directory.")

        option_name = cls._add_config_parameter("export_db")
        parser.add_argument("--export-db",
                            action="store",
                            default=None, type=str,
                            metavar=("PATH"),
                            dest=option_name,
                            help="Export the loaded SolarDB database as DBs "
                                 "into the provided directory.")

        option_name = cls._add_config_parameter("separate_pp")
        parser.add_argument("--separate-pp",
                            action="store",
                            default=False, type=parse_bool_string,
                            metavar=("True/False"),
                            dest=option_name,
                            help="Export power plants separately?")

        option_name = cls._add_config_parameter("separate_inv")
        parser.add_argument("--separate-inv",
                            action="store",
                            default=False, type=parse_bool_string,
                            metavar=("True/False"),
                            dest=option_name,
                            help="Export inverters separately?")

        option_name = cls._add_config_parameter("dt_interval")
        parser.add_argument("--dt-interval",
                            action="store",
                            default=None, type=lambda x : \
                                parse_datetime_interval(x, format="%d.%m.%Y", sep="/"),
                            metavar=("%d.%m.%Y/%d.%m.%Y"),
                            dest=option_name,
                            help="Limit the export operations to given interval, inclusive.")

        option_name = cls._add_config_parameter("month_interval")
        parser.add_argument("--month-interval",
                            action="store",
                            default=None, type=lambda x : parse_list(val=x, typ=int, sep="/"),
                            metavar=("IDX/IDX"),
                            dest=option_name,
                            help="Limit the export operations to given months, for example "
                                 "use 0/1 to include only the first month.")

    @staticmethod
    def format_pp_id(pp_id: PowerPlantID) -> str:
        if pp_id is None:
            return f""
        elif pp_id.is_whole_power_plant():
            return f"pp{pp_id.pp_id}"
        else:
            return f"pp{pp_id.pp_id}_inv{pp_id.inv_id if pp_id.inv_id >= 0 else 'A'}"

    def _prepare_export(self, data: DataLoader, base_path: str,
                        separate_pp: bool, separate_inv: bool,
                        dt_interval: Optional[Tuple[datetime.datetime, datetime.datetime]],
                        month_interval: Optional[ Tuple[ int, int ] ],
                        ) -> ( List[PowerPlantID], List[PowerPlantID],
                               Optional[datetime.datetime],
                               Optional[datetime.datetime]):
        """ Prepare for export using provided data. """

        pathlib.Path(base_path).mkdir(parents=True, exist_ok=True)
        d = data.data

        pp_inv_list = d.list_pp_inverters()
        pp_id_list = np.unique([ i.pp_id for i in pp_inv_list ])
        pp_inv_list_wc = pp_inv_list + [
            PowerPlantID(
                pp_id=pp_id,
                inv_id=None,
            )
            for pp_id in pp_id_list
        ]

        if separate_pp:
            if separate_inv:
                sep_pp_ids = pp_inv_list.copy()
            else:
                sep_pp_ids = [
                    PowerPlantID(
                        pp_id=pp_id,
                        inv_id=None,
                    )
                    for pp_id in pp_id_list
                ]
            tot_pp_ids = [
                PowerPlantID(
                    pp_id=pp_id,
                    inv_id=None,
                )
                for pp_id in pp_id_list
            ]
        else:
            sep_pp_ids = [ None ]
            tot_pp_ids = [ None ]

        self.__l.info(f"\t\t\tTotal power plants: {len(pp_id_list)} | Exported IDs: {len(sep_pp_ids)}")

        if month_interval is not None:
            pp_intervals = {
                pp_id: d.get_pp_interval(pp_id=pp_id)
                for pp_id in pp_id_list
            }
            dt_start = {
                pp_id: pp_intervals[pp_id.pp_id][0] +
                       dateutil.relativedelta.relativedelta(
                           months=month_interval[0])
                for pp_id in pp_inv_list_wc
            }
            dt_end = {
                pp_id: pp_intervals[pp_id.pp_id][0] +
                       dateutil.relativedelta.relativedelta(
                           seconds=-1, months=month_interval[1])
                for pp_id in pp_inv_list_wc
            }
        elif dt_interval is not None:
            dt_start = {
                pp_id: dt_interval[0]
                for pp_id in pp_inv_list_wc
            }
            dt_end = {
                pp_id: dt_interval[1]
                for pp_id in pp_inv_list_wc
            }
            self.__l.info(f"\t\t\tExporting <{dt_start}, {dt_end}>")
        else:
            dt_start = { }
            dt_end = { }
            self.__l.info(f"\t\t\tExporting all data")

        return sep_pp_ids, tot_pp_ids, dt_start, dt_end

    def _export_csv(self, data: DataLoader, base_path: str,
                    separate_pp: bool, separate_inv: bool,
                    dt_interval: Optional[Tuple[datetime.datetime, datetime.datetime]],
                    month_interval: Optional[ Tuple[ int, int ] ],
                    ):
        """ Export the current data to the provided location. """

        self.__l.info(f"\tExporting database CSVs to \"{base_path}\"...")

        sep_pp_ids, tot_pp_ids, dt_start, dt_end = self._prepare_export(
            data=data, base_path=base_path,
            separate_pp=True, separate_inv=separate_inv,
            dt_interval=dt_interval, month_interval=month_interval,
        )
        d = data.data

        # Save power data.
        self.__l.info(f"\t\tExporting power data...")
        for idx, pp_id in enumerate(sep_pp_ids):
            base_name = self.format_pp_id(pp_id=pp_id if separate_pp else None)
            self.__l.info(f"\t\t\t[{idx+1}/{len(sep_pp_ids)}] {pp_id} -> \"{base_name}\"")

            power_df = d.get_pp_power(
                pp_id=pp_id,
                dt_start=dt_start.get(pp_id, None),
                dt_end=dt_end.get(pp_id, None),
                columns=None, all_indices=True,
            ).reset_index()

            power_df.to_csv(
                f"{base_path}/{base_name}{'_' if base_name else ''}solardb_power.csv",
                sep=";", index=False, mode="w" if separate_pp else "a",
            )

        # Save weather data.
        self.__l.info(f"\t\tExporting weather data...")
        for idx, pp_id in enumerate(tot_pp_ids):
            base_name = self.format_pp_id(pp_id=pp_id if separate_pp else None)
            self.__l.info(f"\t\t\t[{idx + 1}/{len(tot_pp_ids)}] {pp_id} -> \"{base_name}\"")

            weather_df = d.get_pp_weather(
                pp_id=pp_id,
                dt_start=dt_start.get(pp_id, None),
                dt_end=dt_end.get(pp_id, None),
                columns=None, all_indices=True
            ).reset_index()

            weather_df.to_csv(
                f"{base_path}/{base_name}{'_' if base_name else ''}solardb_weather.csv",
                sep=";", index=False, mode="w" if separate_pp else "a",
            )

        # Save exogenous data.
        self.__l.info(f"\t\tExporting exogenous data...")
        for idx, pp_id in enumerate(tot_pp_ids):
            base_name = self.format_pp_id(pp_id=pp_id if separate_pp else None)
            self.__l.info(f"\t\t\t[{idx + 1}/{len(tot_pp_ids)}] {pp_id} -> \"{base_name}\"")

            exogenous_df = d.get_pp_exogenous(
                pp_id=pp_id,
                dt_start=dt_start.get(pp_id, None),
                dt_end=dt_end.get(pp_id, None),
                columns=None, all_indices=True
            ).reset_index()

            exogenous_df.to_csv(
                f"{base_path}/{base_name}{'_' if base_name else ''}solardb_exogenous.csv",
                sep=";", index=False, mode="w" if separate_pp else "a",
            )

        # Save meta-data.
        self.__l.info(f"\t\tExporting meta-data...")
        for idx, pp_id in enumerate(sep_pp_ids):
            base_name = self.format_pp_id(pp_id=pp_id if separate_pp else None)
            self.__l.info(f"\t\t\t[{idx + 1}/{len(sep_pp_ids)}] {pp_id} -> \"{base_name}\"")

            meta_df = d.get_pp_info_df(
                pp_id=pp_id,
            ).reset_index()

            meta_df.to_csv(
                f"{base_path}/{base_name}{'_' if base_name else ''}solardb_meta.csv",
                sep=";", index=False, mode="w" if separate_pp else "a",
            )

    def _export_db(self, data: DataLoader, base_path: str,
                   separate_pp: bool, separate_inv: bool,
                   dt_interval: Optional[Tuple[datetime.datetime, datetime.datetime]],
                   month_interval: Optional[Tuple[int, int]],
                   ):
        """ Export the current data to the provided location. """

        self.__l.info(f"\tExporting database DBs to \"{base_path}\"...")

        sep_pp_ids, tot_pp_ids, dt_start, dt_end = self._prepare_export(
            data=data, base_path=base_path,
            separate_pp=True, separate_inv=separate_inv,
            dt_interval=dt_interval, month_interval=month_interval,
        )
        d = data.data

        for idx, pp_id in enumerate(sep_pp_ids):
            base_name = self.format_pp_id(pp_id=pp_id if separate_pp else None)
            self.__l.info(f"\t\t[{idx+1}/{len(sep_pp_ids)}] {pp_id} -> \"{base_name}\"")

            db_path = f"{base_path}/{base_name}{'_' if base_name else ''}solardb.db"
            self.__l.info(f"\t\t\tPreparing database at \"{db_path}\"...")
            sqlite_engine = sa.create_engine(f"sqlite:///{db_path}")
            if separate_pp or idx == 0:
                SolarTableMetaData.drop_all(sqlite_engine)
                SolarTableMetaData.create_all(sqlite_engine)
            conn = sqlite_engine.connect()

            self.__l.info(f"\t\t\tExporting power data...")
            power_df = d.get_pp_power(
                pp_id=pp_id,
                dt_start=dt_start.get(pp_id, None),
                dt_end=dt_end.get(pp_id, None),
                columns=None, all_indices=True
            ).reset_index()
            self.__l.info(f"\t\t\t\tWriting {len(power_df)} records")
            power_df.to_sql(
                name=SolarPowerTable.__tablename__,
                con=conn,
                schema=None,
                if_exists="append",
                index=False,
                chunksize=16384, method="multi",
            )

            self.__l.info(f"\t\t\tExporting weather data...")
            weather_df = d.get_pp_weather(
                pp_id=pp_id,
                dt_start=dt_start.get(pp_id, None),
                dt_end=dt_end.get(pp_id, None),
                columns=None, all_indices=True
            ).reset_index()
            self.__l.info(f"\t\t\t\tWriting {len(weather_df)} records")
            weather_df.to_sql(
                name=SolarWeatherTable.__tablename__,
                con=conn,
                schema=None,
                if_exists="append",
                index=False,
                chunksize=8192, method="multi",
            )

            self.__l.info(f"\t\t\tExporting exogenous data...")
            exogenous_df = d.get_pp_exogenous(
                pp_id=pp_id,
                dt_start=dt_start.get(pp_id, None),
                dt_end=dt_end.get(pp_id, None),
                columns=None, all_indices=True
            ).reset_index()
            exogenous_df.to_sql(
                name=SolarExogenousTable.__tablename__,
                con=conn,
                schema=None,
                if_exists="append",
                index=False, index_label="dt",
                chunksize=16384, method="multi",
            )
            self.__l.info(f"\t\t\t\tWriting {len(exogenous_df)} records")

            self.__l.info(f"\t\t\tExporting meta-data...")
            meta_df = d.get_pp_info_df(
                pp_id=pp_id,
            ).reset_index()
            meta_df.to_sql(
                name=SolarMetaTable.__tablename__,
                con=conn,
                schema=None,
                if_exists="append",
                index=False,
            )
            self.__l.info(f"\t\t\t\tWriting {len(meta_df)} records")

    def process(self):
        """ Perform data export operations. """

        self.__l.info("Starting data save operations...")

        if self.c.export_csv is not None:
            self._export_csv(
                data=self._data_loader,
                base_path=self.c.export_csv,
                separate_pp=self.c.separate_pp,
                separate_inv=self.c.separate_inv,
                dt_interval=self.c.dt_interval,
                month_interval=self.c.month_interval,
            )
        if self.c.export_db is not None:
            self._export_db(
                data=self._data_loader,
                base_path=self.c.export_db,
                separate_pp=self.c.separate_pp,
                separate_inv=self.c.separate_inv,
                dt_interval=self.c.dt_interval,
                month_interval=self.c.month_interval,
            )

        self.__l.info("\tSaving operations finished!")


