# -*- coding: utf-8 -*-

"""
Database access wrapper.
"""

import datetime
import os
from typing import List, Optional, Tuple, Union

import numpy as np
import pandas as pd
import pathlib
import sqlalchemy as sa
import sqlalchemy.orm as sao

from solardb.config import SolarDBConfig
from solardb.common import date_earliest_date_time
from solardb.common import date_latest_date_time
from solardb.common import freq_ceil
from solardb.common import freq_floor
from solardb.common import timedelta_total_seconds
from solardb.db.tables import *


class PowerPlantID(object):
    """
    Class uniquely representing a single power plant or
    its inverter.

    :param pp_id: Identifier of the power plant.
    :param inv_id: Identifier of the inverters.
        None for the whole power plant.
    """

    def __init__(self, pp_id: int,
                 inv_id: Optional[int] = None):
        self._pp_id = int(pp_id)
        self._inv_id = int(inv_id) if inv_id is not None else None

    @property
    def pp_id(self) -> int:
        """ Get identifier of the power plant. """
        return self._pp_id

    @property
    def inv_id(self) -> Optional[int]:
        """ Get the inverter identifier. Returns None for the whole power plant. """
        return self._inv_id

    def is_whole_power_plant(self) -> bool:
        """ Does this instance represent the whole power plant? """
        return self._inv_id is None

    def __lt__(self, other: "PowerPlantID") -> bool:
        return self._pp_id < other._pp_id or (
            self._pp_id == other._pp_id and \
            (
                self._inv_id is None or \
                (
                    other._inv_id is not None and \
                    self._inv_id < other._inv_id
                )
            )
        )

    def __eq__(self, other: "PowerPlantID") -> bool:
        return self._pp_id == other._pp_id and \
               self._inv_id == other._inv_id

    def __hash__(self):
        return hash(( self._pp_id, self._inv_id ))

    def __str__(self):
        """ Get string describing this instance. """
        if self._inv_id is None:
            return f"{self._pp_id}"
        else:
            return f"{self._pp_id}:{self._inv_id}"


PowerPlantIDT = Union[int, Tuple[int, int], PowerPlantID]
""" Type used to represent a power plant. """


def get_pp_id(pp_id: PowerPlantIDT) -> PowerPlantID:
    """ Unify format of provided power-plant identifier. """
    if isinstance(pp_id, PowerPlantID):
        return pp_id
    elif isinstance(pp_id, tuple) or isinstance(pp_id, list):
        return PowerPlantID(
            pp_id=int(pp_id[0]),
            inv_id=-1 if len(pp_id) < 2 else int(pp_id[1]),
        )
    else:
        return PowerPlantID(
            pp_id=int(pp_id),
            inv_id=-1,
        )


def get_all_column_names(table: any) -> List[str]:
    """ Get all column names for given table. """
    return [ col.key for col in table.table().columns ]


def get_non_index_column_names(table: any) -> List[str]:
    """ Get all column names for given table. """
    return [ col.key for col in table.table().columns  if not col.primary_key ]


def get_columns(table: any, names: Optional[List[str]] = None) -> List[sa.Column]:
    """ Get columns of given, optionally filtered by names. """

    all_columns = list(table.table().columns)

    return [
        col for col in all_columns if col.key in names
    ] if names is not None else all_columns


class SolarDBData(object):
    """
    Database access wrapper.

    :param url: URL for the data source using format:
            dialect://user:password@host/dbname
        For example, following connection strings are valid:
            mysql://bob:smith@localhost/test
            sqlite:////tmp/test.db
        Use sqlite://:memory: for in-memory database.
        The database will be automatically initialized
        if necessary.
        URL may also be just a path, in which case it is
        presumed to be a sqlite db file.
    :param perform_logging: Enable logging for the SQL engine?
    :param create_db: Allow creation of new database files?
        Set to False to report non-existent files.
    """

    def __init__(self, url: str,
                 perform_logging: bool = False,
                 create_db: bool = False,
                 ):

        if ":" not in url:
            if not create_db and not pathlib.Path(url).exists():
                raise RuntimeError(f"Provided database file (\"{url}\") does not "
                                   f"exit and create_db is False!")
            url = f"sqlite:///{url}"

        self.engine_config = {}
        if perform_logging:
            self.engine_config["echo"] = True
            self.engine_config["echo_pool"] = True
            self.engine_config["logging_name"] = "SolarDataModel"
            self.engine_config["pool_logging_name"] = "SolarDataModel"
        if url.startswith("mysql"):
            self.engine_config["pool_size"] = 5
            self.engine_config["max_overflow"] = 0

        self._engine = sa.create_engine(
            url,
            **self.engine_config
        )
        self._sess_maker = sao.sessionmaker(bind=self.engine)
        self._sess = None
        self._conn = None

        self.check_create_tables()

    @property
    def engine(self) -> sa.engine.Engine:
        """ Get the database engine. """

        return self._engine

    @property
    def sess(self) -> sao.session.Session:
        """ Get the database session. """

        if self._sess is None:
            self._sess = self._sess_maker()

        return self._sess

    @property
    def conn(self) -> sa.engine.base.Connection:
        """
        Make sure we the connection to the database is valid.
        This method is idempotent.

        :return: Returns database connection.
        """

        if self._conn is None:
            self._conn = self._engine.connect()

        return self._conn

    def check_create_tables(self):
        """
        Check if all necessary tables are present in the
        database and create them if necessary.
        """

        SolarTableMetaData.create_all(self.engine, checkfirst=True)

    def dump_as_sqlite(self, path: str,
                       overwrite: bool = False,
                       batch_size: int = 131072,
                       ):
        """
        Dump current database to sqlite db file.

        :param path: Path to the output file.
        :param overwrite: If the file exists, should it be overwritten?
            When set to False, this method will throw files which already
            exist!
        :param batch_size: How many records should be dumped at one time.
            Too large values may deplete all memory and crash with
            MemoryError!
        """

        if os.path.isfile(path) and not overwrite:
            raise ValueError(f"Database {path} already exists and overwrite is False!")

        sqlite_engine = sa.create_engine(f"sqlite:///{path}")
        SolarTableMetaData.drop_all(sqlite_engine)
        SolarTableMetaData.create_all(sqlite_engine)

        conn = self.conn
        sqlite_conn = sqlite_engine.connect()

        for table_name, table in SolarTableMetaData.tables.items():
            # Batched fetch-insert to save memory.
            records = conn.execute(table.count()).first()[0]
            fetched = 0

            while fetched < records:
                select = conn.execute(table.select().limit(batch_size).offset(fetched))

                fetched += batch_size

                data = [dict(d) for d in select]
                sqlite_conn.execute(table.insert(), data)

    def pp_exists(self, pp_id: PowerPlantIDT
                  ) -> bool:
        """
        Does power plant  with given identifier have any data
        in the currently chosen data source?

        :param pp_id: Identifier of the power plant or inverter.

        :return: Returns True if the power plant has at least
            one power record.
        """

        pp_id = get_pp_id(pp_id=pp_id)

        query = sa.select([
            SolarMetaTable.pp_id
        ]).where(sa.and_(
            SolarPowerTable.pp_id == pp_id.pp_id,
        ))
        if not pp_id.is_whole_power_plant():
            query = query.where(SolarPowerTable.inv_id == pp_id.inv_id)

        return self.conn.execute(query).first() is not None

    def get_pp_info(self, pp_id: Optional[PowerPlantIDT] = None,
                    ) -> List[dict]:
        """
        Get information about specified power plant or inverter.

        :param pp_id: Optional identifier of the power plant or
            inverter. Set to None to get all power plants and
            inverters.

        :return: Returns list of dictionaries containing power
            plant meta-data. Returns empty list if the power
            plant does not exist.
        """

        query = self.sess.query(SolarMetaTable)

        if pp_id is not None:
            pp_id = get_pp_id(pp_id=pp_id)
            query = query.filter_by(pp_id=pp_id.pp_id)
            if not pp_id.is_whole_power_plant():
                query = query.filter_by(inv_id=pp_id.inv_id)

        res = query.all()

        return [ r.to_dict() for r in res ] if res is not None else dict()

    def get_pp_info_df(self, pp_id: Optional[PowerPlantIDT] = None,
                       ) -> pd.DataFrame:
        """
        Get information about all power plants and inverters in
        a Pandas DataFrame, indexed by ( "pp_id", "inv_id" )

        :param pp_id: Optional identifier of the power plant or
            inverter. Set to None to get all power plants and
            inverters.

        :return: Returns a DataFrame containing information about
            all power plants and inverters.
        """

        return pd.DataFrame(
            data=self.get_pp_info(pp_id=pp_id)
        ).set_index([ "pp_id", "inv_id" ])

    def list_pp_inverters(self, pp_id: Optional[PowerPlantIDT] = None
                          ) -> List[PowerPlantID]:
        """
        Enumerate power plants and inverters. Specify pp_id to filter
        by power plant ID.

        :param pp_id: Optional filter for the power plant ID. Inverter ID
            is unused.

        :return: Returns a list of power plant identifiers.
        """

        query = sa.select([
            SolarMetaTable.pp_id,
            SolarMetaTable.inv_id,
        ])

        if pp_id is not None:
            pp_id = get_pp_id(pp_id=pp_id)
            query = query.where(SolarMetaTable.pp_id == pp_id.pp_id)

        id_list = self.conn.execute(query).fetchall()
        id_list = [
            PowerPlantID(
                pp_id=pp_id,
                inv_id=inv_id,
            )
            for pp_id, inv_id in id_list
        ]
        id_list.sort()

        return id_list

    def get_pp_interval(self, pp_id: PowerPlantIDT,
                        ) -> Optional[Tuple[datetime.datetime,
                                            datetime.datetime]]:
        """
        Get data interval for specified power plant or inverter.

        :param pp_id: Identifier of the power plant of inverter.

        :return: Returns inclusive range of data or None if the
            power plant does not exist.
        """

        pp_id = get_pp_id(pp_id=pp_id)

        dt_start = self.conn.execute(sa.select([
            SolarPowerTable.dt
        ]).order_by(
            SolarPowerTable.dt
        ).where(sa.and_(
            SolarPowerTable.pp_id == pp_id.pp_id,
            SolarPowerTable.inv_id == pp_id.inv_id,
        ))).first()[0]

        dt_last = self.conn.execute(sa.select([
            SolarPowerTable.dt
        ]).order_by(
            SolarPowerTable.dt.desc()
        ).where(sa.and_(
            SolarPowerTable.pp_id == pp_id.pp_id,
            SolarPowerTable.inv_id == pp_id.inv_id,
        ))).first()[0]

        return ( dt_start, dt_last ) if dt_start is not None else None

    def get_pp_power(self, pp_id: Optional[PowerPlantIDT] = None,
                     dt_start: Optional[Union[datetime.date, datetime.datetime]] = None,
                     dt_end: Optional[Union[datetime.date, datetime.datetime]] = None,
                     columns: Optional[List["str"]] = None, all_indices: bool = False,
                     ) -> pd.DataFrame:
        """
        Get power data for specified power plant or inverter, optionally
        get data for all power plants or limit to requested interval.

        Returns data frame with requested columns, indexed by "dt"
        date-time or "pp_id", "inv_id", and "dt" if pp_id == None.
        Data is ordered by date, ascending.

        :param pp_id: Optional identifier of the power plant or inverter.
            Specify None to return all power data, adding pp_id and inv_id
            to the index.
        :param dt_start: Optional limit specifying the first included date.
        :param dt_end: Optional limit specifying the last included date.
        :param columns: List of columns to include, by default includes all.
        :param all_indices: Override the index detection and include all
            indices ( "pp_id", "inv_id", "dt" ).

        :return: Returns the requested data in a DataFrame.
        """

        if columns is None:
            columns = get_non_index_column_names(table=SolarPowerTable)

        idx_columns = [ "dt", ]
        if pp_id is None or all_indices:
            idx_columns = [ "pp_id", "inv_id", ] + idx_columns
        elif pp_id.is_whole_power_plant():
            idx_columns = [ "inv_id", ] + idx_columns

        columns = idx_columns + columns
        columns = get_columns(table=SolarPowerTable, names=columns)
        column_names = [ col.key for col in columns ]

        query = sa.select(columns)

        if pp_id is not None:
            pp_id = get_pp_id(pp_id=pp_id)
            query = query.where(SolarPowerTable.pp_id == pp_id.pp_id)
            if not pp_id.is_whole_power_plant():
                query = query.where(SolarPowerTable.inv_id == pp_id.inv_id)

        query = query.order_by(SolarPowerTable.dt)

        if dt_start is not None:
            dt_start = dt_start if isinstance(dt_start, datetime.datetime) else \
                date_earliest_date_time(dt_start)
            query = query.where(SolarPowerTable.dt >= dt_start)

        if dt_end is not None:
            dt_end = dt_end if isinstance(dt_end, datetime.datetime) else \
                date_earliest_date_time(dt_end)
            query = query.where(SolarPowerTable.dt <= dt_end)

        data = self.conn.execute(query).fetchall()

        df = pd.DataFrame(
            data=data,
            columns=column_names,
        ).set_index(idx_columns)

        return df

    def get_pp_weather(self, pp_id: Optional[PowerPlantIDT] = None,
                       dt_start: Optional[Union[datetime.date, datetime.datetime]] = None,
                       dt_end: Optional[Union[datetime.date, datetime.datetime]] = None,
                       columns: Optional[List["str"]] = None, all_indices: bool = False,
                       src_age: Optional[int] = None, src_dt_max: Optional[datetime.datetime] = None,
                       interpolate_smooth: bool = False,
                       ) -> pd.DataFrame:
        """
        Get weather data for specified power plant, optionally get data
        for all power plants or limit to requested interval.

        Returns data frame with requested columns, indexed by "dt"
        date-time or "pp_id", "inv_id", and "dt" if pp_id == None.
        If no src limit is set, index will additionally contain "age".
        Data is ordered by date, ascending.

        :param pp_id: Optional identifier of the power plant. Specify None
            to return all power data, adding pp_id and inv_id to the index.
        :param dt_start: Optional limit specifying the first included date.
        :param dt_end: Optional limit specifying the last included date.
        :param columns: List of columns to include, by default includes all.
        :param all_indices: Override the index detection and include all
            indices ( "pp_id", "dt" ).
        :param src_age: Optional specification of weather age. Set to zero
            to recover the measured weather.
        :param src_dt_max: Optional limit for the maximal source dt, inclusive.
        :param interpolate_smooth: Interpolate the hourly data to P_FREQUENCY
            (5-minute) intervals?

        :return: Returns the requested data in a DataFrame.
        """

        if columns is None:
            columns = get_non_index_column_names(table=SolarWeatherTable)

        if interpolate_smooth and src_age is None and src_dt_max is None:
            raise RuntimeError("Smooth interpolation is only supported for age-limited queries!")

        idx_columns = [ "dt", ]
        if pp_id is None or all_indices:
            idx_columns = [ "pp_id", ] + idx_columns
        if src_age is None and src_dt_max is None:
            idx_columns = idx_columns + [ "age", ]

        columns = idx_columns + columns
        columns = get_columns(table=SolarWeatherTable, names=columns)
        column_names = [ col.key for col in columns ]

        query = sa.select(columns)

        if pp_id is not None:
            pp_id = get_pp_id(pp_id=pp_id)
            query = query.where(SolarWeatherTable.pp_id == pp_id.pp_id)

        query = query.order_by(
            SolarWeatherTable.dt.asc(),
            SolarWeatherTable.age.asc()
        )

        if dt_start is not None:
            dt_start = dt_start if isinstance(dt_start, datetime.datetime) else \
                date_earliest_date_time(dt_start)
            if interpolate_smooth:
                dt_start = freq_floor(dt_start, freq=SolarDBConfig.S_FREQUENCY_S)
            query = query.where(SolarWeatherTable.dt >= dt_start)

        if dt_end is not None:
            dt_end = dt_end if isinstance(dt_end, datetime.datetime) else \
                date_earliest_date_time(dt_end)
            if interpolate_smooth:
                dt_end = freq_ceil(dt_end, freq=SolarDBConfig.S_FREQUENCY_S)
            query = query.where(SolarWeatherTable.dt <= dt_end)

        if src_age is not None:
            query = query.where(SolarWeatherTable.age == src_age)

        if src_dt_max is not None:
            query = query.where(SolarWeatherTable.src_dt <= src_dt_max)

        data = self.conn.execute(query).fetchall()

        df = pd.DataFrame(
            data=data,
            columns=column_names,
        )

        if src_dt_max is not None:
            df = df.groupby("dt").first().reset_index()

        if interpolate_smooth:
            # Add the last record in case we are not limited in this direction.
            if dt_end is None:
                last_idx = df.index.max() + 1
                df.loc[last_idx] = df.loc[df.index.max()]
                df.loc[last_idx, "dt"] = freq_floor(date_latest_date_time(df.dt.max()), SolarDBConfig.P_FREQUENCY_D)
                df.loc[last_idx, "src_dt"] = df.loc[last_idx, "dt"] + (df.iloc[-2]["dt"] - df.iloc[-2]["src_dt"])
                if "ipolated" in df.columns:
                    df.loc[last_idx, "ipolated"] = False
                if "epolated" in df.columns:
                    df.loc[last_idx, "epolated"] = True

            # Detect resampling interval.
            res_dt_start = dt_start or df.dt.min()
            res_dt_end = dt_end or df.dt.max()

            # Interpolate to 5-minute intervals, adding one additional record for interpolation.
            def resample_df(idf: pd.DataFrame) -> pd.DataFrame:
                if "pp_id" in idf.columns:
                    idf = idf.drop(columns="pp_id")
                return idf.set_index("dt").reindex(
                    pd.date_range(
                        start=res_dt_start, end=res_dt_end + SolarDBConfig.P_FREQUENCY_D,
                        freq=SolarDBConfig.P_FREQUENCY_S, closed="left",
                        name="dt",
                    )
                ).interpolate(
                    method="time",
                )

            if "pp_id" in df.columns:
                df = df.groupby("pp_id").apply(resample_df).reset_index()
            else:
                df = resample_df(df).reset_index()

            # Fix non-interpolated columns: src_dt, summary, ipolated, and epolated.
            if "src_dt" in df.columns:
                src_dts = pd.to_numeric(df.reset_index().set_index("dt")["src_dt"])
                src_dts.loc[src_dts < 0] = np.nan
                df["src_dt"] = pd.to_datetime(src_dts.interpolate(method="time")).to_numpy()
            if "summary" in df.columns:
                df.loc[:, "summary"].ffill(inplace=True)
            if "ipolated" in df.columns:
                df.loc[:, "ipolated"].ffill(inplace=True)
            if "epolated" in df.columns:
                df.loc[:, "epolated"].ffill(inplace=True)

            # Remove the last record if not included in the range.
            if dt_end is not None:
                df = df.iloc[:-1]

        df = df.set_index(idx_columns)

        return df

    def get_pp_exogenous(self, pp_id: Optional[PowerPlantIDT] = None,
                         dt_start: Optional[Union[datetime.date, datetime.datetime]] = None,
                         dt_end: Optional[Union[datetime.date, datetime.datetime]] = None,
                         columns: Optional[List["str"]] = None, all_indices: bool = False,
                         ) -> pd.DataFrame:
        """
        Get exogenous data for specified power plant, optionally get data
        for all power plants or limit to requested interval.

        Returns data frame with requested columns, indexed by "dt"
        date-time or "pp_id" and "dt" if pp_id == None.
        Data is ordered by date, ascending.

        :param pp_id: Optional identifier of the power plant. Specify None
            to return all power data, adding pp_id and inv_id to the index.
        :param dt_start: Optional limit specifying the first included date.
        :param dt_end: Optional limit specifying the last included date.
        :param columns: List of columns to include, by default includes all.
        :param all_indices: Override the index detection and include all
            indices ( "pp_id", "dt" ).

        :return: Returns the requested data in a DataFrame.
        """

        if columns is None:
            columns = get_non_index_column_names(table=SolarExogenousTable)

        idx_columns = [ "dt", ]
        if pp_id is None or all_indices:
            idx_columns = [ "pp_id", ] + idx_columns

        columns = idx_columns + columns
        columns = get_columns(table=SolarExogenousTable, names=columns)
        column_names = [ col.key for col in columns ]

        query = sa.select(columns)

        if pp_id is not None:
            pp_id = get_pp_id(pp_id=pp_id)
            query = query.where(SolarExogenousTable.pp_id == pp_id.pp_id)

        query = query.order_by(SolarExogenousTable.dt)

        if dt_start is not None:
            dt_start = dt_start if isinstance(dt_start, datetime.datetime) else \
                date_earliest_date_time(dt_start)
            query = query.where(SolarExogenousTable.dt >= dt_start)

        if dt_end is not None:
            dt_end = dt_end if isinstance(dt_end, datetime.datetime) else \
                date_earliest_date_time(dt_end)
            query = query.where(SolarExogenousTable.dt <= dt_end)

        data = self.conn.execute(query).fetchall()

        df = pd.DataFrame(
            data=data,
            columns=column_names,
        ).set_index(idx_columns)

        return df


