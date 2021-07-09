# -*- coding: utf-8 -*-

"""
Database tables used in the SQL storage.

Includes following tables:
 * SolarPowerTable - Contains 5-minute power data.
 * SolarWeatherTable - Contains 1-hour weather data.
 * SolarExogenousTable - Contains 5-minute exogenous data.
 * SolarMetaTable - Contains per-power-plant meta-data.
"""

import datetime

import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base


# Tables for the primary database:
SolarTableMetaData = sa.MetaData()
SolarDeclarativeTableBase = declarative_base(metadata=SolarTableMetaData)


class SolarPowerTable(SolarDeclarativeTableBase):
    """
    Table containing 5-minute power data.

    Scheme:
     * Index:
      * ( pp_id, inv_id, dt ) : Complete power plant with inv_id = -1, 5-minute resolution.
     * Columns:
      * power_ac, power_dc : Reported values, interpolated to 5-minute interval.
      * energy_hour, energy_day, energy_week, energy_month : Aggregate calculated with power_ac.
      * ipolated, epolated : Flags signalling inter/extrapolation.
    """

    __tablename__ = "power"

    pp_id = sa.Column("pp_id", sa.Integer, primary_key=True)
    """ [-] Unique ID of the power plant. """
    inv_id = sa.Column("inv_id", sa.Integer, primary_key=True)
    """ [-] Power plant unique ID of the inverter. Value of -1 represents the complete power plant. """
    dt = sa.Column("dt", sa.DateTime, primary_key=True)
    """ [DT] Date and time of measurement. """
    power_ac = sa.Column("power_ac", sa.Integer)
    """ [W] Power produced by the inverter. """
    power_dc = sa.Column("power_dc", sa.Integer)
    """ [W] Power produced by the panels. """
    energy_hour = sa.Column("energy_hour", sa.Integer)
    """ [W/h] Energy over the current hour. """
    energy_day = sa.Column("energy_day", sa.Integer)
    """ [W/h] Energy over the current day. """
    energy_week = sa.Column("energy_week", sa.Integer)
    """ [W/h] Energy over the current week. """
    energy_month = sa.Column("energy_month", sa.Integer)
    """ [W/h] Energy over the current month. """
    ipolated = sa.Column("ipolated", sa.Boolean)
    """ [-] Was this record interpolated? """
    epolated = sa.Column("epolated", sa.Boolean)
    """ [-] Was this record extrapolated? """

    @classmethod
    def table(cls) -> sa.Table:
        """ Access the inner table. """
        return cls.__table__

    def to_dict(self) -> dict:
        """ Convert into dict. """

        return {
            SolarPowerTable.pp_id.key: self.pp_id,
            SolarPowerTable.inv_id.key: self.inv_id,
            SolarPowerTable.dt.key: self.dt,
            SolarPowerTable.power_ac.key: self.power_ac,
            SolarPowerTable.power_dc.key: self.power_dc,
            SolarPowerTable.energy_hour.key: self.energy_hour,
            SolarPowerTable.energy_day.key: self.energy_day,
            SolarPowerTable.energy_week.key: self.energy_week,
            SolarPowerTable.energy_month.key: self.energy_month,
            SolarPowerTable.ipolated.key: self.ipolated,
            SolarPowerTable.epolated.key: self.epolated,
        }


class SolarWeatherTable(SolarDeclarativeTableBase):
    """
    Table containing 1-hour weather data.

    Scheme:
     * Index:
      * ( pp_id, dt, age ) : 1-hour resolution, forecast with age > 0, measured with age = 0
     * Columns:
      * summary, precip_int, precip_prob, temp, apparent_temp, dew_point, humidity, pressure,
        wind_speed, wind_bearing, cloud_cover, visibility : Reported values, 1-hour interval.
      * ipolated, epolated : Flags signalling inter/extrapolation.
    """

    __tablename__ = "weather"

    pp_id = sa.Column("pp_id", sa.Integer, primary_key=True)
    """ [-] Unique ID of the power plant. """
    dt = sa.Column("dt", sa.DateTime, primary_key=True)
    """ [DT] Date and time of measurement. """
    age = sa.Column("age", sa.Integer, primary_key=True)
    """ [hr] Age of the forecast. Value of 0 represents measured weather. """
    src_dt = sa.Column("src_dt", sa.DateTime)
    """ [DT] Source date and time of the forecast. """
    summary = sa.Column("summary", sa.String(length=50))
    """ [-] Text summary of the weather. """
    precip_int = sa.Column("precip_int", sa.Float)
    """ [mm/h] Precipitation intensity. """
    precip_prob = sa.Column("precip_prob", sa.Float)
    """ [%] Precipitation probability from unlikely (0.0) to certain (1.0). """
    temp = sa.Column("temp", sa.Float)
    """ [°C] Temperature. """
    apparent_temp = sa.Column("apparent_temp", sa.Float)
    """ [°C] Perceived temperature. """
    dew_point = sa.Column("dew_point", sa.Float)
    """ [°C] Dew point temperature. """
    humidity = sa.Column("humidity", sa.Float)
    """ [%] Humidity from dry (0.0) to humid (1.0). """
    pressure = sa.Column("pressure", sa.Float)
    """ [hPa] Pressure. """
    wind_speed = sa.Column("wind_speed", sa.Float)
    """ [km/h] Wind speed. """
    wind_bearing = sa.Column("wind_bearing", sa.Float)
    """ [°] Wind bearing with north at 0.0, clockwise. """
    cloud_cover = sa.Column("cloud_cover", sa.Float)
    """ [%] Cloud cover from clear (0.0) to overcast (1.0). """
    visibility = sa.Column("visibility", sa.Float)
    """ [km] Visibility up to 16km. """
    ipolated = sa.Column("ipolated", sa.Boolean)
    """ Was this record interpolated? """
    epolated = sa.Column("epolated", sa.Boolean)
    """ Was this record extrapolated? """

    @classmethod
    def table(cls) -> sa.Table:
        """ Access the inner table. """
        return cls.__table__

    def to_dict(self) -> dict:
        """ Convert into dict. """

        return {
            SolarWeatherTable.pp_id.key: self.pp_id,
            SolarWeatherTable.dt.key: self.dt,
            SolarWeatherTable.age.key: self.age,
            SolarWeatherTable.summary.key: self.summary,
            SolarWeatherTable.precip_int.key: self.precip_int,
            SolarWeatherTable.precip_prob.key: self.precip_prob,
            SolarWeatherTable.temp.key: self.temp,
            SolarWeatherTable.apparent_temp.key: self.apparent_temp,
            SolarWeatherTable.dew_point.key: self.dew_point,
            SolarWeatherTable.humidity.key: self.humidity,
            SolarWeatherTable.pressure.key: self.pressure,
            SolarWeatherTable.wind_speed.key: self.wind_speed,
            SolarWeatherTable.wind_bearing.key: self.wind_bearing,
            SolarWeatherTable.cloud_cover.key: self.cloud_cover,
            SolarWeatherTable.visibility.key: self.visibility,
            SolarWeatherTable.ipolated.key: self.ipolated,
            SolarWeatherTable.epolated.key: self.epolated,
        }


class SolarExogenousTable(SolarDeclarativeTableBase):
    """
    Table containing 5-minute exogenous data.

    Scheme:
     * Index:
      * ( pp_id, dt ) : Aggregate for complete power plant, 5-minute resolution.
     * Columns:
      * sun_altitude, sun_azimuth, sun_irradiance : Pre-calculated values.
      * status, error : Status and error codes using the common translation table.
      * clear : Clarity of the day, clear (0) or overcast (1) (keep floating point value!).
     * Implicit Columns:
      * year, day, time : Calculated after query from dt.

    Status and Error codes:
     * 0 : none         : Everything is alright...
     * 1 : info         : Information message not critical for device operation
     * 2 : warning      : Warning message signalling abnormal conditions
     * 3 : error        : Error message, device not operating correctly
     * 4 : wait         : Device is waiting, operation will be resumed later
     * 5 : under        : Under-current or under-voltage condition
     * 6 : over         : Over-current or over-voltage condition
     * 7 : ext_error    : Error caused by external conditions
     * 8 : int_error    : Error caused by internal conditions
     * 9 : unknown      : Unknown status or error condition
    """

    __tablename__ = "exogenous"

    pp_id = sa.Column("pp_id", sa.Integer, primary_key=True)
    """ [-] Unique ID of the power plant. """
    dt = sa.Column("dt", sa.DateTime, primary_key=True)
    """ [DT] Date and time of measurement. """
    sun_altitude = sa.Column("sun_altitude", sa.Float)
    """ [rad] Sun altitude from ground plane. """
    sun_azimuth = sa.Column("sun_azimuth", sa.Float)
    """ [rad] Sun azimuth with north at 0.0, clockwise. """
    sun_irradiance = sa.Column("sun_irradiance", sa.Float)
    """ [W/m^2] Estimated clear-sky irradiance. """
    status = sa.Column("status", sa.Integer)
    """ [-] Status code for power record. """
    error = sa.Column("error", sa.Integer)
    """ [-] Error code for power record. """
    clear = sa.Column("clear", sa.Float)
    """ [-] Daily clarity from clear (0.0) to overcast (1.0). """

    @classmethod
    def table(cls) -> sa.Table:
        """ Access the inner table. """
        return cls.__table__

    def to_dict(self) -> dict:
        """ Convert into dict. """

        return {
            SolarExogenousTable.pp_id.key: self.pp_id,
            SolarExogenousTable.dt.key: self.dt,
            SolarExogenousTable.sun_altitude.key: self.sun_altitude,
            SolarExogenousTable.sun_azimuth.key: self.sun_azimuth,
            SolarExogenousTable.sun_irradiance.key: self.sun_irradiance,
            SolarExogenousTable.status.key: self.status,
            SolarExogenousTable.error.key: self.error,
            SolarExogenousTable.clear.key: self.clear,
        }


class SolarMetaTable(SolarDeclarativeTableBase):
    """
    Table containing per-inverter power plant meta-data.

    Scheme:
     * Index:
      * ( pp_id, inv_id ) : One record per power plant's inverter with complete inv_id = -1.
     * Columns:
      * freq, capacity, inverters, interval : Information about the power plant
      * location, pos_lat, pos_long : Location and anonymized coordinates.
    """

    __tablename__ = "meta"

    pp_id = sa.Column("pp_id", sa.Integer, primary_key=True)
    """ [-] Unique ID of the power plant. """
    inv_id = sa.Column("inv_id", sa.Integer, primary_key=True)
    """ [-] Power plant unique ID of the inverter. Value of -1 represents the complete power plant. """
    freq = sa.Column("freq", sa.Integer)
    """ [min] Frequency of reporting. """
    capacity = sa.Column("capacity", sa.Float)
    """ [kWp] Production capacity of the power plant. """
    inverters = sa.Column("inverters", sa.Float)
    """ [-] Total number of power inverters. """
    interval = sa.Column("interval", sa.DateTime)
    """ [-] Starting date and time of the covered data interval. """
    location = sa.Column("location", sa.String(length=16))
    """ [-] Inexact location of the installation. """
    pos_lat = sa.Column("pos_lat", sa.Float)
    """ [°] Inexact latitude of the installation. """
    pos_long = sa.Column("pos_long", sa.Float)
    """ [°] Inexact longitude of the installation. """

    @classmethod
    def table(cls) -> sa.Table:
        """ Access the inner table. """
        return cls.__table__

    def to_dict(self) -> dict:
        """ Convert into dict. """

        return {
            SolarMetaTable.pp_id.key: self.pp_id,
            SolarMetaTable.inv_id.key: self.inv_id,
            SolarMetaTable.freq.key: self.freq,
            SolarMetaTable.capacity.key: self.capacity,
            SolarMetaTable.inverters.key: self.inverters,
            SolarMetaTable.interval.key: self.interval,
            SolarMetaTable.location.key: self.location,
            SolarMetaTable.pos_lat.key: self.pos_lat,
            SolarMetaTable.pos_long.key: self.pos_long,
        }
