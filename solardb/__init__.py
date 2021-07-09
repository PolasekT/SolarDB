# -*- coding: utf-8 -*-

__docformat__ = "javadoc en"
__doc__ = \
"""
@brief SolarDB API - Python API for the SolarDB photovoltaic dataset
@author Tomas Polasek (ipolasek@fit.vutbr.cz)
@license MIT
@version 0.0.1
"""

__author__ = "Tomas Polasek"
__copyright__ = "Copyright 2021"
__credits__ = "Tomas Polasek"
__license__ = "MIT"
__version__ = "0.0.1"
__maintainer__ = "Tomas Polasek"
__email__ = "ipolasek@fit.vutbr.cz"
__status__ = "Development"

hard_deps = (
    "numpy",
    "pandas",
    "scipy",
    "sklearn",
    "sqlalchemy",
)
missing_deps = [ ]

for dep in hard_deps:
    try:
        __import__(dep)
    except ImportError as e:
        missing_deps.append(f"{dep}: {e}")

if len(missing_deps) > 0:
    raise ImportError(f"Failed to locate some dependencies: \n" + "\n".join(missing_deps))
