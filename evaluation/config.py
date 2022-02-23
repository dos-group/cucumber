import logging
from datetime import timedelta

import pandas as pd

LOG_LEVEL = logging.ERROR

START_DATE = pd.to_datetime("2022-01-18 00:00:00")
FINAL_DATE = pd.to_datetime("2022-02-02 00:00:00")
END_DATE = FINAL_DATE - timedelta(days=1)

POWER_STATIC = 30
POWER_MAX = 180
TIME_STEP = 10

SOLAR_MAX = 400  # e.g. https://na.panasonic.com/us/energy-solutions/solar/evervolttm-modules/evervolttm-series-module-1
