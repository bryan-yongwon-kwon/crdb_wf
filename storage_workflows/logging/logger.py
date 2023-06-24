import logging
import sys
import datetime
from pytz import timezone, utc

class Logger:

    def __init__(self, level=logging.INFO, name="Storage Workflows") -> None:
        logging.Formatter.converter = customTime
        logging.basicConfig(stream=sys.stdout, 
                            level=level,
                            format="[%(asctime)s] %(levelname)s: %(message)s",
                            datefmt='%Y-%m-%d %H:%M:%S')
        self._logger = logging.getLogger(name=name)

    def info(self, msg):
        self._logger.info(msg)

    def error(self, msg):
        self._logger.error(msg)

    def warning(self, msg):
        self._logger.warning(msg)

    def critical(self, msg):
        self._logger.critical(msg)

    def debug(self, msg):
        self._logger.debug(msg)

def customTime(*args):
    utc_dt = utc.localize(datetime.datetime.utcnow())
    my_tz = timezone("US/Pacific")
    converted = utc_dt.astimezone(my_tz)
    return converted.timetuple()