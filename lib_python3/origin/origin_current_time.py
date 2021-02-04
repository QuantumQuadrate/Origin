import calendar
import time
from configparser import ConfigParser


def current_time(config: ConfigParser):
    """
    Figures out the current time in the format that origin wants
    Args:
        config : config object
    Returns:
        time in format desired by origin
    """

    #Unix time (in UTC)
    if config.get('Server', "timestamp_type") == "uint64":
        return int(time.time()*2**32)
    return calendar.timegm(time.gmtime())
