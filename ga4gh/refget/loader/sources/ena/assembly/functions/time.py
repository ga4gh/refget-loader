# -*- coding: utf-8 -*-
"""Common methods to modify timestamps and other time-related variables"""

import datetime

def timestamp():
    """Get the current date and time as an ISO 8601 format string

    :return: ISO 8601 string of current date and time
    :rtype: str
    """

    fmt = "%Y-%m-%dT%H:%M:%S"
    return datetime.datetime.now().strftime(fmt)
