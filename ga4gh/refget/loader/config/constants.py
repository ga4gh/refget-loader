# -*- coding: utf-8 -*-
"""Contains constants classes for use throughout the library"""

class Status(object):
    """Constants related to whether a process/load/upload was successful

    Attributes:
        SUCCESS (int): indicates a successful operation
        FAILURE (int): indicates an unsuccessful operation
    """

    SUCCESS = 1
    FAILURE = -1

class JsonObjectType(object):
    """Constants related to different json objects in the config

    Attributes:
        CONFIG (int): integer related to overall config file
        SOURCE (int): integer related to "source" property in config
        DESTINATION (int): integer related to "destination" property in config
        RUNTIME (int): integer related to "runtime" property in config
    """

    CONFIG = 0
    SOURCE = 1
    DESTINATION = 2
    RUNTIME = 3
