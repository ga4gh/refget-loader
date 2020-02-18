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

class JsonFiletype(object):
    """Constants related to different json files

    Attributes:
        SOURCE (int): integer related to the "source" json config file
        DESTINATION (int): integer related to the "destination" json config file
    """

    SOURCE = 0
    DESTINATION = 1
