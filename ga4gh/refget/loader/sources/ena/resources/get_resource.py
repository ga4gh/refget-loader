# -*- coding: utf-8 -*-
"""Methods to access settings from config files"""

import configparser
import inspect
import os

def get_resource_dir():
    """Get path to resource (ini settings) directory

    :return: resource (ini settings) directory
    :rtype: str
    """

    return os.path.dirname(inspect.getmodule(get_resource_dir).__file__)

def get_ini_path(filename):
    """Get full path to a specific settings file, based on requested filename

    :param filename: base name of settings file to be accessed
    :type filename: str
    :return: full path to settings file
    :rtype: str
    """

    resource_dir = get_resource_dir()
    return os.path.join(resource_dir, filename)

def get_checkpoint_path():
    """Get full path to checkpoint ini file

    :return: path to checkpoint ini file
    :rtype: str
    """

    return get_ini_path("checkpoint.ini")

def get_settings_path():
    """Get full path to settings ini file

    :return: path to settings ini file
    :rtype: str
    """

    return get_ini_path("settings.ini")
    
def parse_ini(ini_path):
    """Load .ini config file as dictionary

    :param ini_path: full path to .ini file
    :type ini_path: str
    :return: loaded dictionary of settings in .ini file
    :rtype: dict[str, dict]
    """

    config = configparser.ConfigParser()
    config.read(ini_path)
    return config

def parse_checkpoint_ini():
    """Load checkpoint ini as dictionary

    :return: loaded checkpoint ini
    :rtype: dict[str, dict]
    """

    return parse_ini(get_checkpoint_path())

def parse_settings_ini():
    """Load settings ini as dictionary

    :return: loaded settings ini
    :rtype: dict[str, dict]
    """

    return parse_ini(get_settings_path())
