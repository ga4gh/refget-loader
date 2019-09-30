import configparser
import inspect
import os

def get_resource_dir():
    return os.path.dirname(inspect.getmodule(get_resource_dir).__file__)

def get_ini_path(filename):
    resource_dir = get_resource_dir()
    return os.path.join(resource_dir, filename)

def get_checkpoint_path():
    return get_ini_path("checkpoint.ini")

def get_settings_path():
    return get_ini_path("settings.ini")
    
def parse_ini(ini_path):
    config = configparser.ConfigParser()
    config.read(ini_path)
    return config

def parse_checkpoint_ini():
    return parse_ini(get_checkpoint_path())

def parse_settings_ini():
    return parse_ini(get_settings_path())
