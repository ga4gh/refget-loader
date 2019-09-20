import configparser
import inspect
import os

def get_resource_dir():
    return os.path.dirname(inspect.getmodule(get_resource_dir).__file__)

def get_checkpoint_path():
    resource_dir = get_resource_dir()
    checkpoint_filename = "checkpoint.ini"
    return os.path.join(resource_dir, checkpoint_filename)
    
def parse_ini(ini_path):
    config = configparser.ConfigParser()
    config.read(ini_path)
    return config

def parse_checkpoint_ini():
    return parse_ini(get_checkpoint_path())