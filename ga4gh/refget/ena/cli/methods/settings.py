import click
import configparser
from ga4gh.refget.ena.resources.get_resource import *

@click.group()
def settings():
    """view or modify ena-refget-scheduler application settings"""

@settings.command()
def view():
    """view all current settings"""

    settings_ini = parse_settings_ini()
    settings = settings_ini["refget_ena_settings"]
    keys = sorted(settings.keys())
    print("Property\tValue")
    for key in keys:
        print("{}\t{}".format(key, settings[key]))

@settings.command()
@click.argument("property")
@click.argument("value")
def update(**kwargs):
    """modify an existing settings property"""

    prop, val = kwargs["property"], kwargs["value"]
    settings_ini = parse_settings_ini()
    key_set = set(settings_ini["refget_ena_settings"].keys())

    if prop in key_set:
        settings_ini["refget_ena_settings"][prop] = val
        with open(get_settings_path(), "w") as configfile:
            settings_ini.write(configfile)
        print("updated value of {} to {}".format(prop, val))
    else:
        print(prop + " not in scheduler settings")
