# -*- coding: utf-8 -*-
"""Click group and subcommands to view/modify settings"""

import click
import configparser
from ga4gh.refget.ena.resources.get_resource import *

@click.group()
def settings():
    """view or modify ena-refget-scheduler application settings"""

@settings.command()
def view():
    """view all current settings"""

    # get all settings from the settings ini, and print them line by line
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

    # get all settings from the settings ini
    prop, val = kwargs["property"], kwargs["value"]
    settings_ini = parse_settings_ini()
    key_set = set(settings_ini["refget_ena_settings"].keys())

    # check that the submitted property is an actual setting in the settings ini
    # if not, print exit message, if so, update the settings ini with the new
    # value
    if prop in key_set:
        settings_ini["refget_ena_settings"][prop] = val
        with open(get_settings_path(), "w") as configfile:
            settings_ini.write(configfile)
        print("updated value of {} to {}".format(prop, val))
    else:
        print(prop + " not in scheduler settings")
