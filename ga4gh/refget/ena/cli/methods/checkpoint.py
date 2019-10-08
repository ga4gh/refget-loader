# -*- coding: utf-8 -*-
"""Checkpoint-related click group and subcommands"""

import click
import configparser
import datetime
import re
from ga4gh.refget.ena.resources.get_resource import *

@click.group()
def checkpoint():
    """view or modify the sequence checkpoint date"""

@checkpoint.command()
def view():
    """view the current sequence checkpoint date"""

    checkpoint_ini = parse_checkpoint_ini()
    run_start = checkpoint_ini["refget_ena_checkpoint"]["run_start"]
    msg = "the ena-refget-scheduler is currently configured to run, starting " \
          + "from " + run_start
    print(msg)

@checkpoint.command()
@click.argument("date")
def update(**kwargs):
    """change the sequence checkpoint to a new date using YYYY-MM-DD format"""

    def validate_valid_format(date):
        err_msg = "date not in valid format, use YYYY-MM-DD"

        # check if date is in valid format, YYYY-MM-DD
        # raise Exception if not
        date_s = date.split("-")
        if not len(date_s) == 3:
            raise Exception(err_msg)
        year, month, day = date_s
        year_p = re.compile("\d{4}")
        month_day_p = re.compile("\d{2}")
        v_components = [
            {"val": year, "pattern": year_p},
            {"val": month, "pattern": month_day_p},
            {"val": day, "pattern": month_day_p}
        ]
        for v_component in v_components:
            value, pattern = [v_component[k] for k in ["val", "pattern"]]
            if not pattern.search(value):
                raise Exception(err_msg)
        
        # create a date object from the provided date, if it is older than
        # the oldest accepted time, do not update and raise Exception
        d = datetime.date(int(year), int(month), int(day))
        oldest_date_string = \
            parse_checkpoint_ini()["refget_ena_checkpoint"]["absolute_start"]
        oldest_date = datetime.date(
            *[int(a) for a in oldest_date_string.split("-")]
        )

        if d < oldest_date:
            raise Exception("cannot update date, proposed date must be after "
                + oldest_date_string)

    try:
        # get date string and validate, if OK, set the config value to
        # the proposed date and write to config file
        date_string = kwargs["date"]
        validate_valid_format(date_string)
        config = parse_checkpoint_ini()
        config["refget_ena_checkpoint"]["run_start"] = date_string
        with open(get_checkpoint_path(), "w") as configfile:
            config.write(configfile)
        print("ena checkpoint updated to " + date_string + ". execute "
            + "'ena-refget-scheduler checkpoint view' to confirm")
            
    except Exception as e:
        print(e)
