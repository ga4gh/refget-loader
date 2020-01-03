# -*- coding: utf-8 -*-
"""Schedule-related command, runs process/upload for specified number of days"""

import click
import datetime
import logging
import os
from ga4gh.refget.ena.resources.get_resource import *
from ga4gh.refget.ena.utils.processing.single_date import process_single_date

@click.command()
@click.option("-n", "--number-of-days", type=click.INT, default=1,
    help="starting from the checkpoint date, process all seqs deployed on "
        + "ena across the number of specified days (default: 1)")
def schedule(**kwargs):
    """schedule all batch jobs for multiple days after the current checkpoint"""

    # parse args
    n_days = kwargs["number_of_days"]
    
    # get processing dir
    root_dir = parse_settings_ini()["refget_ena_settings"]["processing_dir"]
    if not os.path.exists(root_dir):
        print(root_dir + " does not exist. update processing_dir via "
            + "'ena-refget-scheduler settings update'")
    else:
        # execute the 'process single day' functionality, modifying the config 
        # file by one day each time
        for i in range(0, kwargs["number_of_days"]):
            config = parse_checkpoint_ini()
            date_string = config["refget_ena_checkpoint"]["run_start"]
            year, month, day = date_string.split("-")
            
            # create sub directory, logfile
            sub_dir = os.path.join(root_dir, year, month, day)
            if not os.path.exists(sub_dir):
                os.makedirs(sub_dir)
            logfile = os.path.join(sub_dir, "logfile.txt")
            logging.basicConfig(
                filename=logfile,
                format='%(asctime)s\t%(levelname)s\t%(message)s',
                level=logging.DEBUG,
            )
            logging.info("logs for sequences uploaded on: " + date_string)

            # processing method
            process_single_date(date_string, sub_dir)

            # create the next date and update the config file
            date = datetime.date(*[int(a) for a in [year, month, day]])
            next_date = date + datetime.timedelta(days=1)
            next_date_string = next_date.strftime("%Y-%m-%d")
            config["refget_ena_checkpoint"]["run_start"] = next_date_string
            with open(get_checkpoint_path(), "w") as configfile:
                config.write(configfile)
            logging.info("set checkpoint date to " + next_date_string)

            # remove logging handler so new log file is written to the next date
            for handler in logging.root.handlers:
                logging.root.removeHandler(handler)
