import click
import datetime
import os
from ga4gh.refget.ena.resources.get_resource import *
from ga4gh.refget.ena.cli.methods.process import process

@click.command()
@click.option("-n", "--number-of-days", type=click.INT, default=1,
    help="starting from the checkpoint date, process all seqs deployed on "
        + "ena across the number of specified days (default: 1)")
@click.option("-d", "--processing-dir", default=os.getcwd(),
    help="root processing dir. seqs will be processed in a sub-directory "
        + "according to the date they were deployed on ena")
def schedule(**kwargs):
    """schedule all batch jobs for multiple days after the current checkpoint"""

    # parse args
    n_days = kwargs["number_of_days"]
    root_dir = kwargs["processing_dir"]

    # execute the 'process single day' functionality, modifying the config file
    # by one day each time
    for i in range(0, kwargs["number_of_days"]):
        config = parse_checkpoint_ini()
        date_string = config["refget_ena_checkpoint"]["run_start"]
        year, month, day = date_string.split("-")
        
        # create sub directory
        sub_dir = os.path.join(root_dir, year, month, day)
        if not os.path.exists(sub_dir):
            os.makedirs(sub_dir)

        # processing method
        process(date_string, sub_dir)

        # create the next date and update the config file
        date = datetime.date(*[int(a) for a in [year, month, day]])
        next_date = date + datetime.timedelta(days=1)
        next_date_string = next_date.strftime("%Y-%m-%d")
        config["refget_ena_checkpoint"]["run_start"] = next_date_string
        with open(get_checkpoint_path(), "w") as configfile:
            config.write(configfile)
