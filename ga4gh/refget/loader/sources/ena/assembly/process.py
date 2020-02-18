import datetime
import logging
import os
from ga4gh.refget.loader.sources.ena.assembly.process_date import process_date

def ena_assembly_process(config_obj, source_config, destination_config):
    root_dir = config_obj["processing_dir"]
    date_string = config_obj["start_date"]
    n_days = config_obj["number_of_days"]

    for i in range(0, n_days):
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
        process_date(date_string, sub_dir, config_obj, source_config, 
            destination_config)

        # create the next date and update the config file
        date = datetime.date(*[int(a) for a in [year, month, day]])
        next_date = date + datetime.timedelta(days=1)
        next_date_string = next_date.strftime("%Y-%m-%d")
        date_string = next_date_string
        logging.info("set checkpoint date to " + next_date_string)

        # remove logging handler so new log file is written to the next date
        for handler in logging.root.handlers:
            logging.root.removeHandler(handler)
