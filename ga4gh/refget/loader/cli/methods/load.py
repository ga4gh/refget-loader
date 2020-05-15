# -*- coding: utf-8 -*-
"""High-level command to process/load refget seqs and upload to object store

The load command accepts 2 json config files:
1. the "source" where the original/unprocessed seqs are located
2. the "destination" where the refget processed seqs will be sent to

The load command will process the seqs from their original format, then 
upload the refget seqs to the requested destination
"""

import click
import json
import sys
from ga4gh.refget.loader.config.constants import Status, JsonObjectType
from ga4gh.refget.loader.refget_loader import RefgetLoader
from ga4gh.refget.loader.validator import Validator

@click.command()
@click.option("-c", "--config",
    help="JSON config file describing reference sequence source, upload "
         + "destination, and runtime environment")
def load(**kwargs):
    """process seqs into refget format, and upload to object store"""

    def validate_config():
        """validate all aspects of config file

        Raises:
            Exception: raised if config file not specified
        """

        def validate_single_config(objtype, fp):
            """validate a single config file

            Arguments:
                objtype (int): JsonObjectType integer
                fp (str): json file path

            Raises:
                Exception: raised when Json file does not meet schema
            """

            validator = Validator(objtype, fp)
            result = validator.validate()
            if result["status"] != Status.SUCCESS:
                raise Exception(result["message"])
            
        # validate config file is specified
        if not kwargs["config"]:
            raise Exception("config (-c) JSON file required")

        # validate config file meets basic requirements
        validate_single_config(JsonObjectType.CONFIG, kwargs["config"])
        config_loaded = json.load(open(kwargs["config"]))

        # write "source" and "destination" sub-objects to their own files
        # validate "source" and "destination" configs
        source_path = ".source.json"
        dest_path = ".destination.json"
        open(source_path, "w").write(json.dumps(config_loaded["source"]))
        open(dest_path, "w").write(json.dumps(config_loaded["destination"]))
        validate_single_config(JsonObjectType.SOURCE, source_path)
        validate_single_config(JsonObjectType.DESTINATION, dest_path)
        
    try:
        validate_config()
        loader = RefgetLoader(kwargs["config"])
        loader.prepare_processing_jobs()
        loader.execute_jobs()
        
    except Exception as e:
        print(e)
        sys.exit(1)
