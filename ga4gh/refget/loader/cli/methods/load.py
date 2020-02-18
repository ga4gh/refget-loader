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
from ga4gh.refget.loader.config.constants import Status
from ga4gh.refget.loader.config.methods import METHODS
from ga4gh.refget.loader.validation.validator import \
    validate_source, validate_destination

@click.command()
@click.option("-s", "--source",
    help="JSON file describing reference sequence source")
@click.option("-d", "--destination",
    help="JSON file describing cloud resource destination")
def load(**kwargs):
    """process seqs into refget format, and upload to object store"""

    try:
        # validate both source and destination files have been specified
        if not kwargs["source"] or not kwargs["destination"]:
            raise Exception("source (-s) and destination (-d) JSON files "
                + "required")

        # validate both source and destination files fulfill the requirements
        # of their matching json schemas
        validations = [
            {"method": validate_source, "filepath": kwargs["source"]},
            {"method": validate_destination, "filepath": kwargs["destination"]}
        ]

        for validation in validations:
            result = validation["method"](validation["filepath"])
            if result["status"] != Status.SUCCESS:
                raise Exception(result["message"])
        
        # load the correct processing method according to the source config,
        # then execute it
        source_obj = json.load(open(kwargs["source"]))
        processing_method = METHODS["processing"][source_obj["type"]]
        processing_method(source_obj, kwargs["source"], kwargs["destination"])

    except Exception as e:
        print(e)
