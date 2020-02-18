# -*- coding: utf-8 -*-
"""Upload click command, uploads all files from a processed flat file"""

import click
import json
from ga4gh.refget.loader.config.sources import SOURCES

@click.command()
@click.argument("manifest")
def upload(**kwargs):
    "upload sequences and metadata according to file manifest"

    def parse_manifest(manifest_path):
        """load components of the manifest file

        Arguments:
            manifest_path (str): path to manifest file
        
        Returns:
            (list): loaded components of the manifest file
                elements of the return list:
                    source_config (str): path to source config file
                    destination_config (str): path to upload config file
                    seq_table (list): all data rows in the manifest table
                    additional_table (list): additional upload rows 
        """

        # open manifest file, parse source_config and destination config
        manifest_file = open(manifest_path, "r")
        manifest_file.readline()
        source_config = manifest_file.readline().split(":")[1].strip()
        destination_config = manifest_file.readline().split(":")[1].strip()

        seq_table = []
        additional_table = []
        add_to_seq_table = True

        # iterate through remaining rows, adding either to the list of seqs
        # or the additional upload table
        for line in manifest_file.readlines():
            if line.startswith("# additional uploads"):
                add_to_seq_table = False
            else:
                if add_to_seq_table:
                    seq_table.append(line)
                else:
                    additional_table.append(line)
                
        return [source_config, destination_config, seq_table, additional_table]

    # parse the manifest file using the above method
    manifest = kwargs["manifest"]
    source_config, destination_config, seq_table, additional_table = \
        parse_manifest(manifest)
    
    # load the destination config, load the correct upload method according
    # to the destination object's "type"
    # then, execute the upload method
    destination_obj = json.load(open(destination_config, "r"))
    destination_type = destination_obj["type"]
    upload_method = METHODS["upload"][destination_type]
    upload_method(destination_obj, seq_table, additional_table)
