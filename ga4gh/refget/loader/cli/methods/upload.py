# -*- coding: utf-8 -*-
"""Upload click command, uploads all files from a processed flat file"""

import click
import json
from ga4gh.refget.loader.config.methods import METHODS
# from ga4gh.refget.ena.utils.uploader import Uploader

# @click.command()
# @click.argument("process_id")
# @click.argument("processing_dir")
# def upload(**kwargs):
#     """upload all sequences, metadata, and csv from processed flat file to s3"""

#     uploader = Uploader(kwargs["process_id"], kwargs["processing_dir"])
#     uploader.validate_and_upload_all()

@click.command()
@click.argument("manifest")
def upload(**kwargs):
    "upload sequences and metadata according to file manifest"

    def parse_manifest(manifest_path):

        manifest_file = open(manifest_path, "r")
        manifest_file.readline()
        source_config = manifest_file.readline().split(":")[1].strip()
        destination_config = manifest_file.readline().split(":")[1].strip()

        seq_table = []
        additional_table = []
        add_to_seq_table = True

        for line in manifest_file.readlines():
            if line.startswith("# additional uploads"):
                add_to_seq_table = False
            else:
                if add_to_seq_table:
                    seq_table.append(line)
                else:
                    additional_table.append(line)
                
        return [source_config, destination_config, seq_table, additional_table]

    manifest = kwargs["manifest"]
    source_config, destination_config, seq_table, additional_table = \
        parse_manifest(manifest)
    
    destination_obj = json.load(open(destination_config, "r"))
    destination_type = destination_obj["type"]
    upload_method = METHODS["upload"][destination_type]
    upload_method(destination_obj, seq_table, additional_table)
