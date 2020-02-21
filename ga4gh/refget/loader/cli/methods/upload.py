# -*- coding: utf-8 -*-
"""Upload click command, uploads all files from a processed flat file"""

import click
import json
from ga4gh.refget.loader.config.destinations import DESTINATIONS

@click.command()
@click.argument("type")
@click.argument("manifest")
def upload(**kwargs):
    "upload sequences and metadata according to file manifest"

    desttype, manifest = [kwargs[a] for a in ["type", "manifest"]]
    upload_class = DESTINATIONS[desttype]
    upload_obj = upload_class(manifest)
    upload_obj.upload()
