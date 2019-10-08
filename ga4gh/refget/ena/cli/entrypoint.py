# -*- coding: utf-8 -*-
"""Main entrypoint into the program"""

import click
from ga4gh.refget.ena.cli.methods.checkpoint import checkpoint
from ga4gh.refget.ena.cli.methods.schedule import schedule
from ga4gh.refget.ena.cli.methods.settings import settings
from ga4gh.refget.ena.cli.methods.upload import upload

@click.group()
def main():
    """process ENA sequences and upload to S3 refget public dataset bucket"""

main.add_command(checkpoint)
main.add_command(schedule)
main.add_command(settings)
main.add_command(upload)
