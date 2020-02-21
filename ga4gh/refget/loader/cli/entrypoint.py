# -*- coding: utf-8 -*-
"""Main entrypoint into the program"""

import click
from ga4gh.refget.loader.cli.methods.load import load
from ga4gh.refget.loader.cli.methods.subcommands import subcommands
from ga4gh.refget.loader.cli.methods.upload import upload

@click.group()
def main():
    """process sequences into refget format and upload to cloud storage"""

main.add_command(load)
main.add_command(subcommands)
main.add_command(upload)
