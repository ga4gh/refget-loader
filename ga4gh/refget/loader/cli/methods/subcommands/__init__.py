import click
from ga4gh.refget.loader.cli.methods.subcommands.ena import ena

@click.group()
def subcommands():
    """internal subcommands for batch jobs"""


subcommands.add_command(ena)