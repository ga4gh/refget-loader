import click
from ga4gh.refget.loader.cli.methods.subcommands.ena.assembly import assembly

@click.group()
def ena():
    "ena-related internal subcommands for batch jobs"

ena.add_command(assembly)