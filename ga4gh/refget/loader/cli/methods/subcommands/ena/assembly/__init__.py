import click
from ga4gh.refget.loader.cli.methods.subcommands.ena.assembly.manifest \
    import manifest

@click.group()
def assembly():
    "ena assembly-related internal commands for batch jobs"

assembly.add_command(manifest)
