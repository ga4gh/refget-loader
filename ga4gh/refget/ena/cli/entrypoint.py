import click

from ga4gh.refget.ena.cli.methods.checkpoint import checkpoint
from ga4gh.refget.ena.cli.methods.schedule import schedule

@click.group()
def main():
    """process ENA sequences and upload to S3 refget public dataset bucket"""

main.add_command(checkpoint)
main.add_command(schedule)