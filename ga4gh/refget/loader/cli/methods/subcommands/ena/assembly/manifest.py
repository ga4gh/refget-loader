import click

@click.command()
@click.argument("processing_dir")
def manifest(**kwargs):
    "generate an upload manifest from processed ENA flatfile"

    processing_dir = kwargs["processing_dir"]
    print(processing_dir)
