import click
from ga4gh.refget.ena.utils.uploader import Uploader

@click.command()
@click.argument("process_id")
@click.argument("processing_dir")
def upload(**kwargs):
    "upload to s3"

    uploader = Uploader(kwargs["process_id"], kwargs["processing_dir"])
    uploader.validate_and_upload_all()
