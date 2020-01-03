import click
from ga4gh.refget.loader.config.constants import Status
from ga4gh.refget.loader.validation.validator import \
    validate_source, validate_destination

@click.command()
@click.option("-s", "--source",
    help="JSON file describing reference sequence source")
@click.option("-d", "--destination",
    help="JSON file describing cloud resource destination")
def load(**kwargs):
    """process and load to cloud storage"""

    try:
        if not kwargs["source"] or not kwargs["destination"]:
            raise Exception("source (-s) and destination (-d) JSON files "
                + "required")

        validations = [
            {"method": validate_source, "filepath": kwargs["source"]},
            {"method": validate_destination, "filepath": kwargs["destination"]}
        ]

        for validation in validations:
            result = validation["method"](validation["filepath"])
            if result["status"] != Status.SUCCESS:
                raise Exception(result["message"])
        
        

    except Exception as e:
        print(e)