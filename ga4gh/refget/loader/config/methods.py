from ga4gh.refget.loader.sources.ena.assembly.process import \
    ena_assembly_process
from ga4gh.refget.loader.destinations.aws.s3.upload import aws_s3_upload

METHODS = {
    "processing": {
        "ena_assembly": ena_assembly_process
    },
    "upload": {
        "aws_s3": aws_s3_upload
    }
}
