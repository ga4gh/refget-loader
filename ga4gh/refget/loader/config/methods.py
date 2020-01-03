from ga4gh.refget.loader.sources.ena.assembly.process import \
    ena_assembly_process

METHODS = {
    "processing": {
        "ena_assembly": ena_assembly_process
    },
    "upload": {
        "aws_s3": ""
    }
}
