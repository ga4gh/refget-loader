from ga4gh.refget.loader.config.constants import JsonFiletype

SCHEMAS = {
    JsonFiletype.SOURCE: {
        "ena_assembly": "ena_assembly.json"
    },

    JsonFiletype.DESTINATION: {
        "aws_s3": "aws_s3.json"
    }
}