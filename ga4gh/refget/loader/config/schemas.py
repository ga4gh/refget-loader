# -*- coding: utf-8 -*-
"""Dictionary of all source and destination json schemas by type

Attributes:
    SCHEMAS (dict): all source and destination json schemas by type
        two-tier dictionary in which schemas are first categorized by whether
        they are related to processing seqs, or uploading processed seqs. The
        key for each schema is its "type" keyword, as it would be found under
        the "type" property for both source and destination json configs
"""

from ga4gh.refget.loader.config.constants import JsonFiletype

SCHEMAS = {
    JsonFiletype.SOURCE: {
        "ena_assembly": "ena_assembly.json"
    },

    JsonFiletype.DESTINATION: {
        "aws_s3": "aws_s3.json"
    }
}