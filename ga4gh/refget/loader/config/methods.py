# -*- coding: utf-8 -*-
"""Dictionary of all processing and upload methods

Attributes:
    METHODS (dict): contains all processing and upload methods
        two-tier dictionary in which methods are first categorized by 
        whether they are related to processing seqs from their source
        (processing), or uploading processed seqs to the object store (upload).
        The key for each method is the "type" keyword, as it would be found
        under the "type" property for both source and destination json configs
"""

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
