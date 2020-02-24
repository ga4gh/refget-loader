# -*- coding: utf-8 -*-
"""Maps upload destination keywords to matching upload class

Attributes:
    DESTINATIONS (dict): maps upload destination keywords to upload class
"""

from ga4gh.refget.loader.destinations.aws.s3.aws_s3_uploader import \
    AwsS3Uploader

DESTINATIONS = {
    "aws_s3": AwsS3Uploader
}
