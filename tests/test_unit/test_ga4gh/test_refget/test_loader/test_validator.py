# -*- coding: utf-8 -*-
"""Unit tests for Validator class

Attributes:
    cases (list): list of test cases to be used in unit test method
"""

import os
from ga4gh.refget.loader.config.constants import JsonObjectType, Status
from ga4gh.refget.loader.validator import Validator

data_dir = os.path.join("tests", "data", "json")

cases = [
    {
        "filetype": JsonObjectType.SOURCE,
        "filepath":  os.path.join(data_dir, "SourceEnaAssemblyCorrect.json"),
        "exp_status": Status.SUCCESS
    },
    {
        "filetype": JsonObjectType.DESTINATION,
        "filepath": os.path.join(data_dir, "DestinationAwsS3Correct.json"),
        "exp_status": Status.SUCCESS,
    },
    {
        "filetype": JsonObjectType.SOURCE,
        "filepath": os.path.join(data_dir, "SourceEnaAssemblyIncorrect0.json"),
        "exp_status": Status.FAILURE,
    },
    {
        "filetype": JsonObjectType.SOURCE,
        "filepath": os.path.join(data_dir, "SourceEnaAssemblyIncorrect1.json"),
        "exp_status": Status.FAILURE
    },
    {
        "filetype": JsonObjectType.SOURCE,
        "filepath": os.path.join(data_dir, "SourceEnaAssemblyIncorrect2.json"),
        "exp_status": Status.FAILURE
    },
    {
        "filetype": JsonObjectType.SOURCE,
        "filepath": os.path.join(data_dir, "SourceEnaAssemblyIncorrect3.yml"),
        "exp_status": Status.FAILURE
    },
    {
        "filetype": JsonObjectType.SOURCE,
        "filepath": os.path.join(data_dir, "FileNotFound.json"),
        "exp_status": Status.FAILURE
    },
]

def test_validator():
    """Test the Validator using multiple cases"""
    
    for case in cases:
        validator = Validator(case["filetype"], case["filepath"])
        result = validator.validate()
        assert result["status"] == case["exp_status"]
