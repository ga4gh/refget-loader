# -*- coding: utf-8 -*-
"""End-to-end testing of the "load" cli operation"""

import click
import os
from click.testing import CliRunner
from ga4gh.refget.loader.cli.methods.load import load

data_dir = os.path.join("tests", "data")
json_dir = os.path.join(data_dir, "json")
out_dir = os.path.join("output", "end")

cases = [
    {
        "config_file": os.path.join(
            data_dir,
            "ConfigEnaAssemblyAwsS3.json"
        )
    }

]

def test_load():
    
    for c in cases:
        runner = CliRunner()
        args = ["-c", c["config_file"]]
        result = runner.invoke(load, args)
        assert result.exit_code == 1
