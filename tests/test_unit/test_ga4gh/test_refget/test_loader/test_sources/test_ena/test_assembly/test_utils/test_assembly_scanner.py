# -*- coding: utf-8 -*-
"""Unit tests for AssemblyScanner class

Attributes:
    data_dir (str): path to data directory
    in_dir (str): path to input data directory
    out_dir (str): path to output data directory
    cases (list): list of test cases
"""

import os
from ga4gh.refget.loader.sources.ena.assembly.utils.assembly_scanner \
    import AssemblyScanner

data_dir = os.path.join("tests", "data")
in_dir = os.path.join(data_dir, "txt")
out_dir = os.path.join(data_dir, "output")

cases = [
    {
        "filepath": os.path.join(out_dir, "AssemblyScannerAccessions.1.txt"),
        "exp_filepath": os.path.join(
            in_dir,
            "AssemblyScannerResults_2019-08-01.txt"
        ),
        "date_string": "2019-08-01",
        "exp_query": "last_updated>=2019-08-01 AND last_updated<2019-08-02"
    },
    {
        "filepath": os.path.join(out_dir, "AssemblyScannerAccessions.2.txt"),
        "exp_filepath": os.path.join(
            in_dir,
            "AssemblyScannerResults_2019-08-02.txt"
        ),
        "date_string": "2019-08-02",
        "exp_query": "last_updated>=2019-08-02 AND last_updated<2019-08-03"
    }
]

def test_assembly_scanner():
    """Test the AssemblyScanner class using multiple cases"""

    for c in cases:
        scanner = AssemblyScanner(c["date_string"])
        
        params = scanner.get_params()
        assert params["result"] == "assembly"
        assert params["query"] == c["exp_query"]
        assert params["fields"] == "accession"
        assert params["display"] == "xml"

        headers = scanner.get_headers()
        assert headers["Content-Type"] == "application/x-www-form-urlencoded"

        temp_fh = open(c["filepath"], "w")
        temp_fh.write("")
        temp_fh.close()
        scanner.generate_accession_list(c["filepath"])
        observed_accessions = open(c["filepath"], "r").read()
        expected_accessions = open(c["exp_filepath"], "r").read()
        assert observed_accessions == expected_accessions
