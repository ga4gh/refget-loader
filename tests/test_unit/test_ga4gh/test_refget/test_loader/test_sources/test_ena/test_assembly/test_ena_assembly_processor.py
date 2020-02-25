# -*- coding: utf-8 -*-
"""Unit tests for ENAAssemblyProcessor

Attributes:
    cases (list): list of test cases
"""

import json
import os
import shutil
from ga4gh.refget.loader.sources.ena.assembly.ena_assembly_processor \
    import ENAAssemblyProcessor
from ga4gh.refget.loader.jobset_status import JobsetStatus

data_dir = os.path.join("tests", "data")
json_dir = os.path.join(data_dir, "json")
out_dir = os.path.join(data_dir, "output")
txt_dir = os.path.join(data_dir, "txt")

cases = [
    {
        "config_file": os.path.join(
            json_dir,
            "ConfigEnaAssemblyAwsS3.json"
        ),
        "date_string": "2019-08-02",
        "success_accessions": [
            "SPQE01"
        ],
        "fail_accessions": [
            "AGTA05",
            "NDFZ01",
            "RPFW01"
        ],
        "exp_accessions_file": os.path.join(
            txt_dir,
            "AssemblyScannerResults_2019-08-02.txt"
        )
    }
]

def get_flatfile_subdir(date_subdir, accession):
    """Get subdirectory where single flatfile is processed

    Arguments:
        date_subdir (str): date-specific processing subdirectory
        accession (str): ENA assembly accession
    
    Returns:
        (str): full path to single flatfile processing subdirectory
    """

    return os.path.join(
        date_subdir,
        "files",
        accession[:2],
        accession
    )

def test_ena_assembly_processor():
    """Test the EnaAssemblyProcessor class using multiple cases"""
    
    for c in cases:
        # remove processing dir if exists
        year, month, day = c["date_string"].split("-")
        processing_subdir = os.path.join(out_dir, year, month, day)
        if os.path.exists(processing_subdir):
            shutil.rmtree(processing_subdir)
        
        config = json.load(open(c["config_file"], "r"))
        ena_proc = ENAAssemblyProcessor(c["config_file"], config)
        ena_proc.prepare_commands()

        # assert accessions list
        obs_accessions_file = os.path.join(
            processing_subdir,
            "accessions_list.txt"
        )
        exp_accessions_file = c["exp_accessions_file"]
        obs_accessions = open(obs_accessions_file, "r").read()
        exp_accessions = open(exp_accessions_file, "r").read()
        assert obs_accessions == exp_accessions

        for acc in c["success_accessions"]:
            flatfile_dir = get_flatfile_subdir(processing_subdir, acc)
            status_file = os.path.join(flatfile_dir, "status.json")
            jobset_status = JobsetStatus(status_file)
            assert os.path.exists(flatfile_dir)
            assert os.path.exists(status_file)
            assert jobset_status.info["status"] == "Running"

            cmd_dir = os.path.join(flatfile_dir, "cmd")
            cmd_template = "ena_assembly.%s.%s.sh"
            cmd_process = os.path.join(
                cmd_dir, cmd_template % ("process", acc))
            cmd_manifest = os.path.join(
                cmd_dir, cmd_template % ("manifest", acc))
            cmd_upload = os.path.join(
                cmd_dir, cmd_template % ("upload", acc))
            
            assert os.path.exists(cmd_process)
            assert os.path.exists(cmd_manifest)
            assert os.path.exists(cmd_upload)

        for acc in c["fail_accessions"]:
            flatfile_dir = get_flatfile_subdir(processing_subdir, acc)
            status_file = os.path.join(flatfile_dir, "status.json")
            jobset_status = JobsetStatus(status_file)
            assert os.path.exists(flatfile_dir)
            assert os.path.exists(status_file)
            assert jobset_status.is_failure() == True
        