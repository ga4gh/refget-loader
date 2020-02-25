# -*- coding: utf-8 -*-
"""Unit tests for JobsetStatus class

Attributes:
    cases (list): list of test cases to use in unit test method(s)
    is_status (dict): expected results based on status
"""

import json
import os
from ga4gh.refget.loader.jobset_status import JobsetStatus

data_dir = os.path.join("tests", "data", "output")

cases = [
    {
        "filepath": os.path.join(data_dir, "status1.json"),
        "status": JobsetStatus.SUCCESS,
        "object_id": "123",
        "message": "successfully retrieved taxid",
        "data": {
            "taxid": "9606"
        }
    },
    {
        "filepath": os.path.join(data_dir, "status2.json"),
        "status": JobsetStatus.FAILED,
        "object_id": "456",
        "message": "couldn't retrieve data",
        "data": {
            "vcf": "data2.vcf"
        }
    },
    {
        "filepath": os.path.join(data_dir, "status3.json"),
        "status": JobsetStatus.RUNNING,
        "object_id": "789"
    },
    {
        "filepath": os.path.join(data_dir, "status4.json"),
        "status": JobsetStatus.NOTSTARTED,
        "object_id": "abc"
    },
]

is_status = {
    JobsetStatus.SUCCESS: {
        "success": True,
        "failure": False
    },
    JobsetStatus.FAILED: {
        "success": False,
        "failure": True
    },
    JobsetStatus.RUNNING: {
        "success": False,
        "failure": False
    },
    JobsetStatus.NOTSTARTED: {
        "success": False,
        "failure": False
    }
}

def test_jobset_status():
    """Test the JobsetStatus class using multiple cases"""

    for c in cases:
        jobset_status = JobsetStatus(c["filepath"])

        if c["status"] == JobsetStatus.SUCCESS:
            jobset_status.set_status_success()
        elif c["status"] == JobsetStatus.FAILED:
            jobset_status.set_status_failure()
        elif c["status"] == JobsetStatus.RUNNING:
            jobset_status.set_status_running()
        elif c["status"] == JobsetStatus.NOTSTARTED:
            jobset_status.set_status_not_started()
        
        jobset_status.set_objectid(c["object_id"])
        if "message" in c.keys():
            jobset_status.set_message(c["message"])
        if "data" in c.keys():
            for k in c["data"].keys():
                jobset_status.set_data(k, c["data"][k])

        jobset_status.write()
        json_obj = json.load(open(c["filepath"]))

        assert json_obj["status"] == c["status"]
        assert json_obj["object_id"] == c["object_id"]
        if "message" in c.keys():
            assert json_obj["message"] == c["message"]
        if "data" in c.keys():
            for k in c["data"].keys():
                assert json_obj["data"][k] == c["data"][k]
        assert jobset_status.is_success() == is_status[c["status"]]["success"]
        assert jobset_status.is_failure() == is_status[c["status"]]["failure"]
