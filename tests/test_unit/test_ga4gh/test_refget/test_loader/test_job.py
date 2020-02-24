# -*- coding: utf-8 -*-
"""Unit tests for Job class

Attributes:
    cases (list): list of test cases to test Job
"""

import os
from ga4gh.refget.loader.job import Job

data_dir = os.path.join("tests", "data", "output")

cases = [
    {
        "cmdfile": os.path.join(data_dir, "job1.sh"),
        "cmd": 'ls -1',
        "jobid": "listdirectory.1",
        "hold_jobid": None
    },
    {
        "cmdfile": os.path.join(data_dir, "job2.sh"),
        "cmd": 'echo "hello world"',
        "jobid": "echo.2",
        "hold_jobid": None
    },
    {
        "cmdfile": os.path.join(data_dir, "job3.sh"),
        "cmd": "cat thefile.txt",
        "jobid": "cat.3",
        "hold_jobid": None
    }
]

def test_job():
    """Test the Job class using mulitple cases"""

    for c in cases:
        open(c["cmdfile"], "w").write(c["cmd"])
        job = Job(c["cmdfile"], c["jobid"], hold_jobid=c["hold_jobid"])
        assert job.get_cmdfile() == c["cmdfile"]
        assert job.get_cmd() == c["cmd"]
