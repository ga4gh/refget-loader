# -*- coding: utf-8 -*-
"""Unit tests for LocalEnvironment class

Attributes:
    data_dir (str): path to data directory
    cmd_dir (str): path to output cli command directory
    cases (list): list of test cases to use in unit test method(s)
"""

import os
from ga4gh.refget.loader.environments.local_environment import LocalEnvironment
from ga4gh.refget.loader.job import Job

data_dir = os.path.join("tests", "data")
cmd_dir = os.path.join(data_dir, "output")

cases = [
    {
        "cmd": "ls -1 ena/",
        "cmdfile": os.path.join(cmd_dir, "list.ena.sh"),
        "jobid": "list.ena",
        "hold_jobid": None
    },
    {
        "cmd": "cat ena/sequence",
        "cmdfile": os.path.join(cmd_dir, "open.ena.sh"),
        "jobid": "open.ena",
        "hold_jobid": "list.ena"
    }
]

def test_local_environment():
    """Test the LocalEnvironment class using multiple cases"""

    for c in cases:
        # write cmd to cmdfile
        open(c["cmdfile"], "w").write(c["cmd"])

        # create job
        job = Job(c["cmdfile"], c["jobid"], hold_jobid=c["hold_jobid"])
        jobs = [job]

        # create environment
        env = LocalEnvironment()
        env.set_jobs(jobs)
        env.execute_jobs()
        