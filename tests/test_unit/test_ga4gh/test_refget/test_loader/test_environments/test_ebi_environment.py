# -*- coding: utf-8 -*-
"""Unit tests for EBIEnvironment class

Attributes:
    data_dir (str): path to data directory
    cmd_dir (str): path to output cli command directory
    cases (list): list of test cases to use in unit test method(s)
"""

import os
import subprocess
from ga4gh.refget.loader.environments.ebi_environment import EBIEnvironment
from ga4gh.refget.loader.job import Job

data_dir = os.path.join("tests", "data")
cmd_dir = os.path.join(data_dir, "output")

cases = [
    {
        "cmd": "ls -1 ena/",
        "cmdfile": os.path.join(cmd_dir, "list.ena.sh"),
        "jobid": "list.ena",
        "hold_jobid": None,
        "exp_bsub_cmd": 'bsub -o %s -e %s -J %s  "%s"' % (
            os.path.join(data_dir, "log", "list.ena.log.out"),
            os.path.join(data_dir, "log", "list.ena.log.err"),
            "list.ena",
            os.path.join(cmd_dir, "list.ena.sh")
        )
    },
    {
        "cmd": "cat ena/sequence",
        "cmdfile": os.path.join(cmd_dir, "open.ena.sh"),
        "jobid": "open.ena",
        "hold_jobid": "list.ena",
        "exp_bsub_cmd": '''bsub -o %s -e %s -J %s -w 'ended(%s)' "%s"''' % (
            os.path.join(data_dir, "log", "open.ena.log.out"),
            os.path.join(data_dir, "log", "open.ena.log.err"),
            "open.ena",
            "list.ena",
            os.path.join(cmd_dir, "open.ena.sh")
        )
    }
]

def test_ebi_environment():
    """Test the EBIEnvironment class using multiple cases"""

    for c in cases:
        # write cmd to cmdfile
        open(c["cmdfile"], "w").write(c["cmd"])

        # create job
        job = Job(c["cmdfile"], c["jobid"], hold_jobid=c["hold_jobid"])
        jobs = [job]

        # create environment
        env = EBIEnvironment()
        env.set_jobs(jobs)

        # prep bsub command
        bsub_cmdfile = env.prep_bsub_command(job)
        bsub_cmd = open(bsub_cmdfile, "r").read().rstrip()
        assert bsub_cmd == c["exp_bsub_cmd"]

        env.execute_job(job)
