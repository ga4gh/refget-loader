# -*- coding: utf-8 -*-
"""Run prepared jobs locally without any enhancements or parallelization"""

import os
from ga4gh.refget.loader.environments.environment import Environment

class LocalEnvironment(Environment):
    """Runs jobs locally without any enhancements/parallelization"""

    def __init__(self):
        """Instantiate a LocalEnvironment object"""

        super(LocalEnvironment).__init__()
    
    def execute_job(self, job):
        """Execute a single job on the local OS"""

        cmd = job.get_cmd()
        os.system(cmd)
