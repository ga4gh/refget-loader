# -*- coding: utf-8 -*-
"""Environment parent class, model for job execution on different environments

The refget loader can be configured to run in different execution contexts,
ie. "Environments." Cli commands are prepared separately then passed to the
Environment component, which will either execute the jobs natively or via 
job scheduler to leverage grid/hpc environments.
"""

class Environment(object):
    """Model for job execution in different contexts/environments

    Attributes:
        (list): Job objects to be executed
    """

    def __init__(self):
        """Instantiate an Environment object"""

        self.jobs = []

    def set_jobs(self, jobs):
        """Sets prepared Job objects to jobs list

        Arguments:
            jobs (list): list of Job objects
        """

        self.jobs = jobs
    
    def execute_jobs(self):
        """Execute/schedule all jobs according to particular environment"""

        # for each job in the jobs list, launch a job according to how jobs
        # are run in that particular execution context
        for job in self.jobs:
            self.execute_job(job)

    def execute_job(self, job):
        """Execute/schedule a single job according to execution context"""
        
        pass
    
    