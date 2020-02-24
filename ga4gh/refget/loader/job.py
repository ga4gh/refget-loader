# -*- coding: utf-8 -*-
"""Contains properties of a single command-line job

Jobs are prepared by the "sequence source" component, which generates all
individual jobs associated with a particular run. These Jobs are then 
submitted to the "environment," which will either execute each Job natively,
or via grid scheduler (qsub, bsub, etc.)
"""

class Job(object):
    """Single command-line job

    Attributes:
        cmdfile (str): path to file containing cli command
        jobid (str): unique job identifier
        hold_jobid (str): blocks execution of this job until another job exits
    """

    def __init__(self, cmdfile, jobid, hold_jobid=None):
        """Instantiate a Job object

        Arguments:
            cmdfile (str): path to file containing cli command
            jobid (str): unique job identifier
            hold_jobid (str): blocks execution of this job until another exits
        """

        self.cmdfile = cmdfile
        self.jobid = jobid
        self.hold_jobid = hold_jobid
    
    def get_cmdfile(self):
        """Get path to command file

        Returns:
            (str): path to command file
        """

        return self.cmdfile
    
    def get_cmd(self):
        """Get cli command stored in command file

        Returns:
            (str): cli command
        """

        fh = open(self.get_cmdfile(), "r")
        cmd = fh.read().strip()
        fh.close()
        return cmd
