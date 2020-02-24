# -*- coding: utf-8 -*-
"""Run prepared jobs on EBI cluster, leveraging the HPC grid environment"""

import os
import stat
from ga4gh.refget.loader.environments.environment import Environment

class EBIEnvironment(Environment):
    """Run prepared jobs on EBI cluster, leveraging the HPC grid environment

    Attributes:
        bsub_template (str): command template to submit jobs to cluster
    """

    def __init__(self):
        super(EBIEnvironment, self).__init__()
        self.bsub_template = 'bsub -o {outlog} -e {errlog} -J {jobid} ' \
            + '{hold} "{cmdfile}"'
    
    def prep_bsub_command(self, job):
        """Wrap cli command in 'bsub' grid scheduler

        The 'bsub' command submits jobs to EBI cluster. Therefore, bsub-wrapped
        commands process/upload refget data using more resources than just
        local execution

        Arguments:
            job (Job): job object to bsub-wrap
        
        Returns:
            (str): path to bsub command file, which contains bsub-wrapped cmd
        """

        # define cmd and log directories, where bsub command will be written
        # and log output written, respectively
        cmddir = os.path.dirname(job.get_cmdfile())
        logdir = os.path.join(os.path.dirname(cmddir), "log")
        os.makedirs(logdir) if not os.path.exists(logdir) else None

        # populate the bsub template command with log path, command file,
        # and hold job information (if applicable)
        bsub_params = {
            "outlog": os.path.join(logdir, job.jobid + ".log.out"),
            "errlog": os.path.join(logdir, job.jobid + ".log.err"),
            "jobid": job.jobid,
            "cmdfile": job.cmdfile,
            "hold": "-w 'ended({})'".format(job.hold_jobid) \
                    if job.hold_jobid else ""
        }
        bsub_cmd = self.bsub_template.format(**bsub_params)

        # write bsub command to disk, grant execute permissions to both bsub 
        # and original command files
        bsub_cmdfile = os.path.join(cmddir, job.jobid + ".bsub")
        open(bsub_cmdfile, "w").write(bsub_cmd)
        os.chmod(job.get_cmdfile(), stat.S_IRWXU)
        os.chmod(bsub_cmdfile, stat.S_IRWXU)
        return bsub_cmdfile
        
    def execute_job(self, job):
        """Submit a single job to the grid scheduler via 'bsub'"""

        os.system(self.prep_bsub_command(job))
