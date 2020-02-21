import os
import stat
from ga4gh.refget.loader.environments.environment import Environment

class EBIEnvironment(Environment):

    def __init__(self):
        super(EBIEnvironment, self).__init__()
        self.bsub_template = 'bsub -o {outlog} -e {errlog} -J {jobid} ' \
            + '{hold} "{cmdfile}"'
    
    def prep_bsub_command(self, job):
        cmddir = os.path.dirname(job.get_cmdfile())
        logdir = os.path.join(os.path.dirname(cmddir), "logs")

        bsub_params = {
            "outlog": os.path.join(logdir, job.jobid + ".log.out"),
            "errlog": os.path.join(logdir, job.jobid + ".log.err"),
            "jobid": job.jobid,
            "cmdfile": job.cmdfile,
            "hold": "-w 'ended({})'".format(job.hold_jobid) \
                    if job.hold_jobid else ""
        }
        bsub_cmd = self.bsub_template.format(**bsub_params)
        bsub_cmdfile = os.path.join(cmddir, job.jobid + ".bsub")
        open(bsub_cmdfile, "w").write(bsub_cmd)
        os.chmod(bsub_cmdfile, stat.S_IRWXU)
        return bsub_cmdfile
        
    def execute_job(self, job):
        os.system(self.prep_bsub_command(job))
