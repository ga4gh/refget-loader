class Job(object):

    def __init__(self, cmdfile, jobid, hold_jobid=None):
        self.cmdfile = cmdfile
        self.jobid = jobid
        self.hold_jobid = hold_jobid