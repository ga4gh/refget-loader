class Job(object):

    def __init__(self, cmdfile, jobid, hold_jobid=None):
        self.cmdfile = cmdfile
        self.jobid = jobid
        self.hold_jobid = hold_jobid
    
    def get_cmdfile(self):
        return self.cmdfile
    
    def get_cmd(self):
        fh = open(self.get_cmdfile(), "r")
        cmd = fh.read().strip()
        fh.close()
        return cmd
