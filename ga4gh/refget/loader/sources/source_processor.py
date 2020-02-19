import os
from ga4gh.refget.loader.job import Job

class SourceProcessor(object):

    def __init__(self, config_file, config):

        self.config_file = config_file
        self.config = config
        self.sourcetype = self.config["type"]
    
    def get_sourcetype(self):
        return self.sourcetype
    
    def prepare_commands(self):
        pass

    def get_jobname(self):
        pass

    def write_cmd_and_get_job(self, cmd, cmdpath, jobid, hold_jobid):
        open(cmdpath, "w").write(cmd)
        job = Job(cmdpath, jobid, hold_jobid=hold_jobid)
        return job

    def upload_job(self, cmddir, manifest_file, jobid, hold_jobid):
        cmd_template = "refget-loader upload {}"
        cmd = cmd_template.format(manifest_file)
        cmdfile = jobid + ".sh"
        cmdpath = os.path.join(cmddir, cmdfile)
        return self.write_cmd_and_get_job(cmd, cmdpath, jobid, hold_jobid)
