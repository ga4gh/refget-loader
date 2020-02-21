import os
from ga4gh.refget.loader.environments.environment import Environment

class LocalEnvironment(Environment):

    def __init__(self):
        super(LocalEnvironment).__init__()
    
    def execute_job(self, job):
        cmd = job.get_cmd()
        os.system(cmd)
