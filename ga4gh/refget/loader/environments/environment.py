# -*- coding: utf-8 -*-

class Environment(object):

    def __init__(self):
        self.jobs = []

    def set_jobs(self, jobs):
        self.jobs = jobs
    
    def execute_jobs(self):
        for job in self.jobs:
            self.execute_job(job)

    def execute_job(self, job):
        pass
    
    