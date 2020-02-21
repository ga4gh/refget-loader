# -*- coding: utf-8 -*-
""""""

import json
from ga4gh.refget.loader.config.sources import SOURCES
from ga4gh.refget.loader.config.environments import ENVIRONMENTS

class RefgetLoader(object):

    def __init__(self, config_file):
        
        self.config_file = config_file
        self.config = self.__load_config()
        self.source_processor = self.__get_source_processor()
        self.destination_uploader = self.__get_destination_uploader()
        self.environment = self.__get_environment()

    def __load_config(self):
        return json.load(open(self.config_file, "r"))
    
    def __get_source_processor(self):
        source_class = SOURCES[self.config["source"]["type"]]
        source_obj = source_class(self.config_file, self.config)
        return source_obj
    
    def __get_destination_uploader(self):
        return ""

    def __get_environment(self):
        environment_class = ENVIRONMENTS[self.config["environment"]]
        environment_obj = environment_class()
        return environment_obj

    def prepare_processing_jobs(self):
        jobs = self.source_processor.prepare_commands()
        self.environment.set_jobs(jobs)
    
    def execute_jobs(self):
        self.environment.execute_jobs()
