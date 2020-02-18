# -*- coding: utf-8 -*-
""""""

import json
from ga4gh.refget.loader.config.sources import SOURCES

class RefgetLoader(object):

    def __init__(self, config_file):
        
        self.config_file = config_file
        self.config = self.__load_config()
        self.source_processor = self.__get_source_processor()
        self.destination_uploader = self.__get_destination_uploader()
        self.runtime = self.__get_runtime()

    def __load_config(self):
        return json.load(open(self.config_file, "r"))
    
    def __get_source_processor(self):
        source_class = SOURCES[self.config["source"]["type"]]
        source_obj = source_class(self.config_file, self.config["source"])
        return source_obj
    
    def __get_destination_uploader(self):
        return ""

    def __get_runtime(self):
        return ""

    def prepare_processing_jobs(self):
        all_commands = self.source_processor.prepare_commands()
        print(all_commands)
