# -*- coding: utf-8 -*-
"""Core class for refget-loader app, prepares and executed all jobs

The RefgetLoader accepts the json config file and prepares the appropriate 
processing and uploading commands (according to "source" and "destination"
attributes), then executes these commands according to the "environment"
"""

import json
from ga4gh.refget.loader.config.sources import SOURCES
from ga4gh.refget.loader.config.environments import ENVIRONMENTS

class RefgetLoader(object):
    """Prepare and execute all subjobs according to config file

    Attributes:
        config_file (str): path to json config file
        config (dict): loaded dictionary from json config file
        source_processor (SourceProcessor): subclass based on "source" prop
        environment (Environment): subclass based on "environment" prop
    """

    def __init__(self, config_file):
        """Instantiate a RefgetLoader object

        Arguments:
            config_file (str): path to json config file
        """
        
        self.config_file = config_file
        self.config = self.__load_config()
        self.source_processor = self.__get_source_processor()
        self.environment = self.__get_environment()

    def __load_config(self):
        """Load config dictionary from file

        Returns:
            (dict): loaded json config file 
        """

        return json.load(open(self.config_file, "r"))
    
    def __get_source_processor(self):
        """Instantiate the correct SourceProcessor subclass based on config

        Returns:
            (SourceProcessor): source processor subclass object
        """

        # gets correct SourceProcessor subclass based on "type" keyword
        # of "source" object in config file, then instantiate it 
        source_class = SOURCES[self.config["source"]["type"]]
        source_obj = source_class(self.config_file, self.config)
        return source_obj

    def __get_environment(self):
        """Instantiate the correct Environment subclass based on config

        Returns:
            (Environment): environment subclass object
        """

        # gets correct Environment subclass based on keyword of "environment"
        # property in config file, then instantiate it
        environment_class = ENVIRONMENTS[self.config["environment"]]
        environment_obj = environment_class()
        return environment_obj

    def prepare_processing_jobs(self):
        """Generate all processing/upload commands and pass to environment"""

        jobs = self.source_processor.prepare_commands()
        self.environment.set_jobs(jobs)
    
    def execute_jobs(self):
        """Execute all jobs loaded on the environment object"""

        self.environment.execute_jobs()
