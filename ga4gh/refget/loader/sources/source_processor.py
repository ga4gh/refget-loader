# -*- coding: utf-8 -*-
"""SourceProcessor parent class, model for job preparation

The refget loader can be configured to process reference sequences from 
different sources into the refget format. Cli commands are prepared by the
specified SourceProcessor
"""

import os
from ga4gh.refget.loader.job import Job

class SourceProcessor(object):
    """Model for job preparation, prepares jobs to produce refget-format seqs

    Attributes:
        config_file (str): path to json config file
        config (dict): loaded config file contents
        source_config (dict): "source" object within overall config
        dest_config (dict): "destination" object within overall config
        sourcetype (str): "type" keyword for "source" object
        desttype (str): "type" keyword for "destination" object
    """

    def __init__(self, config_file, config):
        """Instantiate a SourceProcessor object

        Arguments:
            config_file (str): path to config file
            config (dict): loaded json config contents
        """

        self.config_file = config_file
        self.config = config
        self.source_config = self.config["source"]
        self.dest_config = self.config["destination"]
        self.sourcetype = self.source_config["type"]
        self.desttype = self.dest_config["type"]
    
    def get_sourcetype(self):
        """Get "type" keyword of "source" object

        Returns:
            (str): "type" keyword of "source" object
        """

        return self.sourcetype

    def write_cmd_and_get_job(self, cmd, cmdpath, jobid, hold_jobid):
        """Write a single cli command to command file and get as Job object

        Arguments:
            cmd (str): cli command
            cmdpath (str): path to output file containing cli command
            jobid (str): unique identifier for this job
            hold_jobid (str): waits for completion of another job
        
        Returns:
            (Job): Job object associated with passed command/command file
        """

        open(cmdpath, "w").write(cmd)
        job = Job(cmdpath, jobid, hold_jobid=hold_jobid)
        return job

    def upload_job(self, cmddir, dest_type, manifest_file, jobid, hold_jobid):
        """Write cli command to upload sequences from a manifest file

        Arguments:
            cmddir (str): directory to write command file
            dest_type (str): "type" keyword of desired upload class
            manifest_file (str): path to sequence manifest file
            jobid (str): unique identifier for this job
            hold_jobid (str): waits for completion of another job

        Returns:
            (Job): Job for uploading seqs in manifest file
        """

        cmd_template = "refget-loader upload {} {}"
        cmd = cmd_template.format(dest_type, manifest_file)
        cmdfile = jobid + ".sh"
        cmdpath = os.path.join(cmddir, cmdfile)
        return self.write_cmd_and_get_job(cmd, cmdpath, jobid, hold_jobid)
    
    def prepare_commands(self):
        """Prepares all processing cli commands, subclassed by sources"""

        pass
