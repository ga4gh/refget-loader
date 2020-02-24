# -*- coding: utf-8 -*-
"""Contain and report status for a set of inter-related jobs"""

import json
import os
from ga4gh.refget.loader.sources.ena.assembly.functions.time import timestamp

class JobsetStatus(object):
    """Contain and report status for a set of inter-related jobs

    Attributes:
        NOTSTARTED (int): indicates job set has not started
        RUNNING (int): indicates job set is running
        SUCCESS (int): indicates job set successfully completed
        FAILED (int): indicates job set failed to complete
        filepath (str): path to input/output status file
        info (dict): dictionary containing all status-related attributes
    """

    NOTSTARTED = "NotStarted"
    RUNNING = "Running"
    SUCCESS = "Success"
    FAILED = "Failed"

    def __init__(self, filepath):
        """Instantiate a JobsetStatus object

        Arguments:
            filepath (str): path to input/output status file
        """

        self.filepath = filepath
        # info is set to default, then loaded if a status file already exists
        self.info = {
            "object_id": "NULL",
            "last_modified": timestamp(),
            "message": "",
            "status": JobsetStatus.NOTSTARTED,
            "data": {}
        }
        self.__load()

    def write(self):
        """Write all status information to the status file"""

        self.__update_timestamp()
        fh = open(self.filepath, "w")
        fh.write(str(self))
        fh.close()
    
    def set_objectid(self, object_id):
        """Set unique/object identifier for the job set

        Arguments:
            object_id (str): unique/object identifier
        """

        self.info["object_id"] = object_id

    def set_message(self, message):
        """Set warning/error message associated with the job set execution

        Arguments:
            message (str): warning/error message
        """

        self.info["message"] = message
    
    def set_data(self, key, value):
        """Add data with unique key to data dictionary

        Arguments:
            key (str): unique key to identify value
            value (str): value to place in data dictionary
        """

        self.info["data"][key] = value

    def set_status_not_started(self):
        """Set status to 'Not Started'"""

        self.info["status"] = JobsetStatus.NOTSTARTED
    
    def set_status_running(self):
        """Set status to 'Running'"""

        self.info["status"] = JobsetStatus.RUNNING
    
    def set_status_success(self):
        """Set status to 'Success'"""

        self.info["status"] = JobsetStatus.SUCCESS
    
    def set_status_failure(self):
        """Set status to 'Failure'"""

        self.info["status"] = JobsetStatus.FAILED

    def is_success(self):
        """Checks if the job set execution was successful

        Returns:
            (bool): True if job set execution was successful
        """

        return self.info["status"] == JobsetStatus.SUCCESS
    
    def is_failure(self):
        """Checks if the job set execution failed

        Returns:
            (bool): True if job set execution failed
        
        """

        return self.info["status"] == JobsetStatus.FAILED

    def __update_timestamp(self):
        """Update the timestamp to the current time"""

        self.info["last_modified"] = timestamp()
    
    def __load(self):
        """Load an existing job set status file into memory"""

        if os.path.exists(self.filepath):
            fh = open(self.filepath, "r")
            self.info = json.load(fh)
            fh.close()
    
    def __str__(self):
        """String representation of job set status object

        Returns:
            (str): job set status "info" dictionary as string
        """

        return json.dumps(self.info, indent=4, sort_keys=True) + "\n"
