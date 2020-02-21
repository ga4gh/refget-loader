# -*- coding: utf-8 -*-

import json
import os
from ga4gh.refget.loader.sources.ena.assembly.functions.time import timestamp

class JobsetStatus(object):

    NOTSTARTED = "NotStarted"
    RUNNING = "Running"
    SUCCESS = "Success"
    FAILED = "Failed"

    def __init__(self, filepath):
        self.filepath = filepath
        self.info = {
            "object_id": "NULL",
            "last_modified": timestamp(),
            "message": "",
            "status": JobsetStatus.NOTSTARTED,
            "data": {}
        }
        self.__load()

    def write(self):
        self.__update_timestamp()
        fh = open(self.filepath, "w")
        fh.write(str(self))
        fh.close()
    
    def set_objectid(self, object_id):
        self.info["object_id"] = object_id

    def set_message(self, message):
        self.info["message"] = message
    
    def set_data(self, key, value):
        self.info["data"][key] = value

    def set_status_not_started(self):
        self.info["status"] = JobsetStatus.NOTSTARTED
    
    def set_status_running(self):
        self.info["status"] = JobsetStatus.RUNNING
    
    def set_status_success(self):
        self.info["status"] = JobsetStatus.SUCCESS
    
    def set_status_failure(self):
        self.info["status"] = JobsetStatus.FAILED

    def is_success(self):
        return self.info["status"] == JobsetStatus.SUCCESS
    
    def is_failure(self):
        return self.info["status"] == JobsetStatus.FAILED

    def __update_timestamp(self):
        self.info["last_modified"] = timestamp()
    
    def __load(self):
        if os.path.exists(self.filepath):
            fh = open(self.filepath, "r")
            self.info = json.load(fh)
            fh.close()
    
    def __str__(self):
        return json.dumps(self.info, indent=4, sort_keys=True) + "\n"
