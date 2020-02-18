# -*- coding: utf-8 -*-
"""Validates an JSON file/object matches the appropriate schema"""

import inspect
import json
import os
from jsonschema import RefResolver, validate as jsonschema_validate
from jsonschema.exceptions import ValidationError
from ga4gh.refget.loader.config.constants import Status, JsonFiletype
from ga4gh.refget.loader.config.schemas import SCHEMAS as schema_dict

class Validator(object):
    """Validates an input JSON file/object matches the appropriate schema

    Attributes:
        filetype (int): indicates if it will validate "source" or "destination"
        filepath (str): path to json config file
    """

    def __init__(self, filetype, filepath):
        """Instantiates a Validator

        Args:
            filetype (int): indicates "source" or "destination"
            filepath (str): path to json config file
        """

        self.filetype = filetype
        self.filepath = filepath

    def __validate_json_schema(self, schema_filename):
        """Validate the json instance against the matching schema

        Arguments:
            schema_filename (str): name of schema file by "type" keyword

        Returns:
            (dict): validation success/failure status and associated message 
        """

        # load the correct schema file according to the directory where all
        # schemas are stored, and the schema filename
        schema_dir = os.path.join(
            os.path.dirname(
                os.path.dirname(inspect.getmodule(self).__file__)
            ),
            "config",
            "schemas"
        )
        schema_file = os.path.join(
            schema_dir,
            schema_filename
        )
        resolver = RefResolver('file://' + schema_dir + "/", None)
        schema_obj = json.load(open(schema_file, "r"))
        instance_obj = json.load(open(self.filepath, "r"))

        result = {
            "status": Status.SUCCESS,
            "message": ""
        }

        # attempt to validate the json instance against the schema, catching
        # exceptions if it doesn't validate
        try:
            jsonschema_validate(
                instance=instance_obj,
                schema=schema_obj,
                resolver=resolver
            )
        except ValidationError as e:
            result["status"] = Status.FAILURE,
            result["message"] = str(e)

        return result

    def validate(self):
        """Validate json instance against matching schema"""

        result = {
            "status": Status.SUCCESS,
            "message": ""
        }

        try:
            # validate file path exists
            if not os.path.exists(self.filepath):
                raise Exception("file does not exist: " + self.filepath)

            # validate file contains valid JSON
            obj = None
            try:
                obj = json.load(open(self.filepath, "r"))
            except json.JSONDecodeError as e:
                raise Exception(self.filepath + " is not valid JSON")

            # validate file contains 'type' property, and has a valid value
            if "type" not in obj.keys():
                raise Exception(self.filepath + " missing required 'type' "
                    + "property")
            valid_types = schema_dict[self.filetype].keys()
            if obj["type"] not in valid_types:
                raise Exception(self.filepath + " contains invalid 'type', "
                    + "must be one of: " + ",".join(valid_types))
            
            # validate file contains the correct properties for specified type
            schema_filename = schema_dict[self.filetype][obj["type"]]
            json_schema_result = self.__validate_json_schema(schema_filename)
            if json_schema_result["status"] != Status.SUCCESS:
                raise Exception(self.filepath + " JSON schema validation "
                    + "failed:\n" + json_schema_result["message"])

        # catch all exceptions, setting the validation result to a failure
        # if any exception raised
        except Exception as e:
            result["status"] = Status.FAILURE
            result["message"] = str(e)
        return result

def validate(filetype, filepath):
    """Validate a json instance using the Validator class

    Arguments:
        filetype (int): indicates "source" or "destination"
        filepath (str): path to json config file
    
    Returns:
        (dict): validation result
    """

    v = Validator(filetype, filepath)
    return v.validate()

def validate_source(filepath):
    """Validate a source json config file using the Validator class

    Arguments:
        filepath (str): path to source json config file
    
    Returns:
        (dict) validation result
    """

    return validate(JsonFiletype.SOURCE, filepath)

def validate_destination(filepath):
    """Validate a destination json config file using the Validator class

    Arguments:
        filepath (str): path to destination json config file
    
    Returns:
        (dict): validation result
    """

    return validate(JsonFiletype.DESTINATION, filepath)
