import inspect
import json
import os
from ga4gh.refget.loader.config.constants import Status, JsonFiletype
from ga4gh.refget.loader.config.schemas import SCHEMAS as schema_dict

class Validator(object):
    """validates an input JSON file matches schema"""

    def __init__(self, filetype, filepath):
        self.filetype = filetype
        self.filepath = filepath

    def validate(self):
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
            schema_dir = os.path.join(
                os.path.dirname(
                    os.path.dirname(inspect.getmodule(self).__file__)
                ),
                "config",
                "schemas"
            )
            schema_file = os.path.join(
                schema_dir,
                schema_dict[self.filetype][obj["type"]]
            )
            print(schema_file)


        except Exception as e:
            result["status"] = Status.FAILURE,
            result["message"] = str(e)
        return result

def validate(filetype, filepath):
    v = Validator(filetype, filepath)
    return v.validate()

def validate_source(filepath):
    return validate(JsonFiletype.SOURCE, filepath)

def validate_destination(filepath):
    return validate(JsonFiletype.DESTINATION, filepath)
