import os
import json
from ga4gh.refget.ena.functions.time import timestamp

class Uploader(object):
    
    def __init__(self, process_id, processing_dir):
        self.process_id = process_id
        self.processing_dir = processing_dir

        self.s3_bucket = "ga4gh-refget"

        self.csv = os.path.join(self.processing_dir, "logs", 
            self.process_id + ".loader.csv")
        self.csv_columns = ["timestamp", "completed", "trunc512", "md5",
            "seq_path","json_path"]
        self.csv_columns_dict = {
            self.csv_columns[i]: i for i in range(0, len(self.csv_columns))
        }

        self.status_file = os.path.join(self.processing_dir, "status.txt")
        self.status = {}

    def validate_and_upload_all(self):
        
        try:
            self.status = self.read_status()
            self.validate_all()
            self.upload_all()
            self.status["status"] = "Completed"

        except Exception as e:
            self.status["status"] = "Failed"
            self.status["message"] = str(e)
        finally:
            self.status["last_modified"] = timestamp()
            open(self.status_file, "w").write(
                json.dumps(self.status, indent=4, sort_keys=True) + "\n"
            )
    
    def validate_all(self):
        header = True
        for line in open(self.csv, "r"):
            if header:
                header = False
            else:
                line_d = self.parse_csv_line(line)
                if not line_d["completed"] == "1":
                    raise Exception("Not all sequences processed successfully")

    def upload_all(self):
        header = True
        for line in open(self.csv, "r"):
            if header:
                header = False
            else:
                seq_dict = self.parse_csv_line(line)
                self.upload_sequence_and_json(seq_dict)

    def parse_csv_line(self, line):
        ls = line.rstrip().split(",")
        return {self.csv_columns[i]: ls[i] for i in range(0, len(ls))}
    
    def upload_sequence_and_json(self, seq_dict):
        self.upload_trunc512_and_md5(seq_dict, "seq_path")
        self.upload_trunc512_and_md5(seq_dict, "json_path")
    
    def upload_trunc512_and_md5(self, seq_dict, file_key):

        s3_subdir_dict = {
            "seq_path": "sequence/", 
            "json_path": "metadata/json/"
        }
        s3_suffix_dict = {
            "seq_path": "",
            "json_path": ".json"

        }
        s3_subdir = s3_subdir_dict[file_key]
        s3_suffix = s3_suffix_dict[file_key]
        trunc512_path = s3_subdir + seq_dict["trunc512"] + s3_suffix
        md5_path = s3_subdir + seq_dict["md5"] + s3_suffix

        put_object_template = \
            "aws s3api put-object " \
            + "--bucket {bucket} " \
            + "--key {s3_path} " \
            + "--acl public-read "
        put_object_trunc512_template = put_object_template \
            + "--body {file_path}"
        put_object_md5_template = put_object_template \
            + "--website-redirect-location {redirect}"

        trunc512_parameters = {
            "bucket": self.s3_bucket,
            "s3_path": trunc512_path,
            "file_path": seq_dict[file_key]
        }
        md5_parameters = {
            "bucket": self.s3_bucket,
            "s3_path": md5_path,
            "redirect": trunc512_path
        }
        self.upload(put_object_trunc512_template, trunc512_parameters)
        self.upload(put_object_md5_template, md5_parameters)
    
    def upload(self, template, parameters):
        command = template.format(**parameters)
        os.system(command)
    
    def read_status(self):
        return json.loads(open(self.status_file, "r").read())
