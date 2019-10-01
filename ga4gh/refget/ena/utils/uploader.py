import os

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

    def validate_and_upload_all(self):
        
        try:
            self.validate_all()
            self.upload_all()
        except Exception as e:
            pass
    
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
        # self.upload_trunc512_and_md5(seq_dict, "json_path")
    
    def upload_trunc512_and_md5(self, seq_dict, file_key):
        put_object_template = \
            "aws s3api put-object " \
            + "--bucket {bucket} " \
            + "--key {key} " \
            + "--acl public-read "
        put_object_trunc512_template = put_object_template \
            + "--body {file_path}"
        put_object_md5_template = put_object_template \
            + "--website-redirect-location {redirect}"

        trunc512_parameters = {
            "bucket": self.s3_bucket,
            "key": seq_dict["trunc512"],
            "file_path": seq_dict[file_key]
        }
        md5_parameters = {
            "bucket": self.s3_bucket,
            "key": seq_dict["md5"],
            "redirect": "/" + seq_dict["trunc512"]
        }
        self.upload(put_object_trunc512_template, trunc512_parameters)
        self.upload(put_object_md5_template, md5_parameters)
    
    def upload(self, template, parameters):
        command = template.format(**parameters)
        os.system(command)