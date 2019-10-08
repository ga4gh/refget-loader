# -*- coding: utf-8 -*-
"""Defines Uploader class, uploads sequences/metadata/csv from flatfile to s3"""

import os
import json
import subprocess
from ga4gh.refget.ena.functions.time import timestamp

class Uploader(object):
    """Uploads all sequences, metadata, csv from a single flatfile to s3

    Uploader works on the output files produced from a single run of the
    ena-refget-processor, that is, sequences and metadata named according to
    trunc512 checksum. Sequences are uploaded to the s3 bucket's "sequences"
    directory. Metadata files are uploaded to s3 bucket's "metadata/json"
    directory. The CSV file is uploaded to s3 bucket's "metadata/csv" directory.

    :param process_id: unique id for the flat file being processed
    :type process_id: str
    :param processing_dir: directory where all files were processed
    :type processing_dir: str
    :param s3_bucket: name of s3 bucket that will receive files
    :type s3_bucket: str
    :param loader_csv: full path to loader csv, referencing output seqs/metadata
    :type loader_csv: str
    :param loader_columns: names of loader csv columns
    :type loader_columns: list[str]
    :param loader_dict: column name -> position key, value mapping
    :type loader_dict: dict[str, int]
    :param full_csv: full path to full csv, containing more info than loader
    :type full_csv: str
    :param full_columns: names of full csv columns
    :type full_columns: list[str]
    :param full_dict: column name -> position key, value mapping
    :type full_dict: dict[str, int]
    :param status_file: output json file, reports status of the upload
    :type status_file: str
    :param status: attributes and values to be emitted to status file
    :type status: dict[str, str]
    :param put_object_template: cli template for aws s3 upload
    :type put_object_template: str
    :param subprocess_exit_codes: exit codes from all upload commands
    :type subprocess_exit_codes: list[int]
    """
    
    def __init__(self, process_id, processing_dir):
        """Constructor method"""

        self.process_id = process_id
        self.processing_dir = processing_dir
        self.s3_bucket = "ga4gh-refget"

        self.loader_csv, self.loader_columns, self.loader_dict = \
            self.__initialize_loader_csv()
        self.full_csv, self.full_columns, self.full_dict = \
            self.__initialize_full_csv()
        
        self.status_file = os.path.join(self.processing_dir, "status.json")
        self.status = {}
        self.put_object_template = \
            "aws s3api put-object " \
            + "--bucket {bucket} " \
            + "--key {s3_path} " \
            + "--acl public-read "
        
        self.subprocess_exit_codes = []

    def validate_and_upload_all(self):
        """validate, then upload all seqs, metadata if no errors detected"""
        
        try:
            # get the current status of the process/upload for this flatfile,
            # then run the validation method
            # if validation succeeds, proceed with the upload
            self.status = self.read_status()
            self.pre_upload_validation()
            self.upload_all()
            self.post_upload_validation()
            self.status["status"] = "Completed"

        except Exception as e:
            # any errors raised by the validation or upload methods will set
            # status to Failed
            self.status["status"] = "Failed"
            self.status["message"] = str(e)
        finally:
            # write the modified status dict to status file
            self.status["last_modified"] = timestamp()
            open(self.status_file, "w").write(
                json.dumps(self.status, indent=4, sort_keys=True) + "\n"
            )
    
    def pre_upload_validation(self):
        """validate assembly was processed correctly, seqs are safe to upload

        :raises: Exception
        """

        # check csv file exists
        if not os.path.exists(self.loader_csv):
            raise Exception("ena-refget-processor output csv file not found")

        # all data lines in csv file must exhibit a "completed" status of 1,
        # meaning success
        header = True
        seq_count = 0
        for line in open(self.loader_csv, "r"):
            if header:
                header = False
            else:
                line_d = self.parse_loader_csv_line(line)
                seq_count += 1
                if not line_d["completed"] == "1":
                    raise Exception("Not all sequences processed successfully")
        
        # there must be at least 1 sequence parsed out of the assembly, if 
        # the csv contains no entries an error has occurred
        if seq_count < 1:
            raise Exception(
                "No sequences found in ena-refget-processor csv file")
    
    def post_upload_validation(self):
        """validate no errors were encountered during the upload process"""

        print("Hello, post validation function")
        print(len(self.subprocess_exit_codes))
        print(self.subprocess_exit_codes)
        print("***")

    def upload_all(self):
        """upload all seqs, metadata described in csv, plus the csv itself"""

        header = True
        for line in open(self.loader_csv, "r"):
            if header:
                header = False
            else:
                seq_dict = self.parse_loader_csv_line(line)
                self.upload_sequence_and_json(seq_dict)
        self.upload_full_csv()

    def parse_loader_csv_line(self, line):
        """Load a single csv line as a dictionary

        :param line: csv data line/record
        :type line: str
        :return: column name -> column value key, value mapping
        :rtype: dict[str, str]
        """

        ls = line.rstrip().split(",")
        return {self.loader_columns[i]: ls[i] for i in range(0, len(ls))}
    
    def upload_sequence_and_json(self, seq_dict):
        """upload a single seq and its associated metadata to s3

        :param seq_dict: parsed csv line, contains local seq and metadata path
        :type seq_dict: dict[str, str]
        """

        self.upload_trunc512_and_md5(seq_dict, "seq_path")
        self.upload_trunc512_and_md5(seq_dict, "json_path")
    
    def upload_trunc512_and_md5(self, seq_dict, file_key):
        """upload trunc512 and md5 redirection of a single file

        Uploaded seqs/metadata are stored in files named according to their
        TRUNC512 id. Objects must also be accessible by MD5. This method uploads
        the original TRUNC512 file, as well as an empty file (named by MD5
        digest). The empty file is affixed with a metadata property to redirect
        to the TRUNC512 file.

        :param seq_dict: parsed csv line, contains local seq and metadata path
        :type seq_dict: dict[str, str]
        :param file_key: "seq_path" or "json_path", indicates file type
        :type file_key: str
        """

        # seqs and metadata belong to different S3 subdirectories, and also
        # have different file suffixes
        # destination subdir and suffix are set according to the file_key
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

        # aws command template to upload an object to S3
        # original (trunc512) file has a slightly different command template
        # than the redirection (md5) file
        put_object_trunc512_template = self.put_object_template \
            + "--body {file_path}"
        put_object_md5_template = self.put_object_template \
            + "--website-redirect-location {redirect}"

        # parameters to modify the command templates
        trunc512_parameters = {
            "bucket": self.s3_bucket,
            "s3_path": trunc512_path,
            "file_path": seq_dict[file_key]
        }
        md5_parameters = {
            "bucket": self.s3_bucket,
            "s3_path": md5_path,
            "redirect": "/" + trunc512_path
        }

        # upload both trunc512 and md5 files by rendering the command template,
        # then executing the mature command
        self.upload(put_object_trunc512_template, trunc512_parameters)
        self.upload(put_object_md5_template, md5_parameters)
    
    def upload_full_csv(self):
        """Upload the full csv generated by ena-refget-processor"""

        s3_csv_subdir = "metadata/csv/"
        s3_csv_path = s3_csv_subdir + os.path.basename(self.full_csv)
        put_object_template = self.put_object_template \
            + "--body {file_path}"
        upload_params = {
            "bucket": self.s3_bucket,
            "s3_path": s3_csv_path,
            "file_path": self.full_csv
        }
        self.upload(put_object_template, upload_params)

    def upload(self, template, parameters):
        """execute an upload cli command from command template and params

        :param template: S3 upload command template
        :type template: str
        :param parameters: parameters to render a complete command from template
        :type parameters: dict[str, str]
        """

        command = template.format(**parameters)
        exit_code = subprocess.call(command)
        self.subprocess_exit_codes.append(exit_code)
    
    def read_status(self):
        """load status file json as a dictionary

        :return: contents of status file
        :rtype: dict[str, str]
        """

        return json.loads(open(self.status_file, "r").read())

    def __initialize_loader_csv(self):
        """initialize loader csv related attributes

        :return: csv, column names, column name dict for loader csv
        :rtype: list
        """

        csv = os.path.join(self.processing_dir, "logs", 
            self.process_id + ".loader.csv")
        columns = [
            "timestamp",
            "completed",
            "trunc512",
            "md5",
            "seq_path",
            "json_path"
        ]
        columns_dict = {columns[i]: i for i in range(0, len(columns))}
        return [csv, columns, columns_dict]

    def __initialize_full_csv(self):
        """initialize full csv related attributes

        :return: csv, column names, column name dict for full csv
        :rtype: list
        """

        csv = os.path.join(self.processing_dir, "logs",
            self.process_id + ".full.csv")
        columns = [
            "trunc512",
            "md5",
            "length",
            "sha512",
            "trunc512_base64",
            "insdc",
            "ena_type",
            "species",
            "biosample",
            "taxon"
        ]
        columns_dict = {columns[i]: i for i in range(0, len(columns))}
        return [csv, columns, columns_dict]
