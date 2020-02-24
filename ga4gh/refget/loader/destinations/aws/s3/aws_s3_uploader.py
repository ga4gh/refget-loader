# -*- coding: utf-8 -*-
"""Upload processed refget seqs to AWS S3 bucket from file manifest

This module contains a "destination" upload class, which submits processed
refget seqs to a remote object store based on the seqs contains in the file
manifest. This method submits seqs to an AWS S3 bucket.
The bytes for each reference sequence are stored in an object named according
to the primary id in the file manifest. Then, empty objects are created 
according to the seq's secondary id(s). The secondary id object(s) redirect
to the primary id.
"""

import os
import subprocess
from ga4gh.refget.loader.destinations.uploader import Uploader
from ga4gh.refget.loader.jobset_status import JobsetStatus

class AwsS3Uploader(Uploader):
    """Upload processed refget seqs to AWS S3 bucket from file manifest

    Attributes:
        put_object_template (str): awscli put/upload cli command template
        put_primary_template (str): upload by primary id (hold sequence bytes)
        put_secondary_template (str): upload by secondary id (redirects)
        base_config (dict): parameters to populate command templates
    """
    
    def __init__(self, manifest):
        """Instantiate an AwsS3Uploader object

        Arguments:
            manifest (str): path to sequence manifest file 
        """

        super(AwsS3Uploader, self).__init__(manifest)

        # generic template for putting a seq on s3 
        self.put_object_template = \
            "aws s3api put-object " \
            + "--bucket {bucket_name} " \
            + "--key {s3_path} " \
            + "--acl public-read " \
            + "{aws_profile} "
        
        # template for putting a seq by its primary id
        self.put_primary_template = self.put_object_template \
            + "--body {file_path}"
        
        # template for putting a seq by a secondary id
        self.put_secondary_template = self.put_object_template \
            + "--website-redirect-location {redirect}"

        # add an aws profile to the command if a profile needs to be used
        # (ie. is in the destination config file)
        self.base_config = {"bucket_name": self.dest_config["bucket_name"]}
        self.base_config["aws_profile"] = \
            "--profile " + self.dest_config["profile"] \
            if "profile" in self.dest_config.keys() \
            else ""
    
    def upload_seq_entry(self, entry):
        """Upload a single manifest sequence entry to S3

        Arguments:
            entry (str): entry in manifest sequence table

        Returns:
            (list): exit codes for multiple upload subprocesses
        """

        return self.__upload_manifest_entry(entry)
    
    def upload_additional_entry(self, entry):
        """Upload a single manifest additional entry to S3

        Arguments:
            entry (str): entry in manifest additional table
        
        Returns:
            (list): exit codes for upload subprocesses
        """

        return self.__upload_additional_entry(entry)
        
    def __upload_manifest_entry(self, line):
        """Upload a single manifest entry/reference sequence to S3

        Arguments:
            line (str): manifest sequence entry/line

        Returns:
            (list): exit codes for multiple upload subprocesses
        """

        ecs = [] # exit codes

        # parse the manifest line, multiple secondary ids start at the 4th
        # column 
        ls = line.rstrip().split("\t")
        completed, seq, metadata, primary_id = ls[:4]
        secondary_ids = ls[4:]

        seq_primary_path = "sequence/" + primary_id
        metadata_primary_path = "metadata/json/" + primary_id + ".json"
        
        # upload both sequence and metadata according to the primary checksum
        ecs.append(self.__upload_file_primary_checksum(
            s3_path=seq_primary_path, file_path=seq))
        ecs.append(self.__upload_file_primary_checksum(
            s3_path=metadata_primary_path, file_path=metadata))

        # for each secondary id in the manifest entry, upload empty sequence
        # and metadata files, which will redirect to the primary entry
        for secondary_id in secondary_ids:
            seq_secondary_path = "sequence/" + secondary_id
            metadata_secondary_path = "metadata/json/" + secondary_id + ".json"
            ecs.append(self.__upload_file_secondary_checksum(
                s3_path=seq_secondary_path, redirect="/"+seq_primary_path))
            ecs.append(self.__upload_file_secondary_checksum(
                s3_path=metadata_secondary_path,
                redirect="/"+metadata_primary_path
            ))
        
        return ecs

    def __upload_file_primary_checksum(self, **kwargs):
        """Upload reference sequence file bytes to its primary checksum

        Arguments:
            kwargs (dict): parameters to format the aws upload cli command
        
        Returns:
            (int): exit status code 
        """

        kwargs.update(self.base_config)
        return self.__execute_upload(self.put_primary_template, kwargs)

    def __upload_file_secondary_checksum(self, **kwargs):
        """Upload empty redirect file, named according to secondary checksum/id

        Arguments:
            kwargs (dict): parameters to format the aws upload cli command
        
        Returns:
            (int): exit status code
        """

        kwargs.update(self.base_config)
        return self.__execute_upload(self.put_secondary_template, kwargs)

    def __upload_additional_entry(self, line):
        """Upload additional file to s3

        Arguments:
            line (str): additional upload entry line in manifest
        
        Returns:
            (list): exit status codes
        """

        ls = line.rstrip().split("\t")
        
        params = {"file_path": ls[0], "s3_path": ls[1]}
        params.update(self.base_config)
        ecs = [self.__execute_upload(self.put_primary_template, params)]
        return ecs

    def __execute_upload(self, template, parameters):
        """Generic upload command, executes formatted upload-related cli command

        Arguments:
            template (str): unformatted upload cli command template
            parameters (dict): parameters to format the upload cli command
        
        Returns:
            (int): exit status code
        """

        cmd = template.format(**parameters).split(" ")
        exit_code = subprocess.call(cmd)
        return exit_code
