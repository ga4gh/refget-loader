# -*- coding: utf-8 -*-
"""Upload processed refget seqs to AWS S3 bucket from file manifest

This module contains a "destination" upload method, which submits processed
refget seqs to a remote object store based on the seqs contains in the file
manifest. This method submits seqs to an AWS S3 bucket.
The bytes for each reference sequence are stored in an object named according
to the primary id in the file manifest. Then, empty objects are created 
according to the seq's secondary id(s). The secondary id object(s) redirect
to the primary id.
"""

import os

def aws_s3_upload(config_obj, seq_table, additional_table):
    """upload processed refget seqs to AWS S3 bucket from file manifest

    Arguments:
        config_obj (dict): loaded destination config
        seq_table (list): seqs to upload in manifest format
        additional_table (list): additional files to upload in manifest format
    """
    
    # generic template for putting a seq on s3 
    put_object_template = \
        "aws s3api put-object " \
        + "--bucket {bucket_name} " \
        + "--key {s3_path} " \
        + "--acl public-read " \
        + "{aws_profile} "
    
    # template for putting a seq by its primary id
    put_primary_template = put_object_template \
        + "--body {file_path}"
    
    # template for putting a seq by a secondary id
    put_secondary_template = put_object_template \
        + "--website-redirect-location {redirect}"

    # add an aws profile to the command if a profile needs to be used (ie. is
    # in the destination config file
    base_config = {"bucket_name": config_obj["bucket_name"]}
    base_config["aws_profile"] = \
        "--profile " + config_obj["profile"] \
        if "profile" in config_obj.keys() \
        else ""
    
    def upload_manifest_entry(line):
        """upload a single manifest entry/reference sequence to S3

        Arguments:
            line (str): manifest entry
        """

        # parse the manifest line, multiple secondary ids start at the 4th
        # column 
        ls = line.rstrip().split("\t")
        completed, seq, metadata, primary_id = ls[:4]
        secondary_ids = ls[4:]

        seq_primary_path = "sequence/" + primary_id
        metadata_primary_path = "metadata/json/" + primary_id + ".json"
        
        # upload both sequence and metadata according to the primary checksum
        upload_file_primary_checksum(s3_path=seq_primary_path, file_path=seq)
        upload_file_primary_checksum(s3_path=metadata_primary_path,
            file_path=metadata)

        # for each secondary id in the manifest entry, upload empty sequence
        # and metadata files, which will redirect to the primary entry
        for secondary_id in secondary_ids:
            seq_secondary_path = "sequence/" + secondary_id
            metadata_secondary_path = "metadata/json/" + secondary_id + ".json"
            upload_file_secondary_checksum(s3_path=seq_secondary_path,
                redirect="/"+seq_primary_path)
            upload_file_secondary_checksum(s3_path=metadata_secondary_path,
                redirect="/"+metadata_primary_path)

    def upload_file_primary_checksum(**kwargs):
        """Upload reference sequence file bytes to its primary checksum

        Arguments:
            kwargs (dict): parameters to format the aws upload cli command
        """

        kwargs.update(base_config)
        upload(put_primary_template, kwargs)

    def upload_file_secondary_checksum(**kwargs):
        """Upload empty redirect file, named according to secondary checksum/id

        Arguments:
            kwargs (dict): parameters to format the aws upload cli command
        """

        kwargs.update(base_config)
        upload(put_secondary_template, kwargs)

    def upload_additional_entry(line):
        """Upload additional file to s3

        Arguments:
            line (str): additional upload entry line in manifest
        """

        ls = line.rstrip().split("\t")
        params = {"file_path": ls[0], "s3_path": ls[1]}
        params.update(base_config)
        upload(put_primary_template, params)

    def upload(template, parameters):
        """Generic upload command, executes formatted upload-related cli command

        Arguments:
            template (str): unformatted upload cli command template
            parameters (dict): parameters to format the upload cli command
        """

        cmd = template.format(**parameters)
        os.system(cmd)

    # run all upload commands for seq lines in the upload manifest
    header = True
    for line in seq_table:
        if header:
            header = False
        else:
            upload_manifest_entry(line)
    
    # run all upload commands for additional data lines in the upload manifest
    header = True
    for line in additional_table:
        if header:
            header = False
        else:
            upload_additional_entry(line)
