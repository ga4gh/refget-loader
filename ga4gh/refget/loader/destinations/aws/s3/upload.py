import os

def aws_s3_upload(config_obj, table):
    
    put_object_template = \
        "aws s3api put-object " \
        + "--bucket {bucket_name} " \
        + "--key {s3_path} " \
        + "--acl public-read " \
        + "{aws_profile} "
    
    put_primary_template = put_object_template \
        + "--body {file_path}"
    
    put_secondary_template = put_object_template \
        + "--website-redirect-location {redirect}"

    base_config = {"bucket_name": config_obj["bucket_name"]}
    base_config["aws_profile"] = \
        config_obj["profile"] \
        if "profile" in config_obj.keys() \
        else ""
    
    def upload_manifest_entry(line):
        ls = line.rstrip().split("\t")
        completed, seq, metadata, primary_id = ls[:4]
        secondary_ids = ls[4:]

        seq_primary_path = "sequence/" + primary_id
        metadata_primary_path = "metadata/json/" + primary_id + ".json"
        
        # upload files by primary checksum
        # seq
        upload_file_primary_checksum(s3_path=seq_primary_path, file_path=seq)
        # metadata
        upload_file_primary_checksum(s3_path=metadata_primary_path,
            file_path=metadata)

        # upload empty redirect files by secondary checksums
        for secondary_id in secondary_ids:
            seq_secondary_path = "sequence/" + secondary_id
            metadata_secondary_path = "metadata/json/" + secondary_id + ".json"
            # seq
            upload_file_secondary_checksum(s3_path=seq_secondary_path,
                redirect=seq_primary_path)
            # metadata
            upload_file_secondary_checksum(s3_path=metadata_secondary_path,
                redirect=metadata_primary_path)

    def upload_file_primary_checksum(**kwargs):
        params = kwargs.update(base_config)
        upload(put_primary_template, params)

    def upload_file_secondary_checksum(**kwargs):
        params = kwargs.update(base_config)
        upload(put_secondary_template, params)

    def upload(template, parameters):
        cmd = template.format(parameters)
        os.system(cmd)

    header = True
    for line in table:
        if header:
            header = False
        else:
            upload_manifest_entry(line)
