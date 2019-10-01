import json
import os
from ga4gh.refget.ena.functions.time import timestamp
from ga4gh.refget.ena.resources.get_resource import parse_settings_ini

def write_cmd_and_bsub(cmd, cmd_dir, cmd_name):
    cmd_file = os.path.join(cmd_dir, cmd_name + ".sh")
    bsub_file = os.path.join(cmd_dir, cmd_name + ".bsub")
    bsub = 'bsub "{}"'.format(cmd_file)
    open(cmd_file, "w").write(cmd + "\n")
    open(bsub_file, "w").write(bsub + "\n")

def write_process_cmd_and_bsub(subdir, file_path, process_id, cmd_dir):
    perl_script = parse_settings_ini()["refget_ena_settings"]["perl_script"]
    cmd_template = "{} --store-path {} --file-path {} --process-id {}"
    cmd = cmd_template.format(perl_script, subdir, file_path, process_id)
    write_cmd_and_bsub(cmd, cmd_dir, "process")

def make_upload_command():
    pass

def process_single_flatfile(processing_dir, accession, url):

    url_basename = os.path.basename(url)
    url_id = url_basename.split(".")[0]
    subdir = os.path.join(processing_dir, "files", url_basename[:2], url_id)
    cmd_dir = os.path.join(subdir, "cmd")
    log_dir = os.path.join(subdir, "log")
    for d in [subdir, cmd_dir, log_dir]:
        if not os.path.exists(d):
            os.makedirs(d)

    status_fp = os.path.join(subdir, "status.txt")
    status_dict = {
        "accession": accession,
        "url": url,
        "status": "NotAttempted",
        "message": "None",
        "last_modified": timestamp()
    }

    try:
        dat_orig = url.replace("ftp://ftp.ebi.ac.uk", "/nfs/ftp")
        dat_link = os.path.join(subdir, url_basename)

        if not os.path.exists(dat_orig):
            raise Exception(
                "input .dat file could not be located at the path: " 
                + dat_orig
            )
        os.symlink(dat_orig, dat_link)

        
        bsub_template = ""
        
    except Exception as e:
        status_dict["status"] = "Failed"
        status_dict["message"] = str(e)
    finally:
        status_dict["last_modified"] = timestamp()
        open(status_fp, "w").write(
            json.dumps(status_dict, indent=4, sort_keys=True) + "\n")
