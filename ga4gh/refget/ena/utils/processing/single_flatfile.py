import json
import os
from ga4gh.refget.ena.functions.time import timestamp
from ga4gh.refget.ena.resources.get_resource import parse_settings_ini

def write_cmd_and_bsub(cmd, cmd_dir, log_dir, cmd_name, job_id, 
    hold_jobname=None):
    job_name = "{}.{}".format(cmd_name, job_id)
    logfile_out = os.path.join(log_dir, job_name + ".log.out")
    logfile_err = os.path.join(log_dir, job_name + ".log.err")

    cmd_file = os.path.join(cmd_dir, cmd_name + ".sh")
    bsub_file = os.path.join(cmd_dir, cmd_name + ".bsub")
    bsub = 'bsub -o {} -e {} -J {} '.format(logfile_out, logfile_err, job_name)
    if hold_jobname:
        bsub += '-w done({}) '.format(hold_jobname)
    bsub += '"{}"'.format(cmd_file)

    open(cmd_file, "w").write(cmd + "\n")
    open(bsub_file, "w").write(bsub + "\n")
    os.chmod(cmd_file, 0o744)
    os.chmod(bsub_file, 0o744)
    return bsub_file

def write_process_cmd_and_bsub(subdir, file_path, job_id, cmd_dir, log_dir):
    perl_script = parse_settings_ini()["refget_ena_settings"]["perl_script"]
    cmd_template = "{} --store-path {} --file-path {} --process-id {}"
    cmd = cmd_template.format(perl_script, subdir, file_path, job_id)
    return write_cmd_and_bsub(cmd, cmd_dir, log_dir, "process", job_id)

def write_upload_cmd_and_bsub(subdir, job_id, cmd_dir, log_dir):
    cmd_template = "ena-refget-scheduler upload {}"
    cmd = cmd_template.format(subdir)
    return write_cmd_and_bsub(cmd, cmd_dir, log_dir, "upload", job_id)

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
        if os.path.exists(dat_link):
            os.remove(dat_link)
        os.symlink(dat_orig, dat_link)

        process_bsub_file = write_process_cmd_and_bsub(
            subdir, dat_link, url_id, cmd_dir)
        upload_bsub_file = write_upload_cmd_and_bsub(subdir, url_id, cmd_dir)
        os.system(process_bsub_file)
        os.system(upload_bsub_file)
        
    except Exception as e:
        status_dict["status"] = "Failed"
        status_dict["message"] = str(e)
    finally:
        status_dict["last_modified"] = timestamp()
        open(status_fp, "w").write(
            json.dumps(status_dict, indent=4, sort_keys=True) + "\n")
