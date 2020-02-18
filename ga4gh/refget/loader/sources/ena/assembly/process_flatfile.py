# -*- coding: utf-8 -*-
"""Process ENA assemblies associated with a single .dat.gz flatfile"""

import json
import logging
import os
from ga4gh.refget.loader.sources.ena.assembly.functions.time import timestamp

def write_cmd_and_bsub(cmd, cmd_dir, log_dir, cmd_name, job_id, 
    hold_jobname=None):
    """Write command and bsub files for a single batch job/component

    Arguments:
        cmd (str): cli command
        cmd_dir (str): path to batch command directory
        log_dir (str): path to logs directory
        cmd_name (str): name describing the type of job being undertaken
        job_id (str): unique id distinguising this run from others of same type
        hold_jobname (str): this job will wait for the specified job to complete

    Returns:
        (str): path to bsub command file
    """

    # set full job name from command name and unique id
    # set log files
    job_name = "{}.{}".format(cmd_name, job_id)
    logfile_out = os.path.join(log_dir, job_name + ".log.out")
    logfile_err = os.path.join(log_dir, job_name + ".log.err")

    # set command and bsub file paths, set bsub command
    # write command, bsub to specified file paths, modify permissions so they
    # can be run
    cmd_file = os.path.join(cmd_dir, cmd_name + ".sh")
    bsub_file = os.path.join(cmd_dir, cmd_name + ".bsub")
    bsub = 'bsub -o {} -e {} -J {} '.format(logfile_out, logfile_err, job_name)
    if hold_jobname:
        bsub += "-w 'ended({})' ".format(hold_jobname)
    bsub += '"{}"'.format(cmd_file)

    open(cmd_file, "w").write(cmd + "\n")
    open(bsub_file, "w").write(bsub + "\n")
    os.chmod(cmd_file, 0o744)
    os.chmod(bsub_file, 0o744)
    return bsub_file

def write_process_cmd_and_bsub(subdir, perl_script, file_path, job_id, cmd_dir,
    log_dir):
    """Write batch files for processing (ena-refget-processor) step

    Arguments:
        subdir (str): directory where output seqs will be written
        perl_script (str): path to ena-refget-processor perl script
        file_path (str): path to input .dat flat file
        job_id (str): unique id distinguishing it from other process jobs
        cmd_dir (str): path to batch command directory
        log_dir (str): path to logs directory
    
    Returns:
        (str) path to bsub command file
    """

    cmd_template = "{} --store-path {} --file-path {} --process-id {}"
    cmd = cmd_template.format(perl_script, subdir, file_path, job_id)
    return write_cmd_and_bsub(cmd, cmd_dir, log_dir, "process", job_id)

def write_manifest_cmd_and_bsub(subdir, job_id, source_config, 
    destination_config, cmd_dir, log_dir):
    """Write batch files for producing the upload manifest

    Arguments:
        subdir (str): directory where output seqs were written
        job_id (str): unique id of preprocessing run
        source_config (str): path to source json config
        destination_config (str): path to destination json config
        cmd_dir (str): path to batch command directory
        log_dir (str): path to logs directory
    
    Returns:
        (str): path to bsub command file
    """

    hold_jobname = "process.{}".format(job_id)
    cmd_template = "refget-loader subcommands ena assembly manifest " \
        + "{} {} {} {}"
    cmd = cmd_template.format(subdir, job_id, source_config, destination_config)
    return write_cmd_and_bsub(cmd, cmd_dir, log_dir, "manifest", job_id,
        hold_jobname=hold_jobname)

def write_upload_cmd_and_bsub(manifest, job_id, cmd_dir, log_dir):
    """Write batch files for uploading files in the file manifest

    Arguments:
        manifest (str): path to upload manifest file
        job_id (str): unique id of manifest generation run
        cmd_dir (str): path to batch command directory
        log_dir (str): path to logs directory

    Returns:
        (str): path to bsub command file
    """
    
    hold_jobname = "manifest.{}".format(job_id)
    cmd_template = "refget-loader upload {}"
    cmd = cmd_template.format(manifest)
    return write_cmd_and_bsub(cmd, cmd_dir, log_dir, "upload", job_id,
        hold_jobname=hold_jobname)

def process_flatfile(processing_dir, accession, url, config_obj, source_config,
    destination_config):
    """Submit preprocessing, manifest generation, and upload jobs for flatfile

    Individual components of processing ENA assemblies and uploading to the 
    destination in the destination config are split into 3 components:
    1. process via ena-refget-processor
    2. generate the file manifest
    3. upload to destination
    The three components are submitted as cluster batch jobs, in which each
    jobs waits for the previous job to finish

    Arguments:
        processing_dir (str): directory to write processed files
        accession (str): unique flatfile accession (from AssemblyScanner list)
        url (str): FTP url for this flatfile (from AssemblyScanner list)
        config_obj (dict): loaded source json config
        source_config (str): path to source json config
        destination_config (str): path to destination json config
    """

    perl_script = config_obj["ena_refget_processor_script"]
    logging.debug("{} - flatfile process attempt".format(accession))

    # create the processing sub-directory to prevent too many files in 
    # NFS filesystem
    # create directory to hold batch commands and logs
    url_basename = os.path.basename(url)
    url_id = url_basename.split(".")[0]
    subdir = os.path.join(processing_dir, "files", url_basename[:2], url_id)
    cmd_dir = os.path.join(subdir, "cmd")
    log_dir = os.path.join(subdir, "log")
    for d in [subdir, cmd_dir, log_dir]:
        if not os.path.exists(d):
            os.makedirs(d)

    # initialize the status if the file hasn't been created, otherwise load
    # status from the file
    status_fp = os.path.join(subdir, "status.json")
    status_dict = {
        "accession": accession,
        "url": url,
        "status": "NotAttempted",
        "message": "None",
        "last_modified": timestamp()
    }
    if os.path.exists(status_fp):
        status_dict = json.loads(open(status_fp, "r").read())

    # only execute if status is not "Completed"
    if status_dict["status"] != "Completed":
        try:
            status_dict["status"] = "InProgress"

            # the ftp url maps to a local file path onsite, we do not need
            # to download the flatfile to process
            dat_orig = url.replace("ftp://ftp.ebi.ac.uk", "/nfs/ftp")
            dat_link = os.path.join(subdir, url_basename)

            # if local file doesn't exist, set status to "Failed"
            if not os.path.exists(dat_orig):
                raise Exception(
                    "input .dat file could not be located at the path: " 
                    + dat_orig
                )
            # symlink the flatfile to the working directory
            if os.path.exists(dat_link):
                os.remove(dat_link)
            os.symlink(dat_orig, dat_link)

            manifest = subdir + "/" + "/logs/" + url_id \
                + ".manifest.csv"

            # create cmd and bsub files for both components:
            # 1. ena-refget-processor
            # 2. generate manifest from full and loader csv
            # 3. upload
            process_bsub_file = write_process_cmd_and_bsub(subdir, perl_script,
                dat_link, url_id, cmd_dir, log_dir)
            manifest_bsub_file = write_manifest_cmd_and_bsub(subdir, url_id,
                source_config, destination_config, cmd_dir, log_dir)
            upload_bsub_file = write_upload_cmd_and_bsub(manifest, url_id, 
                cmd_dir, log_dir)
                
            os.system(process_bsub_file)
            os.system(manifest_bsub_file)
            os.system(upload_bsub_file)

        except Exception as e:
            # any exceptions in the above will set the status to "Failed",
            # to be retried later
            status_dict["status"] = "Failed"
            status_dict["message"] = str(e)
            logging.error("{} - flatfile process attempt failed: {}".format(
                accession, str(e)))
        finally:
            # write the status dictionary to the status file
            status_dict["last_modified"] = timestamp()
            open(status_fp, "w").write(
                json.dumps(status_dict, indent=4, sort_keys=True) + "\n")
