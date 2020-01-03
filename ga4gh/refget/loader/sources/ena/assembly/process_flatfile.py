# -*- coding: utf-8 -*-
"""Process all assemblies for a single flatfile"""

import json
import logging
import os
from ga4gh.refget.loader.sources.ena.assembly.functions.time import timestamp

def write_cmd_and_bsub(cmd, cmd_dir, log_dir, cmd_name, job_id, 
    hold_jobname=None):
    """Write command and bsub files for a single batch job/component

    :param cmd: cli command
    :type cmd: str
    :param cmd_dir: path to batch command directory
    :type cmd_dir: str
    :param log_dir: path to logs directory
    :type log_dir: str
    :param cmd_name: name describing the type of job being undertaken
    :type cmd_name: str
    :param job_id: unique id distinguising this run from others of the same type
    :type job_id: str
    :param hold_jobname: this job will wait for the specified job to complete
    :type hold_jobname: str, optional
    :return: path to bsub command file
    :rtype: str
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

    :param subdir: directory where output seqs will be written
    :type subdir: str
    :param file_path: path to input .dat flat file
    :type file_path: str
    :param job_id: unique id distinguishing it from other process jobs
    :type job_id: str
    :param cmd_dir: path to batch command directory
    :type cmd_dir: str
    :param log_dir: path to logs directory
    :type log_dir: str
    :return: path to bsub command file
    :rtype: str
    """

    cmd_template = "{} --store-path {} --file-path {} --process-id {}"
    cmd = cmd_template.format(perl_script, subdir, file_path, job_id)
    return write_cmd_and_bsub(cmd, cmd_dir, log_dir, "process", job_id)

def write_upload_cmd_and_bsub(subdir, job_id, cmd_dir, log_dir):
    """Write batch files for upload step

    :param subdir: directory where output seqs will be written
    :type subdir: str
    :param job_id: unique id distinguishing it from other upload jobs
    :type job_id: str
    :param cmd_dir: path to batch command directory
    :type cmd_dir: str
    :param log_dir: path to logs directory
    :type log_dir: str
    :return: path to bsub command file
    :rtype: str
    """

    hold_jobname = "process.{}".format(job_id)
    cmd_template = "ena-refget-scheduler upload {} {}"
    cmd = cmd_template.format(job_id, subdir)
    return write_cmd_and_bsub(cmd, cmd_dir, log_dir, "upload", job_id,
        hold_jobname=hold_jobname)

def process_flatfile(config_obj, processing_dir, accession, url):
    """submit process and upload jobs for a single flatfile

    There are 2 components to getting flatfiles to S3: processing via 
    ena-refget-processor, and the upload of processed files. These 2 components
    are submitted as separate batch jobs, where the second job waits until the
    first has completed.

    :param processing_dir: directory to write processed files
    :type processing_dir: str
    :param accession: unique flatfile accession (from AssemblyScanner list)
    :type accession: str
    :param url: FTP url for this flatfile (from AssemblyScanner list)
    :type url: str
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

            # create cmd and bsub files for both components:
            # 1. ena-refget-processor
            # 2. upload
            process_bsub_file = write_process_cmd_and_bsub(subdir, perl_script,
                dat_link, url_id, cmd_dir, log_dir)
            upload_bsub_file = write_upload_cmd_and_bsub(subdir, url_id, 
                cmd_dir, log_dir)

            #TODO: un-comment these when ready to execute
            os.system(process_bsub_file)
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
