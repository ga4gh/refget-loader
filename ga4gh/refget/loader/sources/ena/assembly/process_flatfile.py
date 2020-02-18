# -*- coding: utf-8 -*-
"""Process ENA assemblies associated with a single .dat.gz flatfile"""

'''

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

'''