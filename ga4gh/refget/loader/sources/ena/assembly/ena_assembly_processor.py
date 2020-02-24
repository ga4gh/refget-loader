# -*- coding: utf-8 -*-
"""SourceProcessor subclass, process all ENA assemblies specified period

The ENAAssemblyProcessor SourceProcessor subclass will process all ENA
assemblies uploaded to ENA on certain date(s) into refget format, then upload
to specified destination.
"""

import datetime
import json
import logging
import os
from ga4gh.refget.loader.jobset_status import JobsetStatus
from ga4gh.refget.loader.sources.source_processor import SourceProcessor
from ga4gh.refget.loader.sources.ena.assembly.functions.time import timestamp
from ga4gh.refget.loader.sources.ena.assembly.utils.assembly_scanner \
    import AssemblyScanner


class ENAAssemblyProcessor(SourceProcessor):
    """Process ENA Assembly sequences into refget format

    Attributes:
        ena_refget_processor_script (str): path to ena processor script
        processing_dir (str): top-level processing/working directory
        start_date (str): YYYY-MM-DD day to start scanning for assemblies
        number_of_days (int): how many days to scan for assemblies (from start)
    """

    def __init__(self, config_file, config):
        """Instantiate an ENAAssemblyProcessor object

        Arguments:
            config_file (str): path to json config file
            config (str): load json config
        """

        super(ENAAssemblyProcessor, self).__init__(config_file, config)

        # set ENAAssembly-specific properties according to config file contents
        self.ena_refget_processor_script = \
            self.source_config["ena_refget_processor_script"]
        self.processing_dir = self.source_config["processing_dir"]
        self.start_date = self.source_config["start_date"]
        self.number_of_days = self.source_config["number_of_days"]

    def prepare_commands(self):
        """Get Jobs to process assemblies over the specified period

        Returns:
            (list): list of all prepared cli Jobs
        """

        all_jobs = []

        root_dir = self.processing_dir
        date_string = self.start_date
        n_days = self.number_of_days

        # for each day, starting from the start date, incrementing by 1 until
        # the number of days has been reached
        for i in range(0, n_days):
            year, month, day = date_string.split("-")
        
            # create sub directory, logfile
            sub_dir = os.path.join(root_dir, year, month, day)
            if not os.path.exists(sub_dir):
                os.makedirs(sub_dir)
            logfile = os.path.join(sub_dir, "logfile.txt")
            logging.basicConfig(
                filename=logfile,
                format='%(asctime)s\t%(levelname)s\t%(message)s',
                level=logging.DEBUG,
            )
            logging.info("logs for sequences uploaded on: " + date_string)

            # get all processing jobs for a single day and add to overall
            # jobs list
            all_jobs += self.prepare_single_date_commands(date_string, sub_dir)

            # create the next date and update the config file
            date = datetime.date(*[int(a) for a in [year, month, day]])
            next_date = date + datetime.timedelta(days=1)
            next_date_string = next_date.strftime("%Y-%m-%d")
            date_string = next_date_string
            logging.info("set checkpoint date to " + next_date_string)

            # remove logging handler, new log file is written to the next date
            for handler in logging.root.handlers:
                logging.root.removeHandler(handler)
        
        return all_jobs
    
    def prepare_single_date_commands(self, date_string, processing_dir):
        """Get commands to process ENA assemblies deployed on the same date

        Arguments:
            date_string (str): YYYY-MM-DD, date to scan and process
            processing_dir (str): directory to process all seqs for given date

        Returns:
            (list): list of all prepared cli Jobs for a single date 
        """

        date_jobs = [] # array of job objects

        # generate the accession list via AssemblyScanner,
        # if accession list already exists, skip list re-generation
        accession_list_file = os.path.join(
            processing_dir, "accessions_list.txt")
        logging.info("generating accessions list from search API scan")
        scanner = AssemblyScanner(date_string)
        scanner.generate_accession_list(accession_list_file)

        # for each accession (line) in the list file, send the accession and
        # url to the process_single_flatfile method
        accessions_urls = []
        header = True
        for line in open(accession_list_file, "r"):
            if header:
                header = False
            else:
                accession, url =\
                    line.strip().split("\t")
                accessions_urls.append([accession, url])
        
        for accession, url in accessions_urls:
            flatfile_jobs = self.prepare_single_flatfile_commands(
                processing_dir, accession, url)
            date_jobs += flatfile_jobs
        
        return date_jobs
    
    def prepare_single_flatfile_commands(self, processing_dir, accession, url):
        """Get commands to process and generate manifest for single flatfile

        Individual components of processing ENA assemblies and uploading to the 
        destination in the destination config are split into 2 components:
        1. process via ena-refget-processor
        2. generate the file manifest
        3. upload seqs in file manifest to destination
        The jobs generated from both components are returned and passed to the
        runtime layer/execution context

        Arguments:
            processing_dir (str): directory to write processed files
            accession (str): unique flatfile accession (AssemblyScanner list)
            url (str): FTP url for this flatfile (from AssemblyScanner list)

        Returns:
            (list): 3 Jobs for processing and uploading a single flatfile
        """

        flatfile_jobs = []
        logging.debug("{} - flatfile process attempt".format(accession))

        # create the processing sub-directory to prevent too many files in 
        # NFS filesystem
        # create directory to hold batch commands and logs
        url_basename = os.path.basename(url)
        urlid = url_basename.split(".")[0]
        subdir = os.path.join(processing_dir, "files", url_basename[:2], urlid)
        cmd_dir = os.path.join(subdir, "cmd")
        log_dir = os.path.join(subdir, "log")
        for d in [subdir, cmd_dir, log_dir]:
            if not os.path.exists(d):
                os.makedirs(d)

        # initialize the status if the file hasn't been created, otherwise load
        # status from the file
        jobset_status_filepath = os.path.join(subdir, "status.json")
        jobset_status = JobsetStatus(jobset_status_filepath)
        jobset_status.set_objectid(accession)
        
        # only execute if status is not "Completed"
        if not jobset_status.is_success():
            try:
                jobset_status.set_status_running()
                jobset_status.set_data("url", url)
                jobset_status.set_data("processid", urlid)
                jobset_status.set_message("")

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

                manifest = os.path.join(subdir, urlid + ".manifest.csv")

                # create cmd files for components:
                # 1. ena-refget-processor
                # 2. generate manifest from full and loader csv
                # 3. upload
                jobid_template = "ena_assembly.{}.{}"
                process_jobid = jobid_template.format("process", urlid)
                manifest_jobid = jobid_template.format("manifest", urlid)
                upload_jobid = jobid_template.format("upload", urlid)

                process_job = self.process_job(subdir, cmd_dir,
                    dat_link, urlid, process_jobid)
                manifest_job = self.manifest_job(subdir, cmd_dir, urlid, 
                    manifest_jobid, process_jobid)
                upload_job = self.upload_job(cmd_dir, self.desttype,
                    manifest, upload_jobid, manifest_jobid)
                flatfile_jobs = [process_job, manifest_job, upload_job]

            except Exception as e:
                # any exceptions in the above will set the status to "Failed",
                # to be retried later
                jobset_status.set_status_failure()
                jobset_status.set_message(str(e))
                logging.error("{} - flatfile process attempt failed: {}".format(
                    accession, str(e)))
            finally:
                # write the status dictionary to the status file
                jobset_status.write()
                return flatfile_jobs
    
    def process_job(self, subdir, cmddir, dat_file, processid, jobid):
        """Write cli command to process sequences into refget format

        Arguments:
            subdir (str): flatfile-specific working directory
            cmddir (str): directory to write cli command file
            dat_file (str): path to .dat.gz flatfile to be processed
            processid (str): unique process id for ena-refget-processor
            jobid (str): unique identifier for process Job 
        
        Returns:
            (Job): Job for processing flatfile by ena-refget-processor
        """

        cmd_template = "{} --store-path {} --file-path {} --process-id {}"
        cmd = cmd_template.format(self.ena_refget_processor_script, subdir,
            dat_file, processid)
        cmdfile = jobid + ".sh"
        cmdpath = os.path.join(cmddir, cmdfile)
        return self.write_cmd_and_get_job(cmd, cmdpath, jobid, None)
    
    def manifest_job(self, subdir, cmddir, processid, jobid, hold_jobid):
        """Write cli command to generate file manifest from processor output

        Arguments:
            subdir (str): flatfile-specific working directory
            cmddir (str): directory to write cli command file
            dat_file (str): path to .dat.gz flatfile to be processed
            processid (str): unique process id for manifest generation
            jobid (str): unique identifier for manifest Job
            hold_jobid (str): id for process Job to wait on
        """

        cmd_template = "refget-loader subcommands ena assembly manifest " \
            + "{} {} {}"
        cmd = cmd_template.format(subdir, processid, self.config_file)
        cmdfile = jobid + ".sh"
        cmdpath = os.path.join(cmddir, cmdfile)
        return self.write_cmd_and_get_job(cmd, cmdpath, jobid, hold_jobid)
