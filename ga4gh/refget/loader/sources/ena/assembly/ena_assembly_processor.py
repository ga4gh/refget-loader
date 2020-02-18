# -*- coding: utf-8 -*-
"""Process all ENA assemblies over the period specified in the source config"""

import datetime
import json
import logging
import os
from ga4gh.refget.loader.sources.ena.assembly.functions.time import timestamp
from ga4gh.refget.loader.sources.ena.assembly.utils.assembly_scanner \
    import AssemblyScanner
from ga4gh.refget.loader.sources.source_processor import SourceProcessor

class ENAAssemblyProcessor(SourceProcessor):

    def __init__(self, config_file, config):
        super(ENAAssemblyProcessor, self).__init__(config_file, config)
        self.ena_refget_processor_script = config["ena_refget_processor_script"]
        self.processing_dir = config["processing_dir"]
        self.start_date = config["start_date"]
        self.number_of_days = config["number_of_days"]

    def prepare_commands(self):
        """Get commands to process assemblies over the specified period"""

        root_dir = self.processing_dir
        date_string = self.start_date
        n_days = self.number_of_days

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

            # processing method
            self.prepare_single_date_commands(date_string, sub_dir)

            # create the next date and update the config file
            date = datetime.date(*[int(a) for a in [year, month, day]])
            next_date = date + datetime.timedelta(days=1)
            next_date_string = next_date.strftime("%Y-%m-%d")
            date_string = next_date_string
            logging.info("set checkpoint date to " + next_date_string)

            # remove logging handler so new log file is written to the next date
            for handler in logging.root.handlers:
                logging.root.removeHandler(handler)
    
    def prepare_single_date_commands(self, date_string, processing_dir):
        """Get commands to process ENA assemblies deployed on the same date

        Arguments:
            date_string (str): YYYY-MM-DD, date to scan and process
            processing_dir (str): directory to process all seqs for given date
        """

        # generate the accession list via AssemblyScanner,
        # if accession list already exists, skip list re-generation
        accession_list_file = os.path.join(
            processing_dir, "accessions_list.txt")
        if os.path.exists(accession_list_file):
            logging.info("accessions list already exists, skipping accession "
                        + "search")
        else:
            logging.info("generating accessions list from search API scan")
            scanner = AssemblyScanner(date_string)
            scanner.generate_accession_list(accession_list_file)

        # for each accession (line) in the list file, send the accession and url
        # to the process_single_flatfile method
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
            self.prepare_single_flatfile_commands(processing_dir, accession,
                                                  url)
    
    def prepare_single_flatfile_commands(self, processing_dir, accession, url):
        """Get commands to process and generate manifest for single flatfile

        Individual components of processing ENA assemblies and uploading to the 
        destination in the destination config are split into 2 components:
        1. process via ena-refget-processor
        2. generate the file manifest
        The jobs generated from both components are returned and passed to the
        runtime layer

        Arguments:
            processing_dir (str): directory to write processed files
            accession (str): unique flatfile accession (AssemblyScanner list)
            url (str): FTP url for this flatfile (from AssemblyScanner list)
            config_obj (dict): loaded source json config
            source_config (str): path to source json config
            destination_config (str): path to destination json config
        """

        perl_script = self.ena_refget_processor_script
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

                # create cmd files for components:
                # 1. ena-refget-processor
                # 2. generate manifest from full and loader csv
                # 3. upload
                process_cmdfile = self.write_process_cmd(subdir, cmd_dir,
                    dat_link, url_id)
                manifest_cmdfile = self.write_manifest_cmd(subdir, cmd_dir, 
                    url_id)
                upload_cmdfile = self.write_upload_cmd(cmd_dir, manifest,
                    "ena_assembly.upload", url_id)

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
    
    def process_job(self, subdir, cmddir, dat_file, job_id):
        cmd_template = "{} --store-path {} --file-path {} --process-id {}"
        cmd = cmd_template.format(
            self.ena_refget_processor_script,
            subdir,
            dat_file,
            job_id
        )
        cmdfile = "ena_assembly.process.%s.sh" % job_id
        cmdpath = os.path.join(cmddir, cmdfile)
        open(cmdpath, "w").write(cmd)
        return cmdpath
    
    def manifest_job(self, subdir, cmddir, job_id):
        cmd_template = "refget-loader subcommands ena assembly manifest " \
            + "{} {} {}"
        cmd = cmd_template.format(subdir, job_id, self.config_file)
        cmdfile = "ena_assembly.manifest.%s.sh" % job_id
        cmdpath = os.path.join(cmddir, cmdfile)
        open(cmdpath, "w").write(cmd)
        return cmdpath
