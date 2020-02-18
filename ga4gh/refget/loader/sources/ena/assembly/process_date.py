# -*- coding: utf-8 -*-
"""Process ENA assemblies associated with a single date into refget format"""

import logging
import os
from ga4gh.refget.loader.sources.ena.assembly.utils.assembly_scanner \
    import AssemblyScanner
from ga4gh.refget.loader.sources.ena.assembly.process_flatfile \
    import process_flatfile

def process_date(date_string, processing_dir, config_obj, source_config,
    destination_config):
    """Process all ENA assemblies deployed on ena on the same date

    Arguments:
        date_string (str): YYYY-MM-DD, date to scan and process
        processing_dir (str): directory to process all seqs for given date
        config_obj (dict): loaded object from source json config
        source_config (str): path to source json config
        destination_config (str): path to destination json config
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
        process_flatfile(processing_dir, accession, url, config_obj, 
            source_config, destination_config)
