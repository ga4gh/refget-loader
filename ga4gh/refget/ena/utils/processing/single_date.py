import os
from ga4gh.refget.ena.utils.assembly_scanner import AssemblyScanner
from ga4gh.refget.ena.utils.processing.single_flatfile \
    import process_single_flatfile

def process_single_date(date_string, processing_dir):
    """process all seqs that were deployed on ena on the same date"""

    accession_list_file = os.path.join(
        processing_dir, "accessions_list.txt")
    if os.path.exists(accession_list_file):
        pass
    else:
        scanner = AssemblyScanner(date_string)
        scanner.generate_accession_list(accession_list_file)

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
        process_single_flatfile(processing_dir, accession, url)
