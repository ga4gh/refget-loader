from ga4gh.refget.ena.utils.assembly_scanner import AssemblyScanner

def process(date_string, processing_dir):
    """process all seqs that were deployed on ena on the same date"""

    scanner = AssemblyScanner(date_string)
    scanner.process_all()
