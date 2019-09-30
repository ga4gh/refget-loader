import json
import os
from ga4gh.refget.ena.functions.time import timestamp

def process_single_flatfile(processing_dir, accession, url):

    url_basename = os.path.basename(url)
    subdir = os.path.join(processing_dir, "files", url_basename[:2], 
        url_basename.split(".")[0]
    )
    if not os.path.exists(subdir):
        os.makedirs(subdir)

    status_fp = os.path.join(subdir, "status.txt")
    status_dict = {
        "accession": accession,
        "url": url,
        "status": "NotAttempted",
        "message": "None",
        "last_modified": timestamp()
    }

    try:
        symlink_from = url.replace("ftp://ftp.ebi.ac.uk", "/nfs/ftp")
        symlink_to = os.path.join(subdir, url_basename)

        if not os.path.exists(symlink_from):
            raise Exception(
                "input .dat file could not be located at the path: " 
                + symlink_from
            )

        os.symlink(symlink_from, symlink_to)
        
    except Exception as e:
        status_dict["status"] = "Failed"
        status_dict["message"] = str(e)
    finally:
        status_dict["last_modified"] = timestamp()
        open(status_fp, "w").write(
            json.dumps(status_dict, indent=4, sort_keys=True) + "\n")
