import json
import os
from ga4gh.refget.loader.jobset_status import JobsetStatus

class Uploader(object):

    def __init__(self, manifest):
        self.manifest = manifest
        self.jobset_status = None
        self.config_file = None
        self.config = None
        self.dest_config = None
        self.seq_table = None
        self.additional_table = None
        self.__parse_manifest()
        self.__set_jobset_status()
    
    def __set_jobset_status(self):

        fp = os.path.join(os.path.dirname(self.manifest), "status.json")
        self.jobset_status = JobsetStatus(fp)
    
    def __parse_manifest(self):
        """load components of the manifest file

        Arguments:
            manifest_path (str): path to manifest file
        
        Returns:
            (list): loaded components of the manifest file
                elements of the return list:
                    config (str): path to config file
                    seq_table (list): all data rows in the manifest table
                    additional_table (list): additional upload rows 
        """

        # open manifest file, parse source_config and destination config
        manifest_fh = open(self.manifest, "r")
        manifest_fh.readline()
        config_file = manifest_fh.readline().split(":")[1].strip()
        config_fh = open(config_file, "r")
        config = json.load(config_fh)
        dest_config = config["destination"]

        seq_table = []
        additional_table = []
        add_to_seq_table = True

        # iterate through remaining rows, adding either to the list of seqs
        # or the additional upload table
        for line in manifest_fh.readlines():
            if line.startswith("# additional uploads"):
                add_to_seq_table = False
            else:
                if add_to_seq_table:
                    seq_table.append(line)
                else:
                    additional_table.append(line)

        self.config_file = config_file
        self.config = config
        self.dest_config = dest_config
        self.seq_table = seq_table
        self.additional_table = additional_table

    def upload(self):

        if not self.jobset_status.is_success() \
        and not self.jobset_status.is_failure():

            try:
                exit_codes = []
                exit_codes += self.upload_seq_entries()
                exit_codes += self.upload_additional_entries()
                collapsed = list(set(exit_codes))
                if len(collapsed) == 1 and collapsed[0] == 0:
                    self.jobset_status.set_status_success()
                else:
                    raise Exception("one or more non-zero status codes "
                        + "encountered during upload of manifest sequences")
            except Exception as e:
                self.jobset_status.set_status_failure()
                self.jobset_status.set_message(str(e))
            finally:
                self.jobset_status.write()
    
    def upload_seq_entries(self):
        exit_codes = []
        for line in self.seq_table[1:4]: # exclude header line
            exit_codes += self.upload_seq_entry(line)
        return exit_codes

    def upload_additional_entries(self):
        exit_codes = []
        for line in self.additional_table[1:]: # exclude header line
            exit_codes += self.upload_additional_entry(line)
        return exit_codes
    
    def upload_seq_entry(self, entry):
        pass

    def upload_additional_entry(self, entry):
        pass