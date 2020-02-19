# -*- coding: utf-8 -*-
"""Generate upload manifest from ena-refget-processor results

The ena-refget-processor will produce several loader/log csvs, containing all
information necessary to upload sequences (by primary checksum and secondary
checksums) to the object store. This cli command reformats the 
ena-refget-processor into "manifest" format
"""

import click
import os
from ga4gh.refget.loader.jobset_status import JobsetStatus

@click.command()
@click.argument("processing_dir")
@click.argument("file_id")
@click.argument("config_file")
def manifest(**kwargs):
    "generate an upload manifest from processed ENA flatfile"

    def load_csv(csv_path):
        """Parse csv contents into dictionary

        Arguments:
            csv_path (str): path to csv file that will be parsed

        Returns:
            (dict): csv contents loaded as dictionary 
        """

        row_key = "trunc512"
        csv_dict = {}
        columns = []
        parse_header = True
        for line in open(csv_path, "r"):
            ls = line.rstrip().split(",")
            if parse_header: # get column keys by their position in the header 
                columns = ls
                parse_header = False
            else: # set row according to its trunc512 identifier, also
                # setting attributes by column key
                subdict = {columns[i]: ls[i] for i in range(0, len(ls))}
                csv_dict[subdict[row_key]] = subdict

        return csv_dict

    # directories are set by the ena-refget-processor
    # set directory structure and load the full and loader csvs as dicts 
    processing_dir = kwargs["processing_dir"]
    file_id = kwargs["file_id"]
    logs_dir = os.path.join(processing_dir, "logs")
    full_csv_path = os.path.join(logs_dir, file_id + ".full.csv")
    loader_csv_path = os.path.join(logs_dir, file_id + ".loader.csv")
    status_path = os.path.join(processing_dir, "status.json")
    jobset_status = JobsetStatus(status_path)

    if not jobset_status.is_success() and not jobset_status.is_failure():
        try:

            full_csv_dict = load_csv(full_csv_path)
            loader_csv_dict = load_csv(loader_csv_path)

            # manifest file template, contains paths to source and upload 
            # config, header row, and data rows
            output_manifest_path = os.path.join(logs_dir, 
                file_id + ".manifest.csv")
            output_content_template = \
                "# Refget loader manifest\n" \
                + "# config file: {}\n" \
                + "{}\n" \
                + "{}\n"
            output_header = ["completed", "seq", "metadata", "primary_id",
                "trunc512_id", "md5_id"]
            output_lines = []

            # for each row in the loader csv, use the contents of the loader csv
            # and its matching entry in the full csv to create a manifest entry
            # add manifest entry to the list
            for trunc512 in loader_csv_dict.keys():
                if int(loader_csv_dict[trunc512]["completed"]) != 1:
                    raise Exception(
                        "Incorrectly processed seq(s) in loader table")

                loader_csv_subdict = loader_csv_dict[trunc512]
                full_csv_subdict = full_csv_dict[trunc512]
                output_lines.append("\t".join([
                    loader_csv_subdict["completed"],
                    loader_csv_subdict["seq_path"],
                    loader_csv_subdict["json_path"],
                    full_csv_subdict["ga4gh"],
                    loader_csv_subdict["trunc512"],
                    loader_csv_subdict["md5"],
                ]))

            # the full csv is uploaded as well, add additional config lines in 
            # the manifest for uploading full csv
            output_lines.append("# additional uploads")
            output_lines.append("\t".join(["source", "destination"]))
            output_lines.append("\t".join([
                full_csv_path,
                "metadata/csv/" + file_id + ".full.csv"
            ]))

            # populate manifest template and write
            output_content = output_content_template.format(
                kwargs["config_file"],
                "\t".join(output_header),
                "\n".join(output_lines)
            )
            
            open(output_manifest_path, "w").write(output_content)

        except Exception as e:
            jobset_status.set_status_failure()
            jobset_status.set_message(str(e))
        finally:
            jobset_status.write()
