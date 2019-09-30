import datetime
import os
import re
import requests
from xml.etree import ElementTree
from urllib.parse import urlencode, quote

class AssemblyScanner(object):
    
    def __init__(self, date_string):
        self.date_string = date_string
        self.url = "https://www.ebi.ac.uk/ena/data/warehouse/search"
        self.query_template = "last_updated>={current_date} AND " \
                              + "last_updated<{next_date}"
        self.query = self.__initialize_query()
        self.chunk_size = 8192
    
    def get_params(self):
        return {
            "result": "assembly",
            "query": self.query,
            "fields": "accession",
            "display": "xml"
        }
    
    def get_headers(self):
        return {
            "Content-Type": "application/x-www-form-urlencoded"
        }

    def make_request(self):
        return requests.post(
            self.url, 
            data=self.get_params(),
            headers=self.get_headers(),
            stream=True
        )
    
    def assemblies_xml_generator(self):
        response = self.make_request()
        aggregate_chunk_string = ""
        for chunk in response.iter_content(chunk_size=self.chunk_size):
            chunk_string = chunk.decode()
            aggregate_chunk_string += chunk_string

            assembly_pattern = re.compile("<ASSEMBLY.+?</ASSEMBLY>", re.DOTALL)
            assembly_iter = assembly_pattern.finditer(aggregate_chunk_string)

            final_end_position = 0
            for assembly_match in assembly_iter:
                start = assembly_match.start()
                end = assembly_match.end()
                assembly_xml = assembly_match.string[start:end]
                final_end_position = end
                yield assembly_xml

            aggregate_chunk_string = aggregate_chunk_string[final_end_position:]
    
    def generate_accession_list(self, file_path):

        if os.path.exists(file_path):
            os.remove(file_path)

        output_file = open(file_path, "a")
        header = "\t".join(
            ["Accession", "URL"])+"\n"
        output_file.write(header)

        for assembly_xml in self.assemblies_xml_generator():
            accession = None
            url = None
            status = "NotAttempted"
            message = "N/A"
            last_modified = datetime.datetime.now().strftime(
                "%Y-%m-%dT%H:%M:%S")

            root = ElementTree.fromstring(assembly_xml)
            accession = re.compile(
                'accession=\"(.+?)\"').search(assembly_xml).group(1)
            for assembly_links in root.iter("ASSEMBLY_LINKS"):
                for assembly_link in assembly_links.iter("ASSEMBLY_LINK"):
                    for url_link in assembly_link.iter("URL_LINK"):
                        label = url_link.find("LABEL").text
                        if label == "WGS_SET_FLATFILE":
                            url = url_link.find("URL").text
            
            if accession and url:
                output_line = "\t".join(
                    [accession, url]
                ) + "\n"
                output_file.write(output_line)
        output_file.close()
    
    def __initialize_query(self):
        year, month, day = self.date_string.split("-")
        date = datetime.date(*[int(a) for a in [year, month, day]])
        next_date = date + datetime.timedelta(days=1)
        next_date_string = next_date.strftime("%Y-%m-%d")
        format_d = {
            "current_date": self.date_string,
            "next_date": next_date_string
        }
        return self.query_template.format(**format_d)