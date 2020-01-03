# -*- coding: utf-8 -*-
"""Defines AssemblyScanner class, uses ENA search API to find assemblies"""

import datetime
import logging
import os
import re
import requests
from urllib.parse import urlencode, quote
from xml.etree import ElementTree

class AssemblyScanner(object):
    """Builds list of all assemblies deployed to ENA on a specific date

    AssemblyScanner uses the ENA search API to search all assemblies made
    available on ENA on a specific date. The returned assembly XML is parsed 
    for all accessions and ftp links, which are used to build a master list of 
    accessions by date. The accessions list can be used as input to the 
    ena-refget-processor.

    :param date_string: YYYY-MM-DD date string used to define search window
    :type date_string: str
    :param url: base url to ENA assembly search API
    :type url: str
    :param query_template: url query string template
    :type query_template: str
    :param query: mature query string, modified from template with current date
    :type query: str
    :param chunk_size: size (bytes) of each chunk in response stream
    :type chunk_size: int
    """
    
    def __init__(self, date_string):
        """Constructor method"""

        self.date_string = date_string
        self.url = "https://www.ebi.ac.uk/ena/data/warehouse/search"
        self.query_template = "last_updated>={current_date} AND " \
                              + "last_updated<{next_date}"
        self.query = self.__initialize_query()
        self.chunk_size = 8192
    
    def get_params(self):
        """Get all data parameters for the search request

        :return: url parameters
        :rtype: dict[str, str]
        """

        return {
            "result": "assembly",
            "query": self.query,
            "fields": "accession",
            "display": "xml"
        }
    
    def get_headers(self):
        """Get all headers for the search request

        :return: HTTP headers
        :rtype: dict[str, str]
        """

        return {
            "Content-Type": "application/x-www-form-urlencoded"
        }

    def make_request(self):
        """Execute request to the ENA search API

        :return: HTTP response from search request
        :rtype: class:`requests.models.Response`
        """

        return requests.post(
            self.url, 
            data=self.get_params(),
            headers=self.get_headers(),
            stream=True
        )
    
    def assemblies_xml_generator(self):
        """Generator function, yields the assembly xml of a single assembly

        This generator function makes a request to the ENA search API, and
        streams the response body. As the body is streamed, each chunk is 
        parsed for the presence of any complete assembly xml, thereby creating
        an iterator, in which each element is an xml string for a single 
        assembly.

        Yields:
            (str): xml for a single assembly 
        """

        # make search API request, and iterate over response body by chunk size
        response = self.make_request()
        aggregate_chunk_string = ""
        for chunk in response.iter_content(chunk_size=self.chunk_size):

            # add the current chunk to any unused sections of the previous
            # chunk
            chunk_string = chunk.decode()
            aggregate_chunk_string += chunk_string

            # find all assembly start/end tags by regex
            assembly_pattern = re.compile("<ASSEMBLY.+?</ASSEMBLY>", re.DOTALL)
            assembly_iter = assembly_pattern.finditer(aggregate_chunk_string)

            # yield each detected assembly
            final_end_position = 0
            for assembly_match in assembly_iter:
                start = assembly_match.start()
                end = assembly_match.end()
                assembly_xml = assembly_match.string[start:end]
                final_end_position = end
                yield assembly_xml

            # remove XML that has already been parsed and yielded from the 
            # aggregated chunk string to prevent it from continuously growing,
            # and to prevent assemblies from being yielded multiple times
            aggregate_chunk_string = aggregate_chunk_string[final_end_position:]
    
    def generate_accession_list(self, file_path):
        """Writes assembly accessions to list file

        :param file_path: path to write output file
        :type file_path: str
        """

        # open the new file and write list header
        if os.path.exists(file_path):
            os.remove(file_path)
        output_file = open(file_path, "a")
        header = "\t".join(
            ["Accession", "URL"])+"\n"
        output_file.write(header)

        # runs the generator
        for assembly_xml in self.assemblies_xml_generator():
            accession = None
            url = None
            status = "NotAttempted"
            message = "N/A"
            last_modified = datetime.datetime.now().strftime(
                "%Y-%m-%dT%H:%M:%S")

            # for each assembly, load the XML as a tree, get the accession id,
            # as well as the flatfile ftp url
            # add these as a new line in the list
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
        """Format the query string template with date string of interest

        :return: mature query string, will search assemblies for specified date
        :rtype: str
        """

        # get the specified date, as well as the next date
        # the query string will include the interval that is:
        # >= specified date, AND
        # < next date
        year, month, day = self.date_string.split("-")
        date = datetime.date(*[int(a) for a in [year, month, day]])
        next_date = date + datetime.timedelta(days=1)
        next_date_string = next_date.strftime("%Y-%m-%d")
        format_d = {
            "current_date": self.date_string,
            "next_date": next_date_string
        }
        return self.query_template.format(**format_d)
