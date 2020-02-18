# -*- coding: utf-8 -*-
"""Dictionary of all source processing classes

Attributes:
    SOURCES (dict): contains all source processing classes. The key for each 
        class is the "type" keyword, as it would be found under the "type"
        property for both source and destination json configs
"""

from ga4gh.refget.loader.sources.ena.assembly.ena_assembly_processor import \
    ENAAssemblyProcessor

SOURCES = {
    "ena_assembly": ENAAssemblyProcessor
}
