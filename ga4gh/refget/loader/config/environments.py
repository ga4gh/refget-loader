# -*- coding: utf-8 -*-
"""Maps 'environment' keywords to matching Environment subclass

Attributes:
    ENVIRONMENTS (dict): maps 'environment' kewords to Environment subclass
"""

from ga4gh.refget.loader.environments.ebi_environment import EBIEnvironment
from ga4gh.refget.loader.environments.local_environment import LocalEnvironment

ENVIRONMENTS = {
    "local": LocalEnvironment,
    "EBI": EBIEnvironment
}
