
from ga4gh.refget.loader.environments.local_environment import LocalEnvironment
from ga4gh.refget.loader.environments.ebi_environment import EBIEnvironment

ENVIRONMENTS = {
    "local": LocalEnvironment,
    "EBI": EBIEnvironment
}