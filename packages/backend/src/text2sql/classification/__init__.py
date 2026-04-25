"""Component 2: Domain classification.

Two distinct sub-components:

  2a — Table-to-domain mapping (build-time, deterministic):
       parse ApiModel.json's `entityDefinitions[].domains[]`,
       inherit empty mappings from aggregate root or descriptor referrers.
       NO LLM in the common case. Output: table_classification.json.

  2b — Query domain classifier (runtime, LLM-driven):
       given a user NL question + the domain catalog, return a ranked list of
       (domain, confidence). Drives the cluster-routing step of the §9 pipeline.
"""

from text2sql.classification.catalog import DomainCatalog, load_domain_catalog
from text2sql.classification.query_classifier import (
    QueryClassification,
    QueryDomainClassifier,
)
from text2sql.classification.table_mapping import (
    TableClassification,
    TableClassificationOutput,
    map_tables,
    read_table_mapping,
    write_table_mapping,
)

__all__ = [
    "DomainCatalog",
    "QueryClassification",
    "QueryDomainClassifier",
    "TableClassification",
    "TableClassificationOutput",
    "load_domain_catalog",
    "map_tables",
    "read_table_mapping",
    "write_table_mapping",
]
