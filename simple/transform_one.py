from typing import Optional, Tuple

from simple.fetch_notice import get_one
from simple.rdf_transform import do_transform
from simple.read_mapping_suites import read_mapping_suites
from ted_sws.notice_metadata_processor.services.notice_eligibility import check_package
from pathlib import Path
from extract_metadata import extract_metadata
from ted_sws.notice_transformer.adapters.rml_mapper import RMLMapper


def latest_mapping_suite(metadata, mapping_suites):
    eligible_suites = filter(lambda ms: check_package(ms, metadata), mapping_suites)
    return max(eligible_suites, key=lambda ms: ms.version, default=None)


repository_path = Path("/home/cvasquez/github.com/OP-TED/ted-rdf-mapping/mappings")
rml_mapper_path = Path("/home/cvasquez/github.com/OP-TED/ted-rdf-conversion-pipeline/.rmlmapper/rmlmapper.jar")

xml_str = get_one()
metadata = extract_metadata(xml_str)

mapping_suites = read_mapping_suites(repository_path=repository_path)
mapping_suite = latest_mapping_suite(metadata, mapping_suites)

rml_mapper = RMLMapper(rml_mapper_path=rml_mapper_path)
transformed_notice = do_transform(xml_str=xml_str, mapping_suite=mapping_suite, rml_mapper=rml_mapper)

print(transformed_notice)
