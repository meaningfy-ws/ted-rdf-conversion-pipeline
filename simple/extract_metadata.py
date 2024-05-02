from tests.conftest import FakeNoticeRepository  # to defeat circular dependency

from ted_sws.notice_metadata_processor.services.metadata_normalizer import normalise_notice, \
    extract_and_normalise_notice_metadata
from ted_sws.core.model.metadata import NormalisedMetadata
from ted_sws.core.model.manifestation import XMLManifestation


def extract_metadata(xml_str: str) -> NormalisedMetadata:
    xml_manifestation = XMLManifestation(object_data=xml_str)
    return extract_and_normalise_notice_metadata(xml_manifestation)
