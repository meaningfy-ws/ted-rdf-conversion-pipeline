import json
import os
import pathlib
from typing import Iterator, List

from ted_sws import config
from ted_sws.core.model.transform import MappingSuite, FileResource, TransformationRuleSet, SHACLTestSuite, \
    SPARQLTestSuite, MetadataConstraints, TransformationTestData, MappingSuiteType, \
    MetadataConstraintsStandardForm, MetadataConstraintsEform
from ted_sws.data_manager.adapters import inject_date_string_fields, remove_date_string_fields
from ted_sws.data_manager.adapters.repository_abc import MappingSuiteRepositoryABC
from line_profiler_pycharm import profile

# Constants
MS_METADATA_FILE_NAME = "metadata.json"
MS_TRANSFORM_FOLDER_NAME = "transformation"
MS_MAPPINGS_FOLDER_NAME = "mappings"
MS_RESOURCES_FOLDER_NAME = "resources"
MS_VALIDATE_FOLDER_NAME = "validation"
MS_SHACL_FOLDER_NAME = "shacl"
MS_SPARQL_FOLDER_NAME = "sparql"
MS_TEST_DATA_FOLDER_NAME = "test_data"


@profile
def read_file_resources(path: pathlib.Path) -> List[FileResource]:
    if not path.exists():
        return []

    files = [file for file in path.iterdir() if file.is_file()]
    return [
        FileResource(file_name=file.name, file_content=file.read_text(encoding="utf-8"), original_name=file.name)
        for file in files]

@profile
def read_flat_file_resources(path: pathlib.Path, extension=None, with_content=True) -> List[FileResource]:
    file_resources = []
    for root, dirs, files in os.walk(path):
        for f in files:
            file_extension = pathlib.Path(f).suffix
            if extension is not None and file_extension != extension:
                continue
            file_path = pathlib.Path(os.path.join(root, f))
            file_resource = FileResource(file_name=file_path.name,
                                         file_content=file_path.read_text(encoding="utf-8") if with_content else "",
                                         original_name=file_path.name)
            file_resources.append(file_resource)
    return file_resources

@profile
def read_transformation_rule_set(package_path: pathlib.Path) -> TransformationRuleSet:
    mappings_path = package_path / MS_TRANSFORM_FOLDER_NAME / MS_MAPPINGS_FOLDER_NAME
    resources_path = package_path / MS_TRANSFORM_FOLDER_NAME / MS_RESOURCES_FOLDER_NAME
    resources = read_file_resources(resources_path)
    rml_mapping_rules = read_file_resources(mappings_path)
    return TransformationRuleSet(resources=resources, rml_mapping_rules=rml_mapping_rules)

@profile
def read_shacl_test_suites(package_path: pathlib.Path) -> List[SHACLTestSuite]:
    shacl_path = package_path / MS_VALIDATE_FOLDER_NAME / MS_SHACL_FOLDER_NAME
    shacl_test_suite_paths = [x for x in shacl_path.iterdir() if x.is_dir()]
    return [
        SHACLTestSuite(identifier=shacl_test_suite_path.name, shacl_tests=read_file_resources(shacl_test_suite_path))
        for shacl_test_suite_path in shacl_test_suite_paths]

@profile
def read_sparql_test_suites(package_path: pathlib.Path) -> List[SPARQLTestSuite]:
    sparql_path = package_path / MS_VALIDATE_FOLDER_NAME / MS_SPARQL_FOLDER_NAME
    sparql_test_suite_paths = [x for x in sparql_path.iterdir() if x.is_dir()]
    return [SPARQLTestSuite(identifier=sparql_test_suite_path.name,
                            sparql_tests=read_file_resources(sparql_test_suite_path)) for sparql_test_suite_path in
            sparql_test_suite_paths]

@profile
def read_test_data_package(package_path: pathlib.Path) -> TransformationTestData:
    test_data_path = package_path / MS_TEST_DATA_FOLDER_NAME
    test_data = read_flat_file_resources(test_data_path)
    return TransformationTestData(test_data=test_data)

@profile
def read_package_metadata(package_path: pathlib.Path) -> dict:
    metadata_path = package_path / MS_METADATA_FILE_NAME
    if metadata_path.exists():
        with open(metadata_path, 'r', encoding="utf-8") as file:
            return json.load(file)
    return {}

@profile
def check_metadata_keys(metadata):
    required_keys = [
        'metadata_constraints',
        # 'created_at',
        # 'title',
        # 'ontology_version',
        # 'mapping_suite_hash_digest',
        # 'mapping_type',
        # 'version',
        # 'identifier'
    ]
    return all(key in metadata and metadata[key] is not None for key in required_keys)


@profile
def read_mapping_suites(repository_path: pathlib.Path) -> List[MappingSuite]:
    return [
        MappingSuite(
            **metadata,
            transformation_rule_set=read_transformation_rule_set(package_path),
            shacl_test_suites=read_shacl_test_suites(package_path),
            sparql_test_suites=read_sparql_test_suites(package_path),
            transformation_test_data=read_test_data_package(package_path)
        )
        for package_path in repository_path.iterdir() if
        package_path.is_dir() and (package_path / MS_METADATA_FILE_NAME).exists()
        for metadata in [read_package_metadata(package_path)] if metadata and check_metadata_keys(metadata)
    ]

# PACKAGE_PATH = pathlib.Path(__file__).parent.parent.resolve() / "tests" / "test_data"
# result = list(list_mapping_suites(repository_path=PACKAGE_PATH))
# print(len(result))
