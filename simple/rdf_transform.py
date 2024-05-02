from pathlib import Path

from ted_sws.core.model.transform import MappingSuite
from ted_sws.data_manager.adapters.mapping_suite_repository import MappingSuiteRepositoryInFileSystem
from ted_sws.notice_transformer.adapters.rml_mapper import RMLMapperABC
import tempfile
from line_profiler_pycharm import profile


@profile
def do_transform(xml_str: str, mapping_suite: MappingSuite, rml_mapper: RMLMapperABC):
    with tempfile.TemporaryDirectory() as temp_dir:
        package_path = Path(temp_dir) / mapping_suite.identifier

        # Writes the mapping suite in the tmp directory.
        # profile -> ~4% of the time
        mapping_suite_repository = MappingSuiteRepositoryInFileSystem(repository_path=package_path.parent)
        mapping_suite_repository.add(mapping_suite=mapping_suite)

        data_source_path = package_path / "data"
        data_source_path.mkdir(parents=True, exist_ok=True)
        notice_path = data_source_path / "source.xml"
        with notice_path.open("w", encoding="utf-8") as file:
            file.write(xml_str)
        result = rml_mapper.execute(package_path=package_path)
        return result
