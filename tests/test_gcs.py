import logging

from tests.file_system_tester import FileSystemTester

LOGGER = logging.getLogger(__name__)


def test_gcs():
	logging.basicConfig(level=logging.INFO)

	tester = FileSystemTester.apply("gs://ai-team-data/fileapi-py/testing")
	tester.test()
	if len(tester.errors) != 0:
		for error in tester.errors:
			LOGGER.error(error)
		raise Exception("GCS test failed.")
	else:
		LOGGER.info("GCS test completed successfully.")
