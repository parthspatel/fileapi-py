import logging

from tests.file_system_tester import FileSystemTester

LOGGER = logging.getLogger(__name__)


def test_local():
	logging.basicConfig(level=logging.INFO)

	tester = FileSystemTester.apply("./resources")
	tester.test()
	if len(tester.errors) != 0:
		for error in tester.errors:
			LOGGER.error(error)
		raise Exception("Local test failed.")
	else:
		LOGGER.info("Local test completed successfully.")
