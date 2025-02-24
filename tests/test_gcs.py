import logging
import os

from tests.file_system_tester import FileSystemTester
import dotenv
import pytest

LOGGER = logging.getLogger(__name__)
dotenv.load_dotenv()


def test_gcs():
    logging.basicConfig(level=logging.INFO)

    root = os.getenv("GCS_ROOT")
    if not root:
        return pytest.skip("GCS_ROOT environment variable not set.")

    tester = FileSystemTester.apply(root)
    tester.test()
    if len(tester.errors) != 0:
        for error in tester.errors:
            LOGGER.error(error)
        raise Exception("GCS test failed.")
    else:
        LOGGER.info("GCS test completed successfully.")
