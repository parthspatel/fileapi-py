from .__version__ import __version__
from .fileapi import *
from .async_fileapi import *
from .storage_options import *


@contextmanager
def open(path: Union[str, FileAPI], mode: str):
    """
    Open a file with the given path and mode.
    """
    with FileAPI.apply(path=path).create_input_stream(mode=mode) as io_stream:
        yield io_stream

@asynccontextmanager
async def open(path: Union[str, FileAPI], mode: str):
    """
    Open a file with the given path and mode.
    """
    async with AsyncFileAPI.apply(path=path).create_input_stream(mode=mode) as io_stream:
        yield io_stream