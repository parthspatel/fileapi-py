import asyncio
import inspect
import os
from functools import wraps
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Any, Union, Optional, List, Callable, Dict, Type

import fsspec
import fsspec.asyn
from io import TextIOWrapper

from . import storage_options as lib_storage_options
from .fileapi import FileAPI, _get_fs_and_path


class AsyncFileAPI(FileAPI):
    """
    An asynchronous version of FileAPI that provides non-blocking I/O operations.
    """

    def __init__(
            self,
            path: str,
            *,
            fs: Optional[fsspec.AbstractFileSystem] = None,
            resolved_path: Optional[str] = None,
            storage_options: Optional[lib_storage_options.StorageOptions] = None,
    ):
        """
        Initialize an AsyncFileAPI object. Similar to FileAPI, but ensures the filesystem
        is configured for asynchronous operation if possible.
        """
        super().__init__(path, fs=fs, resolved_path=resolved_path, storage_options=storage_options)
        # Ensure the filesystem is async-compatible if possible
        if hasattr(self.fs, 'async_impl') and self.fs.async_impl is False:
            # Try to get async version of filesystem if available
            try:
                async_fs = fsspec.filesystem(
                    self.fs.protocol, asynchronous=True,
                    **(storage_options or {})
                )
                self.fs = async_fs
            except Exception:
                pass  # Fall back to synchronous version if async not available

    @classmethod
    async def apply(cls, path: Union[str, FileAPI], storage_options: Optional[Dict] = None) -> "AsyncFileAPI":
        """
        Asynchronous version of apply method to create an AsyncFileAPI object.
        """
        if isinstance(path, FileAPI):
            resolved_storage_options = {**path.storage_options, **(storage_options or {})}
            return cls(
                path=path.path, fs=path.fs, resolved_path=path.resolved_path,
                storage_options=resolved_storage_options
            )
        else:
            resolved_storage_options = lib_storage_options.default() if storage_options is None else storage_options
            return cls(path=path, fs=None, resolved_path=None, storage_options=resolved_storage_options)

    @asynccontextmanager
    async def create_output_stream(self, mode="wb") -> Union[TextIOWrapper, fsspec.spec.AbstractBufferedFile]:
        """
        Asynchronously create an output stream for writing to the file.
        """
        if hasattr(self.fs, 'open_async'):
            async with self.fs.open_async(self.resolved_path, mode) as f:
                yield f
        else:
            # Fallback to synchronous version in a thread
            with super().create_output_stream(mode) as f:
                yield f

    @asynccontextmanager
    async def create_input_stream(self, mode="rb") -> Union[TextIOWrapper, fsspec.spec.AbstractBufferedFile]:
        """
        Asynchronously create an input stream for reading from the file.
        """
        if hasattr(self.fs, 'open_async'):
            async with self.fs.open_async(self.resolved_path, mode) as f:
                yield f
        else:
            # Fallback to synchronous version in a thread
            with super().create_input_stream(mode) as f:
                yield f

    async def read(self, codec: str = "utf-8") -> str:
        """
        Asynchronously read the file and return the content as a string.
        """
        async with self.create_input_stream() as stream:
            if hasattr(stream, 'read_async'):
                content = await stream.read_async()
            else:
                # Run in thread pool if not async-native
                loop = asyncio.get_event_loop()
                content = await loop.run_in_executor(None, stream.read)
            return content.decode(codec)

    async def write(self, string: str) -> None:
        """
        Asynchronously write a string to the file.
        """
        async with self.create_output_stream() as f:
            if hasattr(f, 'write_async'):
                await f.write_async(string.encode())
            else:
                # Run in thread pool if not async-native
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, lambda _: f.write(string.encode()))

    async def create_input_stream_lines(self, codec: str = "utf-8") -> AsyncGenerator[str, None]:
        """
        Asynchronously create an input stream for reading lines from the file.
        """
        async with self.create_input_stream() as source:
            if hasattr(source, 'readlines_async'):
                async for line in source.readlines_async():
                    yield line.decode(codec)
            else:
                # Fallback to reading line by line in thread executor
                for line in source:
                    yield line.decode(codec)

    async def delete(self) -> bool:
        """
        Asynchronously delete the file.
        """
        try:
            if hasattr(self.fs, 'delete_async'):
                await self.fs.delete_async(self.resolved_path, recursive=True)
            else:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    None, lambda _: self.fs.delete(self.resolved_path, recursive=True)
                )
            return True
        except Exception as e:
            self.__logger__.error(f"Failed to delete file {self} due to exception {e}")
            return False

    async def list_generator(self) -> AsyncGenerator["AsyncFileAPI", None]:
        """
        Asynchronously list the files in the directory.
        """
        path = self.fs._strip_protocol(self.resolved_path)

        if hasattr(self.fs, 'ls_async'):
            paths = await self.fs.ls_async(path, detail=False)
        else:
            loop = asyncio.get_event_loop()
            paths = await loop.run_in_executor(None, lambda _: self.fs.ls(path, detail=False))

        for full_path in paths:
            self.fs.unstrip_protocol(full_path)
            yield AsyncFileAPI(
                full_path,
                fs=self.fs,
                resolved_path=full_path,
                storage_options=self.storage_options,
            )

    async def list(self) -> List["AsyncFileAPI"]:
        """
        Asynchronously list the files in the directory and return as a list.
        """
        result = []
        async for item in self.list_generator():
            result.append(item)
        return result

    async def exists(self) -> bool:
        """
        Asynchronously check if the file exists.
        """
        if hasattr(self.fs, 'exists_async'):
            return await self.fs.exists_async(self.resolved_path)
        else:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, lambda _: self.fs.exists(self.resolved_path))

    async def exist(self) -> bool:
        """
        Asynchronously check if the file exists (alias for exists).
        """
        return await self.exists()

    async def is_directory(self) -> bool:
        """
        Asynchronously check if the path is a directory.
        """
        if hasattr(self.fs, 'isdir_async'):
            return await self.fs.isdir_async(self.resolved_path)
        else:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, lambda _: self.fs.isdir(self.resolved_path))

    async def is_file(self) -> bool:
        """
        Asynchronously check if the path is a file.
        """
        if hasattr(self.fs, 'isfile_async'):
            return await self.fs.isfile_async(self.resolved_path)
        else:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, lambda _: self.fs.isfile(self.resolved_path))

    async def copy_to(self, dest: Union["AsyncFileAPI", str]) -> bool:
        """
        Asynchronously copy the file to another location.
        """
        errs = []
        dest = await AsyncFileAPI.apply(dest, self.storage_options) if not isinstance(dest, AsyncFileAPI) else dest

        if await self.is_directory():
            listing = await self.list_children()
            for child in listing:
                rel_name = child.path_string[child.path_string.index(self.path_string) + len(self.path_string):]
                rel_name = rel_name.lstrip("/")
                dest_child = dest / rel_name

                self.__logger__.debug(
                    f"Copying file from {{src = {child}, rel = {rel_name}, dest = {dest}, final = {dest_child}}}")

                try:
                    async with child.create_input_stream() as src_stream:
                        async with dest_child.create_output_stream() as dest_stream:
                            if hasattr(src_stream, 'read_async') and hasattr(dest_stream, 'write_async'):
                                content = await src_stream.read_async()
                                await dest_stream.write_async(content)
                            else:
                                # Fallback to synchronous in thread
                                content = src_stream.read()
                                dest_stream.write(content)
                except Exception as e:
                    errs.append(e)
        else:
            try:
                async with self.create_input_stream() as src_stream:
                    async with dest.create_output_stream() as dest_stream:
                        if hasattr(src_stream, 'read_async') and hasattr(dest_stream, 'write_async'):
                            content = await src_stream.read_async()
                            await dest_stream.write_async(content)
                        else:
                            # Fallback to synchronous in thread
                            content = src_stream.read()
                            dest_stream.write(content)
            except Exception as e:
                errs.append(e)

        if len(errs) > 0:
            for e in errs:
                self.__logger__.error(f"Failed to copy file due to exception {e}")
            raise RuntimeError(
                f"Failed to copy file to {dest} due to {len(errs)} exception{'s' if len(errs) > 1 else ''}")

        return True

    async def move_to(self, dest: "AsyncFileAPI") -> bool:
        """
        Asynchronously move the file to another location.
        """
        if await self.copy_to(dest):
            return await self.delete()
        return False

    # Add more async methods as needed...

    async def list_children(
            self, maxdepth=None, storage_options: Optional[lib_storage_options.StorageOptions] = None
    ) -> List["AsyncFileAPI"]:
        """
        Asynchronously list all children files recursively.
        """
        result = []
        async for item in self.list_children_generator(maxdepth, storage_options):
            result.append(item)
        return result

    async def list_children_generator(
            self, maxdepth=None, storage_options: Optional[lib_storage_options.StorageOptions] = None
    ) -> AsyncGenerator["AsyncFileAPI", None]:
        """
        Asynchronously generate all children files recursively.
        """
        path = self.fs._strip_protocol(self.resolved_path)

        if await self.is_directory():
            resolved_storage_options = (
                {**self.storage_options, **storage_options} if storage_options else self.storage_options
            )

            # Handle walk operation - this is complex to do asynchronously
            if hasattr(self.fs, 'walk_async'):
                async for parent, dirs, files in self.fs.walk_async(path, maxdepth=maxdepth, detail=False):
                    for f in files:
                        full_path: str = str(os.path.join(parent, f))
                        full_qualified_path: str = self.fs.unstrip_protocol(full_path)
                        yield AsyncFileAPI(
                            full_qualified_path,
                            fs=self.fs,
                            resolved_path=full_path,
                            storage_options=resolved_storage_options,
                        )
            else:
                # Fallback to synchronous walk in thread executor
                loop = asyncio.get_event_loop()
                walk_results = await loop.run_in_executor(
                    None, lambda _: list(self.fs.walk(path, maxdepth=maxdepth, detail=False))
                )

                for parent, dirs, files in walk_results:
                    for f in files:
                        full_path: str = str(os.path.join(parent, f))
                        full_qualified_path: str = self.fs.unstrip_protocol(full_path)
                        yield AsyncFileAPI(
                            full_qualified_path,
                            fs=self.fs,
                            resolved_path=full_path,
                            storage_options=resolved_storage_options,
                        )
        else:
            # if not a directory, return the file itself
            yield self