import logging
import tempfile
from io import TextIOWrapper

import fsspec
import os
from contextlib import contextmanager
from typing_extensions import Iterator, Optional, List, LiteralString, Generator, Tuple, Callable, Any, Dict

from . import storage_options as lib_storage_options


class FileAPI:
	__logger__ = logging.getLogger(__name__)

	def __init__(self,
			path: str,
			*,
			fs: Optional[fsspec.AbstractFileSystem],
			resolved_path: Optional[LiteralString | str],
			storage_options: lib_storage_options.StorageOptions,
	):

		self.path: str
		self.fs: fsspec.AbstractFileSystem
		self.resolved_path: str
		self.storage_options: lib_storage_options.StorageOptions

		self.path = path
		self.storage_options = storage_options

		if fs is None:
			if resolved_path is None:
				self.fs, self.resolved_path = _get_fs_and_path(path, storage_options=storage_options)
			else:
				self.resolved_path = resolved_path
				self.fs, _ = _get_fs_and_path(path, storage_options=storage_options)
		elif resolved_path is None:
			self.fs = fs
			self.resolved_path = _get_path(fs, path)
		else:
			self.fs = fs
			self.resolved_path = resolved_path
			if "auto_mkdir" in self.storage_options:
				self.fs.auto_mkdir = storage_options["auto_mkdir"]
			if "cache_type" in self.storage_options:
				self.fs.cache_type = storage_options["cache_type"]

	@classmethod
	def apply(cls, path: LiteralString | 'FileAPI' | str, storage_options: Optional[Dict] = None) -> 'FileAPI':
		if isinstance(path, FileAPI):
			resolved_storage_options = {**path.storage_options, **storage_options}
			return cls(path.path, fs=path.fs, resolved_path=path.resolved_path, storage_options=resolved_storage_options)
		else:
			resolved_storage_options = lib_storage_options.default() if storage_options is None else storage_options
			return cls(path, fs=None, resolved_path=None, storage_options=resolved_storage_options)

	def __str__(self):
		return self.path_string

	def is_resolved_path(self) -> bool:
		return os.path.isabs(self.path)

	def mk_dirs(self):
		self.fs.makedirs(self.resolved_path, exist_ok=True)

	def is_relative_path(self) -> bool:
		return not os.path.isabs(self.path)

	def __truediv__(self, child_name: LiteralString) -> 'FileAPI':
		return FileAPI(os.path.join(self.path, child_name), fs=self.fs, resolved_path=None, storage_options=self.storage_options)

	def delete(self) -> bool:
		try:
			self.fs.delete(self.resolved_path, recursive=True)
			return True
		except Exception as e:
			self.__logger__.error(f"Failed to delete file {self} due to exception {e}")
			return False

	def list_children_generator(self, maxdepth=None, storage_options: Optional[lib_storage_options.StorageOptions] = None) -> Generator['FileAPI', None, None]:
		path = self.fs._strip_protocol(self.resolved_path)

		if self.is_directory():
			resolved_storage_options = {**self.storage_options, **storage_options} if storage_options else self.storage_options
			for parent, dirs, files in self.fs.walk(path, maxdepth=maxdepth, detail=False):
				for f in files:
					full_path: str = str(os.path.join(parent, f))
					full_qualified_path: str = self.fs.unstrip_protocol(full_path)
					yield FileAPI(full_qualified_path, fs=self.fs, resolved_path=full_path, storage_options=resolved_storage_options)
		else:
			# if not a directory, return the file itself
			yield self

	def list_children(self, maxdepth=None, storage_options: Optional[lib_storage_options.StorageOptions] = None) -> List['FileAPI']:
		return list(self.list_children_generator(maxdepth, storage_options))

	def find_children(self, prefix: str, recursive: bool = True) -> Iterator['FileAPI']:
		paths = self.fs.glob(os.path.join(self.resolved_path, prefix))
		for path in paths:
			yield FileAPI.apply(path)

	def size(self) -> int:
		return self.fs.size(self.resolved_path)

	def md5(self) -> Optional[str]:
		return self.fs.checksum(self.resolved_path)

	@contextmanager
	def create_output_stream(self, mode="wb") -> TextIOWrapper | fsspec.spec.AbstractBufferedFile:
		with self.fs.open(self.resolved_path, mode) as f:
			yield f

	@contextmanager
	def create_input_stream(self, mode="rb") -> TextIOWrapper | fsspec.spec.AbstractBufferedFile:
		with self.fs.open(self.resolved_path, mode) as f:
			yield f

	@contextmanager
	def create_buffered_source(self):
		with self.create_input_stream() as stream:
			yield stream

	def read(self, codec: str = "utf-8") -> str:
		with self.create_input_stream() as stream:
			return stream.read().decode(codec)

	def create_input_stream_lines(self, codec: str = "utf-8") -> Generator[str, None, None]:
		with self.create_buffered_source() as source:
			for line in source:
				yield line.decode(codec)

	def write_error_stack(self, t: Exception):
		self.write(str(t))

	def write(self, string: str) -> None:
		with self.create_output_stream() as f:
			f.write(string.encode())

	def write_error(self, t: Exception):
		self.write(str(t))

	def with_output(self, f: Callable[[TextIOWrapper | fsspec.spec.AbstractBufferedFile], Any]):
		with self.create_output_stream() as stream:
			return f(stream)

	def with_input(self, f: Callable[[TextIOWrapper | fsspec.spec.AbstractBufferedFile], Any]):
		with self.create_input_stream() as stream:
			return f(stream)

	def exist(self) -> bool:
		return self.fs.exists(self.resolved_path)

	def exists(self) -> bool:
		return self.exist()

	def is_directory(self) -> bool:
		return self.fs.isdir(self.resolved_path)

	def is_file(self) -> bool:
		return self.fs.isfile(self.resolved_path)

	def concat_from(self, source: List['FileAPI']) -> bool:
		with self.create_output_stream() as dest_stream:
			for src in source:
				with src.create_input_stream() as src_stream:
					dest_stream.write(src_stream.read())
		return True

	def relativized(self, base: 'FileAPI') -> LiteralString | bytes | str:
		return os.path.relpath(self.resolved_path, start=base.resolved_path)

	def copy_to(self, dest: 'FileAPI') -> bool:
		with self.create_input_stream() as src_stream:
			with dest.create_output_stream() as dest_stream:
				dest_stream.write(src_stream.read())
		return True

	def move_to(self, dest: 'FileAPI') -> bool:
		if self.copy_to(dest):
			return self.delete()
		return False

	def wc(self) -> (int, int, int):
		content = self.read()
		lines = content.splitlines()
		words = content.split()
		return len(lines), len(words), len(content)

	@property
	def path_string(self) -> str:
		return self.path

	def stage_locally(self) -> 'FileAPI':
		with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
			local_path = tmp_file.name
		local_fs = fsspec.filesystem('file')
		local_file_api = FileAPI(local_path, fs=local_fs, resolved_path=local_path, storage_options=self.storage_options)
		self.copy_to(local_file_api)
		return local_file_api

# @lru_cache(maxsize=None)
def _get_fs_and_path(path: str, *, storage_options: Optional[lib_storage_options.StorageOptions] = None) -> Tuple[fsspec.AbstractFileSystem, LiteralString]:
	fs, _, paths = fsspec.get_fs_token_paths(path, storage_options=storage_options)
	return fs, paths[0]


def _get_path(fs: fsspec.AbstractFileSystem, path: str) -> LiteralString:
	return fs._strip_protocol(path)


def _make_hashable(d: Optional[Dict]) -> Tuple:
	return tuple(sorted(d.items())) if d else tuple()
