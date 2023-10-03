#  Copyright 2021 Collate
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""
.lkml files parser
"""
from pathlib import Path
from typing import Dict, List, Optional

import lkml
from metadata.ingestion.source.dashboard.looker.models import (
    Includes,
    LkmlFile,
    LookMlView,
    ViewName,
)
from metadata.readers.file.base import ReadException
from metadata.utils.logger import ingestion_logger


from ingestion.src.metadata.readers.file.api_reader import ApiReader
from ingestion.src.metadata.readers.file.local import LocalReader

logger = ingestion_logger()

EXTENSIONS = (".lkml", ".lookml")
IMPORTED_PROJECTS_DIR = "imported_projects"


class SingletonMeta(type):
    """
    The Singleton class can be implemented in different ways in Python. Some
    possible methods include: base class, decorator, metaclass. We will use the
    metaclass because it is best suited for this purpose.
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        """
        Possible changes to the value of the `__init__` argument do not affect
        the returned instance.
        """
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class BulkLkmlParser(metaclass=SingletonMeta):
    """
    Parses and caches the visited files & views.

    Here we'll just parse VIEWs, as we are already getting
    the explores from the API.

    The goal is to make sure we parse files only once
    and store the results of the processed views.

    We want to parse the minimum number of files each time
    until we find the view we are looking for.

    Approach:
    When we parse, we parse all files *.view.lkml to get all view and cached them. It can speed up the process and avoid
    infinity loop when parsing includes.
    """

    def __init__(self, reader: LocalReader):
        self._views_cache: Dict[ViewName, LookMlView] = {}
        self._visited_files: Dict[Includes, List[Includes]] = {}

        # To store the raw string of the lkml explores
        self.parsed_files: Dict[Includes, str] = {}
        self.parsed_view: Dict[str, List[Includes]] = {}

        self.reader = reader
        self.__parse_all_views()

    def __parse_all_views(self):
        file_paths = self.reader.get_local_files(search_key=".view.lkml")
        for _path in file_paths:
            file = self._read_file(Includes(_path))
            lkml_file = LkmlFile.parse_obj(lkml.load(file))
            self.parsed_files[Includes(_path)] = file
            for view in lkml_file.views:
                view.source_file = _path
                self._views_cache[view.name] = view

    def _read_file(self, path: Includes) -> str:
        """
        Read the LookML file
        """
        suffixes = Path(path).suffixes

        # Check if any suffix is in our extension list
        if not set(suffixes).intersection(set(EXTENSIONS)):
            for suffix in EXTENSIONS:
                try:
                    return self.reader.read(path + suffix)
                except ReadException as err:
                    logger.debug(f"Error trying to read the file [{path}]: {err}")

        else:
            return self.reader.read(path)

        raise ReadException(f"Error trying to read the file [{path}]")

    def get_view_from_cache(self, view_name: ViewName) -> Optional[LookMlView]:
        """
        Check if view is cached, and return it.
        Otherwise, return None
        """
        if view_name in self._views_cache:
            return self._views_cache[view_name]

        return None

    def find_view(self, view_name: ViewName, path: Includes) -> Optional[LookMlView]:
        """
        Parse an incoming file (either from a `source_file` or an `include`),
        cache the views and return the list of includes to parse if
        we still don't find the view afterwards
        """
        cached_view = self.get_view_from_cache(view_name)
        if cached_view:
            return cached_view

        logger.warning(f"BulkLkmlParser::find_view: can't find view {view_name}")
        return None

    def __repr__(self):
        """
        Customize string repr for logs
        """
        if isinstance(self.reader, ApiReader):
            return (
                f"Parser at [{self.reader.credentials.repositoryOwner.__root__}/"
                f"{self.reader.credentials.repositoryName.__root__}]"
            )
        else:
            return f"Parser at [{self.reader}]"
