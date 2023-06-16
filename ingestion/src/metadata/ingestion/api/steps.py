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
Abstract definition of each step
"""
import traceback
from abc import ABC, abstractmethod
from typing import Any, Iterable

from metadata.ingestion.api.common import Entity
from metadata.ingestion.api.status import Either, StackTraceError
from metadata.ingestion.api.step import Step, WorkflowFatalError
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()


class InvalidSourceException(Exception):
    """
    The source config is not getting the expected
    service connection
    """


class Source(Step, ABC):
    """
    Abstract source implementation. The workflow will run
    its next_record and pass them to the next step.
    """

    metadata: OpenMetadata
    connection_obj: Any
    service_connection: Any

    @abstractmethod
    def prepare(self):
        pass

    @abstractmethod
    def test_connection(self) -> None:
        pass

    def run(self) -> Iterable[Either]:
        """
        Run the step and handle the status and exceptions
        """
        try:
            yield from self._run()
        except WorkflowFatalError as err:
            logger.error(f"Fatal error running step [{self}]: [{err}]")
            raise err
        except Exception as exc:
            error = f"Encountered exception running step [{self}]: [{exc}]"
            logger.warning(error)
            self.status.failed(
                StackTraceError(
                    name="unknown", error=error, stack_trace=traceback.format_exc()
                )
            )


class Sink(Step, ABC):
    """All Sinks must inherit this base class."""

    @abstractmethod
    def _run(self, record: Entity) -> Either:
        """
        Send the data somewhere, e.g., the OM API
        """


class Stage(Step, ABC):
    """All Stages must inherit this base class."""

    @abstractmethod
    def _run(self, record: Entity) -> Either:
        """
        Store the data in a file to be processed in bulk
        """


class BulkSink(Step, ABC):
    """All Stages must inherit this base class."""

    @abstractmethod
    def _run(self) -> Either:
        """
        Read the configured file and send the data somewhere,
        e.g., the OM API
        """


class Processor(Step, ABC):
    """All Processor must inherit this base class"""

    @abstractmethod
    def _run(self, *args, **kwargs) -> Either:
        """
        Post process a given entity and return it
        or a new one
        """
