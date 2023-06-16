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
Each of the ingestion steps: Source, Sink, Stage,...
"""
import traceback
from abc import ABC, abstractmethod
from typing import Any, Generic, Optional

from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    OpenMetadataConnection,
)
from metadata.ingestion.api.closeable import Closeable
from metadata.ingestion.api.common import Entity
from metadata.ingestion.api.status import StackTraceError, Status
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()


class WorkflowFatalError(Exception):
    """
    To be raised when we need to stop the workflow execution.
    E.g., during a failed Test Connection.

    Anything else will keep the workflow running.
    """


class Step(ABC, Closeable, Generic[Entity]):
    """All Sinks must inherit this base class."""

    status: Status

    def __init__(self):
        self.status = Status()

    @classmethod
    @abstractmethod
    def create(
        cls, config_dict: dict, metadata_config: OpenMetadataConnection
    ) -> "Step":
        pass

    @abstractmethod
    def _run(self, *args, **kwargs) -> Optional[Any]:
        """
        Main entrypoint to execute the step
        """

    def run(self, *args, **kwargs) -> Optional[Any]:
        """
        Run the step and handle the status and exceptions
        """
        try:
            # Either[Entity, StackTraceError]
            result = self._run(*args, **kwargs)

            if isinstance(result, StackTraceError):
                self.status.failed(result)
                return None

            self.status.scanned(result)
            return result
        except WorkflowFatalError as err:
            logger.error(f"Fatal error running step [{self}]: [{err}]")
            raise err
        except Exception as exc:
            error = f"Unhandled exception during workflow processing: [{exc}]"
            logger.warning(error)
            self.status.failed(StackTraceError(name="Unhandled", error=error, stack_trace=traceback.format_exc()))
    def get_status(self) -> Status:
        return self.status

    @abstractmethod
    def close(self) -> None:
        pass
