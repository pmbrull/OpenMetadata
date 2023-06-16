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
Base workflow definition.

To be extended by any other workflow:
- ingestion
- lineage
- usage
- profiler
- test suite
- data insights
"""
import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, Tuple, TypeVar

from metadata.config.common import WorkflowExecutionError
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    OpenMetadataConnection,
)
from metadata.generated.schema.entity.services.ingestionPipelines.ingestionPipeline import (
    PipelineState,
    PipelineStatus,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    OpenMetadataWorkflowConfig,
)
from metadata.ingestion.api.parser import parse_workflow_config_gracefully
from metadata.ingestion.api.step import Step
from metadata.ingestion.api.steps import BulkSink, Processor, Sink, Source, Stage
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.timer.repeated_timer import RepeatedTimer
from metadata.timer.workflow_reporter import get_ingestion_status_timer
from metadata.utils.logger import ingestion_logger, set_loggers_level

logger = ingestion_logger()

T = TypeVar("T")

SUCCESS_THRESHOLD_VALUE = 90
REPORTS_INTERVAL_SECONDS = 60


class BaseWorkflow(ABC):
    """
    Base workflow implementation
    """

    config: OpenMetadataWorkflowConfig
    _run_id: Optional[str] = None
    metadata_config: OpenMetadataConnection
    metadata: OpenMetadata

    # All workflows require a source as a first step
    source: Source
    # All workflows execute a series of steps, aside from the source
    steps: Tuple[Step]

    def __init__(
        self, config: OpenMetadataWorkflowConfig
    ):  # pylint: disable=too-many-locals
        """
        Disabling pylint to wait for workflow reimplementation as a topology
        """
        self.config = config
        self._timer: Optional[RepeatedTimer] = None

        set_loggers_level(config.workflowConfig.loggerLevel.value)

        self.metadata_config: OpenMetadataConnection = (
            self.config.workflowConfig.openMetadataServerConfig
        )
        self.metadata = OpenMetadata(config=self.metadata_config)

        self.set_ingestion_pipeline_status(state=PipelineState.running)

        # Informs the `source` and the rest of `steps` to execute
        self.set_steps()

    @abstractmethod
    def set_steps(self):
        """
        initialize the tuple of steps to run for each workflow
        and the source
        """

    def _execute_internal(self):
        """
        Internal execution that needs to be filled
        by each ingestion workflow.

        Pass each record from the source down the pipeline:
        Source -> (Processor) -> Sink
        or Source -> (Processor) -> Stage -> BulkSink
        """
        for record in self.source.run():
            self.source.status.scanned(record)
            processed_record = record
            for step in self.steps:
                # We only process the records for these Step types
                if processed_record is not None and isinstance(step, (Stage, Processor, Sink)):
                    processed_record = step.run(processed_record)

        bulk_sink = next(
            (step for step in self.steps if isinstance(step, BulkSink)), None
        )
        if bulk_sink:
            bulk_sink.run()

    def execute(self) -> None:
        """
        Main entrypoint
        """
        self.timer.trigger()
        try:
            self._execute_internal()

            # If we reach this point, compute the success % and update the associated Ingestion Pipeline status
            self.update_ingestion_status_at_end()

        # Any unhandled exception breaking the workflow should update the status
        except Exception as err:
            self.set_ingestion_pipeline_status(PipelineState.failed)
            raise err

        # Force resource closing. Required for killing the threading
        finally:
            self.stop()

    def stop(self) -> None:
        """
        Main stopping logic
        """
        for step in self.steps:
            step.close()

        self.metadata.close()
        self.timer.stop()

    @property
    def timer(self) -> RepeatedTimer:
        """
        Status timer: It will print the source & sink status every `interval` seconds.
        """
        if not self._timer:
            self._timer = get_ingestion_status_timer(
                interval=REPORTS_INTERVAL_SECONDS, logger=logger, workflow=self
            )

        return self._timer

    @classmethod
    def create(cls, config_dict: dict) -> "Workflow":
        config = parse_workflow_config_gracefully(config_dict)
        return cls(config)

    def _get_source_success(self):
        return self.source.get_status().calculate_success()

    def update_ingestion_status_at_end(self):
        """
        Once the execute method is done, update the status
        as OK or KO depending on the success rate.
        """
        pipeline_state = PipelineState.success
        if SUCCESS_THRESHOLD_VALUE <= self._get_source_success() < 100:
            pipeline_state = PipelineState.partialSuccess
        self.set_ingestion_pipeline_status(pipeline_state)

    @property
    def run_id(self) -> str:
        """
        If the config does not have an informed run id, we'll
        generate and assign one here.
        """
        if not self._run_id:
            if self.config.pipelineRunId:
                self._run_id = str(self.config.pipelineRunId.__root__)
            else:
                self._run_id = str(uuid.uuid4())

        return self._run_id

    def set_ingestion_pipeline_status(
        self,
        state: PipelineState,
    ) -> None:
        """
        Method to set the pipeline status of current ingestion pipeline
        """

        # if we don't have a related Ingestion Pipeline FQN, no status is set.
        if self.config.ingestionPipelineFQN:
            if state in (PipelineState.queued, PipelineState.running):
                pipeline_status = PipelineStatus(
                    runId=self.run_id,
                    pipelineState=state,
                    startDate=datetime.now().timestamp() * 1000,
                    timestamp=datetime.now().timestamp() * 1000,
                )
            else:
                pipeline_status = self.metadata.get_pipeline_status(
                    self.config.ingestionPipelineFQN, self.run_id
                )
                # if workflow is ended then update the end date in status
                pipeline_status.endDate = datetime.now().timestamp() * 1000
                pipeline_status.pipelineState = state

            self.metadata.create_or_update_pipeline_status(
                self.config.ingestionPipelineFQN, pipeline_status
            )

    def raise_from_status(self, raise_warnings=False):
        """
        Method to raise error if failed execution
        and updating Ingestion Pipeline Status
        """
        try:
            self._raise_from_status_internal(raise_warnings)
        except WorkflowExecutionError as err:
            self.set_ingestion_pipeline_status(PipelineState.failed)
            raise err

    def _raise_from_status_internal(self, raise_warnings=False):
        """
        Check the status of all steps
        """
        if (
            self.source.get_status().failures
            and self._get_source_success() < SUCCESS_THRESHOLD_VALUE
        ):
            raise WorkflowExecutionError(
                "Source reported errors", self.source.get_status()
            )

        for step in self.steps:
            if step.status.failures:
                raise WorkflowExecutionError(
                    f"{step.__class__.__name__} reported errors", step.get_status()
                )
            if raise_warnings and step.status.warnings:
                raise WorkflowExecutionError(
                    f"{step.__class__.__name__} reported warnings", step.get_status()
                )

    def result_status(self) -> int:
        """
        Returns 1 if source status is failed, 0 otherwise.
        """
        if self.source.get_status().failures:
            return 1
        return 0
