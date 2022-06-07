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
Generic source to build SQL connectors.
"""

from datetime import datetime
from typing import Iterable, Optional, Tuple

from ingestion.build.lib.metadata.generated.schema.type.entityReference import EntityReference
from metadata.generated.schema.api.data.createDatabase import CreateDatabaseRequest
from sqlalchemy.engine import Connection
from sqlalchemy.engine.base import Engine
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import Session

from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    OpenMetadataConnection,
)
from metadata.generated.schema.entity.services.databaseService import DatabaseService
from metadata.generated.schema.metadataIngestion.databaseServiceMetadataPipeline import (
    DatabaseServiceMetadataPipeline,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.ingestion.api.source import SourceStatus
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.source.database.dbt_source import DBTSource
from metadata.ingestion.source.database.sql_column_handler import SqlColumnHandler
from metadata.ingestion.source.database.sqlalchemy_source import SqlAlchemySource, SQLSourceStatus
from metadata.utils.connections import (
    create_and_bind_session,
    get_connection,
    test_connection,
)
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()


class CommonDbSourceService(DBTSource, SqlColumnHandler, SqlAlchemySource):
    def __init__(
        self,
        config: WorkflowSource,
        metadata_config: OpenMetadataConnection,
    ):
        self.config = config
        self.source_config: DatabaseServiceMetadataPipeline = (
            self.config.sourceConfig.config
        )
        # It will be one of the Unions. We don't know the specific type here.
        self.service_connection = self.config.serviceConnection.__root__.config
        self.status = SQLSourceStatus()

        self.metadata_config = metadata_config
        self.metadata = OpenMetadata(metadata_config)

        self.service = self.metadata.get_service_or_create(
            entity=DatabaseService, config=config
        )
        self.engine: Engine = get_connection(self.service_connection)
        self.test_connection()

        self._session = None  # We will instantiate this just if needed
        self._connection = None  # Lazy init as well
        self.data_profiler = None
        self.data_models = {}
        self.table_constraints = None
        self.inspector = None
        self.database_source_state = set()
        self.profile_date = datetime.now()
        super().__init__()

    def prepare(self):
        self._parse_data_model()

    @classmethod
    def create(cls, config_dict: dict, metadata_config: OpenMetadataConnection):
        pass

    def standardize_schema_table_names(
        self, schema: str, table: str
    ) -> Tuple[str, str]:
        return schema, table

    def _get_database_name(self) -> str:
        if hasattr(self.service_connection, "database"):
            return self.service_connection.database_request or "default"
        return "default"

    def get_database_request_and_inspector(self) -> Iterable[Tuple[CreateDatabaseRequest, Inspector]]:
        """
        Default implementation for sources that do not have
        the concept of database.
        :return: Inspector and "default" db
        """
        inspector = inspect(self.engine)
        database_request = CreateDatabaseRequest(
            name=self._get_database_name(),
            service=EntityReference(
                id=self.service.id, type=self.service_connection.type.value
            ),
        )

        yield database_request, inspector

    def get_schemas(self, inspector: Inspector) -> str:
        for schema in inspector.get_schema_names():
            yield schema

    def get_table_description(
        self, schema_name: str, table_name: str, inspector: Inspector
    ) -> str:
        description = None
        try:
            table_info: dict = inspector.get_table_comment(table_name, schema_name)
        # Catch any exception without breaking the ingestion
        except Exception as err:  # pylint: disable=broad-except
            logger.warning(f"Table Description Error : {str(err)}")
        else:
            description = table_info["text"]
        return description

    def is_partition(self, table_name: str, schema_name: str, inspector: Inspector) -> bool:
        self.inspector = inspector
        return False

    def get_table_names(
        self, schema_name: str, inspector: Inspector
    ) -> Optional[Iterable[Tuple[str, str]]]:
        if self.source_config.includeTables:
            for table in inspector.get_table_names(schema_name):
                yield table, "Regular"
        if self.source_config.includeViews:
            for view in inspector.get_view_names(schema_name):
                yield view, "View"

    def get_view_definition(
        self, table_type: str, table_name: str, schema_name: str, inspector: Inspector
    ) -> Optional[str]:

        if table_type == "View":
            try:
                view_definition = inspector.get_view_definition(table_name, schema_name)
                view_definition = (
                    "" if view_definition is None else str(view_definition)
                )
                return view_definition
            except NotImplementedError:
                return ""

    def test_connection(self) -> None:
        """
        Used a timed-bound function to test that the engine
        can properly reach the source
        """
        test_connection(self.engine)

    @property
    def session(self) -> Session:
        """
        Return the SQLAlchemy session from the engine
        """
        if not self._session:
            self._session = create_and_bind_session(self.engine)

        return self._session

    @property
    def connection(self) -> Connection:
        """
        Return the SQLAlchemy connection
        """
        if not self._connection:
            self._connection = self.engine.connect()

        return self._connection

    def close(self):
        self._create_dbt_lineage()
        if self.connection is not None:
            self.connection.close()

    def get_status(self) -> SourceStatus:
        return self.status

    def fetch_table_tags(
        self, table_name: str, schema_name: str, inspector: Inspector
    ) -> None:
        """
        Method to fetch tags associated with table
        """
        pass
