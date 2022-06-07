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
Generic source to build database connectors.
"""
import traceback
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Iterable, List, Optional, Set, Tuple, Union

from metadata.generated.schema.entity.data.database import Database
from sqlalchemy.engine.reflection import Inspector

from metadata.generated.schema.api.data.createDatabase import CreateDatabaseRequest
from metadata.generated.schema.api.data.createDatabaseSchema import (
    CreateDatabaseSchemaRequest,
)
from metadata.generated.schema.api.data.createTable import CreateTableRequest
from metadata.generated.schema.entity.data.databaseSchema import DatabaseSchema
from metadata.generated.schema.entity.data.table import (
    Column,
    DataModel,
    Table,
    TableData,
)
from metadata.generated.schema.entity.services.databaseService import (
    DatabaseConnection,
    DatabaseService,
)
from metadata.generated.schema.metadataIngestion.databaseServiceMetadataPipeline import (
    DatabaseServiceMetadataPipeline,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.ingestion.api.common import Entity
from metadata.ingestion.api.source import Source, SourceStatus
from metadata.ingestion.models.ometa_table_db import OMetaDatabaseAndTable
from metadata.ingestion.models.ometa_tag_category import OMetaTagAndCategory
from metadata.ingestion.models.table_metadata import DeleteTable
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.utils import fqn
from metadata.utils.filters import filter_by_schema, filter_by_table
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()


@dataclass
class SQLSourceStatus(SourceStatus):
    """
    Reports the source status after ingestion
    """

    success: List[str] = field(default_factory=list)
    failures: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    filtered: List[str] = field(default_factory=list)

    def scanned(self, record: str) -> None:
        self.success.append(record)
        logger.info(f"Table Scanned: {record}")

    def filter(self, record: str, err: str) -> None:
        self.filtered.append(record)
        logger.warning(f"Filtered Table {record} due to {err}")


class SqlAlchemySource(Source, ABC):

    source_config: DatabaseServiceMetadataPipeline
    status: SQLSourceStatus
    config: WorkflowSource
    service: DatabaseService
    metadata: OpenMetadata
    database_source_state: Set
    # Big union of types we want to fetch dynamically
    service_connection: DatabaseConnection.__fields__["config"].type_

    @abstractmethod
    def get_database_request_and_inspector(self) -> Iterable[Tuple[CreateDatabaseRequest, Inspector]]:
        """
        Method Yields Inspector objects for each available database.
        It will also prepare the CreateDatabaseRequest object to yield
        """

    @abstractmethod
    def get_schemas(self, inspector: Inspector) -> str:
        """
        Method Yields schemas available in database
        """

    @abstractmethod
    def standardize_schema_table_names(
        self, schema: str, table: str
    ) -> Tuple[str, str]:
        """
        Method formats Table & Schema names if required
        """

    @abstractmethod
    def get_table_description(
        self, schema_name: str, table_name: str, inspector: Inspector
    ) -> str:
        """
        Method returns the table level comment
        """

    @abstractmethod
    def is_partition(self, table_name: str, schema_name: str, inspector: Inspector) -> bool:
        """
        Method to check if the table is partitioned table
        """

    @abstractmethod
    def get_data_model(self, database_name: str, schema_name: str, table_name: str) -> DataModel:
        """
        Method to fetch data models
        """

    @abstractmethod
    def get_table_names(
        self, schema_name: str, inspector: Inspector
    ) -> Optional[Iterable[Tuple[str, str]]]:
        """
        Method to fetch table & view names
        """

    @abstractmethod
    def get_columns(
        self, schema_name: str, table_name: str, inspector: Inspector
    ) -> Optional[List[Column]]:
        """
        Method to fetch table columns data
        """

    @abstractmethod
    def get_view_definition(
        self, table_type, table_name: str, schema_name: str, inspector: Inspector
    ) -> Optional[str]:
        """
        Method to fetch view definition
        """

    @abstractmethod
    def fetch_column_tags(self, column: dict, col_obj: Column) -> None:
        """
        Method to fetch tags associated with column
        """

    @abstractmethod
    def fetch_table_tags(
        self, table_name: str, schema_name: str, inspector: Inspector
    ) -> None:
        """
        Method to fetch tags associated with table
        """

    def get_database(self, database_name) -> Database:
        """
        GET the database from OMeta API
        :param database_name: name, to build the FQN
        :return: Database Entity
        """

        database_fqn = fqn.build(
            self.metadata,
            entity_type=Database,
            service_name=self.service.name.__root__,
            database_name=database_name,
        )

        return self.metadata.get_by_name(entity=Database, fqn=database_fqn)

    def get_schema_request(self, schema_name: str, database_request: CreateDatabaseRequest) -> CreateDatabaseSchemaRequest:
        """
        Method to get DatabaseSchema entity from schema name and database entity
        """
        return CreateDatabaseSchemaRequest(
            name=schema_name,
            database=database_request.service,  # TODO THIS IS NOT CORRECT, SHOULD BE THE DB ID
            service=EntityReference(
                id=self.service.id, type=self.service_connection.type.value
            ),
        )

    def next_record(self) -> Iterable[Entity]:
        """
        Method to fetch all tables, views & mark delete tables
        """
        for database_request, inspector in self.get_database_request_and_inspector():

            yield database_request  # Will be created by the sink
            database_entity = self.get_database(database_name=database_request.name.__root__)

            for schema_name in self.get_schemas(inspector):

                yield schema_request
                use schema_entity

                try:
                    # clear any previous source database state
                    self.database_source_state.clear()
                    if filter_by_schema(
                        self.source_config.schemaFilterPattern, schema_name=schema_name
                    ):
                        self.status.filter(schema_name, "Schema pattern not allowed")
                        continue

                    if self.source_config.includeTables:
                        yield from self.fetch_tables(inspector, schema_name)

                    if self.source_config.markDeletedTables:
                        schema_fqn = fqn.build(
                            self.metadata,
                            entity_type=DatabaseSchema,
                            service_name=self.config.serviceName,
                            database_name=self._get_database_name(),
                            schema_name=schema_name,
                        )
                        yield from self.delete_tables(schema_fqn)

                except Exception as err:
                    logger.debug(traceback.format_exc())
                    logger.error(err)

    def _get_table_request(
        self, schema_request: CreateDatabaseSchemaRequest, table_name: str, table_type: str, inspector: Inspector
    ) -> CreateTableRequest:
        """
        Method to get table entity
        """
        description = self.get_table_description(schema_request.name.__root__, table_name, inspector)
        self.table_constraints = None
        table_columns = self.get_columns(schema_request.name.__root__, table_name, inspector)
        # TODO MISSING SCHEMA -> we need the ID?? how??
        table_request = CreateTableRequest(
            name=table_name,
            tableType=table_type,
            description=description,
            columns=table_columns,
            viewDefinition=self.get_view_definition(
                table_type, table_name, schema_request, inspector
            ),
            databaseSchema=...
        )
        if self.table_constraints:
            table_request.tableConstraints = self.table_constraints

        return table_request

    def fetch_tables(
        self, inspector: Inspector, schema_name: str
    ) -> Iterable[Union[OMetaDatabaseAndTable, OMetaTagAndCategory]]:
        """
        Scrape an SQL schema and prepare Database and Table
        OpenMetadata Entities
        """
        for table_name, table_type in self.get_table_names(schema_name, inspector):
            try:
                schema_name, table_name = self.standardize_schema_table_names(
                    schema_name, table_name
                )
                if filter_by_table(
                    self.source_config.tableFilterPattern, table_name=table_name
                ):
                    self.status.filter(
                        f"{self.config.serviceName}.{table_name}",
                        "Table pattern not allowed",
                    )
                    continue
                if self.is_partition(table_name, schema_name, inspector):
                    self.status.filter(
                        f"{self.config.serviceName}.{table_name}",
                        "Table is partition",
                    )
                    continue


                schema_request = self.get_schema_request(schema_name=schema_name, database_request=database_request)

                table_request = self._get_table_request(
                    schema_request, table_name, table_type, inspector
                )

                # check if we have any model to associate with
                data_model = self.get_data_model(
                    database_request.name.__root__, schema_name, table_name
                )

                table_schema_and_db = OMetaDatabaseAndTable(
                    table_request=table_request,
                    database_request=database_request,
                    schema_request=self.get_schema_request(schema_name, database_request),
                    data_model=data_model,
                )
                self.register_record(table_schema_and_db)

                yield table_schema_and_db

            except Exception as err:
                logger.debug(traceback.format_exc())
                logger.error(err)
                self.status.failures.append(
                    "{}.{}".format(self.config.serviceName, table_name)
                )

    def register_record(self, table_schema_and_db: OMetaDatabaseAndTable) -> None:
        """
        Mark the record as scanned and update the database_source_state
        """
        table_fqn = fqn.build(
            self.metadata,
            entity_type=Table,
            service_name=self.config.serviceName,
            database_name=str(table_schema_and_db.database_request.name.__root__),
            schema_name=str(table_schema_and_db.schema_request.name.__root__),
            table_name=str(table_schema_and_db.table_request.name.__root__),
        )

        self.database_source_state.add(table_fqn)
        self.status.scanned(table_fqn)

    def delete_tables(self, schema_fqn: str) -> DeleteTable:
        """
        Returns Deleted tables
        """
        # Fetch all tables known by OM in that schema
        database_state = self.metadata.list_all_entities(entity=Table, params={"database": schema_fqn})

        # If a table is not being ingested anymore, flag as deleted
        for table in database_state:
            if str(table.fullyQualifiedName.__root__) not in self.database_source_state:
                yield DeleteTable(table=table)
