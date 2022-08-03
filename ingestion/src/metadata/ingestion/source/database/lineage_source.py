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
Lineage Source Module
"""
import csv
import traceback
from abc import ABC
from typing import Iterable, Iterator, Optional

from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    OpenMetadataConnection,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.generated.schema.type.tableQuery import TableQueries, TableQuery
from metadata.ingestion.api.source import Source, SourceStatus
from metadata.ingestion.lineage.sql_lineage import get_lineage_by_query
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.source.database.common_db_source import SQLSourceStatus
from metadata.ingestion.source.database.query_parser_source import QueryParserSource
from metadata.utils.connections import get_connection, test_connection
from metadata.utils.filters import filter_by_database, filter_by_schema
from metadata.utils.helpers import get_start_and_end
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()


class LineageSource(QueryParserSource, ABC):
    """
    This is the base source to handle Lineage-only ingestion.

    We will still use TableQuery as the data, but only fill up those elements
    that are truly required for the lineage use case, such as:
    - query
    - service
    - database
    - schema
    """

    def get_table_query(self) -> Optional[Iterator[TableQuery]]:
        """
        If queryLogFilePath available in config iterate through log file
        otherwise execute the sql query to fetch TableQuery data.

        This is a simplified version of the UsageSource query parsing.
        """
        if self.config.sourceConfig.config.queryLogFilePath:
            with open(self.config.sourceConfig.config.queryLogFilePath, "r") as file:
                for row in csv.DictReader(file):
                    query_dict = dict(row)
                    yield TableQuery(
                        query=query_dict["query_text"],
                        databaseName=self.get_database_name(query_dict),
                        serviceName=self.config.serviceName,
                        databaseSchema=self.get_schema_name(query_dict),
                    )

        else:
            logger.info(
                f"Scanning query logs for {self.start.date()} - {self.end.date()}"
            )
            try:
                rows = self.engine.execute(
                    self.get_sql_statement(
                        start_time=self.start,
                        end_time=self.end,
                    )
                )
                for row in rows:
                    query_dict = dict(row)
                    try:
                        if filter_by_database(
                            self.source_config.databaseFilterPattern,
                            self.get_database_name(row),
                        ) or filter_by_schema(
                            self.source_config.schemaFilterPattern,
                            schema_name=self.get_schema_name(query_dict),
                        ):
                            continue
                        yield TableQuery(
                            query=query_dict["query_text"],
                            databaseName=self.get_database_name(query_dict),
                            serviceName=self.config.serviceName,
                            databaseSchema=self.get_schema_name(query_dict),
                        )
                    except Exception as err:
                        logger.error(
                            f"Error processing query_dict {query_dict} - {err}"
                        )
                        logger.debug(traceback.format_exc())
            except Exception as err:
                logger.error(f"Source usage processing error - {err}")
                logger.debug(traceback.format_exc())

    def next_record(self) -> Iterable[AddLineageRequest]:
        """
        Based on the query logs, prepare the lineage
        and send it to the sink
        """
        for table_query in self.get_table_query():

            lineages = get_lineage_by_query(
                self.metadata,
                query=table_query.query,
                service_name=table_query.serviceName,
                database_name=table_query.databaseName,
                schema_name=table_query.databaseSchema,
            )

            for lineage_request in lineages or []:
                yield lineage_request
