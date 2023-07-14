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
"""Mode source module"""

import traceback
from typing import Iterable, List, Optional

from metadata.ingestion.api.source import InvalidSourceException
from metadata.ingestion.lineage.parser import LineageParser
from metadata.ingestion.lineage.sql_lineage import search_table_entities
from metadata.ingestion.source.dashboard.dashboard_service import DashboardServiceSource
from metadata.ingestion.source.dashboard.mode import client
from metadata.utils.filters import filter_by_chart
from metadata.utils.helpers import clean_uri
from metadata.utils.logger import ingestion_logger

from metadata.generated.schema.api.data.createChart import CreateChartRequest
from metadata.generated.schema.api.data.createDashboard import CreateDashboardRequest
from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.generated.schema.entity.data.chart import Chart, ChartType
from metadata.generated.schema.entity.data.dashboard import (
    Dashboard as Lineage_Dashboard,
)
from metadata.generated.schema.entity.services.connections.dashboard.modeConnection import (
    ModeConnection,
)
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    OpenMetadataConnection,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.ometa import fqn

logger = ingestion_logger()


class ModeSource(DashboardServiceSource):
    """
    Mode Source Class
    """

    def __init__(
        self,
        config: WorkflowSource,
        metadata_config: OpenMetadataConnection,
    ):
        super().__init__(config, metadata_config)
        self.workspace_name = config.serviceConnection.__root__.config.workspaceName
        self.data_sources = self.client.get_all_data_sources(self.workspace_name)

    @classmethod
    def create(cls, config_dict, metadata_config: OpenMetadataConnection):
        config = WorkflowSource.parse_obj(config_dict)
        connection: ModeConnection = config.serviceConnection.__root__.config
        if not isinstance(connection, ModeConnection):
            raise InvalidSourceException(
                f"Expected ModeConnection, but got {connection}"
            )
        return cls(config, metadata_config)

    def get_dashboards_list(self) -> Optional[List[dict]]:
        """
        Get List of all dashboards
        """
        return self.client.fetch_all_reports(self.workspace_name)

    def get_dashboard_name(self, dashboard: dict) -> str:
        """
        Get Dashboard Name
        """
        return dashboard.get(client.NAME)

    def get_dashboard_details(self, dashboard: dict) -> dict:
        """
        Get Dashboard Details
        """
        return dashboard

    def yield_dashboard(
        self, dashboard_details: dict
    ) -> Iterable[CreateDashboardRequest]:
        """
        Method to Get Dashboard Entity
        """
        dashboard_path = dashboard_details[client.LINKS][client.SHARE][client.HREF]
        dashboard_url = f"{clean_uri(self.service_connection.hostPort)}{dashboard_path}"
        dashboard_request = CreateDashboardRequest(
            name=dashboard_details.get(client.TOKEN),
            sourceUrl=dashboard_url,
            displayName=dashboard_details.get(client.NAME),
            description=dashboard_details.get(client.DESCRIPTION),
            charts=[
                fqn.build(
                    self.metadata,
                    entity_type=Chart,
                    service_name=self.context.dashboard_service.fullyQualifiedName.__root__,
                    chart_name=chart.name.__root__,
                )
                for chart in self.context.charts
            ],
            service=self.context.dashboard_service.fullyQualifiedName.__root__,
        )
        yield dashboard_request
        self.register_record(dashboard_request=dashboard_request)

    def yield_dashboard_lineage_details(
        self, dashboard_details: dict, db_service_name: str
    ) -> Optional[Iterable[AddLineageRequest]]:
        """Get lineage method

        Args:
            dashboard_details
        """
        try:
            response_queries = self.client.get_all_queries(
                workspace_name=self.workspace_name,
                report_token=dashboard_details[client.TOKEN],
            )
            queries = response_queries[client.EMBEDDED][client.QUERIES]
            for query in queries:
                if not query.get("data_source_id"):
                    continue
                data_source = self.data_sources.get(query.get("data_source_id"))
                if not data_source:
                    continue
                lineage_parser = LineageParser(query.get("raw_query"))
                for table in lineage_parser.source_tables:
                    database_schema_name, table = fqn.split(str(table))[-2:]
                    database_schema_name = self.check_database_schema_name(
                        database_schema_name
                    )
                    from_entities = search_table_entities(
                        metadata=self.metadata,
                        database=data_source.get(client.DATABASE),
                        service_name=db_service_name,
                        database_schema=database_schema_name,
                        table=table,
                    )
                    for from_entity in from_entities:
                        to_entity = self.metadata.get_by_name(
                            entity=Lineage_Dashboard,
                            fqn=fqn.build(
                                self.metadata,
                                Lineage_Dashboard,
                                service_name=self.config.serviceName,
                                dashboard_name=dashboard_details.get(client.TOKEN),
                            ),
                        )
                        yield self._get_add_lineage_request(
                            to_entity=to_entity, from_entity=from_entity
                        )
        except Exception as exc:  # pylint: disable=broad-except
            logger.debug(traceback.format_exc())
            logger.error(
                f"Error to yield dashboard lineage details for DB service name [{db_service_name}]: {exc}"
            )

    def yield_dashboard_chart(
        self, dashboard_details: dict
    ) -> Optional[Iterable[CreateChartRequest]]:
        """Get chart method

        Args:
            dashboard_details:
        Returns:
            Iterable[CreateChartRequest]
        """
        response_queries = self.client.get_all_queries(
            workspace_name=self.workspace_name,
            report_token=dashboard_details.get(client.TOKEN),
        )
        queries = response_queries[client.EMBEDDED][client.QUERIES]
        for query in queries:
            response_charts = self.client.get_all_charts(
                workspace_name=self.workspace_name,
                report_token=dashboard_details.get(client.TOKEN),
                query_token=query.get(client.TOKEN),
            )
            charts = response_charts[client.EMBEDDED][client.CHARTS]
            for chart in charts:
                chart_name = chart[client.VIEW_VEGAS].get(client.TITLE)
                try:
                    if filter_by_chart(
                        self.source_config.chartFilterPattern,
                        chart_name,
                    ):
                        self.status.filter(
                            chart_name,
                            "Chart Pattern not Allowed",
                        )
                        continue
                    chart_path = chart[client.LINKS]["report_viz_web"][client.HREF]
                    chart_url = (
                        f"{clean_uri(self.service_connection.hostPort)}{chart_path}"
                    )
                    yield CreateChartRequest(
                        name=chart.get(client.TOKEN),
                        displayName=chart_name,
                        chartType=ChartType.Other,
                        sourceUrl=chart_url,
                        service=self.context.dashboard_service.fullyQualifiedName.__root__,
                    )
                    self.status.scanned(chart_name)
                except Exception as exc:
                    name = chart_name if chart_name else ""
                    error = f"Error to yield dashboard chart [{chart}]: {exc}"
                    logger.debug(traceback.format_exc())
                    logger.warning(error)
                    self.status.failed(name, error, traceback.format_exc())
