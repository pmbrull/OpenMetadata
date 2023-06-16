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
Main entrypoints to create and test connections
for any source.
"""
import traceback
from typing import Any, Callable, cast

from pydantic import BaseModel

from metadata.generated.schema.entity.services.connections.serviceConnection import (
    ServiceConnection,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.generated.schema.tests.testSuite import ServiceType
from metadata.ingestion.api.workflow import InvalidWorkflowJSONException
from metadata.ingestion.models.custom_types import ServiceWithConnectionType
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.utils.class_helper import get_service_class_from_service_type
from metadata.utils.importer import import_connection_fn
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()

GET_CONNECTION_FN_NAME = "get_connection"
TEST_CONNECTION_FN_NAME = "test_connection"


def get_connection_fn(connection: BaseModel) -> Callable:
    """
    Import the get_connection function from the source
    """
    return import_connection_fn(
        connection=connection, function_name=GET_CONNECTION_FN_NAME
    )


def get_test_connection_fn(connection: BaseModel) -> Callable:
    """
    Import the test_connection function from the source
    """
    return import_connection_fn(
        connection=connection, function_name=TEST_CONNECTION_FN_NAME
    )


def get_connection(connection: BaseModel) -> Any:
    """
    Main method to prepare a connection from
    a service connection pydantic model
    """
    return get_connection_fn(connection)(connection)


def _retrieve_service_connection_if_needed(
    source: WorkflowSource, metadata: OpenMetadata, service_type: ServiceType
) -> WorkflowSource:
    """
    We override the current `serviceConnection` source config object if source workflow service already exists
    in OM. When secrets' manager is configured, we retrieve the service connection from the secrets' manager.
    Otherwise, we get the service connection from the service object itself through the default `SecretsManager`.

    :param service_type: source workflow service type
    """
    if not source.serviceConnection and not metadata.config.forceEntityOverwriting:
        service_name = source.serviceName
        try:
            service: ServiceWithConnectionType = cast(
                ServiceWithConnectionType,
                metadata.get_by_name(
                    get_service_class_from_service_type(service_type),
                    service_name,
                ),
            )
            if service:
                source.serviceConnection = ServiceConnection(
                    __root__=service.connection
                )
            else:
                raise InvalidWorkflowJSONException(
                    "The serviceConnection is not informed and we cannot retrieve it from the API"
                    f" by searching for the service name [{service_name}]. Does this service exist in OpenMetadata?"
                )
        except InvalidWorkflowJSONException as exc:
            raise exc
        except Exception as exc:
            logger.debug(traceback.format_exc())
            logger.error(
                f"Unknown error getting service connection for service name [{service_name}]"
                f" using the secrets manager provider [{metadata.config.secretsManagerProvider}]: {exc}"
            )

    return source
