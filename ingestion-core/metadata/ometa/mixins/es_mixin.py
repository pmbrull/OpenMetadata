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
Mixin class containing Lineage specific methods

To be used by OpenMetadata class
"""
import functools
import traceback
from typing import Generic, List, Optional, Type, TypeVar

from pydantic import BaseModel

from metadata.generated.schema.api.createEventPublisherJob import (
    CreateEventPublisherJob,
)
from metadata.generated.schema.system.eventPublisherJob import EventPublisherResult
from metadata.ometa.client import REST, APIError
from metadata.ometa.elasticsearch import ES_INDEX_MAP
from metadata.ometa.logger import logger

T = TypeVar("T", bound=BaseModel)


class ESMixin(Generic[T]):
    """
    OpenMetadata API methods related to Elasticsearch.

    To be inherited by OpenMetadata
    """

    client: REST

    fqdn_search = "/search/query?q=fullyQualifiedName:{fqn}&from={from_}&size={size}&index={index}"

    @functools.lru_cache(maxsize=512)
    def _search_es_entity(
        self,
        entity_type: Type[T],
        query_string: str,
        fields: Optional[List[str]] = None,
    ) -> Optional[List[T]]:
        """
        Run the ES query and return a list of entities that match. It does an extra query to the OM API with the
        requested fields per each entity found in ES.
        :param entity_type: Entity to look for
        :param query_string: Query to run
        :return: List of Entities or None
        """
        response = self.client.get(query_string)

        if response:
            return [
                self.get_by_name(
                    entity=entity_type,
                    fqn=hit["_source"]["fullyQualifiedName"],
                    fields=fields,
                )
                for hit in response["hits"]["hits"]
            ] or None

        return None

    def es_search_from_fqn(
        self,
        entity_type: Type[T],
        fqn_search_string: str,
        from_count: int = 0,
        size: int = 10,
        fields: Optional[List[str]] = None,
    ) -> Optional[List[T]]:
        """
        Given a service_name and some filters, search for entities using ES

        :param entity_type: Entity to look for
        :param fqn_search_string: string used to search by FQN. E.g., service.*.schema.table
        :param from_count: Records to expect
        :param size: Number of records
        :param fields: Fields to be returned
        :return: List of entities
        """
        query_string = self.fqdn_search.format(
            fqn=fqn_search_string,
            from_=from_count,
            size=size,
            index=ES_INDEX_MAP[entity_type.__name__],  # Fail if not exists
        )

        try:
            response = self._search_es_entity(
                entity_type=entity_type, query_string=query_string, fields=fields
            )
            return response
        except KeyError as err:
            logger.debug(traceback.format_exc())
            logger.warning(
                f"Cannot find the index in ES_INDEX_MAP for {entity_type.__name__}: {err}"
            )
        except Exception as exc:
            logger.debug(traceback.format_exc())
            logger.warning(
                f"Elasticsearch search failed for query [{query_string}]: {exc}"
            )
        return None

    def reindex_es(
        self,
        config: CreateEventPublisherJob,
    ) -> Optional[EventPublisherResult]:
        """
        Method to trigger elasticsearch reindex
        """
        try:
            resp = self.client.post(path="/search/reindex", data=config.json())
            return EventPublisherResult(**resp)
        except APIError as err:
            logger.debug(traceback.format_exc())
            logger.debug(f"Failed to trigger es reindex job due to {err}")
            return None

    def get_reindex_job_status(self, job_id: str) -> Optional[EventPublisherResult]:
        """
        Method to fetch the elasticsearch reindex job status
        """
        try:
            resp = self.client.get(path=f"/search/reindex/{job_id}")
            return EventPublisherResult(**resp)
        except APIError as err:
            logger.debug(traceback.format_exc())
            logger.debug(f"Failed to fetch reindex job status due to {err}")
            return None
