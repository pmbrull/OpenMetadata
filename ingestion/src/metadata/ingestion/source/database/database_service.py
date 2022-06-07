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
Draws the ingestion logic for database services
"""
import uuid
from typing import Optional, List, Generic, TypeVar, Type, Iterable, Any

from metadata.generated.schema.entity.services.connections.database.mysqlConnection import MysqlConnection

from metadata.generated.schema.entity.data.table import Table

from metadata.generated.schema.entity.data.databaseSchema import DatabaseSchema

from metadata.generated.schema.entity.data.database import Database

from metadata.generated.schema.entity.services.databaseService import DatabaseService, DatabaseConnection
from metadata.ingestion.api.common import Entity

from metadata.ingestion.api.source import Source, SourceStatus
from pydantic import BaseModel, Extra, create_model

T = TypeVar("T", bound=BaseModel)


# JSON Schema
class TopologyNode(BaseModel, Generic[T]):
    """
    Each node has a producer function, which will
    yield an Entity to be passed to the Sink. Afterwards,
    the producer function will update the Source context
    with the updated element from the OM API.
    """

    class Config:
        extra = Extra.forbid

    type_: Type[T]  # Entity type
    context: str  # context key storing node state
    producer: str  # method name in the source to use to generate the data

    children: Optional[List[str]] = None  # nodes to call execute next
    consumer: Optional[List[str]] = None  # keys in the source context to fetch state from the parent's context


# JSON Schema
class ServiceTopology(BaseModel):
    """
    Bounds all service topologies
    """

    class Config:
        extra = Extra.allow


# JSON Schema
class TopologyContext(BaseModel):
    """
    Bounds all topology contexts
    """

    class Config:
        extra = Extra.allow


# JSON Data
class DatabaseServiceTopology(ServiceTopology):
    """
    Defines the hierarchy in Database Services.
    service -> db -> schema -> table.

    We could have a topology validator. We can only consume
    data that has been produced by any parent node.
    """
    root = TopologyNode(
        type_=DatabaseService,
        context="database_service",
        producer="yield_database_service",
        children=["database"],
    )
    database = TopologyNode(
        type_=Database,
        context="database",
        producer="yield_database",
        children=["databaseSchema"],
        consumer=["database_service"]
    )
    databaseSchema = TopologyNode(
        type_=DatabaseSchema,
        context="database_schema",
        producer="yield_database_schema",
        children=["table"],
        consumer=["database_service", "database"]
    )
    table = TopologyNode(
        type_=Table,
        context="table",
        producer="yield_table",
        consumer=["database_service", "database", "database_schema"]
    )


def get_topology_nodes(topology: ServiceTopology) -> List[TopologyNode]:
    """
    Fetch all nodes from a ServiceTopology
    :param topology: ServiceTopology
    :return: List of nodes
    """
    return [value for key, value in topology.__dict__.items()]


def get_topology_root(topology: ServiceTopology) -> List[TopologyNode]:
    """
    Fetch the roots from a ServiceTopology.

    A node is root if it has no consumers, i.e., can be
    computed at the top of the Tree.
    :param topology: ServiceTopology
    :return: List of nodes that can be roots
    """
    nodes = get_topology_nodes(topology)
    return [node for node in nodes if node.consumer is None]


def create_source_context(topology: ServiceTopology) -> TopologyContext:
    """
    Dynamically build a context based on the topology nodes
    :param topology: ServiceTopology
    :return: TopologyContext
    """
    nodes = get_topology_nodes(topology)
    ctx_fields = {node.context: (Optional[node.type_], None) for node in nodes}
    return create_model("GeneratedContext", **ctx_fields, __base__=TopologyContext)()


def get_topology_node(name: str, topology: ServiceTopology) -> TopologyNode:
    """
    Fetch a topology node by name
    :param name: node name
    :param topology: service topology with all nodes
    :return: TopologyNode
    """
    node = topology.__dict__.get(name)
    if not node:
        raise ValueError(f"{name} node not found in {topology}")

    return node


class TopologyRunnerMixin:
    """
    Prepares the next_record function
    dynamically based on the source topology
    """
    topology: ServiceTopology
    context: TopologyContext

    def process_nodes(self, nodes: List[TopologyNode]) -> Iterable[Entity]:
        """
        Given a list of nodes, either roots or children,
        yield from its producers and process the children.

        The execution tree is created in a depth-first fashion.

        :param nodes: Topology Nodes to process
        :return: recursively build the execution tree
        """
        for node in nodes:
            node_producer = getattr(self, node.producer)
            yield from node_producer()

            child_nodes = [get_topology_node(child, self.topology) for child in node.children] if node.children else []
            yield from self.process_nodes(child_nodes)

    def next_record(self) -> Iterable[Entity]:
        """
        Based on a ServiceTopology, find the root node
        and fetch all source methods in the required order
        to yield data to the sink
        :return: Iterable of the Entities yielded by all nodes in the topology
        """
        yield from self.process_nodes(get_topology_root(self.topology))

    def update_context(self, key: str, value: Any) -> None:
        """
        Update the key of the context with the given value
        :param key: element to update from the source context
        :param value: value to use for the update
        """
        self.context.__dict__[key] = value


class DatabaseServiceSource(TopologyRunnerMixin, Source):
    topology = DatabaseServiceTopology()
    context = create_source_context(topology)

    def yield_database_service(self):
        yield "database_service"
        self.update_context(
            key="database_service",
            value=DatabaseService(
                id=uuid.uuid4(),
                name="service",
                serviceType="Mysql",
                connection=DatabaseConnection(
                    config=MysqlConnection(
                        username="username",
                        password="password",
                        hostPort="http://localhost:1234",
                    )
                ),
            )
        )

    def yield_database(self):
        print("Getting database service context...", self.context.database_service.name.__root__)
        yield "database"

    def yield_database_schema(self):
        yield "database_schema"

    def yield_table(self):
        yield "tableA"
        yield "tableB"

    def create(self):
        pass

    def get_status(self) -> SourceStatus:
        pass

    def prepare(self):
        pass

    def test_connection(self) -> None:
        pass

    def close(self):
        pass
