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
Test logging utilities
"""

from metadata.utils.logger import get_add_lineage_log_str

from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.generated.schema.type.entityLineage import EntitiesEdge, LineageDetails
from metadata.generated.schema.type.entityReference import EntityReference


def test_add_lineage_log_info() -> None:
    """
    We can extract lineage information properly
    """
    add_lineage = AddLineageRequest(
        edge=EntitiesEdge(
            fromEntity=EntityReference(
                id="2aaa012e-099a-11ed-861d-0242ac120002",
                type="table",
                name="random",
            ),
            toEntity=EntityReference(
                id="1aaa012e-099a-11ed-861d-0242ac120002",
                type="...",
                name="...",
            ),
            lineageDetails=LineageDetails(description="something"),
        ),
    )

    assert (
        get_add_lineage_log_str(add_lineage)
        == "table [name: random, id: 2aaa012e-099a-11ed-861d-0242ac120002]"
    )

    add_lineage = AddLineageRequest(
        edge=EntitiesEdge(
            fromEntity=EntityReference(
                id="2aaa012e-099a-11ed-861d-0242ac120002",
                type="table",
            ),
            toEntity=EntityReference(
                id="1aaa012e-099a-11ed-861d-0242ac120002",
                type="...",
            ),
            lineageDetails=LineageDetails(description="something"),
        ),
    )

    assert (
        get_add_lineage_log_str(add_lineage)
        == "table [id: 2aaa012e-099a-11ed-861d-0242ac120002]"
    )
