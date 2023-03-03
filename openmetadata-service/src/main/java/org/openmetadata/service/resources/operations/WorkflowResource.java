package org.openmetadata.service.resources.operations;

import io.swagger.annotations.Api;
import lombok.extern.slf4j.Slf4j;
import org.openmetadata.schema.operations.Workflow;
import org.openmetadata.service.Entity;
import org.openmetadata.service.resources.Collection;
import org.openmetadata.service.util.RestUtil;

import javax.ws.rs.Consumes;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

@Slf4j
@Path("/v1/operations/workflow")
@Api(value = "Operations Workflow collection", tags = "Operations Workflow collection")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Collection(name = "Workflow")
public class WorkflowResource extends EntityResource<Workflow, WorkflowRepository> {

  public static final String COLLECTION_PATH = "/v1/operations/workflow";
  static final String FIELDS = "owner";

  @Override
  public Workflow addHref(UriInfo uriInfo, Workflow workflow) {
    workflow.withHref(RestUtil.getHref(uriInfo, COLLECTION_PATH, workflow.getId()));
    Entity.withHref(uriInfo, workflow.getOwner());
    return workflow;
  }

  @Inject
  public TestConnectionDefinitionResource(CollectionDAO dao, Authorizer authorizer) {
    super(TestConnectionDefinition.class, new TestConnectionDefinitionRepository(dao), authorizer);
  }

  @Override
  public void initialize(OpenMetadataApplicationConfig config) throws IOException {
    List<TestConnectionDefinition> testConnectionDefinitions =
        dao.getEntitiesFromSeedData(".*json/data/testConnections/.*\\.json$");
    for (TestConnectionDefinition testConnectionDefinition : testConnectionDefinitions) {
      dao.initializeEntity(testConnectionDefinition);
    }
  }

  public static class TestConnectionDefinitionList extends ResultList<TestConnectionDefinition> {
    @SuppressWarnings("unused")
    public TestConnectionDefinitionList() {
      // Empty constructor needed for deserialization
    }
  }

}
