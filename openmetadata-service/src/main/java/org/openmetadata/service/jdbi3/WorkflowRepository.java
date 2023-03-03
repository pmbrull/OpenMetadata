package org.openmetadata.service.jdbi3;

import org.openmetadata.schema.operations.Workflow;
import org.openmetadata.service.resources.operations.WorkflowResource;

import static org.openmetadata.service.Entity.WORKFLOW;

public class WorkflowRepository extends EntityRepository<Workflow> {

  private static final String UPDATE_FIELDS = "owner";
  private static final String PATCH_FIELDS = "owner";

  public WorkflowRepository(CollectionDAO dao) {
    super(
        WorkflowResource.COLLECTION_PATH,
        WORKFLOW,
        Workflow.class,
        dao.workflowDAO(),
        dao,
        PATCH_FIELDS,
        UPDATE_FIELDS);
  }

  @Override
  public TestConnectionDefinition setFields(TestConnectionDefinition entity, EntityUtil.Fields fields)
      throws IOException {
    return entity.withOwner(fields.contains(Entity.FIELD_OWNER) ? getOwner(entity) : null);
  }

  @Override
  public void prepare(TestConnectionDefinition entity) throws IOException {
    // validate steps
    if (CommonUtil.nullOrEmpty(entity.getSteps())) {
      throw new IllegalArgumentException("Steps must not be empty");
    }
  }

  @Override
  public void storeEntity(TestConnectionDefinition entity, boolean update) throws IOException {
    EntityReference owner = entity.getOwner();
    // Don't store owner, database, href and tags as JSON. Build it on the fly based on relationships
    entity.withOwner(null).withHref(null);
    store(entity, update);

    // Restore the relationships
    entity.withOwner(owner);
  }

  @Override
  public void storeRelationships(TestConnectionDefinition entity) throws IOException {
    storeOwner(entity, entity.getOwner());
  }

  @Override
  public EntityUpdater getUpdater(
      TestConnectionDefinition original, TestConnectionDefinition updated, Operation operation) {
    return new TestConnectionDefinitionUpdater(original, updated, operation);
  }

  public class TestConnectionDefinitionUpdater extends EntityUpdater {
    public TestConnectionDefinitionUpdater(
        TestConnectionDefinition original, TestConnectionDefinition updated, Operation operation) {
      super(original, updated, operation);
    }

    @Override
    public void entitySpecificUpdate() throws IOException {
      recordChange("steps", original.getSteps(), updated.getSteps());
    }
  }

}
