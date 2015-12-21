package org.gradoop.model.impl.operators.equality;

import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class EqualityByGraphElementIdsTest extends EqualityTestBase {

  @Test
  public void testExecute() throws Exception {

    // 4 graphs : 1-2 of same elements, 1-3 different vertex, 1-4 different edge
    String asciiGraphs = "" +
      "g1[(a:A)-[b:b]->(c:C)];" +
      "g2[(a:A)-[b:b]->(c:C)];" +
      "g3[(d:A)-[e:b]->(c:C)];" +
      "g4[(a:A)-[f:b]->(c:C)]";

    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(asciiGraphs);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> c1
      = loader.getGraphCollectionByVariables("g1", "g3");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> c2
      = loader.getGraphCollectionByVariables("g2", "g3");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> c5
      = loader.getGraphCollectionByVariables("g1","g4");

    collectAndAssertTrue(c1.equalsByGraphElementIds(c2));

    collectAndAssertFalse(c1.equalsByGraphElementIds(c5));

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> c3
      = loader.getGraphCollectionByVariables("g1","g2");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> c4
      = loader.getGraphCollectionByVariables("g3","g4");

    collectAndAssertFalse(c3.equalsByGraphElementIds(c4));

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> emptyCollection =
      GraphCollection.createEmptyCollection(getConfig());

    collectAndAssertTrue(
      emptyCollection.equalsByGraphElementIds(emptyCollection));
    collectAndAssertFalse(c1.equalsByGraphElementIds(emptyCollection));
    collectAndAssertFalse(emptyCollection.equalsByGraphElementIds(c1));
  }
}