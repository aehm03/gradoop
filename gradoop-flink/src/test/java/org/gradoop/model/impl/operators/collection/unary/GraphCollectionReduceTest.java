package org.gradoop.model.impl.operators.collection.unary;

import org.gradoop.model.FlinkTestBase;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;

import static org.junit.Assert.assertTrue;

/**
 * Created by peet on 25.11.15.
 */
public class GraphCollectionReduceTest extends FlinkTestBase {
  public GraphCollectionReduceTest(TestExecutionMode mode) {
    super(mode);
  }

  protected void checkExpectationsEqualResults(
    FlinkAsciiGraphLoader<VertexPojo, EdgePojo, GraphHeadPojo> loader) throws
    Exception {
    // overlap
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> col13 =
      loader.getGraphCollectionByVariables("g1", "g3");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> exp13 =
      loader.getLogicalGraphByVariable("exp13");

    // no overlap
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> col12 =
      loader.getGraphCollectionByVariables("g1", "g2");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> exp12 =
      loader.getLogicalGraphByVariable("exp12");

    // full overlap
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> col14 =
      loader.getGraphCollectionByVariables("g1", "g4");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> exp14 =
      loader.getLogicalGraphByVariable("exp14");

    OverlapCollection<GraphHeadPojo, VertexPojo, EdgePojo> overlap =
      new OverlapCollection<>();

    assertTrue("partial overlap failed",
      overlap.execute(col13).equalsByElementDataCollected(exp13));
    assertTrue("without overlap failed",
      overlap.execute(col12).equalsByElementDataCollected(exp12));
    assertTrue("with full overlap failed",
      overlap.execute(col14).equalsByElementDataCollected(exp14));
  }
}