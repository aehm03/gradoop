package org.gradoop.flink.algorithms.gelly.jaccardindex;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.epgm.LogicalGraphFactory;
import org.gradoop.flink.model.impl.layouts.gve.GVEGraphLayoutFactory;
import org.junit.Test;

public class JaccardIndexTest extends GradoopFlinkTestBase {

  @Test
  public void testEmptyGraph() throws Exception {
    LogicalGraphFactory logicalGraphFactory = new LogicalGraphFactory(config);
    logicalGraphFactory.setLayoutFactory(new GVEGraphLayoutFactory());
    LogicalGraph emptyGraph = logicalGraphFactory.createEmptyGraph();

    LogicalGraph result = emptyGraph.callForGraph(new JaccardIndex("JaccardIndex"));
    collectAndAssertTrue(emptyGraph.equalsByElementData(result));
  }


  @Test
  public void testMinimalUndirectedGraph() throws Exception {
    String minGraph =
      "" + "(v0:v0) -[:e]-> (v1:v1)" + "(v1)    -[:e]-> (v2:v2)" + "(v1)    -[:e]-> (v0)" +
        "(v2)    -[:e]-> (v1)";

    String minRes = "(v0) -[:ji {value : 1.0d}]-> (v2) " + "(v2) -[:ji {value : 1.0d}]-> (v0) ";

    LogicalGraph input =
      getLoaderFromString("input[" + minGraph + "]").getLogicalGraphByVariable("input");
    LogicalGraph expectedResult =
      getLoaderFromString("input[" + minGraph + minRes + "]").getLogicalGraphByVariable("input");
    LogicalGraph result = input.callForGraph(new JaccardIndex("ji"));

    collectAndAssertTrue(result.equalsByElementData(expectedResult));
  }

  /**
   * Test the JaccardIndex implementation on a graph that mimics the undirected simple graph
   * from the gelly unit tests.
   *
   * Input Graph from <a href = "https://github
   * .com/apache/flink/blob/master/flink-libraries/flink-gelly/src/test/java"> AsmTestBase</a>
   *
   * Expected Results from <a href = "https://github
   * .com/apache/flink/blob/master/flink-libraries/flink-gelly/src/test/java/org/apache/flink
   * /graph/library/similarity/JaccardIndexTest.java"> JaccardIndexTest</a>
   */
  @Test
  public void testUndirectedSimpleGraph() throws Exception {

    // Two mirrored directed edges represent one undirected edge
    String inputString = "(v0:v0) --> (v1:v1) (v1) --> (v0)" + "(v0) --> (v2:v2) (v2) --> (v0)" +
      "(v2) --> (v1) (v1) --> (v2)" + "(v2) --> (v3:v3) (v3) --> (v2)" +
      "(v3) --> (v1)  (v1) --> (v3)" + "(v3) --> (v4:v4) (v4) --> (v3)" +
      "(v5:v5) --> (v3) (v3) --> (v5)";

    String expectedResultString = "input[" + inputString +
      // "(0,1,1,4)\n" +
      "(v0) -[:JaccardIndex {value : 0.25d}]-> (v1) " +
      "(v1) -[:JaccardIndex {value : 0.25d}]-> (v0) " +
      // "(0,2,1,4)\n" +
      "(v0) -[:JaccardIndex {value : 0.25d}]-> (v2) " +
      "(v2) -[:JaccardIndex {value : 0.25d}]-> (v0) " +
      // 			"(0,3,2,4)\n" +
      "(v0) -[:JaccardIndex {value : 0.5d}] -> (v3) " +
      "(v3) -[:JaccardIndex {value : 0.5d}] -> (v0) " +
      // 			"(1,2,2,4)\n" +
      "(v1) -[:JaccardIndex {value : 0.5d}] -> (v2) " +
      "(v2) -[:JaccardIndex {value : 0.5d}] -> (v1) " +
      // "(1,3,1,6)\n" +
      "(v1) -[:JaccardIndex {value : 0.16666666666666666d}]-> (v3) " +
      "(v3) -[:JaccardIndex {value : 0.16666666666666666d}]-> (v1) " +
      // "(1,4,1,3)\n" +
      "(v1) -[:JaccardIndex {value : 0.3333333333333333d}]-> (v4) " +
      "(v4) -[:JaccardIndex {value : 0.3333333333333333d}]-> (v1) " +
      // "(1,5,1,3)\n" +
      "(v1) -[:JaccardIndex {value : 0.3333333333333333d}] -> (v5) " +
      "(v5) -[:JaccardIndex {value : 0.3333333333333333d}] -> (v1) " +
      // "(2,3,1,6)\n" +
      "(v2) -[:JaccardIndex {value : 0.16666666666666666d}]-> (v3) " +
      "(v3) -[:JaccardIndex {value : 0.16666666666666666d}]-> (v2) " +
      // "(2,4,1,3)\n" +
      "(v2) -[:JaccardIndex {value : 0.3333333333333333d}]-> (v4) " +
      "(v4) -[:JaccardIndex {value : 0.3333333333333333d}]-> (v2) " +
      // "(2,5,1,3)\n" +
      "(v2) -[:JaccardIndex {value : 0.3333333333333333d}]-> (v5) " +
      "(v5) -[:JaccardIndex {value : 0.3333333333333333d}]-> (v2) " +
      // 	"(4,5,1,1)\n";
      "(v4) -[:JaccardIndex {value : 1.0d}]-> (v5) " +
      "(v5) -[:JaccardIndex {value : 1.0d}]-> (v4) " + "]";

    LogicalGraph input =
      getLoaderFromString("input[" + inputString + "]").getLogicalGraphByVariable("input");

    LogicalGraph expectedResult =
      getLoaderFromString(expectedResultString).getLogicalGraphByVariable("input");

    LogicalGraph result = input.callForGraph(new JaccardIndex("JaccardIndex"));

    collectAndAssertTrue(expectedResult.equalsByElementData(result));
  }
}
