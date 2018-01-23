package org.gradoop.flink.algorithms.gelly.jaccardindex;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.junit.Test;

public class JaccardIndexTest extends GradoopFlinkTestBase {

  /**
   * Testing the Jaccard Index on a graph that mimics the undirected simple graph from the gelly
   * unit tests.
   * Undirected Simple Graph from: https://github
   * .com/apache/flink/blob/master/flink-libraries/flink-gelly/src/test/java/org/apache/flink
   * /graph/asm/AsmTestBase.java
   * Expected Results from: https://github
   * .com/apache/flink/blob/master/flink-libraries/flink-gelly/src/test/java/org/apache/flink
   * /graph/library/similarity/JaccardIndexTest.java
   */
  @Test
  public void testUndirectedSimpleGraph() {
    String graph =
      "input[" + "(v0) --> (v1) " + "(v0) --> (v2) " + "(v2) --> (v1) " + "(v2) --> (v3) " +
        "(v3) --> (v1) " + "(v3) --> (v4) " + "(v5) --> (v3) " + "]";

    String results = "input[" +
      // "(0,1,1,4)\n" +
      "(v0) -[:JaccardIndex {value : 0.25}]-> (v1) " +
      // "(0,2,1,4)\n" +
      "(v0) -[:JaccardIndex {value : 0.25}]-> (v2) " +
      // 			"(0,3,2,4)\n" +
      "(v0) -[:JaccardIndex {value : 0.5}] -> (v3) " +
      // 			"(1,2,2,4)\n" +
      "(v1) -[:JaccardIndex {value : 0.5}] -> (v2) " +
      // "(1,3,1,6)\n" +
      "(v1) -[:JaccardIndex {value : 0.1666}]-> (v3) " +   // TODO precision??
      // "(1,4,1,3)\n" +
      "(v1) -[:JaccardIndex {value : 0.3333}]-> (v4) " +
      // "(1,5,1,3)\n" +
      "(v1) -[:JaccardIndex {value : 0.3333}] -> (v5) " +
      // "(2,3,1,6)\n" +
      "(v2) -[:JaccardIndex {value : 0.1666}]-> (v3) " +
      // "(2,4,1,3)\n" +
      "(v2) -[:JaccardIndex {value : 0.3333}]-> (v4) " +
      // "(2,5,1,3)\n" +
      "(v2) -[:JaccardIndex {value : 0.3333}]-> (v5) " +
      // 	"(4,5,1,1)\n";
      "(v4) -[:JaccardIndex {value : 1}]-> (v5) " + "]";

    LogicalGraph simpleGraph = getLoaderFromString(graph).getLogicalGraphByVariable("input");
    LogicalGraph expectedResult = getLoaderFromString(results).getLogicalGraphByVariable("input");

    LogicalGraph result = simpleGraph.callForGraph(new JaccardIndex());

    expectedResult.equalsByData(result);

  }
}
