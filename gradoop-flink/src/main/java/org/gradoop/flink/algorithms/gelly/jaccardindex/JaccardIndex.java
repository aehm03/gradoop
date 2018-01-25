package org.gradoop.flink.algorithms.gelly.jaccardindex;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.algorithms.gelly.GellyAlgorithm;
import org.gradoop.flink.algorithms.gelly.functions.EdgeToGellyEdgeWithNullValue;
import org.gradoop.flink.algorithms.gelly.functions.VertexToGellyVertexWithNullValue;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.epgm.LogicalGraphFactory;

/**
 * A gradoop operator wrapping {@link org.apache.flink.graph.library.similarity.JaccardIndex}.
 *
 * The input graph must be a simple, undirected graph containing no duplicate
 * edges or self-loops.
 *
 * The Jaccard Similarity of each pair of vertices in the graph with at least one shared neighbor
 * is represented as a pair of edges between these vertices.
 */
public class JaccardIndex extends GellyAlgorithm<NullValue, NullValue> {

  private String edgeLabel;

  /**
   * Creates a new JaccardIndex Algorithm
   *
   * @param edgeLabel Label for the resulting edges that contain the Jaccard Indices
   */
  public JaccardIndex(String edgeLabel) {
    super(new VertexToGellyVertexWithNullValue(), new EdgeToGellyEdgeWithNullValue());
    this.edgeLabel = edgeLabel;
  }

  @Override
  protected LogicalGraph executeInGelly(Graph<GradoopId, NullValue, NullValue> graph) throws
    Exception {

    DataSet<Edge> edges =
      new org.apache.flink.graph.library.similarity.JaccardIndex<GradoopId, NullValue, NullValue>()
        .run(graph).flatMap(new JaccardResultsToEdges(edgeLabel));

    LogicalGraphFactory graphFactory = currentGraph.getConfig().getLogicalGraphFactory();
    LogicalGraph newEdgesAsLogicalGraph =
      graphFactory.fromDataSets(currentGraph.getVertices(), edges);

    return currentGraph.combine(newEdgesAsLogicalGraph);
  }

  @Override
  public String getName() {
    return JaccardIndex.class.getName();
  }

}
