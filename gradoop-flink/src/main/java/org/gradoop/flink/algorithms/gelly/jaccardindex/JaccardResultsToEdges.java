package org.gradoop.flink.algorithms.gelly.jaccardindex;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.graph.library.similarity.JaccardIndex;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;

/**
 * Maps a JaccardIndexResult to mirrored two Edges between the Vertices.
 */
public class JaccardResultsToEdges implements
  FlatMapFunction<JaccardIndex.Result<GradoopId>, Edge> {
  private EdgeFactory edgeFactory = new EdgeFactory();
  private String edgeLabel;

  public JaccardResultsToEdges(String edgeLabel) {
    this.edgeLabel = edgeLabel;
  }

  @Override
  public void flatMap(JaccardIndex.Result<GradoopId> value, Collector<Edge> out) throws Exception {

    double distance = ((double) value.f2.getValue()) / value.f3.getValue();

    Edge e1 = edgeFactory.createEdge(edgeLabel, value.f0, value.f1);
    e1.setProperty("value", distance);
    out.collect(e1);

    Edge e2 = edgeFactory.createEdge(edgeLabel, value.f1, value.f0);
    e2.setProperty("value", distance);
    out.collect(e2);
  }
}
