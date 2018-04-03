package org.gradoop.flink.algorithms.jaccardindex;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.types.IntValue;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.algorithms.jaccardindex.functions.ComputeScores;
import org.gradoop.flink.algorithms.jaccardindex.functions.ConfigurableEdgeKeySelector;
import org.gradoop.flink.algorithms.jaccardindex.functions.GenerateGroupPairs;
import org.gradoop.flink.algorithms.jaccardindex.functions.GenerateGroupSpans;
import org.gradoop.flink.algorithms.jaccardindex.functions.GenerateGroups;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.api.operators.UnaryGraphToValueOperator;
import org.gradoop.flink.model.impl.operators.statistics.IncomingVertexDegrees;
import org.gradoop.flink.model.impl.operators.statistics.OutgoingVertexDegrees;
import org.gradoop.flink.model.impl.tuples.WithCount;

import static org.apache.flink.api.common.ExecutionConfig.PARALLELISM_DEFAULT;
import static org.gradoop.flink.algorithms.jaccardindex.JaccardIndex.NeighborhoodType.IN;
import static org.gradoop.flink.algorithms.jaccardindex.JaccardIndex.NeighborhoodType.OUT;

public class JaccardIndex implements UnaryGraphToGraphOperator{

  /**
   * Default Key for Result Edges
   **/
  public static final String DEFAULT_JACCARD_EDGE_PROPERTY = "value";
  /**
   * Group size for the quadratic expansion of neighbor pairs
   **/
  private static final int DEFAULT_GROUP_SIZE = 64;
  /**
   * Default Edge Label for Results
   **/
  private static final String DEFAULT_JACCARD_EDGE_LABEL = "jaccardSimilarity";
  private String edgeLabel = DEFAULT_JACCARD_EDGE_LABEL;
  private NeighborhoodType neighborhoodType = OUT;
  private Denominator denominator = Denominator.UNION;

  public void setNeighborhoodType(NeighborhoodType neighborhoodType) {
    this.neighborhoodType = neighborhoodType;
  }

  public void setDenominator(Denominator denominator){
    this.denominator = denominator;
  }

  public void setEdgeLabel(String edgeLabel) {
    this.edgeLabel = edgeLabel;
  }

  private LogicalGraph executeInternal(LogicalGraph inputGraph) throws Exception {

    // VertexDegrees
    DataSet<Tuple3<GradoopId, GradoopId, Long>> edgesWithDegree = annotateEdgesWithDegree
      (inputGraph);

    // group span, source, target, degree(t/s), je nach einstellung
    DataSet<Tuple4<IntValue, GradoopId, GradoopId, IntValue>> groupSpans =
      edgesWithDegree.groupBy(0).sortGroup(1, Order.ASCENDING)
        .reduceGroup(new GenerateGroupSpans(DEFAULT_GROUP_SIZE)).setParallelism(PARALLELISM_DEFAULT)
        .name("Generate group spans");

    // group, s, t, d(t)
    DataSet<Tuple4<IntValue, GradoopId, GradoopId, IntValue>> groups =
      groupSpans.rebalance().setParallelism(PARALLELISM_DEFAULT).name("Rebalance")
        .flatMap(new GenerateGroups()).setParallelism(PARALLELISM_DEFAULT).name("Generate groups");

    DataSet<Tuple3<GradoopId, GradoopId, IntValue>> twoPaths = groups
      .groupBy(0, neighborhoodType.equals(IN) ? 1 : 2) // TODO: dafür komme ich in die clean
      // code hölle
      .sortGroup(1, Order.ASCENDING)
      .reduceGroup(new GenerateGroupPairs(DEFAULT_GROUP_SIZE, neighborhoodType, denominator))
      .name("Generate group pairs");

    // t, u, intersection, union
    DataSet<Edge> scoreEdges =
      twoPaths.groupBy(0, 1).reduceGroup(new ComputeScores(edgeLabel, denominator)).name("Compute scores");

    DataSet<Edge> union = scoreEdges.union(inputGraph.getEdges());

    return inputGraph.getConfig().getLogicalGraphFactory()
      .fromDataSets(inputGraph.getVertices(), union);

  }

  /**
   * Returns the Edges from the given LogicalGRaph
   * @param inputGraph
   * @return
   */
  private DataSet<Tuple3<GradoopId,GradoopId,Long>> annotateEdgesWithDegree(LogicalGraph inputGraph) {
    UnaryGraphToValueOperator<DataSet<WithCount<GradoopId>>> degreeOperator =
      getDegreeOperator(neighborhoodType);
    DataSet<WithCount<GradoopId>> degrees = degreeOperator.execute(inputGraph);

    return inputGraph.getEdges().join(degrees).where(new ConfigurableEdgeKeySelector
      (neighborhoodType))
      .equalTo(new KeySelector<WithCount<GradoopId>, GradoopId>() {
        @Override
        public GradoopId getKey(WithCount<GradoopId> value) {
          return value.getObject();
        }
      }).with(new JoinFunction<Edge, WithCount<GradoopId>, Tuple3<GradoopId, GradoopId, Long>>() {
      @Override
      public Tuple3<GradoopId, GradoopId, Long> join(Edge edge,
        WithCount<GradoopId> vertexDegree) {
        return new Tuple3<>(edge.getSourceId(), edge.getTargetId(), vertexDegree.getCount());
      }
    });
  }

  /**
   * Returns the appropriate Vertex Degree Operator depending on the given Neighborhood Type.
   * @param neighborhoodType
   * @return
   */
  private UnaryGraphToValueOperator<DataSet<WithCount<GradoopId>>> getDegreeOperator(
    NeighborhoodType neighborhoodType) {

    if (neighborhoodType.equals(IN)) {
      return new IncomingVertexDegrees();
    }

    return new OutgoingVertexDegrees();
  }

  @Override
  public LogicalGraph execute(LogicalGraph graph) {
    try {
      return  executeInternal(graph);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

  public enum NeighborhoodType {IN, OUT}

  public enum Denominator {UNION, MAX}

  @Override
  public String getName() {
    return JaccardIndex.class.getName();
  }
}