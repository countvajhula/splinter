
import com.tinkerpop.blueprints.*
import com.tinkerpop.blueprints.pgm.*
import com.tinkerpop.blueprints.pgm.impls.neo4j.*
import com.tinkerpop.blueprints.pgm.impls.orientdb.*
import com.tinkerpop.blueprints.pgm.util.*
import com.tinkerpop.blueprints.pgm.util.graphml.GraphMLReader
import com.pilot.*
import GSTUtils


public class TestMetrics {

	public static enum Metrics {
		GETNEIGHBORS("getneighbors"),
		GETEDGESBETWEENVERTICES("getedgesbetweenvertices");

		private final String metricName

		private Metrics(String metricName) {
			this.metricName = metricName
		}

		public String toString() {
			return metricName
		}
	}


	public static void runGetNeighbors(GraphInterface graph, int ntimes, String graphName) {

		println "Running getNeighbors..."

		long numVertices = graph.getVertexCount()

		String testName = graphName + "--" + Metrics.GETNEIGHBORS.toString()
		GraphManagerProxy.startProfiler((GraphInterface)graph, testName)

		for (int i=0; i<ntimes; i++) {
			Vertex v1 = GSTUtils.selectRandomVertex(graph, numVertices)
			if (v1) {
				def neighbors = graph.getNeighbors(v1, null, null) //
			}
		}

		String results = GraphManagerProxy.stopProfiler((GraphInterface)graph)
		println "profiler results:${results}"

	}

	public static void runGetEdgesBetweenVertices(GraphInterface graph, int ntimes, String graphName) {

		println "Running getEdgesBetweenVertices..."

		long numVertices = graph.getVertexCount()

		String testName = graphName + "--" + Metrics.GETEDGESBETWEENVERTICES.toString()
		GraphManagerProxy.startProfiler((GraphInterface)graph, testName)

		for (int i=0; i<ntimes; i++) {
			Vertex v1 = GSTUtils.selectRandomVertex(graph, numVertices)
			Vertex v2 = GSTUtils.selectRandomVertex(graph, numVertices)
			if (v1 && v2) {
				Edge edge = graph.getEdges(v1, v2, null)[0]
			}
		}

		String results = GraphManagerProxy.stopProfiler((GraphInterface)graph)
		println "profiler results:${results}"

	}

}
