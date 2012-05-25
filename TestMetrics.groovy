import com.tinkerpop.blueprints.*
import com.tinkerpop.blueprints.pgm.*
import com.tinkerpop.blueprints.pgm.impls.neo4j.*
import com.tinkerpop.blueprints.pgm.impls.orientdb.*
import com.tinkerpop.blueprints.pgm.util.*
import com.countvajhula.pilot.*
import GSTUtils
import StressTester


//instead of passing in the StressTester, probably better to implement as closures or something
public class TestMetrics {

	public static void getNeighbors(StressTester st, int ntimes) {

		String testName = "getNeighbors"
		println "Running ${testName}..."

		GraphInterface graph = st.initializeGraphDb()
		String graphName = st.getGraphName()
		long numVertices = graph.getVertexCount()

		String testHeader = graphName + "--" + testName
		GraphManagerProxy.startProfiler((GraphInterface)graph, testHeader)

		for (int i=0; i<ntimes; i++) {
			Vertex v1 = GSTUtils.selectRandomVertex(graph, numVertices)
			if (v1) {
				def neighbors = graph.getNeighbors(v1, null, null) //
			}
		}

		String results = GraphManagerProxy.stopProfiler((GraphInterface)graph)
		println "profiler results:${results}"

		graph.shutdown()

	}

	public static void getEdgesBetweenVertices(StressTester st, int ntimes) {

		String testName = "getEdgesBetweenVertices"
		println "Running ${testName}..."

		GraphInterface graph = st.initializeGraphDb()
		String graphName = st.getGraphName()
		long numVertices = graph.getVertexCount()

		String testHeader = graphName + "--" + testName
		GraphManagerProxy.startProfiler((GraphInterface)graph, testHeader)

		for (int i=0; i<ntimes; i++) {
			Vertex v1 = GSTUtils.selectRandomVertex(graph, numVertices)
			Vertex v2 = GSTUtils.selectRandomVertex(graph, numVertices)
			if (v1 && v2) {
				Edge edge = graph.getEdges(v1, v2, null)[0]
			}
		}

		String results = GraphManagerProxy.stopProfiler((GraphInterface)graph)
		println "profiler results:${results}"

		graph.shutdown()

	}

}
