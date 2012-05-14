import java.lang.Math
import java.util.Random
import java.lang.reflect.Method
import com.tinkerpop.blueprints.*
import com.tinkerpop.blueprints.pgm.*
import com.tinkerpop.blueprints.pgm.impls.neo4j.*
import com.tinkerpop.blueprints.pgm.impls.orientdb.*
import com.tinkerpop.blueprints.pgm.util.*
import com.pilot.*
import com.pilot.GraphInterface.GraphProvider
import GSTUtils
import TestMetrics

//N, connectivity-string, out is >
public class StressTester {

	public static final String DB_URL_PREFIX = "./dbs/"

	public static enum GraphConnectivity {
		CONSTANT("constant"),
		LOG("log"),
		LINEAR("linear"),
		NONE("none");

		private final String fnString

		private GraphConnectivity(String fnString) {
			this.fnString = fnString
		}

		public String toString() {
			return fnString
		}
	}

	// Number of vertices
	private long N

	// Number of edges/vertex
	private long edgesPerVertex

	// Name of graph
	private String graphName

	// Graph db provider
	private GraphProvider graphProvider

	// overwrite if existing graph
	private boolean overwrite

	// Map of metrics and #times to run them
	private List metrics


	public StressTester(GraphProvider graphProvider, long N, GraphConnectivity connectivityFn, double arg, boolean overwrite, List metrics) {

		this.N = N
		this.graphProvider = graphProvider
		this.overwrite = overwrite
		this.metrics = metrics

		String argName = arg.toString()
		switch (connectivityFn) {
			case GraphConnectivity.CONSTANT:
				edgesPerVertex = Math.round(arg)
				argName = Math.round(arg).toString()
				break
			case GraphConnectivity.LOG:
				edgesPerVertex = Math.round(Math.log(N)/Math.log(Math.round(arg))) //e.g. arg=2
				argName = Math.round(arg).toString()
				break
			case GraphConnectivity.LINEAR:
				edgesPerVertex = Math.round((double)arg*N)
				break
			case GraphConnectivity.NONE:
				edgesPerVertex = 0
				break
			default:
				println "unknown connectivity function!"
				edgesPerVertex = 0
		}

		// initialize graph db corresponding to supplied provider
		String graphNamePfx
		switch (graphProvider) {
			case GraphProvider.TINKERGRAPH:
				graphNamePfx = "tinkergraph"
				break
			case GraphProvider.ORIENTDB:
				graphNamePfx = "orientdb"
				break
			case GraphProvider.NEO4J:
				graphNamePfx = "neo4j"
				break
			default:
				println "Graph provider not recognized!"
				graphNamePfx = "tinkergraph"
				break
		}

		graphName = graphNamePfx + "_" + N.toString() + "vertices" + "_" + connectivityFn.toString() + argName + "connxns"

	}

	public long getN() {
		return N
	}

	public void setN(long N) {
		this.N = N
	}

	public String getGraphName() {
		return graphName
	}

	public void launch() {
		generateOrInspectGraph()
		runSpecifiedMetrics()
	}

	private void generateOrInspectGraph() {
		// if graph does not exist or overwrite is set to true, generate a random graph
		// according to specification
		// otherwise assert declared # of vertices matches actual number if graph exists
		GraphInterface g = initializeGraphDb()
		long vertexCount = g.getVertexCount()
		g.shutdown()
		if (overwrite || vertexCount == 0) {
			println "Graph empty or overwrite set to true. Recreating graph.\n"
			createRandomGraph()
		} else {
			if (vertexCount != N) {
				println "Warning: declared # of vertices does not match actual # of vertices in the graph"
				this.N = vertexCount
			}
		}
	}

	private void runSpecifiedMetrics() {
		for (metric in metrics) {
			String testName = metric['testName']
			int ntimes = metric['ntimes']

			Method m
			try {
				m = TestMetrics.class.getDeclaredMethod(testName, StressTester.class, Integer.TYPE)
				m.setAccessible(true)
				m.invoke(null, this, ntimes)
			} catch (Exception e) {
				println "No such test: ${testName}! Exception: ${e}"
			}
		}
	}

	public GraphInterface initializeGraphDb() {
		String graphUrl = DB_URL_PREFIX + graphName
		return (GraphInterface)GraphManagerProxy.initializeGraph(graphUrl, graphProvider, false)
	}

	public void addNVertices(GraphInterface graph, long n) {
		println "Adding ${n} vertices..."
		// add n vertices
		for (int i=0; i<n; i++) {
			Vertex vv = graph.addVertex()
			// set some properties?
			if (i % 1000 == 0) {
				println "${i} vertices added..."
			}
		}
	}

	public void addRandomEdges(GraphInterface graph, List vertices) {
		println "Adding random edges to nodes..."
		long numVertices = vertices.size()
		Random random = new Random()
		//randomly pick pairs of vertices - edgesPerVertex vertices for every vertex
		for (v1 in vertices) {
			for (int i=0; i<edgesPerVertex; i++) {
				//pick a random vertex
				long randNum = GSTUtils.nextLong(random, numVertices)
				Vertex v2 = vertices[randNum]
				graph.addEdge(v1, v2, 'knows')
			}
		}
	}

	// adds random edges as per the Erdos-Renyi model
	public void addRandomEdgesER(GraphInterface graph) {
		println "Adding random edges to nodes..."
		long numVertices = N //graph.getVertexCount()
		for (long i=0; i<edgesPerVertex*numVertices; i++) {
			Vertex v1 = GSTUtils.selectRandomVertex(graph, numVertices)
			if (v1) {
				Vertex v2 = GSTUtils.selectRandomVertex(graph, numVertices)
				if (v2) {
					graph.addEdge(v1, v2, 'knows')
				}
			}

			if (i%10000 == 0) {
				println "${i} edges added..."
			}
		}
	}

	public void removeRandomVertices(GraphInterface graph, List vertices) {
		//remove log N vertices
		int numVertices = vertices.size()
		int logN = Math.round((Math.log(numVertices))/(Math.log(2)))
		Random random = new Random()
		for (int i=0; i<logN; i++) {
			numVertices = vertices.size()
			if (numVertices == 0) {
				break
			}
			int randNum = random.nextInt(numVertices)
			Vertex vv = vertices[randNum]
			vertices.remove(randNum)
			graph.removeVertex(vv)
			print '*'
		}
	}

	public void createRandomGraph() {

		GraphInterface graph = initializeGraphDb()
		graph.clear()

		GraphManagerProxy.startProfiler((GraphInterface)graph, graphName)

		//graph.beginManagedTransaction(GraphInterface.MutationIntent.BATCHINSERT)
		graph.beginManagedTransaction()

		addNVertices(graph, N)
		graph.flushTransactionBuffer()
		addRandomEdgesER(graph)

		graph.concludeManagedTransaction()

		String results = GraphManagerProxy.stopProfiler((GraphInterface)graph)
		println "profiler results:${results}"

		graph.shutdown()
	}
}
