#!/usr/bin/env groovy

import com.tinkerpop.blueprints.*
import com.tinkerpop.blueprints.pgm.*
import com.tinkerpop.blueprints.pgm.impls.neo4j.*
import com.tinkerpop.blueprints.pgm.impls.orientdb.*
import com.tinkerpop.blueprints.pgm.util.*
import com.tinkerpop.blueprints.pgm.util.graphml.GraphMLReader
import java.lang.Math
import java.util.Random
import com.pilot.*
import com.pilot.GraphInterface.GraphProvider
import GSTUtils
import TestMetrics

//N, connectivity-string, out is >
public class GraphStressTester {

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


	public GraphStressTester(GraphProvider graphProvider, long N, GraphConnectivity connectivityFn, double arg) {

		this.N = N
		this.graphProvider = graphProvider

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

	public static void main(String[] args) {

		if (!args || args.size() == 0) {
			println "Usage ./GraphStressTest <testdefinitionfile.xml>"
			return
		}

		GraphProvider provider
		GraphConnectivity connectivity
		boolean overwrite = false

		def testconfig = new XmlParser().parse(new File(args[0]))
		def graphs = testconfig.graphs.graph
		def tests = testconfig.tests.test
		for (graph in graphs) {
			def graphType = graph.attributes()['type']
			switch (graphType) {
				case 'tinkergraph':
					provider = GraphProvider.TINKERGRAPH
					break
				case 'neo4j':
					provider = GraphProvider.NEO4J
					break
				case 'orientdb':
					provider = GraphProvider.ORIENTDB
					break
				default:
					println "unrecognized graph provider!"
					//TODO: set to tinkergraph
			}
			def graphOverwrite = graph.attributes()['overwrite']
			if (graphOverwrite.toLowerCase() == "yes") {
				overwrite = true
			} else {
				overwrite = false
			}

			long N = graph.vertices.text().toInteger()
			def connectivityFn = graph.connectivity.function.text()
			double arg //TODO: fix
			switch (connectivityFn) {
				case 'log':
					connectivity = GraphConnectivity.LOG
					arg = graph.connectivity.arg.text().toInteger()
					break
				case 'linear':
					connectivity = GraphConnectivity.LINEAR
					arg = graph.connectivity.arg.text().toDouble()
					break
				case 'constant':
					connectivity = GraphConnectivity.CONSTANT
					arg = graph.connectivity.arg.text().toDouble()
					break
				default:
					connectivity = GraphConnectivity.NONE
					arg = 0
			}

			GraphStressTester gst = new GraphStressTester(provider, N, connectivity, arg)
			GraphInterface g = gst.initializeGraphDb()
			long vertexCount = g.getVertexCount()
			g.shutdown()
			if (overwrite || vertexCount == 0) {
				println "Graph empty or overwrite set to true. Recreating graph.\n"
				gst.createRandomGraph()
			} else {
				if (vertexCount != gst.getN()) {
					println "Warning: declared # of vertices does not match actual # of vertices in the graph"
					gst.setN(vertexCount)
				}
			}

			//for each graph defined, run the tests specified
			for (test in tests) {
				def testName = test.attributes()['name'].toLowerCase()
				int ntimes = test.times.text().toInteger()

				g = gst.initializeGraphDb()

				switch (testName) {
					case TestMetrics.Metrics.GETNEIGHBORS.toString():
						TestMetrics.runGetNeighbors(g, ntimes, gst.getGraphName())
						break
					case TestMetrics.Metrics.GETEDGESBETWEENVERTICES.toString():
						TestMetrics.runGetEdgesBetweenVertices(g, ntimes, gst.getGraphName())
						break
					default:
						println "No such test: ${testName}!"
				}

				g.shutdown()
			}
		}
	}
}
