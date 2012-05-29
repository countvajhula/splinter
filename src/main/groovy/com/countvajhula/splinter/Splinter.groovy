#!/usr/bin/env groovy

package com.countvajhula.splinter

import com.countvajhula.pilot.GraphInterface.GraphProvider
import StressTester
import StressTester.GraphConnectivity

//N, connectivity-string, out is >
/** Graph test manager. Reads and parses test definition from input XML file; initializes
 * and launches stress testers with configured parameters for each graph.
 */
public class Splinter {

	public static List<Map<String,Object>> parseTestMatrixFromXML(File xmlFile) {

		List testMatrix = []

		def testconfig = new XmlParser().parse(xmlFile)
		def graphs = testconfig.graphs.graph
		def tests = testconfig.tests.test
		for (graph in graphs) {
			Map<String,Object> testDefinition = [:]
			// determine graph provider
			def graphType = graph.attributes()['type']
			switch (graphType) {
				case 'tinkergraph':
					testDefinition['provider'] = GraphProvider.TINKERGRAPH
					break
				case 'neo4j':
					testDefinition['provider'] = GraphProvider.NEO4J
					break
				case 'orientdb':
					testDefinition['provider'] = GraphProvider.ORIENTDB
					break
				default:
					println "unrecognized graph provider!"
					//set to tinkergraph?
			}

			// determine overwrite existing graph
			def graphOverwrite = graph.attributes()['overwrite']
			if (graphOverwrite.toLowerCase() == "yes") {
				testDefinition['overwrite'] = true
			} else {
				testDefinition['overwrite'] = false
			}

			// determine specified # of vertices in graph
			testDefinition['N'] = graph.vertices.text().toInteger()

			// determine connectivity function
			def connectivityFn = graph.connectivity.function.text()
			switch (connectivityFn) {
				case 'log':
					testDefinition['connectivity'] = GraphConnectivity.LOG
					testDefinition['arg'] = graph.connectivity.arg.text().toInteger()
					break
				case 'linear':
					testDefinition['connectivity'] = GraphConnectivity.LINEAR
					testDefinition['arg'] = graph.connectivity.arg.text().toDouble()
					break
				case 'constant':
					testDefinition['connectivity'] = GraphConnectivity.CONSTANT
					testDefinition['arg'] = graph.connectivity.arg.text().toDouble()
					break
				default:
					testDefinition['connectivity'] = GraphConnectivity.NONE
					testDefinition['arg'] = 0
			}

			testDefinition['metrics'] = []
			//for each graph defined, configure the tests specified
			for (test in tests) {
				def testName = test.attributes()['name'] //case-sensitive
				int ntimes = test.times.text().toInteger()
				testDefinition['metrics'] += ['testName': testName, 'ntimes': ntimes]
			}

			testMatrix += testDefinition
		}

		return testMatrix
	}

	public static void main(String[] args) {

		if (!args || args.size() == 0) {
			println "Usage ./Splinter <testdefinitionfile.xml>"
			return
		}

		print "Parsing XML file to build test matrix..."
		List<Map<String,Object>> testMatrix = Splinter.parseTestMatrixFromXML(new File(args[0]))
		println "...done."

		println "Running tests..."
		for (testDefinition in testMatrix) {
			GraphProvider provider = testDefinition['provider']
			GraphConnectivity connectivity = testDefinition['connectivity']
			double arg = testDefinition['arg'] //TODO: fix
			long N = testDefinition['N'] //# of vertices
			boolean overwrite = testDefinition['overwrite']
			List metrics = testDefinition['metrics']

			StressTester st = new StressTester(provider, N, connectivity, arg, overwrite, metrics)
			st.launch()
		}

	}
}
