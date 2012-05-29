package com.countvajhula.splinter

import com.tinkerpop.blueprints.*
import com.tinkerpop.blueprints.pgm.*
import com.tinkerpop.blueprints.pgm.impls.neo4j.*
import com.tinkerpop.blueprints.pgm.impls.orientdb.*
import com.tinkerpop.blueprints.pgm.util.*
import com.countvajhula.pilot.*
import java.util.Random

public class SplinterUtils {

	// Random number generator
	static Random randGen

	static {
		randGen = new Random() //initialize random number generator
	}

	static long nextLong(Random rng, long n) {
		// error checking and 2^x checking removed for simplicity.
		long bits, val;
		bits = 0; val = n
		while (bits-val+(n-1) < 0L) {
			bits = (rng.nextLong() << 1) >>> 1;
			val = bits % n;
		} 
		return val;
	}

	static Vertex selectRandomVertex(GraphInterface graph, long numVertices) {
		long randNum = nextLong(randGen, numVertices)
		Vertex v1
		int tries = 0
		while (!v1 && tries<1) { //might be good to set #tries to 1
			v1 = graph.getVertex(randNum)
			tries++
		}

		return v1
	}

	static Vertex selectRandomVertex(GraphInterface graph, long numVertices, List vertices) {
		long randNum = nextLong(randGen, numVertices)
		return vertices[(int)randNum]
	}

}
