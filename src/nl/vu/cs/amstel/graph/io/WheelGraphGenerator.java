package nl.vu.cs.amstel.graph.io;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Random;

import org.apache.log4j.Logger;

import nl.vu.cs.amstel.graph.VertexFactory;
import nl.vu.cs.amstel.graph.VertexState;
import nl.vu.cs.amstel.user.Value;

public class WheelGraphGenerator implements Reader {

	protected static Logger logger = Logger.getLogger("nl.vu.cs.amstel");
	private static Random vertexRand = new Random();
	private static Random edgeRand = new Random();
	
	private int vertices;
	private int edges;
	private int maxVertexValue;
	private int maxEdgeValue;
	
	private int fromVertex;
	private int toVertex;
	private int crtVertex;
	
	private class Partitioner implements InputPartitioner {

		@Override
		public InputPartition[] getPartitions(int workers) throws IOException {
			int perWorker = vertices / workers;
			InputPartition[] partitions = new WheelGraphPartition[workers];
			for (int i = 0; i < workers; i++) {
				int toVertex = (i < workers - 1) ? 
						(i + 1) * perWorker : vertices;
				partitions[i] = new WheelGraphPartition(i * perWorker, toVertex,
						vertices, edges, maxVertexValue, maxEdgeValue);
			}
			return partitions;
		}
		
		public String toString() {
			return "Generating wheel-graph with " + vertices
				+ " vertices having " + edges + " per vertex";
		}
	}
	
	public WheelGraphGenerator(String filename) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(filename));
		// all we need is on the first line of the file
		String data = reader.readLine();
		String[] parts = data.split("\\W");
		vertices = Integer.parseInt(parts[0]);
		edges = Integer.parseInt(parts[1]);
		maxVertexValue = Integer.parseInt(parts[2]);
		maxEdgeValue = Integer.parseInt(parts[3]);
		reader.close();
	}
	
	public void init(InputPartition inputPartition) throws Exception {
		if (inputPartition instanceof WheelGraphPartition) {
			WheelGraphPartition partition = (WheelGraphPartition) inputPartition;
			this.fromVertex = partition.fromVertex;
			this.toVertex = partition.toVertex;
			this.vertices = partition.vertices;
			this.edges = partition.edges;
			this.maxVertexValue = partition.maxVertexValue;
			this.maxEdgeValue = partition.maxEdgeValue;			
			crtVertex = fromVertex;
		} else {
			throw new Exception("Input partition not valid");
		}
	}
	
	@Override
	public void close() throws IOException {
		// nothing to do here
	}

	@Override
	public InputPartitioner getPartitioner() {
		return new Partitioner();
	}

	@Override
	public boolean hasNext() throws IOException {
		return crtVertex < toVertex;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <V extends Value, E extends Value> VertexState<V, E> nextVertex(
			VertexFactory<V, E> factory) throws IOException {
		String vid = "V" + crtVertex;
		String[] edgeTargets = new String[edges];
		V value = factory.createValue(vertexRand.nextInt(maxVertexValue));
		E[] edgeValues = null;
		if (!VertexFactory.hasNullEdgeValue()) {
			edgeValues = (E[]) Array.newInstance(factory.edgeValueClass, edges); 
		}
		for (int i = 0; i < edges; i++) {
			int target = (1 + crtVertex + i) % vertices;
			edgeTargets[i] = "V" + target;
			if (!VertexFactory.hasNullEdgeValue()) {
				int edgeValue = (maxEdgeValue > 1) ? 
						edgeRand.nextInt(maxEdgeValue) : 1;
				edgeValues[i] = factory.createEdgeValue(edgeValue);
			}
		}
		
		crtVertex++;
		return new VertexState<V, E>(vid, value, edgeTargets, edgeValues);
	}

	public String toString() {
		return edges + " edges";
	}
	
}
