package nl.vu.cs.amstel.graph.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Array;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.Random;

import org.apache.log4j.Logger;

import nl.vu.cs.amstel.graph.VertexFactory;
import nl.vu.cs.amstel.graph.VertexState;
import nl.vu.cs.amstel.user.Value;

public class LognormGraphGenerator implements Reader {

	protected static Logger logger = Logger.getLogger("nl.vu.cs.amstel");

	public final static int MAX_EDGE_VALUE = 1;
	public final static int MAX_VERTEX_VALUE = 1000;
	private static Random vertexRand = new Random();
	
	String filename;
	private RandomAccessFile file;
	private int vertices;
	
	// data for the worker instance
	TextFilePartition partition = null;
	private int crtVertex = 0;
	private MappedByteBuffer mappedMem;
	
	private class Partitioner implements InputPartitioner {

		@Override
		public InputPartition[] getPartitions(int workers) throws IOException {
			int perWorker = vertices / workers;
			InputPartition[] partitions = new InputPartition[workers];
			for (int i = 0; i < workers - 1; i++) {
				partitions[i] = new TextFilePartition(filename, i * perWorker,
						perWorker);
			}
			partitions[workers - 1] = new TextFilePartition(filename, 
					(workers - 1) * perWorker, perWorker + vertices % workers);			
			return partitions;
		}
		
		public String toString() {
			return "Generating lognorm-outdegree random graph with "
				+ vertices + " vertices";
		}
		
	}
	
	private void openFile() throws FileNotFoundException {
		file = new RandomAccessFile(filename, "r");
		// the file contains one integer for each vertex
		long fileSize = (new File(filename)).length();
		vertices = (int)(fileSize / 4);
	}
	
	public LognormGraphGenerator(String filename) throws FileNotFoundException {
		this.filename = filename;
		openFile();
	}
	
	@Override
	public void close() throws IOException {
		file.close();
	}

	@Override
	public InputPartitioner getPartitioner() {
		return new Partitioner();
	}

	@Override
	public void init(InputPartition inputPartition) throws Exception {
		if (inputPartition instanceof TextFilePartition) {
			TextFilePartition partition = (TextFilePartition) inputPartition;
			logger.info(partition);
			filename = partition.filename;
			this.partition = partition;
			crtVertex = (int) partition.offset;
			mappedMem = file.getChannel().map(MapMode.READ_ONLY, 
					partition.offset * 4, partition.length * 4);
		}
	}

	@Override
	public boolean hasNext() throws IOException {
		return crtVertex < partition.offset + partition.length;
	}

	private int nextEdgesNo() {
		int edges = 0;
		for (int offset = 0; offset < 32; offset +=8) {
			byte b = mappedMem.get();
			edges += ((int)(b & 0xFF) << offset);
		}
		return edges;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public <V extends Value, E extends Value> VertexState<V, E> nextVertex(
			VertexFactory<V, E> factory) throws IOException {
		int edges = nextEdgesNo();
		edges = edges % vertices;
		String vid = "V" + crtVertex;
		
		String[] edgeTargets = new String[edges];
		V value = factory.createValue(vertexRand.nextInt(MAX_VERTEX_VALUE));
		E[] edgeValues = null;
		if (!VertexFactory.hasNullEdgeValue()) {
			edgeValues = (E[]) Array.newInstance(factory.edgeValueClass, edges); 
		}
		for (int i = 0; i < edges; i++) {
			int target = (1 + crtVertex + i) % vertices;
			edgeTargets[i] = "V" + target;
			if (!VertexFactory.hasNullEdgeValue()) {
				edgeValues[i] = factory.createEdgeValue(1);
			}
		}
		
		crtVertex++;
		return new VertexState<V, E>(vid, value, edgeTargets, edgeValues);
	}

	public String toString() {
		return "" + partition.length * 4 + " bytes";
	}
}
