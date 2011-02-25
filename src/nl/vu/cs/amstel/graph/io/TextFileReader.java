package nl.vu.cs.amstel.graph.io;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Array;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;

import org.apache.log4j.Logger;

import nl.vu.cs.amstel.graph.VertexFactory;
import nl.vu.cs.amstel.graph.VertexState;
import nl.vu.cs.amstel.user.Value;

public class TextFileReader implements Reader {

	protected static Logger logger = Logger.getLogger("nl.vu.cs.amstel");

	private String filename;
	private RandomAccessFile file;
	private long fileSize;

	private MappedByteBuffer mappedMem;
	private ByteArrayOutputStream buffer = new ByteArrayOutputStream();
	private long offset;
	private long length;
	private long bytesRead = 0;
	
	private void openFile() throws FileNotFoundException {
		file = new RandomAccessFile(filename, "rw");
		fileSize = (new File(filename)).length();
	}
	
	/**
	 * constructor called from the Master node
	 * @param filename
	 * @throws FileNotFoundException
	 */
	public TextFileReader(String filename) throws FileNotFoundException {
		this.filename = filename;
		openFile();
	}
	
	/**
	 * constructor called from the Worker nodes
	 * @param partition
	 * @throws Exception 
	 */
	public TextFileReader(InputPartition inputPartition) throws Exception {
		if (inputPartition instanceof TextFilePartition) {
			TextFilePartition partition = (TextFilePartition) inputPartition;
			filename = partition.filename;
			offset = partition.offset;
			length = partition.length;
			openFile();
			mappedMem = file.getChannel().map(MapMode.READ_ONLY, offset, length);
		} else {
			throw new Exception("Input partition not valid");
		}
	}
	
	@Override
	public boolean hasNext() {
		return bytesRead < length;
	}

	private String readLine() {
		byte b;
		buffer.reset();
		while ((bytesRead < length) && ((b = mappedMem.get()) != '\n')) {
			buffer.write(b);
			bytesRead++;
		}
		bytesRead++;
		
		return buffer.toString();		
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public <V extends Value, E extends Value> VertexState<V, E> 
			nextVertex(VertexFactory<V, E> factory) throws IOException {
		String line = readLine();
		String[] tokens = line.split("\\W");
		String vertex = tokens[0];
		V value = factory.createValue(tokens[1]);
		int edgesNo = Integer.parseInt(tokens[2]);
		String[] edges = new String[edgesNo];
		E[] edgeValues = null;
		if (!VertexFactory.hasNullEdgeValue()) {
			edgeValues = (E[]) Array.newInstance(factory.edgeValueClass, 
					edgesNo);
		}
		for (int i = 0; i < edgesNo; i++) {
			edges[i] = tokens[3 + 2 * i];
			if (!VertexFactory.hasNullEdgeValue()) {
				edgeValues[i] = factory.createEdgeValue(tokens[3 + 2 * i + 1]);
			}
		}
		return new VertexState<V, E>(vertex, value, edges, edgeValues);
	}

	@Override
	public InputPartition[] getPartitions(int workers) throws IOException {
		long perWorker = fileSize / workers;
		InputPartition[] partitions = new InputPartition[workers];
		long offset = 0;
		long pos;
		for (int i = 0; i < workers; i++) {
			pos = (i + 1) * perWorker;
			file.seek(pos);
			try {
				while (file.readByte() != '\n') {
					pos++;
				}
				pos++;
			} catch (EOFException e) {
				pos = fileSize;
			}
			partitions[i] = 
				new TextFilePartition(filename, offset, pos - offset);
			offset = pos;
		}
		return partitions;
	}

	@Override
	public void close() throws IOException {
		file.close();
	}

}