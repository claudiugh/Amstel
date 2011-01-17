package nl.vu.cs.amstel.graph;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;

import nl.vu.cs.amstel.VertexIdStorage;
import nl.vu.cs.amstel.user.MessageValue;
import nl.vu.cs.amstel.user.Value;

public class VertexState<V extends Value, E extends Value,
		M extends MessageValue> {

	public static final int LOCAL_INBOX_SIZE = 512;
	
	private int index;
	private String vid;
	private String[] edges;
	private E[] edgeValues;
	private V value;
	
	public VertexState() {
	}
	
	public VertexState(String vid, V value, String[] edges, E[] edgeValues) {
		this.vid = VertexIdStorage.get(vid);
		this.edges = edges;
		this.value = value;
		this.edgeValues = edgeValues;
		
		for (int i = 0; i < edges.length; i++) {
			edges[i] = VertexIdStorage.get(edges[i]);
		}
	}
	
	public void serialize(DataOutputStream out) throws IOException {
		out.writeUTF(vid);
		value.serialize(out);
		out.writeInt(edges.length);
		for (int i = 0; i < edges.length; i++) {
			out.writeUTF(edges[i]);
			edgeValues[i].serialize(out);
		}
	}
	
	@SuppressWarnings("unchecked")
	public void deserialize(DataInputStream in, 
			VertexFactory<V, E> vertexFactory) throws IOException {
		vid = VertexIdStorage.get(in.readUTF());
		value = vertexFactory.createValue();
		value.deserialize(in);
		int edgesNo = in.readInt();
		edges = new String[edgesNo];
		edgeValues = (E[]) Array.newInstance(vertexFactory.edgeValueClass, 
				edgesNo);
		for (int i = 0; i < edgesNo; i++) {
			edges[i] = VertexIdStorage.get(in.readUTF());
			edgeValues[i] = vertexFactory.createEdgeValue();
			edgeValues[i].deserialize(in);
		}
	}

	public String getID() {
		return vid;
	}
	
	public void setIndex(int index) {
		this.index = index;
	}
	
	public int getIndex() {
		return index;
	}
	
	public V getValue() {
		return value;
	}
	
	public String[] getOutEdges() {
		return edges;
	}
	
	public String toString() {
		return "[" + value + "]: " + edges; 
	}
		
}
