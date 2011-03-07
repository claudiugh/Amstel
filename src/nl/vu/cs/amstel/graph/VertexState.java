package nl.vu.cs.amstel.graph;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;

import nl.vu.cs.amstel.VertexIdStorage;
import nl.vu.cs.amstel.user.Value;

public class VertexState<V extends Value, E extends Value> {

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
	
	public void setEdgeIterator(ArrayOutEdgeIterator<E> edgeIterator) {
		edgeIterator.reset(edges, edgeValues);
	}
	
	public void serialize(DataOutputStream out) throws IOException {
		out.writeUTF(vid);
		value.serialize(out);
		out.writeInt(edges.length);
		for (int i = 0; i < edges.length; i++) {
			out.writeUTF(edges[i]);
			if (!VertexFactory.hasNullEdgeValue()) {
				edgeValues[i].serialize(out);
			}
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
			if (!VertexFactory.hasNullEdgeValue()) {
				edgeValues[i] = vertexFactory.createEdgeValue();
				edgeValues[i].deserialize(in);
			}
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
	
	public int getOutdegree() {
		return edges.length;
	}
	
	public String[] getOutEdges() {
		return edges;
	}
	
	public String toString() {
		String edgeStr = "";
		for (int i = 0; i < edges.length; i++) {
			edgeStr += " ->" + edges[i];
			if (!VertexFactory.hasNullEdgeValue()) {
				edgeStr += "(" + edgeValues[i] + ")";
			}
		}
		return vid + "[" + value + "]:" + edgeStr; 
	}
		
}
