package nl.vu.cs.amstel;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import nl.vu.cs.amstel.graph.VertexValueFactory;
import nl.vu.cs.amstel.user.MessageValue;

public class VertexState<V extends Value, M extends MessageValue> {

	public static final int LOCAL_INBOX_SIZE = 512;
	
	private int index;
	private String vid;
	private String[] edges;
	private V value;
	
	public VertexState() {
	}
	
	public VertexState(String vid, String[] edges, V value) {
		this.vid = VertexIdStorage.get(vid);
		this.edges = edges;
		this.value = value;
		
		for (int i = 0; i < edges.length; i++) {
			edges[i] = VertexIdStorage.get(edges[i]);
		}
	}
	
	public void serialize(DataOutputStream out) throws IOException {
		out.writeUTF(vid);
		value.serialize(out);
		out.writeInt(edges.length);
		for (String e : edges) {
			out.writeUTF(e);
		}
	}
	
	public void deserialize(DataInputStream in, 
			VertexValueFactory<V> valuesFactory) throws IOException {
		vid = VertexIdStorage.get(in.readUTF());
		value = valuesFactory.create();
		value.deserialize(in);
		int edgesNo = in.readInt();
		edges = new String[edgesNo];
		for (int i = 0; i < edgesNo; i++) {
			edges[i] = VertexIdStorage.get(in.readUTF());
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
