package nl.vu.cs.amstel;

import ibis.ipl.ReadMessage;
import ibis.ipl.WriteMessage;

import java.io.IOException;

import nl.vu.cs.amstel.user.MessageValue;

public class VertexState<M extends MessageValue> {

	public static final int LOCAL_INBOX_SIZE = 512;
	
	private int index;
	private String vid;
	private String[] edges;
	private int value;
	
	public VertexState() {
	}
	
	public VertexState(String vid, String[] edges, int value) {
		this.vid = vid;
		this.edges = edges;
		this.value = value;
	}
	
	public void serialize(WriteMessage msg) throws IOException {
		msg.writeString(vid);
		msg.writeInt(value);
		msg.writeInt(edges.length);
		for (String e : edges) {
			msg.writeString(e);
		}
	}
	
	public void deserialize(ReadMessage msg) throws IOException {
		vid = msg.readString();
		value = msg.readInt();
		int edgesNo = msg.readInt();
		edges = new String[edgesNo];
		for (int i = 0; i < edgesNo; i++) {
			edges[i] = msg.readString();
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
	
	public int getValue() {
		return value;
	}
	
	public void setValue(int value) {
		this.value = value;
	}
	
	public String[] getOutEdges() {
		return edges;
	}
	
	public String toString() {
		return "[" + value + "]: " + edges; 
	}
		
}
