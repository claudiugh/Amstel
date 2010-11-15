package nl.vu.cs.amstel;

import ibis.ipl.ReadMessage;
import ibis.ipl.WriteMessage;

import java.io.IOException;
import java.util.ArrayList;

public class VertexState {

	private String vid;
	private ArrayList<String> edges;
	private int value;
	private boolean active = true;
	
	public VertexState(String vid, ArrayList<String> edges, int value) {
		this.vid = vid;
		this.edges = edges;
		this.value = value;
	}
	
	public void serialize(WriteMessage msg) throws IOException {
		msg.writeString(vid);
		msg.writeInt(value);
		msg.writeInt(edges.size());
		for (String e : edges) {
			msg.writeString(e);
		}
	}
	
	public static VertexState deserialize(ReadMessage msg) throws IOException {
		String vid = msg.readString();
		int value = msg.readInt();
		int edgesNo = msg.readInt();
		ArrayList<String> edges = new ArrayList<String>();
		for (int i = 0; i < edgesNo; i++) {
			edges.add(msg.readString());
		}
		return new VertexState(vid, edges, value);
	}

	public String getID() {
		return vid;
	}
	
	public String toString() {
		return "[" + vid + "]: " + edges; 
	}
}
