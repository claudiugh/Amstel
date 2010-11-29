package nl.vu.cs.amstel;

import ibis.ipl.ReadMessage;
import ibis.ipl.WriteMessage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.amstel.user.MessageValue;

public class VertexState<M extends MessageValue> {

	private String vid;
	private String[] edges;
	private int value;
	private boolean active = true;
	private boolean hasMessages = false;
	private List<M> inbox = new ArrayList<M>();
	private List<M> futureInbox = new ArrayList<M>();
	
	public VertexState() {
	}
	
	public VertexState(String vid, String[] edges, int value) {
		this.vid = vid;
		this.edges = edges;
		this.value = value;
	}
	
	public List<M> getInbox() {
		return inbox;
	}
	
	public void deliver(M m) {
		hasMessages = true;
		futureInbox.add(m);
	}
	
	private void switchInboxes() {
		inbox.clear();
		List<M> tmp = inbox;
		inbox = futureInbox;
		futureInbox = tmp;
	}
	
	public boolean nextSuperstep() {
		switchInboxes();
		if (hasMessages) {
			active = true;
		}
		hasMessages = false;
		// return active state
		return active;
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
	
	public int getValue() {
		return value;
	}
	
	public void setValue(int value) {
		this.value = value;
	}
	
	public String[] getOutEdges() {
		return edges;
	}
	
	public void setActive(boolean active) {
		this.active = active;
	}
	
	public boolean isActive() {
		return active;
	}
	
	public String toString() {
		return "[" + value + "]: " + edges; 
	}
}
