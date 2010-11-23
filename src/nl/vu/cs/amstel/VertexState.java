package nl.vu.cs.amstel;

import ibis.ipl.ReadMessage;
import ibis.ipl.WriteMessage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.amstel.user.MessageValue;

public class VertexState {

	private String vid;
	private ArrayList<String> edges;
	private int value;
	private boolean active = true;
	private boolean hasMessages = false;
	private List<MessageValue> inbox = new ArrayList<MessageValue>();
	private List<MessageValue> futureInbox = new ArrayList<MessageValue>();
	
	public VertexState(String vid, ArrayList<String> edges, int value) {
		this.vid = vid;
		this.edges = edges;
		this.value = value;
	}
	
	public List<MessageValue> getInbox() {
		return inbox;
	}
	
	public void deliver(MessageValue m) {
		hasMessages = true;
		futureInbox.add(m);
	}
	
	private void switchInboxes() {
		inbox.clear();
		List<MessageValue> tmp = inbox;
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
	
	public int getValue() {
		return value;
	}
	
	public void setValue(int value) {
		this.value = value;
	}
	
	public List<String> getOutEdges() {
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
