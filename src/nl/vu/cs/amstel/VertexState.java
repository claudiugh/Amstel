package nl.vu.cs.amstel;

import ibis.ipl.ReadMessage;
import ibis.ipl.WriteMessage;

import java.io.IOException;

import nl.vu.cs.amstel.msg.MessageOutputBuffer;
import nl.vu.cs.amstel.user.MessageValue;

public class VertexState<M extends MessageValue> {

	public static final int LOCAL_INBOX_SIZE = 512;
	
	private String vid;
	private String[] edges;
	private int value;
	private boolean active = true;
	// local buffer for messages that are meant to be in the futureInbox
	private MessageOutputBuffer<M> localBuffer =
		new MessageOutputBuffer<M>(LOCAL_INBOX_SIZE);
	
	public VertexState() {
	}
	
	public VertexState(String vid, String[] edges, int value) {
		this.vid = vid;
		this.edges = edges;
		this.value = value;
	}
	
	public boolean nextSuperstep() {
		/* 
		if (hasMessages) {
			active = true;
		}
		*/
		// return active state
		return active;
	}
	
	public void deliverLocally(M msg) throws IOException {
		localBuffer.write(msg);
	}
	
	public byte[] getLocalBuffer() {
		if (localBuffer.size() == 0) {
			return null;
		}
		return localBuffer.toByteArray();
	}
	
	public boolean hasLocalMessages() {
		return localBuffer.size() > 0;
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
