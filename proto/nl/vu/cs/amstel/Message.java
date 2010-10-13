package nl.vu.cs.amstel;

import java.util.List;

class InputMessage {
	public int vertexId;
	public int value;
	public List<Integer> edges;
	
	public InputMessage(int vertexId, int value, List<Integer> edges) {
		this.vertexId = vertexId;
		this.value = value;
		this.edges = edges;
	}
}

public class Message<T> {
	
	public int superstep;
	public int dstVid; // destination vertex id
	T payload;
	
	public Message(int superstep, int dstVid, T payload) {
		this.superstep = superstep;
		this.dstVid = dstVid;
		this.payload = payload;
	}
	
	public String toString() {
		return "msg to " + dstVid + " in superstep " + superstep;
	}
}
