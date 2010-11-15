package nl.vu.cs.amstel.msg;

import java.io.IOException;
import java.util.Map;

import nl.vu.cs.amstel.VertexState;

import ibis.ipl.MessageUpcall;
import ibis.ipl.ReadMessage;

public class MessageReceiver implements MessageUpcall {

	private Map<String, VertexState> vertexes;
	
	public MessageReceiver(Map<String, VertexState> vertexes) {
		this.vertexes = vertexes;
	}
	
	@Override
	public void upcall(ReadMessage msg) throws IOException,
			ClassNotFoundException {
		VertexState vertex = VertexState.deserialize(msg);
		vertexes.put(vertex.getID(), vertex);
	}

}
