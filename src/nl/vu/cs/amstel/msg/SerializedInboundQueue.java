package nl.vu.cs.amstel.msg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import ibis.ipl.ReadMessage;
import nl.vu.cs.amstel.graph.VertexState;
import nl.vu.cs.amstel.user.MessageValue;

public class SerializedInboundQueue<M extends MessageValue> 
	implements InboundQueue<M> {

	// this is for resolving vertex id (String) to internal id (int)
	protected Map<String, VertexState<?, ?, M>> vertices;
	
	private List<byte[]>[] inbox;
	private MessageOutputBuffer<M>[] localInbox;
	private MessageIterator<M> msgIterator;
	
	@SuppressWarnings("unchecked")
	public SerializedInboundQueue(int size,
			Map<String, VertexState<?, ?, M>> vertices,
			MessageOutputBuffer[] localInbox,
			MessageIterator<M> msgIterator) {
		this.vertices = vertices;
		this.localInbox = localInbox;
		this.msgIterator = msgIterator;
		inbox = new List[size];
		for (int i = 0; i < inbox.length; i++) {
			inbox[i] = new ArrayList<byte[]>();
		}
	}
	
	@Override
	public void deliver(ReadMessage r) throws IOException {
		int verticesCount = r.readInt();
		//logger.info("received " + vertexesCount + " vertexes");
		for (int i = 0; i < verticesCount; i++) {
			String vertex = r.readString();
			int msgDataSize = r.readInt();
			byte[] msgData = new byte[msgDataSize];
			r.readArray(msgData);
			// save the buffer in corresponding inbox
			inbox[vertices.get(vertex).getIndex()].add(msgData);
		}		
	}

	@Override
	public void deliverLocally(int index, M msg) throws IOException {
		localInbox[index].write(msg);
	}

	@Override
	public void clear(int index) {
		inbox[index].clear();
		localInbox[index].reset();
	}

	@Override
	public boolean hasMessages(int index) {
		return inbox[index].size() > 0 || localInbox[index].size() > 0;
	}

	@Override
	public Iterable<M> getIterator(int index) {
		if (localInbox[index].size() > 0) {
			inbox[index].add(localInbox[index].toByteArray());
		}
		msgIterator.setBuffers(inbox[index]);
		return msgIterator;
	}

}
