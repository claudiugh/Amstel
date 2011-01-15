package nl.vu.cs.amstel.msg;

import ibis.ipl.ReadMessage;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Map;

import org.apache.log4j.Logger;

import nl.vu.cs.amstel.VertexState;
import nl.vu.cs.amstel.user.Combiner;
import nl.vu.cs.amstel.user.MessageValue;

public class CombinedInboundQueue<M extends MessageValue> 
	implements InboundQueue<M> {

	protected static Logger logger = Logger.getLogger("nl.vu.cs.amstel");
	
	protected MessageFactory<M> msgFactory;
	protected Combiner<M> combiner;
	protected Map<String, VertexState<M>> vertices;
	
	private M[] inbox;
	private M[] localInbox;
	private SingleMessageIterator<M> msgIterator;
	private M m;
	
	@SuppressWarnings("unchecked")
	public CombinedInboundQueue(int size, MessageFactory<M> msgFactory,
			Map<String, VertexState<M>> vertices, M[] localInbox) {
		this.msgFactory = msgFactory;
		this.combiner = msgFactory.createCombiner();
		this.m = msgFactory.create();
		this.vertices = vertices;
		this.localInbox = localInbox;
		inbox = (M[]) Array.newInstance(msgFactory.getMessageClass(), size);
		for (int i = 0; i < size; i++) {
			inbox[i] = null;
		}
		msgIterator = new SingleMessageIterator<M>(msgFactory.create());
	}
	
	@Override
	public void deliver(ReadMessage r) throws IOException {
		int verticesCount = r.readInt();
		int bufferSize = r.readInt();
		byte[] buffer = new byte[bufferSize];
		r.readArray(buffer);
		ByteArrayInputStream byteStream = new ByteArrayInputStream(buffer);
		DataInputStream inStream = new DataInputStream(byteStream);
		for (int i = 0; i < verticesCount; i++) {
			String vertex = inStream.readUTF();
			m.deserialize(inStream);
			int index = vertices.get(vertex).getIndex();
			if (inbox[index] == null) {
				inbox[index] = m;
				m = msgFactory.create();
			} else {
				M combined = combiner.combine(m, inbox[index]);
				inbox[index].copy(combined);
			}
		}
	}

	@Override
	public void deliverLocally(int index, M msg) throws IOException {
		if (localInbox[index] == null) {
			localInbox[index] = msgFactory.create();
			localInbox[index].copy(msg);
		} else {
			M combined = combiner.combine(localInbox[index], msg);
			localInbox[index].copy(combined);
		}
	}

	@Override
	public void clear(int index) {
		inbox[index] = null;
		localInbox[index] = null;
	}

	@Override
	public boolean hasMessages(int index) {
		return (inbox[index] != null) || (localInbox[index] != null);
	}

	@Override
	public Iterable<M> getIterator(int index) {
		if (!hasMessages(index)) {
			msgIterator.disable();
			return msgIterator;
		}
		if (inbox[index] != null && localInbox[index] != null) {
			// combine the values
			M combined = combiner.combine(inbox[index], localInbox[index]);
			msgIterator.enable(combined);
		} else if (inbox[index] != null) {
			msgIterator.enable(inbox[index]);
		} else if (localInbox[index] != null) {
			msgIterator.enable(localInbox[index]);
		}
		return msgIterator;
	}

}
